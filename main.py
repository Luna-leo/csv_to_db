import polars as pl
import duckdb
from pathlib import Path
import pyarrow.dataset as ds
import zipfile
from datetime import datetime

def search_csv_file(
    target_path: Path, file_name_pattern: list[str] | None = None
) -> list[Path]:
    """Search for CSV files matching the given patterns under ``target_path``.

    This function also looks inside ``.zip`` archives and extracts any CSV
    files found so they can be processed like regular files.

    Parameters
    ----------
    file_name_pattern : list[str] | None, optional
        List of patterns to filter file names. When omitted, all CSV files are
        returned.
    """

    csv_files = list(target_path.rglob("*.csv"))

    # search for zipped CSV files
    for zip_path in target_path.rglob("*.zip"):
        try:
            with zipfile.ZipFile(zip_path) as zf:
                for member in zf.namelist():
                    if not member.lower().endswith(".csv"):
                        continue
                    extracted_dir = zip_path.parent / "__extracted_csvs__" / zip_path.stem
                    extracted_dir.mkdir(parents=True, exist_ok=True)
                    zf.extract(member, path=extracted_dir)
                    csv_files.append(extracted_dir / member)
        except zipfile.BadZipFile:
            # skip files that are not valid zip archives
            continue

    if not csv_files:
        print(f"No CSV files found in {target_path}")
        return []

    if not file_name_pattern:
        return csv_files

    matched_files = []
    for csv_file in csv_files:
        if any(pat in csv_file.name for pat in file_name_pattern):
            matched_files.append(csv_file)
    return matched_files



def read_pi_file(file_path: Path, encoding="utf-8"):
    # ── 1. ヘッダー読み込み（3行） ─────────────────────
    with open(file_path, encoding=encoding) as f:
        header = [next(f) for _ in range(3)]
    
    # 各行をカンマ区切りで分割 → [[ID1, ID2, ...], [name1, name2, ...], [unit1, unit2, ...]]
    header = [row.strip().split(",") for row in header]

    # 先頭列名を"Datetime"に変更
    param_ids = header[0]
    param_ids[0] = "Datetime"  # 元々の1列目は日時情報

    # ── 2. センサデータ本体のLazyFrame化 ───────────────
    lf = pl.scan_csv(
        file_path,
        has_header=False,
        new_columns=param_ids,
        skip_rows=3,
        try_parse_dates=True,
        infer_schema_length=None,
    )

    # null行の除去
    lf = lf.filter(pl.any_horizontal(pl.all().is_not_null()))

    # パーティショニング用列追加
    lf = lf.with_columns([
        pl.col("Datetime").dt.year().alias("year"),
        pl.col("Datetime").dt.month().alias("month")
    ])

    # ── 3. ヘッダー情報の縦持ちLazyFrame構築 ──────────
    # 1列目は "Datetime" なので除外
    ids, names, units = header[0][1:], header[1][1:], header[2][1:]
    header_records = list(zip(ids, names, units))

    header_lf = pl.LazyFrame(
        header_records,
        schema=["param_id", "param_name", "unit"],
        orient="row",
    )

    return lf, header_lf


def register_header_to_duckdb(header_lf: pl.LazyFrame, db_path: Path, table_name: str = "param_master"):
    # フォルダ作成
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # DuckDBに接続
    with duckdb.connect(db_path) as con:
        # DataFrame化
        header_df = header_lf.collect().to_pandas()
        # テーブル作成（なければ）
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                param_id TEXT,
                param_name TEXT,
                unit TEXT
            )
        """)
        # 既存データ取得
        existing_ids = set(con.execute(f"SELECT param_id FROM {table_name}").fetchall())
        # 未登録データ抽出
        new_rows = header_df[~header_df["param_id"].isin([row[0] for row in existing_ids])]
        # 追記
        if not new_rows.empty:
            con.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?)", new_rows.values.tolist())


def write_parquet_file(lf: pl.LazyFrame, parquet_path:Path, plant_name: str, machine_no:str):

    lf = lf.with_columns([
        pl.col("Datetime").dt.year().alias("year"),
        pl.col("Datetime").dt.month().alias("month"),
        pl.lit(plant_name).alias("plant_name"),
        pl.lit(machine_no).alias("machine_no")
    ])

    df = lf.collect()

    tbl = df.to_arrow()

    ds.write_dataset(
        data=tbl,
        base_dir = parquet_path,
        format="parquet",
        partitioning=["plant_name", "machine_no", "year", "month"],
        existing_data_behavior="overwrite_or_ignore",
        create_dir=True,
    )
    print(f"write {plant_name}/{machine_no} to parquet")


def _ensure_processed_table(con: duckdb.DuckDBPyConnection, table_name: str) -> None:
    """Create the processed files table if it does not exist."""
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            file_path TEXT PRIMARY KEY,
            processed_at TIMESTAMP
        )
        """
    )


def is_processed(file_path: Path, db_path: Path, table_name: str = "processed_files") -> bool:
    """Return ``True`` if ``file_path`` is already recorded as processed."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(db_path) as con:
        _ensure_processed_table(con, table_name)
        result = con.execute(
            f"SELECT 1 FROM {table_name} WHERE file_path = ?",
            [str(file_path)],
        ).fetchone()
        return result is not None


def mark_processed(file_path: Path, db_path: Path, table_name: str = "processed_files") -> None:
    """Record ``file_path`` as processed with the current timestamp."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(db_path) as con:
        _ensure_processed_table(con, table_name)
        con.execute(
            f"INSERT OR REPLACE INTO {table_name} VALUES (?, ?)",
            [str(file_path), datetime.now()],
        )


def process_csv_files(
    file_paths: list[Path],
    parquet_path: Path,
    plant_name: str,
    machine_no: str,
    db_path: Path,
    *,
    force: bool = False,
) -> None:
    """Process CSV files and record processed history in DuckDB."""
    for fp in file_paths:
        if not force and is_processed(fp, db_path):
            print(f"skip {fp} (already processed)")
            continue
        lf, header_lf = read_pi_file(fp)
        register_header_to_duckdb(header_lf, db_path)
        write_parquet_file(lf, parquet_path, plant_name, machine_no)
        mark_processed(fp, db_path)
