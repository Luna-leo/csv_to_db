import polars as pl
import duckdb
from pathlib import Path
import pyarrow.dataset as ds
import zipfile
from datetime import datetime

def search_csv_file(target_path: Path, file_name_pattern: list[str]) -> list[Path]:
    """Search for CSV files matching the given patterns under ``target_path``.

    This function also looks inside ``.zip`` archives and extracts any CSV
    files found so they can be processed like regular files.
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
    """Read a PI CSV file and return data and header LazyFrames.

    Parameters
    ----------
    file_path : Path
        CSV file exported from the PI system.
    encoding : str, optional
        Encoding used to open ``file_path``.

    Returns
    -------
    tuple[pl.LazyFrame, pl.LazyFrame]
        ``(lf, header_lf)`` where ``lf`` contains the sensor data and
        ``header_lf`` stores parameter id, name and unit information.
    """
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
    """Store parameter metadata into a DuckDB table.

    Parameters
    ----------
    header_lf : pl.LazyFrame
        LazyFrame containing ``param_id``, ``param_name`` and ``unit`` columns.
    db_path : Path
        DuckDB database file where the table resides.
    table_name : str, optional
        Name of the table used to store the metadata.
    """
    # フォルダ作成
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # DuckDBに接続
    con = duckdb.connect(db_path)
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
    con.close()


def write_parquet_file(lf: pl.LazyFrame, parquet_path: Path, plant_name: str, machine_no: str):
    """Write sensor data to a partitioned Parquet dataset.

    Parameters
    ----------
    lf : pl.LazyFrame
        LazyFrame containing the sensor measurements.
    parquet_path : Path
        Destination directory for the Parquet dataset.
    plant_name : str
        Plant identifier used for partitioning.
    machine_no : str
        Machine identifier used for partitioning.
    """

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
    """Create the table used to track processed files.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Active DuckDB connection.
    table_name : str
        Name of the history table.
    """
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            file_path TEXT PRIMARY KEY,
            processed_at TIMESTAMP
        )
        """
    )


def is_processed(file_path: Path, db_path: Path, table_name: str = "processed_files") -> bool:
    """Check whether a file has already been processed.

    Parameters
    ----------
    file_path : Path
        Target CSV file.
    db_path : Path
        DuckDB database storing the history table.
    table_name : str, optional
        Name of the table which records processed files.

    Returns
    -------
    bool
        ``True`` when a record for ``file_path`` exists.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path)
    _ensure_processed_table(con, table_name)
    result = con.execute(
        f"SELECT 1 FROM {table_name} WHERE file_path = ?",
        [str(file_path)],
    ).fetchone()
    con.close()
    return result is not None


def mark_processed(file_path: Path, db_path: Path, table_name: str = "processed_files") -> None:
    """Record that a file has been processed.

    Parameters
    ----------
    file_path : Path
        CSV file that was successfully processed.
    db_path : Path
        DuckDB database storing the history table.
    table_name : str, optional
        Name of the table used to store the history.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path)
    _ensure_processed_table(con, table_name)
    con.execute(
        f"INSERT OR REPLACE INTO {table_name} VALUES (?, ?)",
        [str(file_path), datetime.now()],
    )
    con.close()


def process_csv_files(
    file_paths: list[Path],
    parquet_path: Path,
    plant_name: str,
    machine_no: str,
    db_path: Path,
    *,
    force: bool = False,
) -> None:
    """Process CSV files, store them as Parquet and update history.

    Parameters
    ----------
    file_paths : list[Path]
        CSV files to process.
    parquet_path : Path
        Root directory of the output Parquet dataset.
    plant_name : str
        Plant identifier used for partitioning.
    machine_no : str
        Machine identifier used for partitioning.
    db_path : Path
        DuckDB database used for the processed history.
    force : bool, optional
        If ``True``, process files even when they are already recorded.
    """
    for fp in file_paths:
        if not force and is_processed(fp, db_path):
            print(f"skip {fp} (already processed)")
            continue
        lf, header_lf = read_pi_file(fp)
        register_header_to_duckdb(header_lf, db_path)
        write_parquet_file(lf, parquet_path, plant_name, machine_no)
        mark_processed(fp, db_path)
