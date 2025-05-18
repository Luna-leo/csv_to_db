import polars as pl
import duckdb
from pathlib import Path
import pyarrow.dataset as ds

def search_csv_file(target_path: Path, file_name_pattern: list[str]) -> list[Path]:
    target_csv_files = []

    csv_files = target_path.rglob("*.csv")

    if not csv_files:
        print(f"No CSV files found in {target_path}")
        return target_csv_files
    if len(file_name_pattern) == 0:
        target_csv_files = list(csv_files)
        return target_csv_files
    else:
        for cscv_file in csv_files:
            if any(pat in cscv_file.name for pat in file_name_pattern):
                target_csv_files.append(cscv_file)
        return target_csv_files



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
