import polars as pl
from pathlib import Path
import pyarrow.dataset as ds
import pyarrow as pa
from datetime import datetime

def _to_dt(ts):  # str → datetime 変換ユーティリティ
    return ts if isinstance(ts, datetime) else datetime.fromisoformat(ts)

def load_dataset(
    root: str | Path,
    *,
    plant_name: str ,
    machine_no: str,
    start: str | datetime | None = None,
    end:   str | datetime | None = None,
    selected_columns: list[str] | None = None,
) -> pl.DataFrame:

    part_schema = pa.schema([
        ("machine_no", pa.string()),
        ("year",  pa.int16()),
        ("month", pa.int8()),
    ])

    # 1) Arrow Dataset（Directory 形式）
    dataset = ds.dataset(
        root / plant_name,
        format="parquet",
        partitioning=ds.partitioning(part_schema),
    )

    # 2) Lazy スキャン
    lf = pl.scan_pyarrow_dataset(dataset)

    # 3) Polars 式でフィルタを組立（自動 push-down）
    cond = pl.lit(True)
    if machine_no:
        cond &= pl.col("machine_no") == machine_no
    if start:
        sdt = _to_dt(start)
        cond &= (pl.col("year") >  sdt.year - 1) & (
                 (pl.col("year") >  sdt.year) |
                 ((pl.col("year") == sdt.year) & (pl.col("month") >= sdt.month))
        )
        cond &= pl.col("Datetime") >= sdt          # 秒レベル
    if end:
        edt = _to_dt(end)
        cond &= (pl.col("year") <  edt.year + 1) & (
                 (pl.col("year") <  edt.year) |
                 ((pl.col("year") == edt.year) & (pl.col("month") <= edt.month))
        )
        cond &= pl.col("Datetime") <= edt

    lf = lf.filter(cond)

    if selected_columns:
        selected_columns = ["Datetime"] + selected_columns
        # カラムの存在有無確認して存在するカラムだけを選択、存在しないカラムはメッセージで表示
        existing_columns = lf.collect_schema().names()
        missing_columns = set(selected_columns) - set(existing_columns)
        if missing_columns:
            print(f"Warning: The following columns are not found in the dataset: {missing_columns}")
        # 選択されたカラムが存在する場合のみ選択
        selected_columns = [col for col in selected_columns if col in existing_columns]        
        lf = lf.select(selected_columns)

    return lf.collect()