from datetime import datetime
from pathlib import Path
import pyarrow as pa, pyarrow.dataset as ds
import polars as pl

def load_sensor_data(
    root: str | Path,
    start: str | datetime,
    end: str | datetime,
    plant: str | None = None,
    machine: str | None = None,
) -> pl.DataFrame:
    """月までのディレクトリ-パーティション + Datetime 列での範囲フィルタ"""

    # ディレクトリ名 → year/month をパースさせる
    schema = pa.schema([
        ("plant_name", pa.string()),
        ("machine_no", pa.string()),
        ("year", pa.int16()),
        ("month", pa.int8()),
    ])

    ds_parquet = ds.dataset(
        root,
        format="parquet",
        partitioning=ds.partitioning(schema)
    )

    lf = pl.scan_pyarrow_dataset(ds_parquet)

    # 文字列でも datetime でも OK
    if isinstance(start, str):
        start = pl.datetime(start)
    if isinstance(end, str):
        end = pl.datetime(end)

    cond = (pl.col("Datetime").is_between(start, end, closed="both"))
    if plant:
        cond &= pl.col("plant_name") == plant
    if machine:
        cond &= pl.col("machine_no") == machine

    return lf.filter(cond).collect()