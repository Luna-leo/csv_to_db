"""Utility to query sensor data stored as a partitioned Parquet dataset."""

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import polars as pl


def _to_expr(value: str | datetime) -> pl.Expr:
    """Convert ``value`` to a Polars expression."""
    return pl.datetime(value) if isinstance(value, str) else pl.lit(value)

def load_sensor_data(
    root: str | Path,
    start: str | datetime,
    end: str | datetime,
    plant: str | None = None,
    machine: str | None = None,
) -> pl.DataFrame:
    """Load records between ``start`` and ``end`` from a partitioned dataset."""

    # ディレクトリ名 → year/month をパースさせる
    schema = pa.schema(
        [
            ("plant_name", pa.string()),
            ("machine_no", pa.string()),
            ("year", pa.int16()),
            ("month", pa.int8()),
        ]
    )

    root = Path(root)
    dataset = ds.dataset(
        root,
        format="parquet",
        partitioning=ds.partitioning(schema, flavor="directory"),
    )

    start_expr = _to_expr(start)
    end_expr = _to_expr(end)

    lf = pl.scan_pyarrow_dataset(dataset)

    cond = pl.col("Datetime").is_between(start_expr, end_expr, closed="both")
    if plant:
        cond &= pl.col("plant_name") == plant
    if machine:
        cond &= pl.col("machine_no") == machine

    return lf.filter(cond).collect()
