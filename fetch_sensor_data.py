"""Utility to query sensor data stored as a partitioned Parquet dataset."""

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import polars as pl


def _to_datetime(value: str | datetime) -> datetime:
    """Normalize ``value`` to ``datetime``."""
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return value

def load_sensor_data(
    root: str | Path,
    start: str | datetime,
    end: str | datetime,
    plant: str | None = None,
    machine: str | None = None,
) -> pl.DataFrame:
    """Load records between ``start`` and ``end`` from a partitioned dataset.

    On some datasets integer columns may contain floating point values which
    causes ``pyarrow`` to raise ``ArrowInvalid`` when materializing the data.
    To avoid this issue all integer columns (except the partitioning columns)
    are promoted to ``Float64`` before converting to a :class:`polars.DataFrame`.
    """

    # ディレクトリ名から year/month をパースさせる。データセットは Hive 形式
    # ではなく Directory 形式で保存されているため ``flavor="directory"`` を
    # 使用する
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

    start_dt = _to_datetime(start)
    end_dt = _to_datetime(end)

    # Build pyarrow.dataset filter expression first. Using PyArrow directly
    # allows us to cast the resulting table before handing it over to Polars
    # which prevents type mismatches.
    cond = (
        (ds.field("Datetime") >= pa.scalar(start_dt))
        & (ds.field("Datetime") <= pa.scalar(end_dt))
    )
    if plant:
        cond &= ds.field("plant_name") == plant
    if machine:
        cond &= ds.field("machine_no") == machine

    table = dataset.to_table(filter=cond)

    # Promote integer columns to Float64, excluding partition columns.
    new_fields: list[pa.Field] = []
    for f in table.schema:
        if pa.types.is_integer(f.type) and f.name not in {"year", "month"}:
            new_fields.append(pa.field(f.name, pa.float64()))
        else:
            new_fields.append(f)
    table = table.cast(pa.schema(new_fields))

    return pl.from_arrow(table)
