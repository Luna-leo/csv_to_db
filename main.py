import polars as pl
import duckdb
from pathlib import Path
import pyarrow.dataset as ds
import pyarrow as pa
import zipfile
from datetime import datetime
from polars import selectors as cs  
import pyarrow.parquet as pq

from typing import Callable, Dict

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
                    member_path = Path(member)
                    # skip suspicious paths
                    if member_path.is_absolute() or ".." in member_path.parts:
                        print(f"warning: skip invalid path {member} in {zip_path}")
                        continue
                    if not member_path.name.lower().endswith(".csv"):
                        continue
                    extracted_dir = zip_path.parent / "__extracted_csvs__" / zip_path.stem
                    extracted_dir.mkdir(parents=True, exist_ok=True)
                    zf.extract(member, path=extracted_dir)
                    csv_files.append(extracted_dir / member_path)
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


def collect_csv_files(
    targets: list[Path], file_name_pattern: list[str] | None = None
) -> list[Path]:
    """Collect CSV files from directories or paths.

    Each path in ``targets`` may be a directory, a CSV file or a ZIP archive.
    ZIP archives are extracted under a ``__extracted_csvs__`` directory.
    """

    collected: list[Path] = []

    for t in targets:
        if t.is_dir():
            collected.extend(search_csv_file(t, file_name_pattern))
            continue

        if t.is_file():
            suffix = t.suffix.lower()
            if suffix == ".csv":
                if not file_name_pattern or any(p in t.name for p in file_name_pattern):
                    collected.append(t)
                continue
            if suffix == ".zip":
                try:
                    with zipfile.ZipFile(t) as zf:
                        for member in zf.namelist():
                            mp = Path(member)
                            if mp.is_absolute() or ".." in mp.parts:
                                print(f"warning: skip invalid path {member} in {t}")
                                continue
                            if not mp.name.lower().endswith(".csv"):
                                continue
                            extract_dir = t.parent / "__extracted_csvs__" / t.stem
                            extract_dir.mkdir(parents=True, exist_ok=True)
                            zf.extract(member, path=extract_dir)
                            fp = extract_dir / mp
                            if not file_name_pattern or any(p in fp.name for p in file_name_pattern):
                                collected.append(fp)
                except zipfile.BadZipFile:
                    continue

    return collected



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


READERS: Dict[str, Callable[[Path, str], tuple[pl.LazyFrame, pl.LazyFrame]]] = {
    "pi": read_pi_file,
}


def read_file_by_source(file_path: Path, data_source: str, encoding: str = "utf-8"):
    """Dispatch file reading based on ``data_source``."""
    try:
        reader = READERS[data_source.lower()]
    except KeyError:
        raise ValueError(f"Unsupported data source: {data_source}") from None
    return reader(file_path, encoding=encoding)


def register_header_to_duckdb(
    header_lf: pl.LazyFrame,
    db_path: Path,
    plant_name: str,
    machine_no: str,
    data_source: str,
    table_name: str = "param_master",
):
    """Store parameter metadata into a DuckDB table.

    This function also updates ``parameter_id_master`` with any new
    ``param_id`` values found in ``header_lf``.

    Parameters
    ----------
    header_lf : pl.LazyFrame
        LazyFrame containing ``param_id``, ``param_name`` and ``unit`` columns.
    db_path : Path
        DuckDB database file where the table resides.
    plant_name : str
        Plant identifier used for partitioning.
    machine_no : str
        Machine identifier used for partitioning.
    table_name : str, optional
        Name of the table used to store the metadata.
    """
    # フォルダ作成
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # DuckDBに接続

    con = duckdb.connect(db_path)
    # DataFrame化
    header_df = header_lf.collect().to_pandas()
    header_df["plant_name"] = plant_name
    header_df["machine_no"] = machine_no
    header_df["data_source"] = data_source

    # テーブル作成（なければ）
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            param_id TEXT,
            param_name TEXT,
            unit TEXT,
            plant_name TEXT,
            machine_no TEXT,
            data_source TEXT,
            PRIMARY KEY(param_id, plant_name, machine_no, data_source)
        )
        """
    )

    # 既存データ取得
    existing_ids = set(
        con.execute(
            f"SELECT param_id FROM {table_name} WHERE plant_name = ? AND machine_no = ? AND data_source = ?",
            [plant_name, machine_no, data_source],
        ).fetchall()
    )

    # 未登録データ抽出
    new_rows = header_df[~header_df["param_id"].isin([row[0] for row in existing_ids])]

    # 追記
    if not new_rows.empty:
        con.executemany(
            f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?)",
            new_rows[["param_id", "param_name", "unit", "plant_name", "machine_no", "data_source"]].values.tolist(),
        )
    con.close()

    register_param_id_master(
        header_lf,
        db_path,
        plant_name,
        machine_no,
        data_source,
    )


def register_param_id_master(
    header_lf: pl.LazyFrame,
    db_path: Path,
    plant_name: str,
    machine_no: str,
    data_source: str,
    table_name: str = "parameter_id_master",
) -> None:
    """Store param_id mapping information into a DuckDB table.

    Parameters
    ----------
    header_lf : pl.LazyFrame
        LazyFrame containing ``param_id`` and ``param_name``.

        Both ``param_name_en`` and ``param_name_ja`` are initialized
        with this value when new IDs are registered.

    db_path : Path
        DuckDB database file where the table resides.
    plant_name : str
        Plant identifier used for partitioning.
    machine_no : str
        Machine identifier used for partitioning.
    data_source : str
        Identifier for the data format.
    table_name : str, optional
        Name of the table used to store the mapping.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(db_path)

    header_df = header_lf.collect().to_pandas()
    header_df["plant_name"] = plant_name
    header_df["machine_no"] = machine_no
    header_df["data_source"] = data_source
    header_df["insert_date"] = datetime.now()
    # 初期登録時は英語・日本語ともに CSV の param_name を流用する
    header_df["param_name_en"] = header_df["param_name"]
    header_df["param_name_ja"] = header_df["param_name"]


    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            param_id TEXT,
            param_name_en TEXT,
            param_name_ja TEXT,
            plant_name TEXT,
            machine_no TEXT,
            data_source TEXT,
            insert_date TIMESTAMP,
            PRIMARY KEY(param_id, plant_name, machine_no, data_source)
        )
        """
    )

    existing_ids = set(
        con.execute(
            f"SELECT param_id FROM {table_name} WHERE plant_name = ? AND machine_no = ? AND data_source = ?",
            [plant_name, machine_no, data_source],
        ).fetchall()
    )

    new_rows = header_df[~header_df["param_id"].isin([row[0] for row in existing_ids])]

    if not new_rows.empty:
        con.executemany(
            f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?)",
            new_rows[
                [
                    "param_id",
                    "param_name_en",
                    "param_name_ja",
                    "plant_name",
                    "machine_no",
                    "data_source",
                    "insert_date",
                ]
            ].values.tolist(),
        )
    con.close()


def write_parquet_file(
    lf: pl.LazyFrame,
    parquet_path: Path,
    plant_name: str,
    machine_no: str,
    *,
    add_date_columns: bool = False,
) -> tuple[int, int]:

    """Write ``lf`` to ``parquet_path`` partitioned by plant/machine and date.

    Parameters
    ----------
    lf:
        LazyFrame to write. ``year`` and ``month`` columns must be present unless
        ``add_date_columns`` is ``True``.
    parquet_path:
        Destination directory for the partitioned parquet dataset.
    plant_name, machine_no:
        Values used to partition the dataset.
    add_date_columns:
        When ``True`` ``year`` and ``month`` columns are derived from the
        ``Datetime`` column before writing.  This should be disabled if these
        columns were already added upstream.
    Returns
    -------

    tuple[int, int]
        ``(row_count, column_count)`` with the number of rows and columns
        written to the Parquet dataset.

    """

    if add_date_columns:
        lf = lf.with_columns(
            [
                pl.col("Datetime").dt.year().alias("year"),
                pl.col("Datetime").dt.month().alias("month"),
            ]
        )

    lf = lf.with_columns(
        [
            pl.lit(plant_name).alias("plant_name"),
            pl.lit(machine_no).alias("machine_no"),
        ]
    )

    lf = (
        lf
        # year / month を除く数値列を Float64 に統一
        .with_columns(
            cs.numeric()                # ① すべての数値列
            .exclude(["year", "month"]) # ② 除外したい列
            .cast(pl.Float64)           # ③ キャスト
        )
    )

    df = lf.collect()

    row_count = df.height
    column_count = df.width

    tbl = df.to_arrow()

    ds.write_dataset(
        data=tbl,
        base_dir=parquet_path,
        format="parquet",
        partitioning=["plant_name", "machine_no", "year", "month"],
        existing_data_behavior="overwrite_or_ignore",
        create_dir=True,
    )

    return row_count, column_count



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
            file_name TEXT,
            file_mtime TIMESTAMP,
            plant_name TEXT,
            machine_no TEXT,
            data_source TEXT,
            processed_at TIMESTAMP,
            PRIMARY KEY(file_name, file_mtime, plant_name, machine_no, data_source)
        )
        """
    )


def is_processed(
    file_path: Path,
    db_path: Path,
    plant_name: str,
    machine_no: str,
    data_source: str,
    table_name: str = "processed_files",
) -> bool:
    """Check whether a file has already been processed.

    The history table stores the file name, modification time, ``plant_name``
    ``machine_no`` and ``data_source``. A record is considered matching when
    all values are equal.

    Parameters
    ----------
    file_path : Path
        Target CSV file.
    db_path : Path
        DuckDB database storing the history table.
    plant_name : str
        Plant identifier used for partitioning.
    machine_no : str
        Machine identifier used for partitioning.
    data_source : str
        Identifier for the data format.
    table_name : str, optional
        Name of the table which records processed files.

    Returns
    -------
    bool
        ``True`` when a record for ``file_path`` exists.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
    with duckdb.connect(db_path) as con:
        _ensure_processed_table(con, table_name)
        result = con.execute(
            f"SELECT 1 FROM {table_name} WHERE file_name = ? AND file_mtime = ? AND plant_name = ? AND machine_no = ? AND data_source = ?",
            [file_path.name, mtime, plant_name, machine_no, data_source],
        ).fetchone()
        return result is not None


def mark_processed(
    file_path: Path,
    db_path: Path,
    plant_name: str,
    machine_no: str,
    data_source: str,
    table_name: str = "processed_files",
) -> None:
    """Record that a file has been processed.

    The file name and its last modification time are stored so that the same
    file relocated elsewhere will not be mistaken as new.

    Parameters
    ----------
    file_path : Path
        CSV file that was successfully processed.
    db_path : Path
        DuckDB database storing the history table.
    plant_name : str
        Plant identifier used for partitioning.
    machine_no : str
        Machine identifier used for partitioning.
    data_source : str
        Identifier for the data format.
    table_name : str, optional
        Name of the table used to store the history.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(db_path) as con:
        _ensure_processed_table(con, table_name)
        mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
        con.execute(
            f"INSERT OR REPLACE INTO {table_name} VALUES (?, ?, ?, ?, ?, ?)",
            [file_path.name, mtime, plant_name, machine_no, data_source, datetime.now()],
        )


def process_csv_files(
    file_paths: list[Path],
    parquet_path: Path,
    plant_name: str,
    machine_no: str,
    db_path: Path,
    data_source: str,
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
    data_source : str
        Identifier for the data format.
    force : bool, optional
        If ``True``, process files even when they are already recorded.
    """
    for fp in file_paths:
        if not force and is_processed(fp, db_path, plant_name, machine_no, data_source):
            print(f"skip {fp} (already processed)")
            continue
        print(f"processing {fp}")
        lf, header_lf = read_file_by_source(fp, data_source)
        register_header_to_duckdb(header_lf, db_path, plant_name, machine_no, data_source)

        row_count, column_count = write_parquet_file(
            lf, parquet_path, plant_name, machine_no
        )
        mark_processed(fp, db_path, plant_name, machine_no, data_source)
        print(f"processed {fp}: {row_count} rows, {column_count} columns")


def process_targets(
    targets: list[Path],
    parquet_path: Path,
    plant_name: str,
    machine_no: str,
    db_path: Path,
    data_source: str,
    *,
    file_name_pattern: list[str] | None = None,
    force: bool = False,
) -> None:
    """Entry point to process user-specified targets.

    Parameters
    ----------
    targets : list[Path]
        Directories or files (CSV/ZIP) to search.
    parquet_path, plant_name, machine_no, db_path : Path/str
        Parameters forwarded to :func:`process_csv_files`.
    data_source : str
        Identifier for the data format.
    file_name_pattern : list[str] | None, optional
        Patterns used to filter file names.
    force : bool, optional
        When ``True`` reprocess files even if they were already handled.
    """

    csv_files = collect_csv_files(targets, file_name_pattern)
    if not csv_files:
        print("No files to process")
        return
    process_csv_files(
        csv_files,
        parquet_path,
        plant_name,
        machine_no,
        db_path,
        data_source,
        force=force,
    )




if __name__ == "__main__":
    targets = [Path("data")]
    parquet_path = Path("output")
    plant_name = "plant1"
    machine_no = "machine1"
    db_path = Path("history.db")
    data_source = "pi"
    file_name_pattern = ["2023"]
    force = False
    process_targets(
        targets,
        parquet_path,
        plant_name,
        machine_no,
        db_path,
        data_source,
        file_name_pattern=file_name_pattern,
        force=force,
    )