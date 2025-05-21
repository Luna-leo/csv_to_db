from pathlib import Path
from libs.csv_to_db import process_targets


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