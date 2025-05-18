
## Features

- Search function now checks `.zip` archives and extracts any contained CSV
  files under a `__extracted_csvs__` directory for further processing.
- When extracting from `.zip` archives, paths containing `..` or absolute paths
  are skipped with a warning to avoid unintended file writes.
- Processed file history is stored in DuckDB. The ``process_csv_files``
  function can skip files that were already processed or reprocess them
  when ``force=True``.
- ``process_targets`` gathers CSV files from multiple directories or files
  (including ``.zip``) and handles conversion, history updates and master table
  updates in one call.
