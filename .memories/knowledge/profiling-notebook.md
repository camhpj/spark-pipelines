# Profiling Notebook

Verified on 2026-01-12.

## Generation
- Notebook is generated only when `profiling.enabled` is true.
- Output path defaults to `dbfs:/FileStore/profiles/<pipeline_name>` if `profiling.output_dir` is unset.

## Notebook Behavior
- Uses `ProfileReport` from `ydata_profiling`.
- Profiles `semantic.<entity>` for each entry in `profiling.profile_raw_entities`.
- Profiles the output table if `profiling.profile_output` is true.
- Each table is loaded via `spark.table(...).limit(sample_rows)`.
- Writes HTML reports to `output_dir` and displays inline with `displayHTML`.
- Creates output directory with `dbutils.fs.mkdirs`.
