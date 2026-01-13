# Profiling notebook

When `profiling.enabled: true`, the compiler generates a Databricks notebook
`notebooks/profile__<pipeline_name>.py`. The notebook uses Spark DataFrames and
`ydata-profiling` to create HTML reports for sampled tables.

## Configuration

```yaml
profiling:
  enabled: true
  sample_rows: 100000
  sampling_mode: "random"       # random|deterministic
  sampling_seed: 42
  profile_raw_entities: ["patients", "medications"]
  profile_output: true
  output_dir: "dbfs:/FileStore/profiles/client_x"
```

### Fields

- `sample_rows`: maximum rows per table to profile.
- `sampling_mode`:
  - `random`: random ordering via `rand()` then `limit`.
  - `deterministic`: seeded ordering via `rand(seed)` then `limit`.
- `sampling_seed`: seed used in deterministic mode.
- `profile_raw_entities`: canonical entity names to profile from `semantic.<entity>`.
- `profile_output`: include the final output table.
- `output_dir`: DBFS directory for HTML reports. Defaults to
  `dbfs:/FileStore/profiles/<pipeline_name>`.

## Notebook behavior

1. Reads configuration in the first cell.
2. Samples requested semantic views and/or the output table.
3. Writes HTML reports and displays them inline.
4. Prints a simple summary of sampled schemas and runtime.
