# Pipeline Specification (Current Implementation)

Verified on 2026-01-14.

## Top-Level YAML
- Single document with keys: `mapping`, `pipeline`, `features`, and optional `profiling`.
- Parsed by `PipelineDocument` (Pydantic) with `extra="forbid"` to reject unknown keys.

## Mapping
- `mapping.entities` and optional `mapping.references` map canonical names to `{table, columns}`.
- `columns` is a map of canonical column name to physical column name.

## Pipeline
- `pipeline.name`, `pipeline.slug`, `pipeline.version`, `pipeline.execution_target`, `pipeline.grain`.
- `pipeline.spine`: `{entity, key, columns}`.
- `pipeline.output`: `{table, materialization}` where materialization is `table` or `view`.
- `pipeline.databricks`: only used when `pipeline.execution_target == "databricks"`.
  - `semantic_schema_suffix`, `features_schema_suffix`.
- `pipeline.naming`: `prefixing` (enabled, scheme, separator) and `collision_policy` (`fail` or `auto_prefix`).
- `pipeline.validation`: `on_missing_required_column` (`fail` or `warn_skip`).

Previously (2026-01-13):
> - `pipeline.name`, `pipeline.version`, `pipeline.grain`.
> - `pipeline.spine`: `{entity, key, columns}`.
> - `pipeline.output`: `{table, materialization}` where materialization is `table` or `view`.
> - `pipeline.naming`: `prefixing` (enabled, scheme, separator) and `collision_policy` (`fail` or `auto_prefix`).
> - `pipeline.validation`: `on_missing_required_column` (`fail` or `warn_skip`).

## Features
- `features` is a list of `{key, params}`. `params` is a free-form map validated per feature metadata.
- Features are evaluated in list order; features that reference other feature outputs must appear after their dependencies.

## Profiling
- `profiling` supports: `enabled`, `sample_rows`, `sampling_mode`, `sampling_seed`, `profile_raw_entities`, `profile_output`, `output_dir`.

## Decay Notes
- High Risk if Wrong: Databricks mode assumes `pipeline.output.table` is 3-part (`catalog.schema.table`) and that derived schemas are creatable at runtime; validate against your Databricks workspace permissions and SQLMesh behavior before first deployment. Risk Accepted on 2026-01-14 (needs Databricks run to verify).
