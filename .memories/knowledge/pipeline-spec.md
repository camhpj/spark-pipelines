# Pipeline Specification (Current Implementation)

Verified on 2026-01-13.

## Top-Level YAML
- Single document with keys: `mapping`, `pipeline`, `features`, and optional `profiling`.
- Parsed by `PipelineDocument` (Pydantic) with `extra="forbid"` to reject unknown keys.

## Mapping
- `mapping.entities` and optional `mapping.references` map canonical names to `{table, columns}`.
- `columns` is a map of canonical column name to physical column name.

## Pipeline
- `pipeline.name`, `pipeline.version`, `pipeline.grain`.
- `pipeline.spine`: `{entity, key, columns}`.
- `pipeline.output`: `{table, materialization}` where materialization is `table` or `view`.
- `pipeline.naming`: `prefixing` (enabled, scheme, separator) and `collision_policy` (`fail` or `auto_prefix`).
- `pipeline.validation`: `on_missing_required_column` (`fail` or `warn_skip`).

## Features
- `features` is a list of `{key, params}`. `params` is a free-form map validated per feature metadata.
- Features are evaluated in list order; features that reference other feature outputs must appear after their dependencies.

## Profiling
- `profiling` supports: `enabled`, `sample_rows`, `sampling_mode`, `sampling_seed`, `profile_raw_entities`, `profile_output`, `output_dir`.
