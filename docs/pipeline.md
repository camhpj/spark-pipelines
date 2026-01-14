# Pipeline specification

A pipeline YAML is a single document that includes mapping, pipeline metadata,
feature selection, and optional profiling configuration.

## Structure

```yaml
mapping:
  entities:
    patients:
      table: "catalog.schema.patients_raw"
      columns:
        person_id: "member_id"
        date_of_birth: "dob"
        as_of_date: "as_of_date"

pipeline:
  name: "client_x_enriched"
  slug: "client_x_enriched"
  version: "v1.2.3"
  execution_target: "local"  # local|databricks
  grain: "PERSON"
  spine:
    entity: "patients"
    key: "person_id"
    columns:
      - "person_id"
      - "date_of_birth"
  output:
    table: "catalog.schema.enriched_client_x"
    materialization: "table"
  databricks:
    semantic_schema_suffix: "_semantic"
    features_schema_suffix: "_features"
  naming:
    prefixing:
      enabled: false
      scheme: "namespace"  # namespace|feature
      separator: "__"
    collision_policy: "fail"  # fail|auto_prefix
  validation:
    on_missing_required_column: "fail"  # fail|warn_skip

features:
  - key: "age"
    params:
      start: "date_of_birth"
      end: "as_of_date"
  - key: "age_bucket"

profiling:
  enabled: true
  sample_rows: 100000
  sampling_mode: "deterministic"  # random|deterministic
  sampling_seed: 42
  profile_raw_entities: ["patients"]
  profile_output: true
  output_dir: "dbfs:/FileStore/profiles/client_x"
```

## Pipeline section

- `name`: logical pipeline name (used in artifact names).
- `slug`: stable identifier used for namespacing internal schemas (Databricks mode).
- `version`: version tag for traceability (recorded in output metadata).
- `execution_target`: where the compiled SQLMesh project is intended to run (`local` or `databricks`).
- `grain`: logical grain (default `PERSON`).
  - If set to non-`PERSON`, the spine entity/key must be non-default and
    every feature must declare compatibility with the grain.
- `spine`: base entity/key used for joins.
  - `columns` controls which spine columns are selected into the output.
    Features may still reference other mapped spine columns even if they are
    not listed here.
- `output.table`: explicit output table identifier.
  - When `execution_target: databricks`, this must be 3-part: `catalog.schema.table`.
- `output.materialization`: `table` (default) or `view`.
- `databricks`: schema naming options (only used when `execution_target: databricks`).
  - Internal schemas are derived from the output schema and pipeline slug:
    `<base_schema>__<slug>_semantic` and `<base_schema>__<slug>_features`.
- `naming`: prefixing and collision policy for feature columns.
- `validation.on_missing_required_column`:
  - `fail`: stop compilation on missing columns.
  - `warn_skip`: skip the feature (and dependents) and continue.

## Features section

Each feature entry selects a registry key and passes parameters.
Parameter types are validated at compile time.

Feature entries are evaluated in the order they appear. If a feature references
another feature's output column, list the dependency first (otherwise the compiler
will fail or skip the feature, depending on the validation policy).

## Profiling section

If enabled, the compiler generates a Databricks notebook that profiles
raw semantic tables and/or the output table. See `profiling.md` for details.
