# Architecture

This library is a deterministic compiler. It reads a pipeline YAML and writes a
complete SQLMesh project plus supporting artifacts. There is no stateful build
cache and no incremental output merging.

## Compiler pipeline

1. **Load + validate** the pipeline YAML with Pydantic.
2. **Validate** against the semantic contract (required columns, naming rules).
3. **Generate semantic views** for every mapped entity and reference.
4. **Resolve features** from the registry and validate parameters/requirements.
5. **Assemble** the final model using the spine and join models.
6. **Render** SQLMesh models, rendered SQL, compile report, profiling notebook.

## Artifact layout

The compiler wipes the output directory and rewrites it deterministically:

```
<out_dir>/
  sqlmesh.yaml
  models/
    semantic/
      <entity>.sql
      reference__<name>.sql
    features/
      <feature_key>/
        <model>.sql
    marts/
      enriched__<pipeline_name>.sql
  tests/
    <test>.yaml
  notebooks/
    profile__<pipeline_name>.py
  rendered/
    enriched__<pipeline_name>.sql
  manifest/
    compile_report.json
```

The compile report records included/skipped features, resolved table identifiers,
profiling configuration, and the compile timestamp.

## SQLMesh integration

- `sqlmesh.yaml` is generated with `engine.type=databricks` and `dialect=spark`.
- Models use a `MODEL (...)` header and include `kind FULL` for tables.
- The runtime entrypoint loads the project and runs `Context.plan(...); Context.apply(...)`.

## Metadata and naming

- The final model SQL is prefixed with comment lines recording pipeline name,
  pipeline version, compile timestamp, and selected features.
- Column naming collisions are governed by the pipeline naming config:
  `collision_policy=fail` (default) or `auto_prefix`.
- Optional prefixing can be enabled (namespace/feature scheme).

## Feature dependency handling

- Features may depend on other feature outputs by referencing their columns.
- The compiler detects missing dependencies and fails or skips, depending on
  the validation policy.

## Profiling notebook

If profiling is enabled, a Databricks notebook is generated that:
- Samples raw semantic tables and/or the output table.
- Produces ydata-profiling HTML reports in DBFS.
- Supports random or deterministic sampling.
