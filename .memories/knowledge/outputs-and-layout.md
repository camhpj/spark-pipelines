# Outputs and Layout

Verified on 2026-01-14.

## Directory Layout
- `models/semantic/`: semantic view models for entities and references.
- `models/features/<feature_key>/`: models emitted by features (if any).
- `models/marts/`: final enriched model file `enriched__<pipeline_name>.sql`.
- `tests/`: SQLMesh test YAML files emitted by features.
- `notebooks/`: profiling notebooks (if enabled).
- `rendered/`: rendered SQL `enriched__<pipeline_name>.sql`.
- `manifest/`: `compile_report.json`.
- `sqlmesh.yaml`: SQLMesh project config.

## Model Naming
- Local mode (`pipeline.execution_target: local`):
  - Semantic models are named `semantic.<entity>` and `semantic.reference__<name>`.
  - Feature models are typically named `features.<feature_slug>__<purpose>` (feature-defined).
  - Final model uses `pipeline.output.table` as its SQLMesh model name.
- Databricks mode (`pipeline.execution_target: databricks`):
  - Semantic models are written to a derived schema based on the output schema and `pipeline.slug`:
    `<catalog>.<base_schema>__<slug>_semantic.<entity>` and
    `<catalog>.<base_schema>__<slug>_semantic.reference__<name>`.
  - Feature models are written to `<catalog>.<base_schema>__<slug>_features.*`.
  - Final model uses `pipeline.output.table` as its SQLMesh model name (and remains in the base schema).

Previously (2026-01-12):
> - Semantic models are named `semantic.<entity>` and `semantic.reference__<name>`.
> - Final model uses `pipeline.output.table` as its SQLMesh model name.

## Compile Report
- `compile_report.json` includes pipeline metadata, included/skipped features, resolved table names, profiling config, and compile timestamp.
