# Outputs and Layout

Verified on 2026-01-12.

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
- Semantic models are named `semantic.<entity>` and `semantic.reference__<name>`.
- Final model uses `pipeline.output.table` as its SQLMesh model name.

## Compile Report
- `compile_report.json` includes pipeline metadata, included/skipped features, resolved table names, profiling config, and compile timestamp.
