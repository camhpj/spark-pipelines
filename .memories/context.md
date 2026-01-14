# Project Context

## Vocabulary
- SQLMesh: transformation/test framework used to generate project assets (models/macros/tests) for Databricks.
- SQLGlot: SQL parser used for Spark dialect validation.
- SemanticContract: versioned contract of canonical entities, required keys, and column naming rules.
- MappingSpec: YAML mapping from canonical entities/references to physical tables and columns.
- Canonical entities: patients, encounters, medications, procedures, insurance, diagnoses.
- Spine: base entity/key for joins; default spine is patients with key person_id.
- Feature registry: key-based registry of Feature implementations in the shared wheel.
- FeatureAssets: bundle of models, join specs, select expressions, and tests produced by Feature.build().
- Compile: deterministic generation of SQLMesh project, rendered SQL, profiling notebook, and compile report from pipeline YAML.
- Profiling notebook: Databricks .py notebook using ydata-profiling PySpark integration.
- Built-in features: age (computed from start/end date columns) and age_bucket (derived from age).
- Execution target: where the compiled project is intended to run (`local` or `databricks`).
- Pipeline slug: identifier-safe string used to namespace Databricks internal schemas per pipeline.
- Databricks namespaces: derived semantic/features schemas created at runtime in Databricks mode.

## Invariants
- Python >= 3.13.
- Repo uses a src layout; package is under src/spark_preprocessor.
- Use uv for dependency management and Taskfile for common commands.
- Ruff is the sole formatter/linter, invoked via Taskfile.
- Tests are organized into two tiers under `tests/unit/` and `tests/integration/`. Updated on 2026-01-13.
- Pytest is configured in `pyproject.toml` to run with coverage (`pytest-cov`) and fail under 85% total coverage. Updated on 2026-01-13.
- Library has no Spark dependency; Spark is used only in generated notebooks and Databricks runtime jobs.
- Compile is idempotent: the compiler rewrites out_dir deterministically; manual edits are not preserved.
- Compiler wipes out_dir entirely before writing artifacts.
- Compiler outputs include a SQLMesh project, rendered SQL, profiling notebook, and compile report in a fixed layout.
- Semantic models are generated as SQLMesh views from mappings and are the only upstreams referenced by features.
- Default spine entity is patients; default join key is person_id; default join type is LEFT JOIN.
- Pipeline output table name is explicit and materialized as a table; output metadata includes pipeline name, version tag, and compile timestamp.
- Pipeline YAML requires `pipeline.slug`.
- In Databricks mode (`pipeline.execution_target=databricks`), `pipeline.output.table` must be 3-part (`catalog.schema.table`) and internal semantic/features schemas are derived from the base schema and pipeline slug.
- Databricks runtime ensures derived schemas exist (via Spark `CREATE SCHEMA IF NOT EXISTS ...`) before applying the SQLMesh plan.
- Missing required mapping/columns defaults to fail with an optional warn_skip policy.
- `from __future__ import annotations` must not be used (per AGENTS.md).

Previously:
> - ExpressionFeature: feature that contributes select expressions to the final model.
> - ModelFeature: feature that generates SQLMesh models producing joinable columns.

Rationale: the codebase models all features uniformly via `Feature.build()` returning `FeatureAssets`; there are no separate ExpressionFeature/ModelFeature types. Updated on 2026-01-14.
