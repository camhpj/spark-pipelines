# Project Context

## Vocabulary
- SQLMesh: transformation/test framework used to generate project assets (models/macros/tests) for Databricks.
- SQLGlot: SQL parser used for Spark dialect validation.
- SemanticContract: versioned contract of canonical entities, required keys, and column naming rules.
- MappingSpec: YAML mapping from canonical entities/references to physical tables and columns.
- Canonical entities: patients, encounters, medications, procedures, insurance, diagnoses.
- Spine: base entity/key for joins; default spine is patients with key person_id.
- Feature registry: key-based registry of Feature implementations in the shared wheel.
- ExpressionFeature: feature that contributes select expressions to the final model.
- ModelFeature: feature that generates SQLMesh models producing joinable columns.
- FeatureAssets: bundle of models, join specs, select expressions, and tests produced by Feature.build().
- Compile: deterministic generation of SQLMesh project, rendered SQL, profiling notebook, and compile report from pipeline YAML.
- Profiling notebook: Databricks .py notebook using ydata-profiling PySpark integration.

## Invariants
- Python >= 3.13.
- Repo uses a src layout; package is under src/spark_preprocessor.
- Use uv for dependency management and Taskfile for common commands.
- Ruff is the sole formatter/linter, invoked via Taskfile.
- Library has no Spark dependency; Spark is used only in generated notebooks and Databricks runtime jobs.
- Compile is idempotent: the compiler rewrites out_dir deterministically; manual edits are not preserved.
- Compiler outputs include a SQLMesh project, rendered SQL, profiling notebook, and compile report in a fixed layout.
- Semantic models are generated as SQLMesh views from mappings and are the only upstreams referenced by features.
- Default spine entity is patients; default join key is person_id; default join type is LEFT JOIN.
- Pipeline output table name is explicit and materialized as a table; output metadata includes pipeline name, version tag, and compile timestamp.
- Missing required mapping/columns defaults to fail with an optional warn_skip policy.
