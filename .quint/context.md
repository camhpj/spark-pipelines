# Bounded Context

## Vocabulary

- **PipelineSpec**: YAML definition of a pipeline's features and mappings.
- **MappingSpec**: mapping of canonical entities/references to physical tables and columns.
- **SemanticContract**: versioned contract for canonical entities, required keys/columns, and naming rules.
SQLMesh project: generated models/macros/tests executed in Databricks.
- **Compile**: deterministic generation of SQLMesh project, rendered SQL, and profiling notebook.
- **Feature**: registry-discovered unit of logic; ExpressionFeature adds select expressions, ModelFeature builds models.
Enriched output table: final dataset produced by SQLMesh plan/apply.
Canonical entities: patients, encounters, medications, procedures, insurance, diagnoses.
- **Spine**: base entity and join key; default patients/person_id.
Profiling notebook: Databricks notebook using ydata-profiling PySpark integration.

## Invariants

Python >= 3.13 with src layout (src/<package_name>/...).
Use uv for dependency management/execution and Taskfile for standardized commands.
Ruff is the only formatter/linter; ty is required for type checking; pytest is the test runner.
Library has no Spark dependency; Spark is only used in generated Databricks notebooks/jobs.
compile(pipeline.yaml, out_dir) is deterministic and regenerates artifacts (no in-place manual edits preserved).
Generate semantic view models for each mapped entity/reference under models/semantic/ with canonical columns.
Default join strategy is LEFT JOIN on person_id unless pipeline specifies a different spine/key and features declare compatibility.
Features are discovered by registry key from the shared wheel; YAML references features by key.
Use structlog for logging; avoid stdlib logging.
