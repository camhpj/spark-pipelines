# CLI and Runtime

Verified on 2026-01-14.

## CLI Commands
- `compile --pipeline <path> --out <dir>`: compiles pipeline and writes artifacts.
- `render-sql --pipeline <path> --out <dir>`: compiles pipeline and reports rendered SQL path.
- `test --pipeline <path> --project <dir>`: compiles pipeline and parses rendered SQL with SQLGlot (Spark dialect).
- `scaffold --mapping <path> --out <dir>`: generates a starter pipeline YAML from a mapping file.

## Logging
- CLI uses structlog with a filtering bound logger at INFO level.

## Runtime Entry Point
- `spark_preprocessor.runtime.apply_pipeline:main` loads the pipeline document, initializes a SQLMesh `Context`, plans, and applies the plan.
- If `pipeline.execution_target == "databricks"`, the runtime ensures the derived semantic/features schemas exist by executing `CREATE SCHEMA IF NOT EXISTS ...` via the active Spark session before applying.
- `sqlmesh.core.context.Context` is imported inside `main()` (lazy import) to keep module import lightweight. Updated on 2026-01-13.

Previously:
> `spark_preprocessor.runtime.apply_pipeline:main` exists but raises a RuntimeError (not yet implemented).

## Decay Notes
- Possibly Stale: SQLMesh runtime behavior can change across versions; verify `apply_pipeline.py` against the installed SQLMesh API before deployment.
- High Risk if Wrong: Databricks schema creation requires appropriate permissions in the target catalog; a missing permission will fail pipeline runs early. Risk Accepted on 2026-01-14 (needs Databricks run to verify).
