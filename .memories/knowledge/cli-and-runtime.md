# CLI and Runtime

Verified on 2026-01-13.

## CLI Commands
- `compile --pipeline <path> --out <dir>`: compiles pipeline and writes artifacts.
- `render-sql --pipeline <path> --out <dir>`: compiles pipeline and reports rendered SQL path.
- `test --pipeline <path> --project <dir>`: compiles pipeline and parses rendered SQL with SQLGlot (Spark dialect).
- `scaffold --mapping <path> --out <dir>`: generates a starter pipeline YAML from a mapping file.

## Logging
- CLI uses structlog with a filtering bound logger at INFO level.

## Runtime Entry Point
- `spark_preprocessor.runtime.apply_pipeline:main` loads the pipeline document, initializes a SQLMesh `Context`, plans, and applies the plan.

Previously:
> `spark_preprocessor.runtime.apply_pipeline:main` exists but raises a RuntimeError (not yet implemented).

## Decay Notes
- Possibly Stale: SQLMesh runtime behavior can change across versions; verify `apply_pipeline.py` against the installed SQLMesh API before deployment. Verified on 2026-01-13.
