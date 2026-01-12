# CLI and Runtime

Verified on 2026-01-12.

## CLI Commands
- `compile --pipeline <path> --out <dir>`: compiles pipeline and writes artifacts.
- `render-sql --pipeline <path> --out <dir>`: compiles pipeline and reports rendered SQL path.
- `test --pipeline <path> --project <dir>`: compiles pipeline and parses rendered SQL with SQLGlot (Spark dialect).

## Logging
- CLI uses structlog with a filtering bound logger at INFO level.

## Runtime Entry Point
- `spark_preprocessor.runtime.apply_pipeline:main` exists but raises a RuntimeError (not yet implemented).
