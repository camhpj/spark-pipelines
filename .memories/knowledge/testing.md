# Testing

Verified on 2026-01-12.

## Current Tests
- `tests/test_compiler.py` covers:
  - Compile output layout (presence of SQLMesh config, semantic model, final model, rendered SQL, compile report, profiling notebook).
  - Rendered SQL parses with SQLGlot using Spark dialect.
  - `warn_skip` behavior when a required column is missing.
  - Collision policy auto-prefixing, invalid canonical names, and non-PERSON grain compatibility.
  - Feature dependency skip behavior and profiling sampling config emission.
- `tests/test_sqlmesh_duckdb.py` runs a SQLMesh plan/apply cycle against DuckDB.

## Tooling
- Tests run via `task test` which executes `uv run pytest`.
