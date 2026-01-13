# Testing

Verified on 2026-01-13.

## Test Layout
- Unit tests live in `tests/unit/` and use `test_<module>.py` naming (for example `test_cli.py` for `cli.py`).
- Integration tests live in `tests/integration/` and cover end-to-end compilation and a local SQLMesh/DuckDB smoke run.
- `tests/`, `tests/unit/`, and `tests/integration/` include `__init__.py` to avoid pytest import-name collisions when the same basename exists in both tiers (for example `test_compiler.py`).

## Tooling
- Tests run via `task test` which executes `uv run pytest`.
- Pytest is configured in `pyproject.toml` with `pytest-cov` enabled by default and enforces `--cov-fail-under=85`.
- Tests use an autouse fixture (`tests/conftest.py`) to snapshot/restore the global feature registry between tests.

Previously:
> `tests/test_compiler.py` covers:
> - Compile output layout (presence of SQLMesh config, semantic model, final model, rendered SQL, compile report, profiling notebook).
> - Rendered SQL parses with SQLGlot using Spark dialect.
> - `warn_skip` behavior when a required column is missing.
> - Collision policy auto-prefixing, invalid canonical names, and non-PERSON grain compatibility.
> - Feature dependency skip behavior and profiling sampling config emission.
> `tests/test_sqlmesh_duckdb.py` runs a SQLMesh plan/apply cycle against DuckDB.

Rationale: tests were reorganized into `tests/unit/` and `tests/integration/` while keeping the same functional coverage areas. Updated on 2026-01-13.
