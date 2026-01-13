# Dependencies

Verified on 2026-01-13.

## Runtime
- pydantic >=2.12.5
- pyyaml >=6.0.3
- sqlmesh ==0.228.1
- sqlglot >=27.28.0,<27.29.dev0
- structlog >=25.5.0
- ydata-profiling >=4.18.0

## Optional Extras
- `duckdb`: duckdb >=1.0.0, pyarrow >=15.0.0
- `databricks`: databricks-sql-connector >=3.0.0, pyspark >=3.5.0

## Dev
- commitizen >=4.11.0
- duckdb >=1.0.0
- faker >=25.0.0
- ipython >=9.9.0
- jupyterlab >=4.5.1
- pytest >=9.0.2
- pytest-cov >=7.0.0
- ruff >=0.14.11
- ty >=0.0.11

## Decay Notes
- Possibly Stale: Dependency versions can drift quickly; re-verify against `pyproject.toml` and `uv.lock` before making compatibility decisions. Verified on 2026-01-13.
- High Risk if Wrong: `sqlmesh` and `sqlglot` pins affect SQL rendering and runtime compatibility; confirm versions are still intended before shipping. Verified on 2026-01-13.
