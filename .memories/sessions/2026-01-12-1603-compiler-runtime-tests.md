## Summary
Implemented core remaining spec items: runtime apply entrypoint, profiling sampling modes, scaffold command, and stronger compiler/contract validation. Added SQLMesh/DuckDB smoke testing and expanded unit tests for collisions, validation, and profiling configuration. Updated SQLMesh model rendering for parser compatibility and consolidated Databricks extras to include pyspark.

## What Changed
- Removed all `from __future__ import annotations` usages across `src/` to comply with AGENTS.md.
- Added profiling sampling config (random vs deterministic with seed) and notebook generation updates in `src/spark_preprocessor/schema.py` and `src/spark_preprocessor/profiling.py`.
- Implemented scaffold command and mapping loader (`src/spark_preprocessor/scaffold.py`, `src/spark_preprocessor/cli.py`, `src/spark_preprocessor/schema.py`).
- Expanded `SemanticContract` and compiler validation (naming rules, spine column checks, non-PERSON grain compatibility, collision handling, prefix sanitization) in `src/spark_preprocessor/semantic_contract.py` and `src/spark_preprocessor/compiler.py`.
- Implemented SQLMesh runtime entrypoint (`src/spark_preprocessor/runtime/apply_pipeline.py`).
- Fixed SQLMesh model header formatting and mapped TABLE -> FULL in `src/spark_preprocessor/sqlmesh_project.py`.
- Added DuckDB SQLMesh smoke test and new compiler tests in `tests/test_sqlmesh_duckdb.py` and `tests/test_compiler.py`.
- Updated optional dependencies to group `databricks-sql-connector` and `pyspark` under the same extra; refreshed `uv.lock`.
- Ran Taskfile commands: `task fmt`, `task lint`, `task typecheck`, `task test`.

## Why
- Close remaining SPEC.md gaps around runtime execution, profiling control, and validation.
- Enforce repo policy (no `from __future__ import annotations`).
- Increase test coverage and ensure SQLMesh project output is executable (DuckDB smoke test).
- Align extras with Databricks runtime requirements.

## Decisions
- Keep SQLGlot pinned to the range required by `sqlmesh==0.228.1` to avoid dependency conflicts.
- Use seeded random ordering for deterministic profiling sampling.
- Use DuckDB-only SQLMesh execution tests for now.

## Next Steps
- Decide whether to update SQLMesh (or SPEC.md) to reconcile the SQLGlot version requirement.
- Add more built-in features and corresponding tests as functionality expands.
- Consider expanding semantic contract optional columns/types once requirements are defined.
- Review SQLMesh runtime behavior inside Databricks and add tests if needed.

## References
- `src/spark_preprocessor/compiler.py`
- `src/spark_preprocessor/profiling.py`
- `src/spark_preprocessor/runtime/apply_pipeline.py`
- `src/spark_preprocessor/scaffold.py`
- `src/spark_preprocessor/sqlmesh_project.py`
- `tests/test_compiler.py`
- `tests/test_sqlmesh_duckdb.py`
- `pyproject.toml`
