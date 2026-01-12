## Summary
Bootstrapped the core spark-preprocessor implementation: schemas, compiler pipeline, feature registry, built-in age features, CLI, and profiling notebook generator. Added tests covering compile output, rendered SQL parsing, and warn-skip behavior. Updated dependencies to include SQLMesh v0.228.1 and aligned sqlglot pinning, then removed all `from __future__ import annotations` imports per AGENTS.md.

## What Changed
- Added core modules under `src/spark_preprocessor/` (schemas, compiler, SQLMesh project rendering, profiling notebook, errors, semantic contract).
- Implemented feature registry and built-in `age` and `age_bucket` features.
- Added CLI commands for compile/render/test and a runtime stub entrypoint.
- Added unit tests in `tests/test_compiler.py`.
- Updated `pyproject.toml` dependencies (pydantic, pyyaml, sqlmesh 0.228.1) and sqlglot pin; adjusted ruff config.
- Removed `from __future__ import annotations` across code and tests to comply with new repo directive.
- Ran `task fmt`, `task lint`, `task typecheck`, and `task test`.

## Why
To implement the initial end-to-end compile workflow described in SPEC.md, establish built-in feature patterns, and ensure the project is aligned with the updated AGENTS.md constraint against future annotations.

## Decisions
- Pinned SQLMesh to v0.228.1 and sqlglot to 27.28.x to satisfy resolver constraints.
- Implemented age calculation as `months_between(end, start)/12` and age buckets based on the computed `age` column.
- Compiler fully wipes `out_dir` before emitting artifacts.
- Final SQL model name uses the explicit pipeline output table and includes metadata comments.

## Next Steps
- Implement SQLMesh runtime apply logic in `src/spark_preprocessor/runtime/apply_pipeline.py`.
- Confirm or adjust age bucket boundaries/labels as needed.
- Add additional built-in features following the established pattern.
- Expand tests for collision policy, non-person grains, and column_ref validation edge cases.

## References
- `SPEC.md`
- `AGENTS.md`
- `.memories/context.md`
