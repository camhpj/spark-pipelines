## Summary
Improved the repository’s test structure and coverage reporting. Added pytest-cov configuration (including an 85% coverage floor) and reorganized tests into `tests/unit/` and `tests/integration/` while keeping `task test` running both tiers. Added a broad set of unit tests for compiler, schema, CLI, built-ins, registry, SQLMesh project rendering, profiling notebook generation, and runtime entrypoints, increasing total coverage to ~97%.

## What Changed
- Added pytest + coverage configuration in `pyproject.toml`:
  - Default `--cov`/report options and `--cov-fail-under=85`.
  - Coverage run/report settings (branch coverage, omit tests).
- Reorganized tests:
  - Moved integration tests to `tests/integration/`.
  - Moved/renamed unit tests to `tests/unit/` with `test_<module>.py` naming.
  - Added `tests/__init__.py`, `tests/unit/__init__.py`, `tests/integration/__init__.py` to avoid pytest import-name collisions.
- Added a global pytest fixture in `tests/conftest.py` to snapshot/restore the feature registry between tests.
- Added/expanded unit test coverage:
  - Compiler validation, param typing, dependency handling, semantic model rendering, and artifact writing (feature models + SQLMesh tests).
  - Schema parsing and error-wrapping paths (missing files + validation failures).
  - Built-in feature SQL generation (age/age_bucket), registry behavior, semantic contract helpers, SQLMesh project rendering, profiling notebook generation, and runtime apply behavior (including generic exception path).
- Made `sqlmesh` import lazy in `src/spark_preprocessor/runtime/apply_pipeline.py` to keep module import lightweight and unit-test friendly.
- Updated docs to reflect the unit/integration test layout in `docs/testing.md`.
- Ran `task test` repeatedly to validate behavior and measure coverage.

## Why
- The existing tests skewed toward integration-style coverage (full compile + SQLMesh/DuckDB smoke), leaving many pure logic branches under-tested.
- Splitting unit vs integration tests improves clarity, maintenance, and the ability to run fast/local checks while still keeping end-to-end confidence.
- Enforcing an 85% coverage floor provides a durable guardrail against test regressions without requiring perfection.
- The feature registry is global state; snapshot/restore prevents cross-test leakage and flakes.

## Decisions
- Keep `task test` running both tiers by default (pytest collects both under `tests/`).
- Allow unit tests to import `sqlmesh` when useful; avoid hard bans, but keep heavy execution in integration tests.
- Set the CI coverage gate to 85% via pytest-cov (`--cov-fail-under=85`) rather than custom scripting.
- Leave third-party warning noise (131 warnings) unaddressed for now since they originate from dependencies.

## Next Steps
- Consider adding dedicated tasks (e.g., `task test:unit`, `task test:integration`) for faster local workflows while keeping `task test` as the combined run.
- Decide whether to treat warnings as errors for first-party code only (with narrowly scoped filters for third-party warnings).
- Add unit tests for remaining uncovered CLI lines if desired (minor).
- Evaluate whether `MAX_FORK_WORKERS=1` should be set in test env to reduce SQLMesh fork-related warnings.

## References
- `pyproject.toml` (pytest-cov config + coverage gate)
- `tests/unit/` and `tests/integration/` (new layout)
- `tests/conftest.py` (feature registry isolation)
- `docs/testing.md` (updated guidance)
