## Summary

Implemented Databricks-aware model namespacing so pipelines can target monthly schemas without collisions across multiple pipelines. Added `pipeline.slug` and `pipeline.execution_target` to the pipeline schema and used these to derive per-pipeline semantic/features schemas from the output table identifier. Updated compiler, runtime, feature code, tests, and docs to use centralized naming helpers and to create internal schemas at runtime for Databricks runs.

## What Changed

- Added `pipeline.slug`, `pipeline.execution_target` (`local|databricks`), and `pipeline.databricks` suffix options to the pipeline schema (`src/spark_preprocessor/schema.py`).
- Introduced centralized naming utilities for parsing `catalog.schema.table` and constructing Databricks internal schema/model names (`src/spark_preprocessor/model_naming.py`).
- Extended `BuildContext` with helpers to resolve semantic/reference/feature model names across targets (`src/spark_preprocessor/features/base.py`).
- Updated the compiler to:
  - Validate 3-part `output.table` only when `execution_target=databricks`.
  - Generate semantic model names via `BuildContext` and use those names in the final model `FROM` (`src/spark_preprocessor/compiler.py`).
  - Adjust SQLMesh project file naming logic for 3-part model names (`src/spark_preprocessor/compiler.py`).
- Updated profiling notebook generation to reference the resolved semantic model names (`src/spark_preprocessor/profiling.py`).
- Updated the Geisinger feature to use `BuildContext` naming helpers for feature/semantic reference model names (`src/spark_preprocessor/features/geisinger.py`).
- Updated the Databricks runtime entrypoint to ensure derived internal schemas exist before applying the SQLMesh plan (`src/spark_preprocessor/runtime/apply_pipeline.py`).
- Updated docs and examples to include `slug`/`execution_target` and describe Databricks internal schema derivation (`docs/*`, `SPEC.md`, `example/pipeline.yaml`).
- Updated/added tests, including a unit assertion that Databricks-mode compilation emits namespaced semantic/features model references (`tests/unit/test_geisinger_demographics.py`), plus updated payloads to include `slug`.
- Ran formatting and verification via Taskfile: `task fmt` and `task ci` (ruff format/check, ruff check, ty check, pytest).

## Why

Databricks uses 3-part namespaces (catalog, schema, table), and client workflows place monthly data drops into a per-month schema. The previous `semantic.*` / `features.*` schema approach could not guarantee landing internal models in the desired monthly schema and would also collide when multiple pipelines run in the same month. Deriving internal schemas from the base schema plus a stable `pipeline.slug` provides deterministic isolation while keeping output tables in the original monthly schema.

## Decisions

- Added `pipeline.slug` (identifier-safe) to avoid ambiguous sanitization and to support multiple pipelines per monthly schema without internal model collisions.
- Enforced 3-part `pipeline.output.table` only for `execution_target=databricks` to preserve local/DuckDB workflows.
- Kept semantic/features separation via separate derived schemas rather than encoding everything into a single schema or table-name prefixes.
- Made the runtime create internal schemas (fail-fast) to avoid manual pre-provisioning steps.

## Next Steps

- Consider adding an explicit doc example for the derived schema names with a realistic monthly schema (e.g. `ma_20260101__<slug>_semantic`).
- Decide whether to add a Databricks-mode integration smoke test (would require a Databricks test environment; DuckDB will not emulate UC semantics).
- If pipeline name and slug diverge in practice, ensure CLI/scaffold UX makes slug selection obvious.

## References

- SQLMesh Databricks engine docs: https://sqlmesh.readthedocs.io/en/latest/integrations/engines/databricks/
- Internal naming utilities: `src/spark_preprocessor/model_naming.py`
