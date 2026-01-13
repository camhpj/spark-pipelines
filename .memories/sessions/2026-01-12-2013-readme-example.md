## Summary
Expanded documentation to cover project overview, technical implementation, and Databricks-only execution. Added a runnable example directory with Faker-generated data plus scripts for local DuckDB smoke runs. Updated dependency extras and README to reflect new usage guidance.

## What Changed
- Updated `README.md` with high-level overview, implementation details, usage, Databricks-only execution guidance, and optional extras notes.
- Added `example/` directory with `pipeline.yaml`, `generate_data.py`, `run_duckdb.py`, and a generated `data/patients.csv`.
- Added `faker` as a dev dependency and refreshed `uv.lock`.
- Adjusted `.gitignore` to exclude example outputs (`example/out/`, `example/duckdb.db`).

## Why
- Provide clear user-facing documentation and a runnable end-to-end example.
- Ensure the Databricks execution model (in-cluster only) is explicit.
- Supply a deterministic dummy dataset for local testing/demo purposes.

## Decisions
- Use Faker for example data generation and keep the generated CSV in-repo for immediate use.
- Document Databricks usage as in-cluster only with no external connector requirement.

## Next Steps
- Run `task test` if you want full validation after doc/example changes.
- Decide if the example dataset should remain versioned or be generated only.
- Add any additional examples (non-person grain, feature joins) as needed.

## References
- `README.md`
- `example/`
- `pyproject.toml`
- `.gitignore`
