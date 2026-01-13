# Testing

Tests are mandatory and should be added alongside new features or compiler behavior.
The focus is on correctness of feature composition, SQL validity, and schema handling.

## Test tiers

- **Compiler tests**: validate pipeline parsing, error handling, naming rules,
  and generated SQL layout.
- **DuckDB smoke tests**: run a minimal SQLMesh project end-to-end locally.

Spark is not required for local tests.

## Running tests

Use the Taskfile commands (preferred):

```bash
task test
```

## Adding tests

When adding features or compiler behavior:

- Add unit tests to `tests/` for validation and SQL generation.
- Include DuckDB smoke coverage when a feature uses non-trivial SQL or joins.
- Prefer tests that would catch SQL syntax errors or missing functionality.
