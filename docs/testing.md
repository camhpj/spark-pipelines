# Testing

Tests are mandatory and should be added alongside new features or compiler behavior.
The focus is on correctness of feature composition, SQL validity, and schema handling.

## Test tiers

- **Unit tests** (`tests/unit/`): validate pure compiler logic (validation, naming rules,
  SQL rendering) and small utilities in isolation.
- **Integration tests** (`tests/integration/`): compile a pipeline end-to-end and smoke-test
  a minimal SQLMesh project locally (DuckDB).

Spark is not required for local tests.

## Running tests

Use the Taskfile commands (preferred):

```bash
task test
```

## Adding tests

When adding features or compiler behavior:

- Add unit tests to `tests/unit/` for validation and SQL generation.
- Include DuckDB smoke coverage when a feature uses non-trivial SQL or joins.
- Prefer tests that would catch SQL syntax errors or missing functionality.
