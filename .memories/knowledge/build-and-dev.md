# Build and Dev Workflow

Verified on 2026-01-12.

## Tooling
- Dependency management: `uv`.
- Task runner: `Taskfile.yml` with common workflows.

## Common Tasks
- `task install` -> `uv sync`
- `task fmt` -> `uv run ruff format`
- `task lint` -> `uv run ruff check`
- `task test` -> `uv run pytest`
- `task typecheck` -> `uv run ty check`
- `task dev` -> `task fmt`, `task lint`, `task test`
- `task ci` -> `task fmt:check`, `task lint`, `task test`
