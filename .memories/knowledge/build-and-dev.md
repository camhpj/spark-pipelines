# Build and Dev Workflow

Verified on 2026-01-14.

## Tooling
- Dependency management: `uv`.
- Task runner: `Taskfile.yml` with common workflows.

## Common Tasks
- `task install` -> `UV_CACHE_DIR=.uv-cache uv sync`
- `task install:frozen` -> `UV_CACHE_DIR=.uv-cache uv sync --frozen`
- `task fmt` -> `uv run ruff format`
- `task lint` -> `uv run ruff check`
- `task test` -> `uv run pytest` (pytest is configured in `pyproject.toml` to run coverage and enforce `--cov-fail-under=85`).
- `task typecheck` -> `uv run ty check`
- `task dev` -> `task fmt`, `task lint`, `task test`
- `task ci` -> `task fmt:check`, `task lint`, `task test`

Previously:
> `task install` -> `uv sync`

Rationale: `Taskfile.yml` sets a repo-local uv cache (`.uv-cache/`) to avoid permission issues when the default uv cache directory is not accessible. Updated on 2026-01-14.

## Archive

Archived on 2026-01-14 (kept for traceability; the information is still true but is now captured inline above):
> Previously:
> `task test` -> `uv run pytest`
>
> Rationale: pytest is still the underlying command, but the repo now configures default coverage and a minimum coverage threshold in `pyproject.toml`. Updated on 2026-01-13.
