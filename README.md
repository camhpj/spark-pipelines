## Development Setup

This project uses **`uv`** for Python dependency management, **`Task`** for common dev commands, and **Docker** for containerized workflows (where applicable).

### Prerequisites

Make sure the following tools are installed on your system:

#### 1. `uv` (Python package & environment manager)

Install `uv` following the official instructions:

* macOS / Linux:

  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
* Windows (PowerShell):

  ```powershell
  irm https://astral.sh/uv/install.ps1 | iex
  ```

Verify:

```bash
uv --version
```

---

#### 2. Task (Taskfile runner)

This project standardizes developer and CI commands via **Taskfile**.

Install Task:

* macOS:

  ```bash
  brew install go-task/tap/go-task
  ```
* Linux:

  ```bash
  sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b ~/.local/bin
  ```
* Windows:

  ```powershell
  choco install go-task
  ```

Verify:

```bash
task --version
```

---

#### 3. Docker (optional, for dockerized projects)

If you are working on a dockerized variant of this project, install:

* **Docker Desktop** (macOS / Windows)
* **Docker Engine + Compose plugin** (Linux)

Verify:

```bash
docker --version
docker compose version
```

---

## Initial Project Setup

Once prerequisites are installed:

```bash
task install
```

This will:

* Create / sync the virtual environment
* Install all Python dependencies via `uv`

---

## Common Development Commands

```bash
task dev          # format, lint, and test
task lint         # ruff checks
task fmt          # apply formatting
task test         # run pytest
task ci           # CI-equivalent checks (no formatting writes)
```

To see all available tasks:

```bash
task --list
```

---

## Versioning w/ Commitizen
This project is versioned using commitizen with conventional commits.

Basic usage:
```bash
# get current project version
task version  

# commit changes w/ conventional commit
git add -A
git commit -m "feat: cool new feature"

# bump project version
task version:bump

# push w/ tags
git push origin HEAD --follow-tags
```

## Docker Workflow (if applicable)

```bash
task docker:up          # start containers using existing images
task docker:up:build    # rebuild images, then start containers
task docker:down        # stop containers
task docker:down:v      # stop containers and remove volumes (destructive)
task docker:logs        # tail container logs
```

---

## VS Code Setup (Recommended)

### Install Extensions

* **`ty`** (Python language server / tooling)
* **Python** (Microsoft) — optional, but common for debugging and tooling

### Disable the Built-in Python Language Server

To avoid running **two Python language servers simultaneously**, add the following to your VS Code `settings.json`:

```json
{
  "python.languageServer": "None"
}
```

This ensures `ty` is the sole active language server.

---

## Notes & Conventions

* Formatting and linting are handled **exclusively by Ruff**
* Test configuration (pytest flags, markers, etc.) lives in `pyproject.toml`
* Dependency changes should be committed via `uv` lock updates
* Docker volumes are **not** removed by default—use `docker:down:v` explicitly if needed