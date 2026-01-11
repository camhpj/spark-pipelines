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

#### 3. Quint (Structured AI Reasoning)

Here is **Section 3 rewritten to fit cleanly into your document**, aligned in tone, depth, and structure with the surrounding sections. This is **only section 3**, ready to paste in place of the TODO.

---

#### 3. Quint (Structured AI Reasoning)

This project uses **Quint Code** to formalize AI-assisted development with a durable, auditable reasoning process. Quint implements an **ADI cycle** (Abduction → Deduction → Induction → Audit → Decision) and persists reasoning artifacts in a project-local knowledge base that is intended to be committed to the repository.

Quint is used to capture:

* architectural and design decisions
* tradeoff analysis
* testing strategy rationale
* justification for non-obvious implementation choices

Its purpose is to ensure important context and reasoning are not lost in chat history or commit messages.

---

##### Install Quint

Install the Quint CLI using the official install script:

```bash
curl -fsSL https://raw.githubusercontent.com/m0n0x41d/quint-code/main/install.sh | bash
```

Restart your shell if needed, then verify:

```bash
quint-code --help
```

---

##### Initialize Quint for the Project

From the **root of the repository**, run:

```bash
quint-code init
```

This will create:

* `.quint/` — project knowledge base (hypotheses, evidence, audits, decisions)
* `.mcp.json` — Model Context Protocol (MCP) configuration
* MCP command registrations for the active AI client

The `.quint/` directory **is expected to be committed to the repository**. It is part of the project’s durable engineering record.

To scope Quint configuration strictly to this repository (instead of installing global MCP commands):

```bash
quint-code init --local
```

---

##### MCP Configuration (Example with Context7)

Quint integrates with AI tools via the **Model Context Protocol (MCP)**. A typical `.mcp.json` configuration looks like:

```json
{
  "servers": {
    "quint": {
      "command": "quint-code",
      "args": ["serve"]
    },
    "context7": {
      "command": "npx",
      "args": ["-y", "@upstash/context7-mcp"]
    }
  }
}
```

In this setup:

* **quint** exposes structured reasoning commands (e.g. `/q1-hypothesize`, `/q5-decide`)
* **context7** provides up-to-date library and framework documentation context during reasoning

After modifying `.mcp.json`, restart your AI client to reload MCP servers.

---

##### Verify Quint Is Working

Inside your AI client, initialize the reasoning cycle:

```
/q0-init
```

Then generate example hypotheses:

```
/q1-hypothesize "Evaluate the best approach for <describe your problem>"
```

Artifacts should appear under `.quint/`. You can inspect the current state with:

```
/q-status
```

---

##### When to Use Quint

Quint should be used for **non-trivial decisions**, including:

* architectural changes
* interface or schema changes
* performance-sensitive implementations
* testing strategy decisions (e.g. mocking vs containers)
* risky refactors or unclear requirements

The expected cadence for meaningful work is:

1. Reason with Quint
2. Implement code and tests
3. Commit code **and** Quint artifacts together
4. Push to remote

Quint artifacts should be treated as **engineering records**, not temporary scratch files.

---

#### 4. Docker (optional, for dockerized projects)

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