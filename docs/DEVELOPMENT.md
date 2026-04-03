# Development

## Prerequisites

- Python >= 3.10 (3.12 recommended)
- [uv](https://docs.astral.sh/uv/) — package manager
- [act](https://nektosact.com/) — local CI runner (optional, for pre-commit CI)
- Docker (for act and local MinIO)

## Setup

```bash
# Clone and install
git clone <repo-url>
cd iceberg-metadata-migration
uv sync --dev

# Install pre-commit hooks
uv run prek install
```

## Daily Workflow

### Run tests
```bash
uv run pytest tests/ -x
```

### Lint and format
```bash
uv run ruff check .         # lint
uv run ruff check --fix .   # lint + auto-fix
uv run ruff format .        # format
uv run ruff format --check . # format check (CI mode)
```

### Type check
```bash
uv run basedpyright src/
```

### Run full CI locally
```bash
act -j check
```

## Pre-commit Hooks (prek)

Configured in `prek.toml`. Runs on every commit:

1. **ruff** — auto-fix lint issues + format
2. **basedpyright** — type checking
3. **pytest** — fail-fast test run

Install hooks:
```bash
uv run prek install
```

## CI Pipeline

GitHub Actions workflow in `.github/workflows/ci.yml`. Runs on push to `master` and on PRs.

Same checks as pre-commit: ruff → basedpyright → pytest.

To run locally via act:
```bash
act -j check
```

## Claude Code Hooks

Configured in `.claude/settings.json`:

- **PreCommit** — runs `act` before commits (full CI)
- **PostTaskComplete** — runs `basedpyright` after agent edits

## Adding Dependencies

```bash
# Runtime dependency
uv add <package>

# Dev dependency
uv add --group dev <package>
```

## Project Structure

See [ARCHITECTURE.md](ARCHITECTURE.md) for module layout and design decisions.
