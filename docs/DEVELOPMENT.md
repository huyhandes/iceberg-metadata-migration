# Development

## Prerequisites

- Python >= 3.10 (3.12 recommended)
- [uv](https://docs.astral.sh/uv/) — package manager
- [just](https://github.com/casey/just) — task runner
- Docker (for local catalog tests and MinIO)
- [act](https://nektosact.com/) — local CI runner (optional)

### For integration tests (optional)

- AWS CLI configured (profile set in `.env`)
- Terraform >= 1.0

## Setup

```bash
# Clone and install
git clone <repo-url>
cd iceberg-metadata-migration
uv sync --dev

# Install pre-commit hooks
uv run prek install

# Install just (macOS)
brew install just
```

## Quick Reference

```bash
just --list            # Show all available commands
just test              # Unit tests
just lint              # Lint check
just typecheck         # Type check
just check             # All checks (lint + types + tests)
```

## Local Catalog Testing

Start Docker services and seed test data:

```bash
# All catalogs at once
just infra-up && just seed-all

# Or individually
just seed-rest          # Lakekeeper REST catalog
just seed-sql           # SQLite SQL catalog (MinIO only)
just seed-hms           # Hive Metastore

# Run tests
just test-local         # All catalog tests
just test-rest          # Just REST
```

See [INFRASTRUCTURE.md](INFRASTRUCTURE.md) for Docker compose details.

## Integration Testing (AWS)

Requires AWS credentials and Terraform:

```bash
# One-time: provision infra
just tf-init
just tf-apply

# Run integration tests
just test-integration   # All engines (Athena + Glue ETL + EMR Serverless)
just test-athena        # Athena only
just test-glue          # Glue ETL only
just test-emr           # EMR Serverless only
```

Additional `.env` vars required for multi-engine tests: `EMR_APPLICATION_ID`, `EMR_JOB_ROLE_ARN`, `GLUE_JOB_NAME`.

See [TESTING.md](TESTING.md) for full test strategy.

## Daily Workflow

### Run tests
```bash
just test               # Unit tests (fast)
just check              # Full check: lint + types + tests
```

### Lint and format
```bash
just lint               # Check
just lint-fix           # Auto-fix
```

### Type check
```bash
just typecheck
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

Same checks as pre-commit: ruff -> basedpyright -> pytest.

## Adding Dependencies

```bash
# Runtime dependency
uv add <package>

# Dev dependency
uv add --group dev <package>
```

## Project Structure

See [ARCHITECTURE.md](ARCHITECTURE.md) for module layout and design decisions.
