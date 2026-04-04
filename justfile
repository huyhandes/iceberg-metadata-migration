# Iceberg Metadata Migration — Task Runner
# Install just: https://github.com/casey/just

set dotenv-load

# ---------------------------------------------------------------------------
# Local development
# ---------------------------------------------------------------------------

# Seed Iceberg table via Lakekeeper REST catalog
seed-rest:
    docker compose -f infra/docker-compose.yml --profile rest up -d
    uv run python infra/seed/seed_rest.py

# Seed Iceberg table via SQLite SQL catalog
seed-sql:
    docker compose -f infra/docker-compose.yml up -d minio minio-init
    uv run python infra/seed/seed_sql.py

# Seed Iceberg table via Hive Metastore catalog
seed-hms:
    docker compose -f infra/docker-compose.yml --profile hms up -d
    uv run python infra/seed/seed_hms.py

# Seed all catalog types
seed-all:
    docker compose -f infra/docker-compose.yml --profile rest --profile hms up -d
    uv run python infra/seed/seed_rest.py
    uv run python infra/seed/seed_sql.py
    uv run python infra/seed/seed_hms.py

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------

# Run unit tests (fast, no Docker needed)
test:
    uv run pytest tests/ -x --ignore=tests/integration

# Run REST catalog local tests
test-rest:
    uv run pytest -m rest -v

# Run SQL catalog local tests
test-sql:
    uv run pytest -m sql -v

# Run HMS catalog local tests
test-hms:
    uv run pytest -m hms -v

# Run all local catalog tests
test-local:
    uv run pytest -m "rest or sql or hms" -v

# Run full AWS integration tests (requires AWS credentials in .env)
test-integration:
    uv run pytest -m integration -v

# Run everything
test-all:
    uv run pytest -m "rest or sql or hms or integration" -v

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

# Start all Docker services
infra-up:
    docker compose -f infra/docker-compose.yml --profile rest --profile hms up -d

# Stop all Docker services and remove volumes
infra-down:
    docker compose -f infra/docker-compose.yml --profile rest --profile hms down -v

# Terraform init
tf-init:
    cd infra/terraform && terraform init -backend-config=backend.tfvars

# Terraform plan
tf-plan:
    cd infra/terraform && terraform plan

# Terraform apply
tf-apply:
    cd infra/terraform && terraform apply

# Terraform destroy
tf-destroy:
    cd infra/terraform && terraform destroy

# ---------------------------------------------------------------------------
# Code quality
# ---------------------------------------------------------------------------

# Run linter
lint:
    uv run ruff check .
    uv run ruff format --check .

# Run linter with auto-fix
lint-fix:
    uv run ruff check --fix .
    uv run ruff format .

# Run type checker
typecheck:
    uv run basedpyright src/

# Run all checks (lint + typecheck + unit tests)
check:
    uv run ruff check .
    uv run ruff format --check .
    uv run basedpyright src/
    uv run pytest tests/ -x --ignore=tests/integration
