# Lambda container image for iceberg-migrate (ADR-0003).
# x86_64 only. On Apple Silicon pass:  docker build --platform linux/amd64 .

# ---------- stage 1: resolve & install deps from uv.lock ----------
# `uv export` runs inside the build, so no committed requirements.txt and the
# build is reproducible against uv.lock. `pyiceberg[glue]` extras are preserved
# by the export flags.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS deps

WORKDIR /tmp
COPY pyproject.toml uv.lock ./

# Export pinned requirements honoring the lockfile, then install them.
RUN uv export --frozen --no-dev --no-editable --no-emit-project -o requirements.txt \
    && uv pip install --system -r requirements.txt

# ---------- stage 2: runtime image ----------
FROM public.ecr.aws/lambda/python:3.12

# site-packages installed in stage 1 (the AWS base has no compiler; we borrow
# the resolved wheels rather than rebuilding against the base image's pip).
COPY --from=deps /usr/local/lib/python3.12/site-packages /var/lang/lib/python3.12/site-packages

# AWS base sets LAMBDA_TASK_ROOT=/var/task and WORKDIR /var/task already; copy
# the package so it imports as `iceberg_migrate` from /var/task.
COPY src/iceberg_migrate /var/task/iceberg_migrate

CMD ["iceberg_migrate.lambda_handler.handler"]
