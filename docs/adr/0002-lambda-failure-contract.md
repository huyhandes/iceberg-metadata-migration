# Lambda failure contract: raise, don't return a status

On Lambda a successful migration returns the JSON summary (the same shape `render_json`
produces); any **partial or fatal** failure **raises**, so Lambda reports a `FunctionError`.
The caller is Airflow-on-EKS invoking synchronously over a PrivateLink endpoint; its Lambda
operator treats a `FunctionError` as a failed task and retries. Because migration is
non-destructive and idempotent (writes land under `_migrated/`), an automatic retry is safe.

This deliberately collapses the CLI's three-way exit codes (0 success / 1 partial / 2 fatal)
into two outcomes on Lambda (return vs. raise), because an orchestrator only needs
task-succeeded vs. task-failed.

## Considered Options

- **Return summary on success, raise on any failure** (chosen).
- **Always return a `{status: ...}` dict (never raise)** — hides failures from Lambda metrics/alarms and forces the DAG to branch on payload contents.
- **Return on success/partial, raise only on fatal** — more faithful to the exit codes but forces Airflow to handle two failure channels.

## Consequences

Partial writes are not rolled back; a retry re-runs the whole migration from the start,
which is safe precisely because writes are idempotent and non-destructive.
