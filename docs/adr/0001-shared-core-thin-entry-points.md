# Shared migration core with thin entry points

The six-step migration orchestration (discovery → rewrite → validate → write → register →
output) was embedded inside the Typer CLI command, coupled to process exit codes and `rich`
output. To add a Lambda handler without duplicating that wiring, we extracted the
orchestration into a single `run_migration(params) -> MigrationSummary` function (which
raises on failure) that both entry points call. The CLI maps the summary to exit codes and
`rich`; the Lambda handler maps the event to params and the summary to a JSON dict. The
pipeline modules (`discovery`, `rewrite`, `validation`, `writer`, `catalog`) are untouched.

## Considered Options

- **Extract `run_migration()`, both call it** (chosen) — one copy of the orchestration; CLI gets thinner.
- **Duplicate the orchestration in the handler** — zero change to `cli.py`, but two copies drift apart; the classic 3am-bug shape.
- **Invoke the Typer app in-process via argv** — reuses the CLI verbatim but is fragile (argv round-trip) and throws away the structured `MigrationSummary`.
