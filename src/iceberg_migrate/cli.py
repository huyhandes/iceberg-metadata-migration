"""CLI entry point for iceberg-migrate."""
import typer

app = typer.Typer(
    name="iceberg-migrate",
    help="Rewrite Iceberg metadata paths after DataSync migration.",
)

@app.command()
def migrate(
    table_location: str = typer.Option(
        ..., "--table-location", help="S3 URI of the Iceberg table root (e.g., s3://bucket/warehouse/db/table)"
    ),
    source_prefix: str = typer.Option(
        ..., "--source-prefix", help="Original path prefix to replace (e.g., s3a://minio-bucket/warehouse)"
    ),
    dest_prefix: str = typer.Option(
        ..., "--dest-prefix", help="New path prefix (e.g., s3://aws-bucket/warehouse)"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show planned changes without writing"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", help="Print per-file debug output"
    ),
) -> None:
    """Rewrite Iceberg metadata paths from source prefix to destination prefix."""
    typer.echo(f"Table location: {table_location}")
    typer.echo("Not yet implemented — Phase 2+3 will add rewriting and writing.")
    raise typer.Exit(code=0)

if __name__ == "__main__":
    app()
