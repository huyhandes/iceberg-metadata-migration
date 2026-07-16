# Iceberg Metadata Migration

The language of rewriting stale paths in Apache Iceberg (v2/v3) table metadata after
the data has landed on AWS S3, and registering the result in AWS Glue so query engines
can read it. The tool is source-agnostic and non-destructive.

## Language

**Migration**:
Rewriting the stale paths embedded in one Iceberg table's metadata to match its new S3
location, then registering that table in Glue. The whole point of the tool.
_Avoid_: conversion, port, import.

**Source prefix**:
The stale path prefix baked into the original metadata by the system that wrote it — any
scheme (`s3a://`, `hdfs://`, `/mnt/...`).
_Avoid_: old path, origin, from-path.

**Destination prefix**:
The S3 prefix the data now lives under, which the metadata must be rewritten to point at.
_Avoid_: target, new path, to-path.

**Metadata graph**:
The connected set of metadata for one table — `metadata.json` plus its manifest lists and
manifests — loaded and reasoned about together as a unit.
_Avoid_: metadata tree, file set.

**Rewrite**:
Substituting the source prefix with the destination prefix across the metadata graph, in
memory, before anything is written.
_Avoid_: replace, patch, fix-up.

**Migrated metadata**:
The rewritten metadata, written non-destructively under a `_migrated/` subdirectory of the
table location. Original files are never modified.
_Avoid_: output metadata, new metadata.

**Registration**:
Recording the migrated table in the AWS Glue Catalog so engines (Athena, Glue ETL, EMR)
can query it at its new location.
_Avoid_: publish, catalog-ing.

**Invocation**:
One migration run over a single table, triggered by a caller. The unit of retry.
_Avoid_: job, execution, run.

**Entry point**:
A caller-facing adapter that maps its input to a single migration and formats the result —
today the Typer CLI, and (newly) the Lambda handler. The migration logic itself is shared
between them, not owned by either.
_Avoid_: interface, frontend, driver.
