# Architecture

## Module Layout

```
src/iceberg_migrate/
├── cli.py                  # Typer CLI entry point — orchestrates the full pipeline
├── models.py               # Pydantic models: IcebergMetadataGraph, ManifestListFile, ManifestFile
├── s3.py                   # S3 utilities: URI parsing, object fetch
├── discovery/
│   ├── locator.py          # Find latest metadata.json in S3
│   └── reader.py           # Load metadata graph from S3 (metadata.json → Avro files)
├── rewrite/
│   ├── config.py           # RewriteConfig: source/destination prefix pair
│   ├── engine.py           # RewriteEngine: orchestrates rewriting + _migrated/ key remapping
│   ├── metadata_rewriter.py # Rewrite path fields in metadata.json dict
│   ├── avro_rewriter.py    # Rewrite paths in manifest list/manifest Avro records
│   └── graph_loader.py     # Load ALL snapshots' files for time-travel safe rewriting
├── validation/
│   └── validator.py        # Pre-write validation: structural, residual prefix scan, counts
├── writer/
│   └── s3_writer.py        # Bottom-up S3 writer (manifests → manifest lists → metadata)
├── catalog/
│   ├── base.py             # CatalogRegistrar protocol + CatalogConfig
│   ├── registry.py         # Catalog adapter registry (type string → adapter)
│   ├── glue_registrar.py   # AWS Glue adapter: register/update table
│   └── rest_registrar.py   # REST catalog adapter (Polaris, LakeKeeper, etc.)
└── output/
    └── formatter.py        # Human (rich) and JSON output formatting
```

## Data Flow

```
S3 (original metadata)
  │
  ▼
Discovery (locator.py → reader.py)
  │  Finds latest metadata.json, parses JSON, loads Avro manifest lists + manifests
  ▼
IcebergMetadataGraph (models.py)
  │  In-memory typed representation of the full metadata tree
  ▼
Graph Loader (graph_loader.py)
  │  Extends graph to include ALL snapshots' files (not just current)
  ▼
RewriteEngine (engine.py)
  │  1. Rewrites paths via metadata_rewriter.py + avro_rewriter.py
  │  2. Serializes to bytes (orjson for JSON, fastavro for Avro)
  │  3. Remaps S3 keys to _migrated/ paths
  ▼
RewriteResult
  │  Contains rewritten graph + serialized bytes at _migrated/ keys
  ▼
Validator (validator.py)
  │  Structural check, residual prefix scan, manifest count verification
  ▼
S3 Writer (s3_writer.py)
  │  Writes to _migrated/ keys in bottom-up order
  ▼
Catalog Registrar (glue_registrar.py or rest_registrar.py)
  │  Registers table pointing to _migrated/ metadata.json
  ▼
Output (formatter.py)
     Human-readable (rich) or JSON summary
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| fastavro over apache-avro | 8x faster, no IgnoredLogicalType bugs with Iceberg manifest schemas |
| Direct S3 writes over pyiceberg StaticTable | StaticTable is read-only; cannot write back rewritten metadata |
| Direct Glue API over pyiceberg GlueCatalog | Avoids s3fs/FileIO dependency for registration; sets same Iceberg Parameters |
| Non-destructive `_migrated/` writes | Original metadata may be overwritten by sync tools; migrated copy is independent |
| Bottom-up write order | If a write fails mid-way, lower-level files are consistent; metadata pointer updated last |
| All-snapshot rewriting | Time-travel safety: historical snapshots reference old paths too |
