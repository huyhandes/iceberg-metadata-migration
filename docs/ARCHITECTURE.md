# Architecture

## Module Layout

```
src/iceberg_migrate/
├── cli.py                  # Typer CLI entry point — orchestrates the full pipeline
├── models.py               # Pydantic models: IcebergMetadataGraph, ManifestListFile, ManifestFile
├── s3.py                   # S3 utilities: URI parsing, object fetch
├── discovery/
│   ├── compression.py      # Extension-based decompression + suffix stripping
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
Discovery (locator.py → compression.py → reader.py)
  │  Finds latest metadata.json, decompresses if needed, parses JSON
  │  Loads Avro manifest lists + manifests; captures codec from each Avro header
  ▼
IcebergMetadataGraph (models.py)
  │  In-memory typed representation of the full metadata tree
  │  Each ManifestListFile/ManifestFile carries avro_schema + codec
  ▼
Graph Loader (graph_loader.py)
  │  Extends graph to include ALL snapshots' files (not just current)
  ▼
RewriteEngine (engine.py)
  │  0. Format-version gate: rejects format-version >= 4
  │  1. Rewrites paths via metadata_rewriter.py + avro_rewriter.py
  │  2. Serializes to bytes (orjson for JSON, fastavro for Avro)
  │  3. Preserves Avro codec per file on serialization
  │  4. Remaps S3 keys to _migrated/ paths (strips compression suffix)
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
| Extension-based decompression dispatch | Lakekeeper writes `.gz.metadata.json` (hardcoded gzip). Dispatch table in `compression.py` uses filename extension to select decompressor. Stdlib gzip is sufficient — no cramjam or third-party compression library needed. |
| Avro codec preservation on round-trip | Codec read from Avro container header during discovery, stored on model, passed back to `fastavro.writer()`. Prevents silently changing deflate to null or vice versa. |
| Format-version gate (v1/v2/v3 only) | `RewriteEngine` rejects format-version >= 4 before any rewriting. Prevents silent data corruption on unknown metadata structures. |
| Strip .gz extension from migrated metadata keys | Athena uses file extension to determine encoding. A `.gz.metadata.json` key containing plain JSON triggers `GENERIC_INTERNAL_ERROR`. Migrated copies always use plain `.metadata.json` extension. |

## Lessons Learned

These were discovered during integration testing against real catalog-produced metadata.

**Gzip discovery**
Lakekeeper metadata.json is gzip-compressed with a `.gz.metadata.json` extension. Initial integration tests failed with JSON parse errors until decompression was added. The fix was `compression.py` with a dispatch table keyed on filename extension.

**Athena .gz extension**
Writing plain JSON to a `.gz.metadata.json` S3 key causes Athena `GENERIC_INTERNAL_ERROR`. Athena infers encoding from the file extension, not file magic bytes. The fix: `strip_compression_suffix()` in `engine.py` removes compression extensions from output keys so migrated files always land at plain `.metadata.json`.

**Avro codec mismatch**
Reading deflate-compressed Avro (typical from Java catalogs) and writing back with null codec produces valid Avro but breaks query engines that expected the original codec. Fix: capture codec from `avro.codec` header in `reader.py`, store it on the `ManifestListFile`/`ManifestFile` model, and pass it through to `fastavro.writer()` in `engine.py`.

**referenced_data_file (v3)**
Position and equality delete manifests use `data_file.referenced_data_file` (field id 143) to point back to the data file being deleted. If this field is not rewritten, the delete references break after migration — the delete file points to a rewritten data path but the reference is still on the old prefix. The fix: `avro_rewriter.py` rewrites `data_file.referenced_data_file` alongside `data_file.file_path`.

**No cramjam needed**
Stdlib `gzip` handles all observed compression formats. The extension-based dispatch table in `compression.py` is designed to add future codecs (zstd, snappy) as needed, but none have been required yet.
