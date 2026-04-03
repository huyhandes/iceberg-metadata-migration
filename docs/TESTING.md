# Testing

## Running Tests

```bash
# Full suite
uv run pytest tests/ -x

# Specific module
uv run pytest tests/test_rewrite/ -v

# Single test
uv run pytest tests/test_rewrite/test_engine.py::test_rewrite_engine_rewrites_location -v

# With coverage
uv run pytest tests/ --cov=iceberg_migrate --cov-report=term-missing
```

## Test Strategy

### Unit Tests

Each module has a corresponding test file:

| Module | Test File |
|--------|-----------|
| `discovery/locator.py` | `tests/test_discovery/test_locator.py` |
| `discovery/reader.py` | `tests/test_discovery/test_reader.py` |
| `rewrite/config.py` | `tests/test_rewrite/test_config.py` |
| `rewrite/metadata_rewriter.py` | `tests/test_rewrite/test_metadata_rewriter.py` |
| `rewrite/avro_rewriter.py` | `tests/test_rewrite/test_avro_rewriter.py` |
| `rewrite/graph_loader.py` | `tests/test_rewrite/test_graph_loader.py` |
| `rewrite/engine.py` | `tests/test_rewrite/test_engine.py` |
| `validation/validator.py` | `tests/test_validation/test_validator.py` |
| `writer/s3_writer.py` | `tests/test_writer/test_s3_writer.py` |
| `catalog/glue_registrar.py` | `tests/test_catalog/test_glue_registrar.py` |

### Integration Tests

- `tests/test_rewrite/test_integration.py` — end-to-end rewrite pipeline
- `tests/test_integration/test_end_to_end.py` — full CLI pipeline with mocked AWS

### v3-Specific Tests

- `tests/test_rewrite/test_avro_rewriter_v3.py` — deletion vector path rewriting
- `tests/fixtures/v3_manifest_with_dv.py` — v3 manifest fixture with deletion vectors

## Mocking Approach

### AWS Services (moto)

All AWS calls are mocked via `moto`:

```python
from moto import mock_aws

@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        yield client
```

Shared fixtures in `tests/conftest.py`:
- `s3_client` — mocked S3 client
- `aws_clients` — mocked S3 + Glue clients with pre-created `testdb` database

### Avro Fixtures

Tests create Avro data in-memory using fastavro:

```python
def make_manifest_avro(file_paths: list[str]) -> bytes:
    schema = {...}
    records = [{"data_file": {"file_path": p}} for p in file_paths]
    buf = io.BytesIO()
    fastavro.writer(buf, schema, records)
    return buf.getvalue()
```

## What Tests Verify

1. **Path rewriting correctness** — every path-bearing field is rewritten
2. **Avro round-trip** — serialize → deserialize preserves schema and data
3. **Non-destructive writes** — output goes to `_migrated/` keys, originals untouched
4. **Validation gate** — rewritten files have zero residual source prefixes
5. **Count preservation** — manifest counts match before/after rewrite
6. **Idempotent registration** — Glue create vs. update behavior
7. **Error propagation** — S3/Glue failures produce correct exit codes
