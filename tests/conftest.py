import boto3
import pytest
from moto import mock_aws


@pytest.fixture
def s3_client():
    """Provide a mocked S3 client via moto. No real AWS credentials needed."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        yield client
