import boto3
import pytest
from moto import mock_aws


@pytest.fixture
def s3_client():
    """Provide a mocked S3 client via moto. No real AWS credentials needed."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        yield client


@pytest.fixture
def aws_clients():
    """Provide mocked S3 and Glue clients with a pre-created testdb database."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        glue = boto3.client("glue", region_name="us-east-1")
        glue.create_database(DatabaseInput={"Name": "testdb"})
        yield s3, glue
