from src.lambda_handler.transform import dim_currency
import pytest
import boto3
import awswrangler as wr
from datetime import datetime
import pandas as pd
from moto import mock_aws


@pytest.fixture(scope='function')
def s3_client():
    s3_client = boto3.client("s3")
    yield s3_client


@mock_aws
class TestDimCurrency:
    def parquet_file_uploads_to_processed_bucket(self,s3_client):

        s3_client.create_bucket(
        Bucket='ingestion-bucket-funland-e-l',
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )
        s3_client.create_bucket(
        Bucket='processed-bucket-funlanf-e-l',
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )