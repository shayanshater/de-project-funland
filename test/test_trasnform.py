from src.lambda_handler.transform import dim_currency
import pytest
import boto3
import awswrangler as wr
from datetime import datetime
import pandas as pd
from moto import mock_aws
import os


@pytest.fixture(scope='function')
def s3_client():
    s3_client = boto3.client("s3")
    yield s3_client

@pytest.fixture(autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@mock_aws
class TestDimCurrency:
 
    def test_parquet_file_uploads_to_processed_bucket(self,s3_client):

        s3_client.create_bucket(
        Bucket='ingestion-bucket-33-elisa-q',
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )
        s3_client.create_bucket(
        Bucket='processed-bucket-funlanf-e-l-3',
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )
         
        table="currency"
        
        #create a fake design csv file in ingestion bucket
        last_checked = "2000-01-01 00:00:00.000000"
        
        columns= ['currency_id', 'currency_code', 'created_at', 'last_updated']
        
        new_rows = [
            [0, 'GBP', datetime(2022, 11, 3, 14, 20, 51, 563000), datetime(2022, 11, 3, 14, 20, 51, 563000)]
        ]
        
        df = pd.DataFrame(new_rows, columns = columns)
        wr.s3.to_csv(df, f"s3://ingestion-bucket-33-elisa-q/{table}/{last_checked}.csv")
        
        
        ## run the function, which reads from the mocked ingestion bucket
        # and transforms the data (drops two columns) and adds a new column(currency_name) and uploads to processed bucket
        dim_currency(last_checked = last_checked,table= table, ingestion_bucket="ingestion-bucket-33-elisa-q", processed_bucket='processed-bucket-funlanf-e-l-3')
        
        #read the file from processed bucket
        
        df_result = wr.s3.read_parquet(f"s3://processed-bucket-funlanf-e-l-3/{table}/{last_checked}.parquet")
        #create an expected dataframe to match up against our uploaded file
        dim_columns = ['currency_id', 'currency_code', 'currency_name']
        dim_new_rows = [
            [0, 'GBP', 'GBP_Name']
        ]
        df_expected = pd.DataFrame(dim_new_rows, columns = dim_columns)
        
        #df_expected.astype({'design_id': 'int64'}).dtypes
        #assert that uploaded file matches our expected outlook

        assert list(df_expected.values[0]) == list(df_result.values[0])
            