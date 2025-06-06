from src.lambda_handler.transform import dim_design, check_file_exists_in_ingestion_bucket
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
class TestDimDesignFunction:
    def test_new_file_lands_in_processed_bucket(self, s3_client):
        
        #make mocked bucket, ingestion and processed
        s3_client.create_bucket(
        Bucket='ingestion-bucket-124-33',
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )
        s3_client.create_bucket(
        Bucket='processed-bucket-124-33',
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )
        
        #create a fake design csv file in ingestion bucket
        file_marker = "1995-01-01 00:00:00.000000"
        columns = ['design_id', 'created_at', 
                'last_updated', 'design_name', 
                'file_location', 'file_name']
        
        new_rows = [
            [0,datetime(2022, 11, 3, 14, 20, 49, 962000), datetime(2022, 11, 3, 14, 20, 49, 962000),'Wooden', '/usr', 'wooden-20220717-npgz.json' ]
        ]
        
        df = pd.DataFrame(new_rows, columns = columns)
        wr.s3.to_csv(df, f"s3://ingestion-bucket-124-33/design/{file_marker}.csv")
        
        
        ## run the function, which reads from the mocked ingestion bucket
        # and transforms the data (drops two columns) and uploads to processed bucket
        dim_design(last_checked=file_marker, ingestion_bucket="ingestion-bucket-124-33", processed_bucket='processed-bucket-124-33')
        
        #read the file from processed bucket
        
        df_result = wr.s3.read_parquet(f"s3://processed-bucket-124-33/dim_design/1995-01-01 00:00:00.000000.parquet")
        #create an expected dataframe to match up against our uploaded file
        dim_columns = ['design_id', 'design_name', 
                'file_location', 'file_name']
        dim_new_rows = [
            [0, 'Wooden', '/usr', 'wooden-20220717-npgz.json' ]
        ]
        df_expected = pd.DataFrame(dim_new_rows, columns = dim_columns)
        

        #assert that uploaded file matches our expected outlook
        assert list(df_result.values[0]) == list(df_expected.values[0])
        # assert len(df_expected.values[0]) == len(df_result.values[0])
        # assert all([a == b for a, b in zip(df_result.values[0], df_expected.values[0])])

    #def test_logs_error_if_wrong_last_checked_arg_passed(self, s3_client):

@mock_aws          
class TestCheckFileExistsInBucket: 
    def test_logs_error_if_ingestion_bucket_does_not_exist(self, s3_client): 
        assert check_file_exists_in_ingestion_bucket(bucket="wrong", key="nothing") == False
     
    def test_logs_error_if_no_file_ingested(self, s3_client): 
        #mock bucket 
        s3_client.create_bucket(
        Bucket='ingestion-bucket-124-33',
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )

        #create a fake design csv file in ingestion bucket
        file_marker = "1995-01-01 00:00:00.000000"
        columns = ['design_id', 'created_at', 
                'last_updated', 'design_name', 
                'file_location', 'file_name']
        
        new_rows = [
            [0,datetime(2022, 11, 3, 14, 20, 49, 962000), datetime(2022, 11, 3, 14, 20, 49, 962000),'Wooden', '/usr', 'wooden-20220717-npgz.json' ]
        ]
        
        df = pd.DataFrame(new_rows, columns = columns)
        #file not added to s3 bucket 

        assert check_file_exists_in_ingestion_bucket(bucket='ingestion-bucket-124-33', key=f"design/{file_marker}.csv") == False

       

       


    