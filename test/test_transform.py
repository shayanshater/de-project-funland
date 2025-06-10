
from src.lambda_handler.transform import (dim_design, check_file_exists_in_ingestion_bucket, dim_currency,
                                          check_file_exists_in_ingestion_bucket, dim_staff, dim_counterparty,
                                          dim_location, dim_date)

import pytest
import boto3
import awswrangler as wr
from datetime import datetime
import pandas as pd
from moto import mock_aws
import os
import numpy as np




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
           
        #create a fake design csv file in ingestion bucket
        last_checked = "2000-01-01 00:00:00.000000"
        
        columns= ['currency_id', 'currency_code', 'created_at', 'last_updated']
        
        new_rows = [
            [0, 'GBP', datetime(2022, 11, 3, 14, 20, 51, 563000), datetime(2022, 11, 3, 14, 20, 51, 563000)]
        ]
        
        df = pd.DataFrame(new_rows, columns = columns)
        wr.s3.to_csv(df, f"s3://ingestion-bucket-33-elisa-q/currency/{last_checked}.csv")
        
        
        ## run the function, which reads from the mocked ingestion bucket
        # and transforms the data (drops two columns) and adds a new column(currency_name) and uploads to processed bucket
        dim_currency(last_checked = last_checked, ingestion_bucket="ingestion-bucket-33-elisa-q", processed_bucket='processed-bucket-funlanf-e-l-3')
        
        #read the file from processed bucket
        
        df_result = wr.s3.read_parquet(f"s3://processed-bucket-funlanf-e-l-3/currency/{last_checked}.parquet")
        #create an expected dataframe to match up against our uploaded file
        dim_columns = ['currency_id', 'currency_code', 'currency_name']
        dim_new_rows = [
            [0, 'GBP', 'GBP_Name']
        ]
        df_expected = pd.DataFrame(dim_new_rows, columns = dim_columns)
        
        #df_expected.astype({'design_id': 'int64'}).dtypes
        #assert that uploaded file matches our expected outlook

        assert list(df_expected.values[0]) == list(df_result.values[0])
    
    
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
class TestDimStaffFunction:
    def test_dim_staff_function_creates_a_dim_staff_file_in_the_processed_bucket(self, s3_client):
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
        
        file_marker = "1995-01-01 00:00:00.000000"

        #create a fake staff csv file in ingestion bucket
        staff_columns = ['staff_id', 'first_name', 
                'last_name', 'department_id', 'email_address',
                'created_at', 'last_updated']
        
        staff_new_rows = [[1, 'Jeremie', 'Franey', 2, 'jeremie.franey@terrifictotes.com',
             datetime(2022, 11, 3, 14, 20, 51, 563000), datetime(2022, 11, 3, 14, 20, 51, 563000)]]
        
        staff_df = pd.DataFrame(staff_new_rows, columns = staff_columns)
        wr.s3.to_csv(staff_df, f"s3://ingestion-bucket-124-33/staff/{file_marker}.csv")

        #create a fake department csv file in ingestion bucket
        department_columns = ['department_id', 'department_name', 'location',
                        'manager', 'created_at', 'last_updated']
        
        department_new_rows = [[2, 'Purchasing', 'Manchester', 'Naomi Lapaglia',
                         datetime(2022, 11, 3, 14, 20, 49, 962000), datetime(2022, 11, 3, 14, 20, 49, 962000)]]
                               
        department_df = pd.DataFrame(department_new_rows, columns = department_columns)
        wr.s3.to_csv(department_df, f"s3://ingestion-bucket-124-33/department/{file_marker}.csv")


        ## run the function, which reads from the mocked ingestion bucket
        # and transforms the data (drops two columns) and uploads to processed bucket
        dim_staff(last_checked=file_marker, ingestion_bucket="ingestion-bucket-124-33", processed_bucket='processed-bucket-124-33')
        
        #read the file from processed bucket
        
        df_result = wr.s3.read_parquet(f"s3://processed-bucket-124-33/dim_staff/1995-01-01 00:00:00.000000.parquet")

        #create an expected dataframe to match up against our uploaded file
        dim_columns = ['staff_id', 'first_name', 'last_name',
                       'department_name', 'location', 'email_address']
        dim_new_rows = [[1, 'Jeremie', 'Franey', 'jeremie.franey@terrifictotes.com', 'Purchasing', 'Manchester']]

        df_expected = pd.DataFrame(dim_new_rows, columns = dim_columns)

        assert list(df_result.values[0]) == list(df_expected.values[0])

@mock_aws
class TestDimLocationFunction:
    def test_dim_location_file_lands_in_processed_bucket(self, s3_client):
         #make mocked bucket, ingestion and processed
        s3_client.create_bucket(
        Bucket='ingestion-bucket-555-22',
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )
        s3_client.create_bucket(
        Bucket='processed-bucket-555-22',
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )

        #create a fake locationn csv file in ingestion bucket
        file_marker = "1995-01-01 00:00:00.000000"
        columns = ["address_id", "address_line_1", 
                   "address_line_2", "city","country",
                   "created_at", "district", "last_updated",
                   "phone","postal_code"
                 ] 
                     
        new_rows = [[1, '6826 Herzog Via', None,'New Patienceburgh','Turkey', 
                     datetime(2022, 11, 3, 14, 20, 49, 962000),'Avon',
                     datetime(2022, 11, 3, 14, 20, 49, 962000),'1803 637401','28441']]

        df = pd.DataFrame(new_rows, columns = columns)
        wr.s3.to_csv(df, f"s3://ingestion-bucket-555-22/address/{file_marker}.csv")
    
        #run function 
        dim_location(last_checked=file_marker, ingestion_bucket='ingestion-bucket-555-22', processed_bucket='processed-bucket-555-22')

        #read file and get expected result 
        df_result = wr.s3.read_parquet(f"s3://processed-bucket-555-22/dim_location/1995-01-01 00:00:00.000000.parquet")
        df_result["location_id"] = df_result["location_id"].astype(int)
        for col in df_result.columns:
            if col != "location_id":
                df_result[col] = df_result[col].astype(str)


        # df_result = df_result.astype(str)
        dim_location_columns = ["location_id", "address_line_1","address_line_2", "district",
                       "city", "postal_code", "country", "phone"]
        
        dim_location_new_rows = [[1, '6826 Herzog Via', np.nan, 'Avon','New Patienceburgh', '28441', 'Turkey','1803 637401' ]]
        # df_result = df_result.astype(str)
       
        df_expected = pd.DataFrame(dim_location_new_rows, columns = dim_location_columns)
        # df_expected = df_expected.astype(str)
        df_expected["location_id"] = df_expected["location_id"].astype(int)
        for col in df_expected.columns:
            if col != "location_id":
                df_expected[col] = df_expected[col].astype(str)

        #assert that uploaded file matches our expected outlook
        assert list(df_result.values[0]) == list(df_expected.values[0])

    def test_dim_location_file_lands_in_processed_bucket_when_postal_code_has_dash(self, s3_client):
         #make mocked bucket, ingestion and processed
        s3_client.create_bucket(
        Bucket='ingestion-bucket-555-22',
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )
        s3_client.create_bucket(
        Bucket='processed-bucket-555-22',
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )

        #create a fake locationn csv file in ingestion bucket
        file_marker = "1995-01-01 00:00:00.000000"
        columns = ["address_id", "address_line_1", 
                   "address_line_2", "city","country",
                   "created_at", "district", "last_updated",
                   "phone","postal_code"
                 ] 
                     
        new_rows = [[
            2, '179 Alexie Cliffs', None, 'Aliso Viejo', 'San Marino',
            datetime(2022, 11, 3, 14, 20, 49, 962000), None,
            datetime(2022, 11, 3, 14, 20, 49, 962000), '9621 880720', '99305-7380'
        ]]

        df = pd.DataFrame(new_rows, columns = columns)
        wr.s3.to_csv(df, f"s3://ingestion-bucket-555-22/address/{file_marker}.csv")

        #run function 
        dim_location(last_checked=file_marker, ingestion_bucket='ingestion-bucket-555-22', processed_bucket='processed-bucket-555-22')

        #read file and get expected result 
        df_result = wr.s3.read_parquet(f"s3://processed-bucket-555-22/dim_location/1995-01-01 00:00:00.000000.parquet")
        df_result["location_id"] = df_result["location_id"].astype(int)
        for col in df_result.columns:
            if col != "location_id":
                df_result[col] = df_result[col].astype(str)


        # df_result = df_result.astype(str)
        dim_location_columns = ["location_id", "address_line_1","address_line_2", "district",
                       "city", "postal_code", "country", "phone"]
        
        dim_location_new_rows = [[2, '179 Alexie Cliffs', np.nan, np.nan, 'Aliso Viejo', '99305-7380', 'San Marino', '9621 880720']]
        # df_result = df_result.astype(str)

        df_expected = pd.DataFrame(dim_location_new_rows, columns = dim_location_columns)
        # df_expected = df_expected.astype(str)
        df_expected["location_id"] = df_expected["location_id"].astype(int)
        for col in df_expected.columns:
            if col != "location_id":
                df_expected[col] = df_expected[col].astype(str)

        #assert that uploaded file matches our expected outlook
        assert list(df_result.values[0]) == list(df_expected.values[0])

@mock_aws
class TestDimCounterpartyFunction:
    def test_dim_counterparty_file_lands_in_processed_bucket(self,s3_client):
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
        
        file_marker = "1995-01-01 00:00:00.000000"

        counterparty_columns=['commercial_contact', 'counterparty_id', 'counterparty_legal_name', 'created_at', 'delivery_contact', 'last_updated', 'legal_address_id']
        
        new_rows_counterparty=[['Micheal Toy', 1, 'Fahey and Sons', datetime(2022, 11, 3, 14, 20, 51, 563000), 'Mrs. Lucy Runolfsdottir', datetime(2022, 11, 3, 14, 20, 51, 563000), 15]]
        
        df_counterparty=pd.DataFrame(new_rows_counterparty,columns=counterparty_columns)
        wr.s3.to_csv(df_counterparty, f"s3://ingestion-bucket-124-33/counterparty/{file_marker}.csv" )
        
        address_columns=['address_id', 'address_line_1', 'address_line_2', 'city', 'country', 'created_at', 'district', 'last_updated', 'phone', 'postal_code'  ]
        new_rows_address=[[15, '605 Haskell Trafficway', 'Axel Freeway', 'East Bobbie', 'Heard Island and McDonald Islands', datetime(2022, 11, 3, 14, 20, 49, 962000), None, datetime(2022, 11, 3, 14, 20, 49, 962000), '9687 937447', '88253-4257']]
        df_address=pd.DataFrame(new_rows_address,columns=address_columns)
        wr.s3.to_csv(df_address, f"s3://ingestion-bucket-124-33/address/{file_marker}.csv")
        
        dim_counterparty(file_marker, "ingestion-bucket-124-33", 'processed-bucket-124-33', s3_client)
        
        #read the file from processed bucket
        df_result = wr.s3.read_parquet(f"s3://processed-bucket-124-33/dim_counterparty/1995-01-01 00:00:00.000000.parquet")
        df_result = df_result.replace(np.nan, None)
        
        dim_counterparty_columns=[
        'counterparty_id', 
        'counterparty_legal_name', 
        'counterparty_legal_address_line_1', 
        'counterparty_legal_address_line_2',
        'counterparty_legal_district',
        'counterparty_legal_city', 
        'counterparty_legal_postal_code', 
        'counterparty_legal_country', 
        'counterparty_legal_phone_number' 
        ]
  
        dim_counterparty_new_rows=[[1, 'Fahey and Sons', 
                                    '605 Haskell Trafficway', 'Axel Freeway', 
                                    'East Bobbie', 'Heard Island and McDonald Islands', 
                                    None, '9687 937447', '88253-4257']]

        df_expected = pd.DataFrame(dim_counterparty_new_rows, columns = dim_counterparty_columns)



        assert list(df_result.values[0]) == list(df_expected.values[0])
        assert set(df_result.columns) == set(df_expected.columns)


class TestDimDateFunction:
    def test_dim_date_return_correct_date(self):
        #assign
        start = '2022-11-03'
        end = '2022-11-05'

        #act
        result = dim_date(start, end)
        print(result)

        #assert
        assert result.iloc[0]['year'] == 2022
        assert result.iloc[0]['month'] == 11
        assert result.iloc[0]['day'] == 3
        assert result.iloc[0]['day_of_week']  == 3
        assert result.iloc[0]['day_name'] == 'Thursday'
        assert result.iloc[0]['month_name'] == 'November'
        assert result.iloc[0]['quarter'] == 4



@mock_aws
class TestFactSalesOrderFunction:
    pass


@mock_aws          
class TestCheckFileExistsInBucket: 
    def test_logs_error_if_ingestion_bucket_does_not_exist(self, s3_client): 
        assert check_file_exists_in_ingestion_bucket(bucket="wrong", filename="nothing") == False
     
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

        assert check_file_exists_in_ingestion_bucket(bucket='ingestion-bucket-124-33', filename=f"design/{file_marker}.csv") == False

       

       
