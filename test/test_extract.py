
from src.lambda_handler.extract import get_db_credentials, get_last_checked, create_db_connection, update_last_checked, extract_new_rows, convert_new_rows_to_df_and_upload_to_s3_as_csv,get_bucket_name

import pytest
from pg8000.native import DatabaseError, InterfaceError, Connection
from moto import mock_aws
import boto3
from botocore.config import Config
from datetime import datetime
import json
import os
from botocore.exceptions import ClientError
import os
from dotenv import load_dotenv
import awswrangler as wr
import pandas as pd
from awswrangler import exceptions

load_dotenv()


@pytest.fixture(scope="module")
def tables_to_import():
    '''Runs seed before starting tests, yields, runs tests,
       then closes connection to db'''
    return ["counterparty", "currency", 
                    "department", "design", "staff", "sales_order",
                    "address", "payment", "purchase_order", 
                    "payment_type", "transaction"]
    
    
    
@pytest.fixture(scope='function')
def ssm_client():
    my_config = Config(
        region_name = 'eu-west-2'
    )
    return boto3.client('ssm', config = my_config)

@pytest.fixture(scope='function')
def sm_client():
    my_config = Config(
    region_name = 'eu-west-2'
        )
    return boto3.client('secretsmanager', config = my_config)

@pytest.fixture(scope='function')
def db_conn():
    
    conn = Connection(
        user = os.getenv("totesys_user"),
        password = os.getenv("totesys_password"),
        database = os.getenv("totesys_database"),
        host = os.getenv("totesys_host"),
        port = os.getenv("totesys_port")
    )
    return conn

@pytest.fixture(scope='function')

def s3_client():
    my_config = Config(
    region_name = 'eu-west-2'
    )
    return boto3.client('s3', config = my_config)



@mock_aws
class TestGetLastChecked:
    def test_get_last_checked_obtains_the_correct_date(self, ssm_client):
        new_date = str(datetime.now())
        ssm_client.put_parameter(
            Name = 'last_checked',
            Value = new_date,
            Type = 'String'
        )
        
        last_checked = get_last_checked(ssm_client)
        assert last_checked["last_checked"] == new_date
    
    def test_get_last_checked_returns_error_if_no_datetime_available(self, ssm_client):
        with pytest.raises(ssm_client.exceptions.ParameterNotFound):
            get_last_checked(ssm_client)

        
@mock_aws
class TestGetDBCredentials():
    def test_get_db_credentials_fetches_the_correct_username(self, sm_client):
        #assign
        secret_dict = {
            "DB_USER":"user",
            "DB_PASSWORD":"password",
            "DB_HOST":"host",
            "DB_PORT":"5432",
            "DB_NAME":"test_db"
            }

        secret_string = json.dumps(secret_dict)
                
        sm_client.create_secret(
            Name = "warehouse_totesys_credentials",
            SecretString = secret_string
            )
        
        #action
        result = get_db_credentials(sm_client)

        #assert
        assert result == secret_dict

    
    def test_get_db_credentials_returns_error_if_no_credentials_available(self, sm_client):
        with pytest.raises(sm_client.exceptions.ResourceNotFoundException):
            get_db_credentials(sm_client)


# Test for db_connection

class TestDBConnection:
    @pytest.mark.skip("The remote database is not accessible, change the remote database details and remove this skip condition")
    def test_create_db_connection_creates_a_connection(self, sm_client):
        #assign
        db_credentials = get_db_credentials(sm_client)

        #action
        result = create_db_connection(db_credentials)

        #assert
        assert result


    def test_create_db_connection_raise_error_when_wrong_credentials_supplied(self, sm_client):
        #assign
        db_credentials = {
            "DB_USER": "Funland",
            "DB_PASSWORD": "Fun_password",
            "DB_NAME": "Fun_name",
            "DB_HOST": "Fun_NC",
            "DB_PORT": 5432
        }

        #action

        #assert
        with pytest.raises(InterfaceError):
            create_db_connection(db_credentials)


# @mock_aws
# class TestGetDataFromDB:
#     @pytest.mark.skip()
#     def test_all_tables_exist(self, conn):
#         tables_to_check = ["counterparty", "currency", 
#                     "department", "design", "staff", "sales_order",
#                     "address", "payment", "purchase_order", 
#                     "payment_type", "transaction"]
        
        
#         for table_name in tables_to_check:
#             base_query = f"""SELECT EXISTS (SELECT FROM information_schema.tables \
#                         WHERE table_name = '{table_name}')"""
#             expect = conn.run(base_query)
#             assert expect == [[True]]

    

class TestGetBucketName:
    def test_get_bucket_name_gets_the_correct_bucket_name_which_is_dynamic(self):
        #assign
        os.environ["S3_INGESTION_BUCKET"] = "funland-ingestion-bucket-11"
    
        #action
        result = get_bucket_name()
        expected = {"ingestion_bucket": "funland-ingestion-bucket-11"}

        #assert
        assert result == expected
        del os.environ["S3_INGESTION_BUCKET"] 

    def test_get_bucket_name_raises_error_if_bucket_not_found(self):
        #assign
        #env variable deleted above       
        #action
        result = get_bucket_name()
        expected = {"ingestion_bucket": 'None'}
        #assert
        assert result == expected

    @mock_aws
    def test_env_var_matches_bucket_name(self, s3_client): 
        #assign
        #create mock s3 bucket
        #create fake environment variable 
        s3_client.create_bucket(Bucket="funland-ingestion-bucket-11", CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        os.environ["S3_INGESTION_BUCKET"] = "funland-ingestion-bucket-11"
        
        #Action
        #result = call the function 
        #expected = list of bucket names 
        result = get_bucket_name()
        buckets_list = s3_client.list_buckets()["Buckets"][0]

        #Assert 
        assert result["ingestion_bucket"] == buckets_list["Name"]

    @mock_aws
    def test_bucket_name_does_not_match_env_variable(self, s3_client): 
        #assign
        s3_client.create_bucket(Bucket="funland-ingestion-bucket-11", CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        os.environ["S3_INGESTION_BUCKET"] = "funland-ingestion-bucket-33"

        #Action
        result = get_bucket_name()
        buckets_list = s3_client.list_buckets()["Buckets"][0]

        #Assert 
        assert result["ingestion_bucket"] != buckets_list["Name"]       

      
            
@mock_aws
class TestUpdateLastChecked:
    def test_update_last_checked_updates(self,ssm_client):
        now=str(datetime.now())
        #print(now)
        ssm_client.put_parameter(
        Name = "last_checked",
        Value = now,
        Type="String")
        
        last_checked=update_last_checked(ssm_client)

        assert datetime.strptime(last_checked,"%Y-%m-%d %H:%M:%S.%f") > datetime.strptime(now,"%Y-%m-%d %H:%M:%S.%f")
   


class TestExtractNewRows:
    def test_extract_new_rows_returns_all_data(self, db_conn):   
        
        
        column_names, new_rows  = extract_new_rows("address", "1995-01-01 00:00:00.000000", db_conn)
        
        assert len(new_rows) > 0
        assert len(column_names) == len(new_rows[0])
        
    def test_extract_new_rows_returns_no_data(self, db_conn):   
        
        
        column_names, new_rows  = extract_new_rows("address", "2030-01-01 00:00:00.000000", db_conn)
        
        assert new_rows == []
        assert len(column_names) > 0
        
    def test_extract_new_rows_returns_some_but_not_all_data(self, db_conn):
        column_names, new_rows  = extract_new_rows("payment", "2025-06-05 07:55:11.631000", db_conn)
        
        
        #print(new_rows)
        assert len(new_rows) >= 5
        assert len(column_names) == len(new_rows[0])

@mock_aws  
class TestConvertNewRowsToDfAndUploadToS3:
    def test_function_convert_new_rows_to_dataframe(self,s3_client):
    
        s3_client.create_bucket(
            Bucket='testbucket',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
                }
            )
        column_names=['age','height']
        new_rows=[[18,192.0], [33,177.4]]
        last_checked="2020-01-01 00:00:00.000000"

        convert_new_rows_to_df_and_upload_to_s3_as_csv("testbucket","person",column_names,new_rows,last_checked)
    
    
        df_read = wr.s3.read_csv(f"s3://testbucket/person/{last_checked}.csv")
        df_read=df_read.drop("Unnamed: 0", axis=1) 
        #print(df_read.columns)
        assert list(df_read.columns.values)==['age','height']

    def test_function_gives_a_error(self,s3_client):
        s3_client.create_bucket(
            Bucket='testbucket',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
                }
            )
        column_names=['age','height']
        new_rows=[[18,192.0], [33,177.4]]
        last_checked="2020-01-01 00:00:00.000000"
        with pytest.raises(Exception):
            convert_new_rows_to_df_and_upload_to_s3_as_csv("testingbucket","person",column_names,new_rows,last_checked)







    



            

   
        
        
        
    
            

