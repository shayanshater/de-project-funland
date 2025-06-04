from src.extract import get_db_credentials, get_last_checked, create_db_connection, get_bucket_name
import pytest
from pg8000.native import DatabaseError, InterfaceError
from moto import mock_aws
import boto3
from datetime import datetime
import json
# from botocore.exceptions import ClientError



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
    return boto3.client('ssm')

@pytest.fixture(scope='function')
def sm_client():
    return boto3.client('secretsmanager')

@pytest.fixture(scope='function')
def s3_client():
    return boto3.client('s3')



@mock_aws
class TestGetLastChecked:
    def test_get_last_checked_obtains_the_correct_vaiable(self, ssm_client):
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
            Name = "db_creds",
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

    
@mock_aws
class TestGetBucketName:
    def test_get_bucket_name_gets_the_correct_bucket_name_which_is_dynamic(self, s3_client):
        #assign

        buckets_list = [
            {'Name': 'funland-ingestion-bucket-20250604125203238900000004',
             'CreationDate': datetime.datetime(2025, 6, 4, 12, 52, 5, tzinfo=tzutc())
             }         
        ]

        s3_client. #creat s3 buckets and use our func to see if it can
        #retreive these newly created mock buckets!
        # the buckets_list provided above (line 139) - does it serve the same purpose as
        #creating mock buckets using s3 client in line 145?



        #action
        result = get_bucket_name()
        expected = {"ingestion_bucket": 'funland-ingestion-bucket-20250604125203238900000004'}

        #assert
        assert result == expected


        # secret_string = json.dumps(secret_dict)
                
        # sm_client.create_secret(
        #     Name = "db_creds",
        #     SecretString = secret_string
        #     )
        
        # #action
        # result = get_db_credentials(sm_client)

        # #assert
        # assert result == secret_dict


    
    def test_get_bucket_name_raises_error_if_bucket_not_found(self, s3_client):
        
        #assign
        expected = "Ingestion Bucket does not exist!"
        
        #action
        result = get_bucket_name()
        
        #assert
        assert result == expected
    

            

        
        
        
        
    
            

