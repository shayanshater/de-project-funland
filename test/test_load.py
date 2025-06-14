from src.lambda_handler.load import (load_dim_staff, load_dim_location,
                                     load_dim_currency)
import pytest
import pg8000
from dotenv import load_dotenv
import os
import awswrangler as wr
import pandas as pd
from moto import mock_aws
import boto3
import datetime
import numpy as np

load_dotenv()

@pytest.fixture(autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    
    
@pytest.fixture(scope='function')
def s3_client():
    s3_client = boto3.client("s3")
    yield s3_client


@pytest.fixture(scope='function')
def db_conn():

    conn = pg8000.Connection(
            user = os.getenv('local_user'),
            password =  os.getenv('local_password'),
            host = os.getenv('local_host'),
            port = os.getenv('local_port')
        )
    
    conn.rollback()
    conn.autocommit = True
    
    db_name = os.getenv('local_database')
    
    
    conn.run(f'DROP DATABASE IF EXISTS {db_name} WITH (FORCE);')
    conn.run(f'CREATE DATABASE {db_name};')
    print('db made')
    
    
    
    conn_to_db = pg8000.Connection(
            user = os.getenv('local_user'),
            password =  os.getenv('local_password'),
            host = os.getenv('local_host'),
            port = os.getenv('local_port'),
            database = db_name
        )
    
    
    with open('test/make_warehouse_schema.sql', 'r') as sql_file:
        query = sql_file.read()
        conn_to_db.run(query)
        yield conn_to_db
        conn_to_db.close()
    conn.run(f'DROP DATABASE IF EXISTS {db_name} WITH (FORCE);',)
    print('db dropped')
    conn.close()
    
    
    



@mock_aws
class TestLoadDimStaff:
    def test_load_dim_staff_reads_from_ingested_and_loads_warehouse_correctly(self, db_conn, s3_client):
        processed_bucket_name = 'processed-bucket'
        
        
        s3_client.create_bucket(
        Bucket=processed_bucket_name,
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )
        
        
        dim_staff_columns = ['staff_id', 'first_name', 
                                'last_name','department_name', 
                                'location', 'email_address']
        
        dim_staff_rows = [[1, 'Jeremie', 
                         'Franey', 'jeremie.franey@terrifictotes.com',
                         'Purchasing', 'Manchester']]

        
        df_dim_staff = pd.DataFrame(dim_staff_rows,columns=dim_staff_columns)
        
        
        last_checked = "2000-01-01 00:00:00.000000"
        file_key = f'dim_staff/{last_checked}.parquet'
        
        wr.s3.to_parquet(
            df_dim_staff,
            f"s3://{processed_bucket_name}/{file_key}"
        )
        
        
        load_dim_staff(last_checked, processed_bucket_name, db_conn)
        
        result = db_conn.run("SELECT * FROM dim_staff;")
        
        assert list(result) == dim_staff_rows
        
        


@mock_aws
class TestLoadDimLocation:
    def test_load_dim_location_reads_from_ingested_and_loads_warehouse_correctly(self, db_conn, s3_client):
        processed_bucket_name = 'processed-bucket'
        
        
        s3_client.create_bucket(
        Bucket=processed_bucket_name,
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )
        
        
        dim_location_columns = [
            "location_id", "address_line_1",
            "address_line_2", "district",
            "city", "postal_code", 
            "country", "phone"
        ]
        
        dim_location_rows = [
            [1, '6826 Herzog Via', 
             None, 'Avon',
             'New Patienceburgh', '28441', 
             'Turkey','1803 637401' ]
        ]

        
        df_dim_location = pd.DataFrame(dim_location_rows, columns = dim_location_columns)
        
        
        last_checked = "2000-01-01 00:00:00.000000"
        file_key = f'dim_location/{last_checked}.parquet'
        
        wr.s3.to_parquet(
            df_dim_location,
            f"s3://{processed_bucket_name}/{file_key}"
        )
        
        
        load_dim_location(last_checked, processed_bucket_name, db_conn)
        
        result = db_conn.run("SELECT * FROM dim_location;")
        
        assert list(result) == dim_location_rows



@mock_aws
class TestLoadDimCurrency:
    def test_load_dim_currency_reads_from_ingested_and_loads_warehouse_correctly(self, db_conn, s3_client):
        processed_bucket_name = 'processed-bucket'
        
        
        s3_client.create_bucket(
        Bucket=processed_bucket_name,
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )
        
        
        dim_currency_columns = [
            'currency_id', 'currency_code', 'currency_name'
        ]
        
        dim_currency_rows = [
            [0, 'GBP', 'Great British Pound']
        ]

        
        df_dim_currency = pd.DataFrame(dim_currency_rows, columns = dim_currency_columns)
        
        
        last_checked = "2000-01-01 00:00:00.000000"
        file_key = f'dim_currency/{last_checked}.parquet'
        
        wr.s3.to_parquet(
            df_dim_currency,
            f"s3://{processed_bucket_name}/{file_key}"
        )
        
        
        load_dim_currency(last_checked, processed_bucket_name, db_conn)
        
        result = db_conn.run("SELECT * FROM dim_currency;")
        
        assert list(result) == dim_currency_rows