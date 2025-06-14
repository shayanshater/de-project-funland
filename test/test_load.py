from src.lambda_handler.load import (load_dim_staff, load_dim_location,
                                    load_dim_currency, load_dim_design,
                                    load_dim_counterparty, load_dim_date,
                                    load_fact_sales_order)
import pytest
import pg8000
from dotenv import load_dotenv
import os
import awswrangler as wr
import pandas as pd
from moto import mock_aws
import boto3
import datetime as dt
import numpy as np
from decimal import Decimal




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
        
        
        
        
@mock_aws
class TestLoadDimDesign:
    def test_load_dim_design_reads_from_ingested_and_loads_warehouse_correctly(self, db_conn, s3_client):
        processed_bucket_name = 'processed-bucket'
        
        
        s3_client.create_bucket(
        Bucket=processed_bucket_name,
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )
        
        
        dim_design_columns = ['design_id', 'design_name', 
                'file_location', 'file_name']
        
        dim_design_rows = [
            [0, 'Wooden', '/usr', 'wooden-20220717-npgz.json' ]
        ]

        
        df_dim_design = pd.DataFrame(dim_design_rows, columns = dim_design_columns)
        
        
        last_checked = "2000-01-01 00:00:00.000000"
        file_key = f'dim_design/{last_checked}.parquet'
        
        wr.s3.to_parquet(
            df_dim_design,
            f"s3://{processed_bucket_name}/{file_key}"
        )
        
        
        load_dim_design(last_checked, processed_bucket_name, db_conn)
        
        result = db_conn.run("SELECT * FROM dim_design;")
        
        assert list(result) == dim_design_rows
        
        
@mock_aws
class TestLoadDimCounterParty:
    def test_load_dim_counterparty_reads_from_ingested_and_loads_warehouse_correctly(self, db_conn, s3_client):
        processed_bucket_name = 'processed-bucket'
        
        
        s3_client.create_bucket(
        Bucket=processed_bucket_name,
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )
        
        
        dim_counterparty_columns = [
        'counterparty_id', 'counterparty_legal_name', 
        'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2',
        'counterparty_legal_district','counterparty_legal_city', 
        'counterparty_legal_postal_code', 'counterparty_legal_country', 
        'counterparty_legal_phone_number' 
        ]
        
        dim_counterparty_rows = [[1, 'Fahey and Sons', 
                                    '605 Haskell Trafficway', 'Axel Freeway', 
                                    None, 'East Bobbie', 
                                    '88253-4257', 'Heard Island and McDonald Islands',  
                                    '9687 937447']]

        
        df_dim_counterparty = pd.DataFrame(dim_counterparty_rows, columns = dim_counterparty_columns)
        
        
        last_checked = "2000-01-01 00:00:00.000000"
        file_key = f'dim_counterparty/{last_checked}.parquet'
        
        wr.s3.to_parquet(
            df_dim_counterparty,
            f"s3://{processed_bucket_name}/{file_key}"
        )
        
        
        load_dim_counterparty(last_checked, processed_bucket_name, db_conn)
        
        result = db_conn.run("SELECT * FROM dim_counterparty;")
        
        assert list(result) == dim_counterparty_rows
        
        
@mock_aws
class TestLoadDimDate:
    def test_load_dim_date_reads_from_ingested_and_loads_warehouse_correctly(self, db_conn, s3_client):
        processed_bucket_name = 'processed-bucket'
        
        
        s3_client.create_bucket(
        Bucket=processed_bucket_name,
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )
        
        
        dim_date_columns =  ["date_id", "year", 
                             "month", "day", 
                              "day_name", 
                             "month_name", "quarter"]
        
        dim_date_rows = [
            [pd.Timestamp('2022-11-10'), 2022, 11, 10, 'Thursday', 'November', 4],
            [pd.Timestamp('2022-11-11'), 2022, 11, 11, 'Friday', 'November', 4],
            [pd.Timestamp('2022-11-12'), 2022, 11, 12, 'Saturday', 'November', 4],
            [pd.Timestamp('2022-11-13'), 2022, 11, 13, 'Sunday', 'November', 4],
            [pd.Timestamp('2022-11-14'), 2022, 11, 14, 'Monday', 'November', 4],
            [pd.Timestamp('2022-11-15'), 2022, 11, 15, 'Tuesday', 'November', 4]
        ]

        
        df_dim_date = pd.DataFrame(dim_date_rows, columns = dim_date_columns)
        
        
        last_checked = "2000-01-01 00:00:00.000000"
        file_key = f'dim_date/{last_checked}.parquet'
        
        wr.s3.to_parquet(
            df_dim_date,
            f"s3://{processed_bucket_name}/{file_key}"
        )
        
        
        load_dim_date(last_checked, processed_bucket_name, db_conn)
        
        result = db_conn.run("SELECT * FROM dim_date;")
        
        
        expected_dim_date_rows = [
            [dt.date(2022,11,10), 2022, 11, 10, 'Thursday', 'November', 4],
            [dt.date(2022,11,11), 2022, 11, 11, 'Friday', 'November', 4],
            [dt.date(2022,11,12), 2022, 11, 12, 'Saturday', 'November', 4],
            [dt.date(2022,11,13), 2022, 11, 13, 'Sunday', 'November', 4],
            [dt.date(2022,11,14), 2022, 11, 14, 'Monday', 'November', 4],
            [dt.date(2022,11,15), 2022, 11, 15, 'Tuesday', 'November', 4]
        ]
        
        
        
        assert list(result) == expected_dim_date_rows
        
        
        
@mock_aws
class TestLoadFactSalesOrder:
    def test_load_fact_sales_order_reads_from_ingested_and_loads_warehouse_correctly(self, db_conn, s3_client):
        processed_bucket_name = 'processed-bucket'
        
        
        s3_client.create_bucket(
        Bucket=processed_bucket_name,
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
            },
        )
        
        ## loading dim date
        
        dim_date_columns =  ["date_id", "year", 
                             "month", "day", 
                              "day_name", 
                             "month_name", "quarter"]
        
        dim_date_rows = [
            [pd.Timestamp('2022-11-3'), 2022, 11, 3, 'Thursday', 'November', 4],
            [pd.Timestamp('2022-11-07'), 2022, 11, 7, 'Monday', 'November', 4],
            [pd.Timestamp('2022-11-08'), 2022, 11, 8, 'Tuesday', 'November', 4]
        ]

        
        df_dim_date = pd.DataFrame(dim_date_rows, columns = dim_date_columns)
        
        
        last_checked = "2000-01-01 00:00:00.000000"
        file_key = f'dim_date/{last_checked}.parquet'
        
        wr.s3.to_parquet(
            df_dim_date,
            f"s3://{processed_bucket_name}/{file_key}"
        )
        
        load_dim_date(last_checked, processed_bucket_name, db_conn)
        
        
         # loading dim staff
        
        
        dim_staff_columns = ['staff_id', 'first_name', 
                                'last_name','department_name', 
                                'location', 'email_address']
        
        dim_staff_rows = [[19, 'Jeremie', 
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
        
        #loading counterparty
        
        dim_counterparty_columns = [
        'counterparty_id', 'counterparty_legal_name', 
        'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2',
        'counterparty_legal_district','counterparty_legal_city', 
        'counterparty_legal_postal_code', 'counterparty_legal_country', 
        'counterparty_legal_phone_number' 
        ]
        
        dim_counterparty_rows = [[8, 'Fahey and Sons', 
                                    '605 Haskell Trafficway', 'Axel Freeway', 
                                    None, 'East Bobbie', 
                                    '88253-4257', 'Heard Island and McDonald Islands',  
                                    '9687 937447']]

        
        df_dim_counterparty = pd.DataFrame(dim_counterparty_rows, columns = dim_counterparty_columns)
        
        
        last_checked = "2000-01-01 00:00:00.000000"
        file_key = f'dim_counterparty/{last_checked}.parquet'
        
        wr.s3.to_parquet(
            df_dim_counterparty,
            f"s3://{processed_bucket_name}/{file_key}"
        )
        
        load_dim_counterparty(last_checked, processed_bucket_name, db_conn)
        #loading design table
        
        dim_design_columns = ['design_id', 'design_name', 
                'file_location', 'file_name']
        
        dim_design_rows = [
            [3, 'Wooden', '/usr', 'wooden-20220717-npgz.json' ]
        ]

        
        df_dim_design = pd.DataFrame(dim_design_rows, columns = dim_design_columns)
        
        
        last_checked = "2000-01-01 00:00:00.000000"
        file_key = f'dim_design/{last_checked}.parquet'
        
        wr.s3.to_parquet(
            df_dim_design,
            f"s3://{processed_bucket_name}/{file_key}"
        )
        
        
        load_dim_design(last_checked, processed_bucket_name, db_conn)
        
        
        
        ## loading location table
        
        dim_location_columns = [
            "location_id", "address_line_1",
            "address_line_2", "district",
            "city", "postal_code", 
            "country", "phone"
        ]
        
        dim_location_rows = [
            [8, '6826 Herzog Via', 
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
        
        
        
        
        fact_sales_order_columns = [
        'sales_record_id', 'sales_order_id',
        'created_date','created_time', 
        'last_updated_date', 'last_updated_time',
        'sales_staff_id','counterparty_id',
        'units_sold', 'unit_price',
        'currency_id', 'design_id',
        'agreed_payment_date', 'agreed_delivery_date', # check 
        'agreed_delivery_location_id']
        
        fact_sales_order_rows =fact_sales_order_new_rows= [
        [2, 2,
        dt.date(2022,11,3), dt.time(14, 20, 52, 186000), 
        dt.date(2022,11,3), dt.time(14, 20, 52, 186000), 
        19, 8,
        42972, Decimal('3.94'),
        2, 3,
        dt.date(2022,11,8),dt.date(2022,11,7),
        8]]

        
        df_fact_sales_order = pd.DataFrame(fact_sales_order_new_rows, columns = fact_sales_order_columns)
        
        
        last_checked = "2000-01-01 00:00:00.000000"
        file_key = f'fact_sales_order/{last_checked}.parquet'
        
        wr.s3.to_parquet(
            df_fact_sales_order,
            f"s3://{processed_bucket_name}/{file_key}"
        )
        
        load_fact_sales_order(last_checked, processed_bucket_name, db_conn)
        
        result = db_conn.run("SELECT * FROM fact_sales_order;")
        
        assert list(result) == fact_sales_order_rows