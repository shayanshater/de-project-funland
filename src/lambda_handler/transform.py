from src.lambda_handler.extract import get_db_credentials, get_last_checked, create_db_connection, update_last_checked, extract_new_rows, convert_new_rows_to_df_and_upload_to_s3_as_csv,get_bucket_name,lambda_handler

import os
import logging
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
import pandas as pd
import awswrangler as wr

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler_transform(event, context):
    """_summary_

    Args:
        event (_type_): _description_
        context (_type_): _description_

    Returns:
        _type_: _description_
    """
    pass


ssm_client=boto3.client('ssm')
last_checked = get_last_checked(ssm_client)["last_checked"]

tables_to_import = ["transaction", "sales_order", 
                        "payment","counterparty", 
                        "currency", "department", 
                        "design", "staff",
                        "address", "purchase_order",
                        "payment_type"]
    
table=tables_to_import[5]
ingestion_bucket = get_bucket_name()["ingestion_bucket"]

#print(ingestion_bucket)

def dim_currency(ingestion_bucket,table,last_checked,processed_bucket):

    """
    We will read the csv file for the currency table from the s3 ingestion bucket using awswrangler.
    
    Compare the column names in the currency table and dim_currency table.

    We will make a dataframe using pandas for this table with the needed columns. 
    
    Convert it to parquet file and then upload it to the processed bucket.

    ARGS:ingestion_bucket, table, last_checked


    """
    #reading the csv file

    #df_read = wr.s3.read_csv("s3://s-o-s3-bucket-prefix-20250520143017315000000001/project_test_with_wrangler.csv")
    df_currency = wr.s3.read_csv(f"s3://{ingestion_bucket}/{table}/{last_checked}.csv")
    
    #columns_currency=[currency_id, currency_code, created_at, last_updated]
    #columns_dim_currency=[currency_id, currency_code, currency_name]

    #dropping the columns that we dont need
    df_dim_currency=df_read.drop(["Unnamed: 0", "created_at", "last_updated"], axis=1)

    #we have to add a new column(currency_name)
    df_dim_currency=df_dim_currency.assign(currency_name=lambda x: x['currency_code'] + '_Name')
    
    print(df_dim_currency)
    #upload to s3 as a parquet file
    wr.s3.to_parquet(df_dim_currency,f"s3://{processed_bucket}/{table}/{last_checked}.parquet")   #need processed bucket as a argument as well



    

