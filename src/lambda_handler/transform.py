
import logging
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
import pandas as pd
import awswrangler as wr
import botocore.exceptions

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



def dim_currency(last_checked,ingestion_bucket,processed_bucket):

    """
    We will read the csv file for the currency table from the s3 ingestion bucket using awswrangler.
    
    Compare the column names in the currency table and dim_currency table.

    We will make a dataframe using pandas for this table with the needed columns. 
    
    Convert it to parquet file and then upload it to the processed bucket.

    ARGS:ingestion_bucket,last_checked, processed_bucket
    """

    if not check_file_exists_in_ingestion_bucket(bucket=ingestion_bucket, key=f"currency/{last_checked}.csv"):
        logger.info(f"Key: '{key}' does not exist!")
        return 'No file found'
    
    #reading the csv file
    df_currency = wr.s3.read_csv(f"s3://{ingestion_bucket}/currency/{last_checked}.csv")
    
    #columns_currency=[currency_id, currency_code, created_at, last_updated]
    #columns_dim_currency=[currency_id, currency_code, currency_name]

    #dropping the columns that we dont need
    df_dim_currency=df_currency.drop(["Unnamed: 0", "created_at", "last_updated"], axis=1)

    #we have to add a new column(currency_name)
    df_dim_currency=df_dim_currency.assign(currency_name=lambda x: x['currency_code'] + '_Name')
    logger.info("dim_design dataframe has been created")
    
    #upload to s3 as a parquet file
    try:
        wr.s3.to_parquet(df_dim_currency,f"s3://{processed_bucket}/currency/{last_checked}.parquet")   #need processed bucket as a argument as well
        logger.info(f"dim_currency parquet has been uploaded to ingestion s3 at: s3://{processed_bucket}/currency/{last_checked}.csv")
    except botocore.exceptions.ClientError as client_error:
        logger.error(f"there has been a error in converting to parquet and uploading for dim_design {str(client_error)}")


def check_file_exists_in_ingestion_bucket(bucket, key):
    s3_client = boto3.client("s3")
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        logger.info(f"Key: '{key}' found!")
        return True
    except s3_client.exceptions.NoSuchBucket as NoSuchBucket: 
        logger.info(f"Bucket: '{bucket}' does not exist!")
        return False
    except botocore.exceptions.ClientError as ClientError:
        if ClientError.response["Error"]["Code"] == "404":
            logger.info(f"Key: '{key}' does not exist!")
            return False



    

