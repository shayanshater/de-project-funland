

import logging
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
import pandas as pd
import awswrangler as wr
import botocore.exceptions



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)



def lambda_handler(event, context):
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
    
    file_key = f"currency/{last_checked}.csv"

    if not check_file_exists_in_ingestion_bucket(bucket=ingestion_bucket, filename=file_key):
        logger.info(f"File_key: '{file_key}' does not exist!")
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




def dim_design(last_checked, ingestion_bucket, processed_bucket):
    """
    Summary:
    read the file (as a dataframe) that was uploaded (design/<timestamp>.csv) at the extract section  
    If there wasnt any file uploaded, return a skip message and stop right here.
    
    transform the dataframe (drop the last_updated and created_at columns).
    
    upload to s3 as a parquet.
    
    Args:
        last_checked (str): a string which marks the files 
        that were uploaded to the ingestion bucket that we 
        need to pick out and use in this function.
    """
    
    file_key = f"design/{last_checked}.csv"
    
    if not check_file_exists_in_ingestion_bucket(bucket=ingestion_bucket, filename=file_key):
        logger.info(f"Key: '{file_key}' does not exist!")
        return 'No file found'
    
    
    file_path_s3 = f's3://{ingestion_bucket}/{file_key}' 
    design_df = wr.s3.read_csv(file_path_s3)
    dim_design_df = design_df.drop(['Unnamed: 0','last_updated', "created_at"], axis=1)
    logger.info("dim_design dataframe has been created")
    
    processed_file_key = f"dim_design/{last_checked}.parquet" # TODO: check the .parquet
    try:
        wr.s3.to_parquet(dim_design_df, f"s3://{processed_bucket}/{processed_file_key}")
        logger.info(f"dim_design parquet has been uploaded to ingestion s3 at: s3://{processed_bucket}/{processed_file_key}")
    except botocore.exceptions.ClientError as client_error:
        logger.error(f"there has been a error in converting to parquet and uploading for dim_design {str(client_error)}")
        
        
        
def dim_staff():
    pass


def dim_location():
    pass

def check_file_exists_in_ingestion_bucket(bucket, filename):

    """
    Summary:
    This function accesses the AWS S3 ingestion bucket and check if a specific file exists.
    If it doesn't exist, it returns 'File' does not exist, and logs the error.

    Args:
    bucket, file_name

    Returns:
        Boolean.
    """
    s3_client = boto3.client("s3")
    try:
        s3_client.head_object(Bucket=bucket, Key=filename)
        logger.info(f"Key: '{filename}' found!")
        return True
    except s3_client.exceptions.NoSuchBucket as NoSuchBucket: 
        logger.info(f"Bucket: '{bucket}' does not exist!")
        return False
    except botocore.exceptions.ClientError as ClientError:
        if ClientError.response["Error"]["Code"] == "404":
            logger.info(f"Key: '{filename}' does not exist!")
            return False

    

     
