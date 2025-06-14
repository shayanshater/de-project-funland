import awswrangler as wr
import logging
import boto3
import botocore
import botocore.exceptions
import json
import os
import pg8000

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)



##################################################################################
# Lambda Handler
##################################################################################
    


def lambda_handler(event, context):
    
    sm_client = boto3.client('secretsmanager')
    
    
    last_checked = event['myresult']['timestamp_to_transform']
    processed_bucket = os.getenv('S3_PROCESSED_BUCKET')
    db_credentials = get_db_credentials(sm_client)
    
    db_conn = pg8000.Connection(
        user = db_credentials['WAREHOUSE_DB_USER'],
        password = db_credentials['WAREHOUSE_DB_PASSWORD'],
        database = db_credentials['WAREHOUSE_DB_NAME'],
        host = db_credentials['WAREHOUSE_DB_HOST'],
        port = db_credentials['WAREHOUSE_DB_PORT']
    )

    
    logger.info("dim_staff has begun loading")
    load_dim_staff(last_checked, processed_bucket, db_conn)
    logger.info("dim_staff has finished loading")
    
    
    
    logger.info("dim_location has begun loading")
    load_dim_location(last_checked, processed_bucket, db_conn)
    logger.info("dim_location has finished loading")
    
    
    logger.info("dim_currency has begun loading")
    load_dim_currency(last_checked, processed_bucket, db_conn)
    logger.info("dim_currency has finished loading")
    
    logger.info("dim_design has begun loading")
    load_dim_design(last_checked, processed_bucket, db_conn)
    logger.info("dim_design has finished loading")
    
    logger.info("dim_counterparty has begun loading")
    load_dim_counterparty()
    logger.info("dim_counterparty has finished loading")
    
    


    logger.info("dim_date has started loading")
    load_dim_date(last_checked, processed_bucket, db_conn)
    logger.info("dim_date has finished loading")


    logger.info("fact_sales_order has started loading")
    load_fact_sales_order(last_checked, processed_bucket, db_conn)
    logger.info("fact_sales_order has finished loading")


##################################################################################
# Helper functions for Lambda Handler
##################################################################################
    
    
def load_dim_staff(last_checked, processed_bucket, db_conn):
    file_key = f"dim_staff/{last_checked}.parquet"
    
    if not check_file_exists_in_ingestion_bucket(bucket= processed_bucket, filename= file_key):
        logger.warning(f"{file_key} was not found for this run")
        return f"{file_key} was not found for this run"
    
    
    logger.info(f"beginning to read parquet file for dim_staff from {processed_bucket}")
    df = wr.s3.read_parquet(
        f"s3://{processed_bucket}/{file_key}"
    )
    logger.info(f"finished reading parquet file for dim_staff from {processed_bucket}")
    
    
    
    logger.info(f"loading dim_staff in warehouse")
    wr.postgresql.to_sql(
        df = df,
        table= 'dim_staff',
        schema='public',
        con=db_conn,
        use_column_names=True
    )
    logger.info(f"finished loading dim_staff")
    
    
def load_dim_location(last_checked, processed_bucket, db_conn):
    file_key = f"dim_location/{last_checked}.parquet"
    
    
    if not check_file_exists_in_ingestion_bucket(bucket= processed_bucket, filename= file_key):
        logger.warning(f"{file_key} was not found for this run")
        return f"{file_key} was not found for this run"
    
    logger.info(f"beginning to read parquet file for dim_location from {processed_bucket}")
    df = wr.s3.read_parquet(
        f"s3://{processed_bucket}/{file_key}"
    )
    logger.info(f"finished reading parquet file for dim_location from {processed_bucket}")
    
    
    logger.info(f"loading dim_location in warehouse")
    wr.postgresql.to_sql(
        df = df,
        table= 'dim_location',
        schema='public',
        con=db_conn,
        use_column_names=True
    )
    logger.info(f"finished loading dim_location")
    
    
    
    
def load_dim_currency(last_checked, processed_bucket, db_conn):
    file_key = f"dim_currency/{last_checked}.parquet"
    
    
    if not check_file_exists_in_ingestion_bucket(bucket= processed_bucket, filename= file_key):
        logger.warning(f"{file_key} was not found for this run")
        return f"{file_key} was not found for this run"
    
    logger.info(f"beginning to read parquet file for dim_currency from {processed_bucket}")
    df = wr.s3.read_parquet(
        f"s3://{processed_bucket}/{file_key}"
    )
    logger.info(f"finished reading parquet file for dim_currency from {processed_bucket}")
    
    
    logger.info(f"loading dim_currency in warehouse")
    wr.postgresql.to_sql(
        df = df,
        table= 'dim_currency',
        schema='public',
        con=db_conn,
        use_column_names=True
    )
    logger.info(f"finished loading dim_currency")
    
def load_dim_design(last_checked, processed_bucket, db_conn):
    file_key = f"dim_design/{last_checked}.parquet"
    
    
    if not check_file_exists_in_ingestion_bucket(bucket= processed_bucket, filename= file_key):
        logger.warning(f"{file_key} was not found for this run")
        return f"{file_key} was not found for this run"
    
    logger.info(f"beginning to read parquet file for dim_design from {processed_bucket}")
    df = wr.s3.read_parquet(
        f"s3://{processed_bucket}/{file_key}"
    )
    logger.info(f"finished reading parquet file for dim_design from {processed_bucket}")
    
    
    logger.info(f"loading dim_design in warehouse")
    wr.postgresql.to_sql(
        df = df,
        table= 'dim_design',
        schema='public',
        con=db_conn,
        use_column_names=True
    )
    logger.info(f"finished loading dim_design")
    
    
def load_dim_counterparty(last_checked, processed_bucket, db_conn):
    file_key = f"dim_counterparty/{last_checked}.parquet"
    
    
    if not check_file_exists_in_ingestion_bucket(bucket= processed_bucket, filename= file_key):
        logger.warning(f"{file_key} was not found for this run")
        return f"{file_key} was not found for this run"
    
    logger.info(f"beginning to read parquet file for dim_counterparty from {processed_bucket}")
    df = wr.s3.read_parquet(
        f"s3://{processed_bucket}/{file_key}"
    )
    logger.info(f"finished reading parquet file for dim_counterparty from {processed_bucket}")
    
    
    logger.info(f"loading dim_counterparty in warehouse")
    wr.postgresql.to_sql(
        df = df,
        table= 'dim_counterparty',
        schema='public',
        con=db_conn,
        use_column_names=True
    )
    logger.info(f"finished loading dim_counterparty")
    
    
def load_dim_date(last_checked, processed_bucket, db_conn):
    file_key = f"dim_date/{last_checked}.parquet"
    
    
    if not check_file_exists_in_ingestion_bucket(bucket= processed_bucket, filename= file_key):
        logger.warning(f"{file_key} was not found for this run")
        return f"{file_key} was not found for this run"
    
    logger.info(f"beginning to read parquet file for dim_date from {processed_bucket}")
    df = wr.s3.read_parquet(
        f"s3://{processed_bucket}/{file_key}"
    )
    logger.info(f"finished reading parquet file for dim_date from {processed_bucket}")
    
    
    logger.info(f"loading dim_date in warehouse")
    wr.postgresql.to_sql(
        df = df,
        table= 'dim_date',
        schema='public',
        con=db_conn,
        use_column_names=True
    )
    logger.info(f"finished loading dim_date")
    
    
    
def load_fact_sales_order(last_checked, processed_bucket, db_conn):
    file_key = f"fact_sales_order/{last_checked}.parquet"
    
    
    if not check_file_exists_in_ingestion_bucket(bucket= processed_bucket, filename= file_key):
        logger.warning(f"{file_key} was not found for this run")
        return f"{file_key} was not found for this run"
    
    logger.info(f"beginning to read parquet file for fact_sales_order from {processed_bucket}")
    df = wr.s3.read_parquet(
        f"s3://{processed_bucket}/{file_key}"
    )
    logger.info(f"finished reading parquet file for fact_sales_order from {processed_bucket}")
    
    
    logger.info(f"loading fact_sales_order in warehouse")
    wr.postgresql.to_sql(
        df = df,
        table= 'fact_sales_order',
        schema='public',
        con=db_conn,
        use_column_names=True
    )
    logger.info(f"finished loading fact_sales_order")
    
##################################################################################
# Utility functions
##################################################################################
    

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
        
        
def get_db_credentials(sm_client): # test and code complete
    """_summary_
    This functions should return a dictionary of all 
    the db credentials obtained from secret manager
    
    Returns:
    dictionary of credentials
    {"DB_USER":"totesys", DB_PASSWORD:".......}

    """
    try:
        response = sm_client.get_secret_value(SecretId = 'warehouse_totesys_credentials')
        db_credentials = json.loads(response["SecretString"])
        return db_credentials

    except sm_client.exceptions.ResourceNotFoundException as par_not_found_error:
        logger.error(f"get_last_checked: The parameter was not found: {str(par_not_found_error)}")
        raise par_not_found_error
    except botocore.exceptions.ClientError as error:
        logger.error(f"get_last_checked: There has been an error: {str(error)}")
        raise error
