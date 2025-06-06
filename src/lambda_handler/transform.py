import botocore.exceptions
import awswrangler as wr
import boto3
import botocore
import logging

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
    
    if not check_file_exists_in_ingestion_bucket(bucket=ingestion_bucket, key=file_key):
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

def check_file_exists_in_ingestion_bucket(bucket, key):
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket, Key=key)
        logger.info(f"Key: '{key}' found!")
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.info(f"Key: '{key}' does not exist!")
            return False