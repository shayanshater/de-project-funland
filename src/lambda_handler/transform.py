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


def dim_location(last_checked, ingestion_bucket, processed_bucket):
    """
    Summary:
    read the csv file (as a dataframe) that was uploaded (address/<timestamp>.csv) at the extract section.
    If there was no 'location' file uploaded, return a skip message and stop at this point.

    To transform the dim_location dataframe:
    - drop the last_updated and created_at columns
    - 'address_line_2' and 'district' columns could be null. 
    Every other columns are not null.

    Upload dim_location as parquet to s3 processed bucket

    Args:
    ingestion_bucket (str): Source S3 bucket
    processed_bucket (str): Destination S3 bucket
    last_checked (str): Timestamp to locate the file
    """

    file_key = f"address/{last_checked}.csv"

    try:    
        if not check_file_exists_in_ingestion_bucket(bucket=ingestion_bucket, filename=file_key):
            logger.info(f"No file found at '{file_key}'. Skipping dim_location transformation.")
            return 'No file found'
        
        # read address...csv file from ingestion bucket
        file_path_s3 = f's3://{ingestion_bucket}/{file_key}' 
        location_df = wr.s3.read_csv(file_path_s3)
        logger.info(f"File {file_key} read successfully from ingestion bucket.")

        #addressID to be updated to locationID 
        dim_location_col_name_df = location_df.rename(columns = {"address_id" : "location_id"})

        # drop unneccessery columns
        dim_location_df = dim_location_col_name_df.drop(['Unnamed: 0', 'last_updated', "created_at"], axis=1)
        logger.info("dim_location dataframe has been created and transformed.")

        #change order of columns 
        final_columns = ["location_id", "address_line_1","address_line_2", "district",
                       "city", "postal_code", "country", "phone"]
        varchar = ["address_line_1","address_line_2", "district",
                       "city", "postal_code", "country", "phone"]
        pd.DataFrame(varchar, dtype = str)
        dim_location_df = dim_location_df[final_columns]


        # save to processed s3 bucket as parquet
        processed_file_key = f"dim_location/{last_checked}.parquet"
        wr.s3.to_parquet(dim_location_df, f"s3://{processed_bucket}/{processed_file_key}")
        logger.info(f"dim_location parquet has been uploaded to s3://{processed_bucket}/{processed_file_key}")
        return('dim_location transformation and upload complete')

    # error handeling
    except botocore.exceptions.ClientError as client_error:
        logger.error(f"S3 client error at transform dim_location: {str(client_error)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in dim_location transform: {str(e)}")
        raise


def dim_counterparty(last_checked, ingestion_bucket, processed_bucket):
    """
    Reads and transforms counterparty and address data into a dimension table.

    Steps:
        - Load 'counterparty/<timestamp>.csv' and 'address/<timestamp>.csv'
        - Join them on 'legal_address_id' = 'address_id'
        - Drop unnecessary columns:
            ['last_checked', 'created_at', 'legal_address_id', 'address_id', 'commercial_contact', 'delivery_contact']
        - Rename address fields to counterparty_legal_* format
            (counterparty_id and counterparty_legal_name are same name in source as target.)
            'address_line_1' => counterparty_legal_address_line_1
            'address_line_2' => counterparty_legal_address_line_2
            'district' => counterparty_legal_district
            'city' => 'counterparty_legal_city'
            'postal_code' => 'counterparty_legal_postal_code'
            'country' => 'counterparty_legal_country'
            'phone' => 'counterparty_legal_phone_number'

        - Save result as parquet to processed S3 bucket

    Args:
        last_checked (str): Timestamp for the file name
        ingestion_bucket (str): Source S3 bucket
        processed_bucket (str): Destination S3 bucket
    """

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

    

            