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
        
        #pd.DataFrame(varchar, dtype = str)
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
        
        
def dim_staff(last_checked, ingestion_bucket, processed_bucket):
    """
    Summary:
    Read staff and department CSVs from S3 ingestion bucket. If either is missing, return a skip message.
    Transformations:
    - Join staff and department on department_id
    - Drop unnecessary columns:[department_id, manager, last_updated, created_at]
    - Ensure no NULL values in required fields
      note: 'department_name' and 'location' could be null at source ==> need to be overwritten??
    - update data type of culomn: 'email_address' - as email address from varchar
    - Save result as parquet to processed bucket
    Args:
        last_checked (str): Timestamp string used to locate the files
        ingestion_bucket (str): S3 source bucket
        processed_bucket (str): S3 destination bucket
    """
    key_staff = f"staff/{last_checked}.csv"
    key_department = f"department/{last_checked}.csv"
    try:
        # Check both files exist
        if not check_file_exists_in_ingestion_bucket(bucket=ingestion_bucket, filename=key_staff):
            logger.warning(f"Missing file: {key_staff}")
            return 'Missing staff file'
        # if not check_file_exists_in_ingestion_bucket(bucket=ingestion_bucket, key=key_department):
        #     logger.warning(f"Missing file: {key_department}")
        #     return 'Missing department file'
        # Read both CSVs
        staff_path = f"s3://{ingestion_bucket}/{key_staff}"
        department_path = f"s3://{ingestion_bucket}/{key_department}"
        staff_df = wr.s3.read_csv(staff_path)
        department_df = wr.s3.read_csv(department_path)
        logger.info("Staff and department files loaded successfully.")
        # Merge on department_id
        merged_df = pd.merge(staff_df, department_df, on="department_id", how="left")
        # Drop unwanted columns
        drop_cols = [col for col in ['Unnamed: 0_x', 'Unnamed: 0_y', 'department_id', 'manager', 
                                     'last_updated_x', 'last_updated_y', 'created_at_x',
                                     'created_at_y']]# if col in merged_df.columns]
        dim_staff_df = merged_df.drop(columns=drop_cols, axis=1)
        # Check for missing values in required columns
        # required_cols = ['staff_id', 'first_name', 'last_name', 'email_address']
        # if dim_staff_df[required_cols].isnull().any().any():
        #     logger.error("Null values found in required dim_staff columns.")
        #     raise ("Null values in NOT NULL fields.")
        # Upload as parquet
        output_key = f"dim_staff/{last_checked}.parquet"
        wr.s3.to_parquet(dim_staff_df, f"s3://{processed_bucket}/{output_key}")
        logger.info(f"dim_staff uploaded successfully to s3://{processed_bucket}/{output_key}")
        return 'dim_staff transformation complete'
    except botocore.exceptions.ClientError as e:
        logger.error(f"S3 client error: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error in dim_staff: {e}")
        raise e


def dim_counterparty(last_checked, ingestion_bucket, processed_bucket, s3_client):
    
    key_counterparty = f"counterparty/{last_checked}.csv"

    if not check_file_exists_in_ingestion_bucket(bucket=ingestion_bucket, filename=key_counterparty):
        logger.warning(f"Missing file: {key_counterparty}")
        return 'Missing staff file'

    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=ingestion_bucket, Prefix='address/')
    address_files = response['Contents']        
    key_address = max(address_files, key=lambda x: x['LastModified'])['Key']
    

    counterparty_path = f"s3://{ingestion_bucket}/{key_counterparty}"
    address_path = f"s3://{ingestion_bucket}/{key_address}"

    counterparty_df = wr.s3.read_csv(counterparty_path)
    address_df = wr.s3.read_csv(address_path)
    logger.info("Counterparty and address files loaded successfully.")
    
    
    counterparty_df = counterparty_df.rename(columns={"legal_address_id": "address_id"})

    
    merged_df = pd.merge(counterparty_df, address_df, on='address_id', how="outer")
    
    
    columns=['Unnamed: 0_x', 'Unnamed: 0_y', 'commercial_contact', 'created_at_x',
              'delivery_contact', 'last_updated_x', 'address_id',
              'address_id','created_at_y', 'last_updated_y']
    dim_counterparty_df = merged_df.drop(columns=columns, axis=1)
    
    
    dim_counterparty_df = dim_counterparty_df.rename(columns={
        'address_line_1' : 'counterparty_legal_address_line_1',
        'address_line_2' : 'counterparty_legal_address_line_2',
        'city' : 'counterparty_legal_city',
        'country' : 'counterparty_legal_country',
        'district' : 'counterparty_legal_district',
        'phone' : 'counterparty_legal_phone_number',
        'postal_code' : 'counterparty_legal_postal_code'
    })
    
    output_key = f"dim_counterparty/{last_checked}.parquet"
    wr.s3.to_parquet(dim_counterparty_df, f"s3://{processed_bucket}/{output_key}")
    logger.info(f"dim_counterparty uploaded successfully to s3://{processed_bucket}/{output_key}")
    return 'dim_counterparty transformation complete'


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


def dim_date():
    pass


def fact_sales_order(last_checked,ingestion_bucket,processed_bucket):
       
    key_sales = f"sales_order/{last_checked}.csv"

    if not check_file_exists_in_ingestion_bucket(bucket=ingestion_bucket, filename=key_sales):
        logger.warning(f"Missing file: {key_sales}")
        return 'Missing staff file'
    
    file_path_s3 = f's3://{ingestion_bucket}/{key_sales}' 
    fact_sales_df = wr.s3.read_csv(file_path_s3)
    fact_sales_df = fact_sales_df.drop(["Unnamed: 0"], axis=1)
    
    # SERIAL ID needed for sales_record_id?
    fact_sales_df["sales_record_id"] = fact_sales_df["sales_order_id"]

    #convert created_at to datetime obj, then split into created_date and created_time
    fact_sales_df["created_at"] = pd.to_datetime(fact_sales_df["created_at"])
    for d in fact_sales_df["created_at"]:
        fact_sales_df["created_date"] = d.date()
        fact_sales_df["created_time"] = d.time()
    
    #convert last_updated to datetime obj, then split into last_updated_date and last_updated_time
    fact_sales_df["last_updated"] = pd.to_datetime(fact_sales_df["last_updated"])
    for d in fact_sales_df["last_updated"]:
        fact_sales_df["last_updated_date"] = d.date()
        fact_sales_df["last_updated_time"] = d.time()
    
    fact_sales_df = fact_sales_df.drop(["created_at"], axis=1)
    fact_sales_df = fact_sales_df.drop(["last_updated"], axis=1)

      #change sales_id to sales_staff_id
    #fact_sales_df = fact_sales_df.rename({"staff_id": "sales_staff_id"})
    fact_sales_df["sales_staff_id"] = fact_sales_df["staff_id"]
    fact_sales_df = fact_sales_df.drop(["staff_id"], axis=1)

    #change delivery date from varchar to datetime 
    fact_sales_df["agreed_delivery_date"] = pd.to_datetime(fact_sales_df["agreed_delivery_date"]).dt.date

    #change agreed_payment_date from varchar to datetime
    fact_sales_df["agreed_payment_date"] = pd.to_datetime(fact_sales_df["agreed_payment_date"]).dt.date
    #change order of columns
    final_columns = [
        'sales_record_id', 'sales_order_id',
        'created_date','created_time', 
        'last_updated_date', 'last_updated_time',
        'sales_staff_id','counterparty_id',
        'units_sold', 'unit_price',
        'currency_id', 'design_id',
        'agreed_payment_date', 'agreed_delivery_date',
        'agreed_delivery_location_id']
    
    fact_sales_df = fact_sales_df[final_columns]

    logger.info("fact_sales dataframe has been created")

    output_key = f"fact_sales_order/{last_checked}.parquet"
    wr.s3.to_parquet(fact_sales_df, f"s3://{processed_bucket}/{output_key}")
    logger.info(f"fact_sales uploaded successfully to s3://{processed_bucket}/{output_key}")
    return 'fact_sales transformation complete'


