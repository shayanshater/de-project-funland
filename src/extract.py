import os
import logging
from pg8000.native import Connection, identifier, literal, DatabaseError, InterfaceError
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
import json
import pandas as pd
import awswrangler as wr
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# connect to totesys database using the funciton below

def lambda_handler(event, context):
    """ summary
    
    - obtain last_checked via ssm and store in a variable 
    (in a specified timezone).
    Store this in a variable so that it can be 
    passed onto the next lambda handler(Transform) in order to 
    transform the newly updated rows.
        tests:
        - check that last_checked is a datetime in the past and not in the future in the given time zone?
        - check, change, check to see of obtaining is dynamic?
        
    
    - obtain db_creds via secret manager and store in a variable
        tests:
        - check the password is a string and the port is an int etc. 
    
    - create db connection using db creds
        tests:
        - check that successful connection is made.
        - check for a unsuccessful connection.
    
    - query for the new data using connection and last_checked
        test:
        - test that query list is same length as number of tables.
        - test that the rows that are chosen have a last_updated 
        date after our last_checked date.
        (checking each last_updated cloumn in a for loop)
        
    -format the data into a pandas df and upload to s3 bucket.
        test:
        - test that the file exists
        - test that the content is valid
    
    - update last checked variable ## figure out where to do this step so we have access to the latest files for transform step.
        test:
        - test to see if the function updates the 
        variable in the parameter store as expected
    
   
    
    - obtain bucket name
        test:
        - that the bucket name starts with funland-ingestion-bucket-...... where the rest is numbers.
    

    

    Args:
        no inputs as this is the first lambda
        
        
    Returns:
        dictionary
        Optional:
            - maybe the old timestamp?
            - maybe a success message?
            {"timestamp":"2020....", "message":"extract successful"}
    """
    ssm_client=boto3.client("ssm")
    sm_client=boto3.client("secretsmanager")
    last_checked = get_last_checked(ssm_client)["last_checked"]
    db_credentials = get_db_credentials(sm_client)
    db_conn = create_db_connection(db_credentials)
    
    tables_to_import = ["counterparty", "currency", "department", "design", "staff",
                    "sales_order", "address", "payment", "purchase_order",
                    "payment_type", "transaction"]
    
    
    ingestion_bucket = obtain_bucket_name()["ingestion_bucket"]

    
    for table in tables_to_import:
        column_names, new_rows = extract_new_rows(table, last_checked, db_conn)
        if new_rows:
            convert_new_rows_to_df_and_upload_to_s3_as_csv(ingestion_bucket, table, column_names, new_rows,last_checked)
    
    update_last_checked(ssm_client)
    return {"message":"success", "timestamp_to_transform": last_checked}

##################################################################################
# Useful functions for the Lambda Handler
##################################################################################
    
def get_last_checked(ssm_client): # test and code complete
    """
    Summary:
    Access the aws parameter store, and obtain the last_checked parameter.
    Store the parameter and its value in a dictionary and return it.
    
    Returns:
        dictionary of paramter name and value
        {"last_checked" : "2020...."}
    """
    
    try:
        response = ssm_client.get_parameter(
        Name='last_checked',
        WithDecryption=True
        )
        result = {"last_checked": response['Parameter']['Value']}
        return result

    except ssm_client.exceptions.ParameterNotFound as par_not_found_error:
        logger.error(f"get_last_checked: The parameter was not found: {str(par_not_found_error)}")
        raise par_not_found_error
    except ClientError as error:
        logger.error(f"get_last_checked: There has been an error: {str(error)}")
        raise error
    
    
   
def get_db_credentials(sm_client): # test and code complete
    """_summary_
    This functions should return a dictionary of all 
    the db credentials obtained from secret manager
    
    Returns:
    dictionary of credentials
    {"DB_USER":"totesys", DB_PASSWORD:".......}

    """
    try:
        response = sm_client.get_secret_value(SecretId = 'db_creds')
        db_credentials = json.loads(response["SecretString"])
        return db_credentials

    except sm_client.exceptions.ResourceNotFoundException as par_not_found_error:
        logger.error(f"get_last_checked: The parameter was not found: {str(par_not_found_error)}")
        raise par_not_found_error
    except ClientError as error:
        logger.error(f"get_last_checked: There has been an error: {str(error)}")
        raise error


def create_db_connection(db_credentials): #test and code complete
    """ Summary:
    Connect to the totesys database using credentials fetched from 
    AWS Parameter Store (a separate function employed for this purpose).
    Uses Connection module from pg8000.native library 
    (from pg8000.native import Connection)

    Return Connection
    """
    try:
        return Connection(
            user = db_credentials["DB_USER"],
            password = db_credentials["DB_PASSWORD"],
            database = db_credentials["DB_NAME"],
            host = db_credentials["DB_HOST"],
            port = db_credentials["DB_PORT"]
        )
    except InterfaceError as interface_error:
        logger.error(f"create_db_connection: cannot connect to database: {interface_error}")
        raise interface_error
    except Exception as error:
        logger.error(f"create_db_connection: there has been an error: {error}")
        raise error
    

def extract_new_rows(table_name, last_checked, db_connection): 
    """ 
    Summary :
        Use connection object to query for rows in a given table where 
        the last_updated is after our last_checked variable.
        
        returns a tuple of column names and new rows.
    
    Args:
    
        table_name (str):
        name of the table to query for
        
        last_checked (datetime object): 
        should be the datetime object store in parameter store
        
        db_connection (object):
        a connection object to the totesys database
    
    
    Returns:
        - extract new data from updated tables using SQL query.
        - the data will be a list of lists. 
        
        returns a tuple of (column_names, new_rows):
    """
    
    last_checked_dt_obj = datetime.strptime(last_checked, "%Y-%m-%d %H:%M:%S.%f")
    query = f"""
    SELECT * FROM {identifier(table_name)} WHERE last_updated > {literal(last_checked_dt_obj)}
    """
    
    try:
        new_rows = db_connection.run(query)
        column_names = [column['name'] for column in db_connection.columns]
        return column_names, new_rows
    except DatabaseError as db_error:
        logger.error(f"There has been a database error: {str(db_error)}")
    except Exception as error:
        logger.error(f"There has been an error: {str(error)}")
    finally:
        if db_connection:
            db_connection.close()
    


def convert_new_rows_to_df_and_upload_to_s3_as_csv(ingestion_bucket, table, column_names, new_rows,last_checked):
    """
    Summary:
    This function will take the column names and new row data, 
    and create a pandas dataframe from this.
    From here, this dataframe is uploaded directly to given s3 bucket as
    a csv file.
    

    Args:
        ingestion_bucket (str): name of the ingestion bucket
        table (str): name of the table with the new data
        column_names (list): list of column names
        new_rows (list): nested list of new row values
        
        
    returns:
        sucess message through a log
    """
    
    #convert new rows to a dataframe
    df = pd.DataFrame(new_rows,columns=column_names)
    #convert dataframe to a csv file
    try:
        wr.s3.to_csv(df, f"s3://{ingestion_bucket}/{table}/{last_checked}.csv")
    except Exception as error:
        logger.errorr(f"convert_new_rows_to_df_and_upload_to_s3_as_csv: There has been a dataframe error: {str(error)}")
        raise error

    
def update_last_checked(ssm_client):
    """
    Summary:
    Initialise ssm_client using boto3.client("ssm")
    Use AWS parameter store to access/update the 'last_checked' parameter
    Use .put_parameter method to update (using Overwrite=TRUE) 
    the last_checked time each time extract_lambda_handler is run.
            
    """

    now=str(datetime.now())

    try:
        last_checked = ssm_client.put_parameter(
            Name = "last_checked",
            Value = now,
            Description='Timestamp of each Lambda execution',
            Type="String",
            Overwrite=True
        )
        return now
    except Exception as e:
        logger.error(f"There has been an error: {str(e)}")
        raise e
    


    
def get_bucket_name(): #completed with tests
    """
    Summary : this function should obtain the ingestion bucket name from the
    environment variables and return it.
    
    
    Returns:
    dict {"ingestion_bucket" : "funland-ingestion-bucket-......."}
    
    """

    bucket_name = os.environ.get("S3_INGESTION_BUCKET")
    print(bucket_name)
     
    return {"ingestion_bucket":f'{bucket_name}'}






    
    


         