import os
import logging
from pg8000.native import Connection, identifier, literal, DatabaseError
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
import json


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
    
    - update last checked variable ## figure out where to do this step so we have access to the latest files for transform step.
        test:
        - test to see if the function updates the 
        variable in the parameter store as expected
    
    - format the queried data into a python dictionary
        test:
        - the format of the dictionary is correct.
        - the tables in the dictionary are only the tables that are available.

    - convert this data into a json string
        test:
        - returns a string
    
    - obtain bucket name
        test:
        - that the bucket name starts with funland-ingestion-bucket-...... where the rest is numbers.
    
    - upload to s3 ingetion bucket using filename structure table/datetime.json 
    - log the success of the operation
        tests:
        - right filename?
        - successful upload message?
    

    Args:
        no inputs as this is the first lambda
        
        
    Returns:
        dictionary
        Optional:
            - maybe the old timestamp?
            - maybe a success message?
            {"timestamp":"2020....", "message":"extract successful"}
    """
    
    
def get_last_checked():
    """_summary_
    Acess the aws parameter store, and obtain the last_checked parameter.
    Store the parameter and its value in a dictionary and return it.
    
    Returns:
        dictionary of paramter name and value
        {"last_checked" : "2020...."}
    """
    
def get_db_credentials():
    """_summary_
    This functions should return a dictionary of all 
    the db credentials obtained from secret manager
    
    Returns:
    dictionary of credentials
    {"DB_USER":"totesys", DB_PASSWORD:".......}
    """
    client.get_secret(name = 'db_password') # do this for all the credentials
    #format it in a nice way
    #return it    



def create_db_connection(credentials):
    """ Summary:
    Connect to the totesys database using credentials fetched from 
    AWS Parameter Store (a separate function employed for this purpose).
    Uses Connection module from pg8000.native library 
    (from pg8000.native import Connection)



    Return Connection
    """
    return Connection(
        user = credentials["DB_USER"],
        password = credentials["DB_PASSWORD"],
        database = credentials["DB_NAME"],
        host = credentials["DB_HOST"],
        port = credentials["DB_PORT"]
    )
    
def extract_new_rows(last_checked, db_connection):
    """ 
    Summary :
        Use connection object to query for rows where 
        the last_updated is after our last_checked variable.
        
        Uses the queried output as the input for format_queried_data function
        and returns a formatted dictionary of tables, their columns and values.
    
    Args:
        last_checked (datetime object): 
        should be the datetime object store in parameter store
        
        db_connection:
        a connection object to the totesys database
    
    
    Returns:
        a dictionary of table_names and columns and values.
        {
        'address': [{'address_id': 1, ....} .... ],
        'counterparty': [{'commercial_contact': 'Micheal Toy', ...} ...] 
        }
    
    """
    table_data = conn.run(f"SELECT * FROM {identifier(table)};")
    formatted_table_data = format_queried_data(table_data,column_names,table_name)
    return formatted_table_data
    
    
    
    
def update_last_checked():
    """
    Summary:
    Use .put_parameter method to update (using Overwrite=TRUE) 
    the last_checked time each time get_data_from_db() func is run.
    Initialise ssm_client using boto3
    
    
    
    """
    
    
def format_queried_data():
    """
    
    """
    
    
    
def convert_dict_to_json_string(data):
    """
    
    """
    
    
def obtain_bucket_name():
    """
    Summary : this function should obtain the ingestion bucket name from the
    environment variables and return it.
    
    
    Returns:
    dict {"ingestion_bucket" : "funland-project-......."}
    
    """


def upload_files_to_s3():
    """
    
    """











def connect_to_db():
    return Connection(
        user=os.getenv("totesys_user"),
        password=os.getenv("totesys_password"),
        database=os.getenv("totesys_database"),
        host=os.getenv("totesys_host"),
        port=int(os.getenv("totesys_port"))
    )

# list of all tables in the db
tables_to_import = ["counterparty", "currency", "department", "design", "staff",
                    "sales_order", "address", "payment", "purchase_order",
                    "payment_type", "transaction"]


## AWS Param Store

def write_last_ingested_to_ssm():
    ssm_client = boto3.client('ssm')
    param_name = 'lambda_timestamp'
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    try:
        ssm_client.put_parameter(
            Name=param_name,
            Description='Timestamp of each Lambda execution',
            Value=timestamp,
            Type="String",
            Overwrite=True
        )
    except ClientError as essm:
        logger.error(f"SSM error: {str(essm)}")
        return { 'statusCode': 404, 'Body' : {str(essm)}}

def read_last_ingested_from_ssm(param_name):
    # Initialize the SSM client
    ssm_client = boto3.client('ssm')
    
    # Define the parameter name
    param_name = 'lambda_timestamp'
    
    # Retrieve the parameter value
    response = ssm_client.get_parameter(Name=param_name)
    print(response)
    read_last_ingested = response['Parameter']['Value']
    
    # Use the timestamp as needed
    print(f"Timestamp retrieved: {read_last_ingested}")
    return read_last_ingested
    
    # return {
    #     'statusCode': 200,
    #     'body': timestamp
    # }


# for table in tables_to_import:
#     last_updated = table[last_updated]

def get_data_from_db(tables_to_import):
    """
    connect to totesys db and query to extract the data from the relevant tables. 
    ARGS:
    db_name:
    password:
    name
      
    Returns:
    data from all tables as a Python dictionary
    
    """
    conn = connect_to_db()
    tables_data =  {}
    # last_ingestion = #refer to logs from lambda runs for timestamp
   
    #Check if it’s the first time we’re running

    # if os.path.exists("last_ingested.txt"): # check how to amend this to integrate AWS Parameter store for last checked time!!!
    #     with open("last_ingested.txt", "r") as f:
    #         last_ingested = f.read().strip()
    # else:
    #     last_ingested = None
    
    #We’ll use this time at the end to say: "This is the last time I checked."
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")            
    last_ingested = get_last_ingested_from_ssm(param_name)

    try:
        for table in tables_to_import:
            if last_ingested is None:  # meaning if this is the first ever run
                table_data = conn.run(
                    f"SELECT * FROM {identifier(table)};")
            else:
                table_data = conn.run(
                    f"SELECT * FROM {identifier(table)} WHERE last_updated > {literal(last_ingested)}") # use AWS param store!            
            column_names = [column['name'] for column in conn.columns]
            formatted_tables_data = {
                'tables' : [dict(zip(column_names, t)) for t in table_data]
                }
            tables_data[table] = formatted_tables_data["tables"]
        
        #This updates our file so that next time we run the code, we know where we left off.
        with open("last_ingested.txt", "w") as f:
                f.write(timestamp)
        # pprint(tables_data)
        return tables_data
    
    except DatabaseError as err:
        logger.error(f"Database error occured: {str(err)}")
        return {
            "statusCode": 404,
            "body": json.dumps(f"Database error occured: {str(err)}")
        }
    finally:  
        conn.close()
    
# tables_data_dict=data_from_db(tables_to_import)

def convert_to_json_and_upload_to_s3():
    """
    This function will take a Python dictionary of data in multiple tables 
    (including tablename, column names within each table and the values), which is the
    output of the function 'get_data_from_db'and convert it to json that will be stored on the landing bucket
    with the specified file format.    
    
    Args:
    boto3 s3 client
    output from "get_data_from_db" fuction
        
    returns:
    string stating what's been done: "file {filename} has been uploaded to landing bucket"
    """

    s3_client = boto3.client("s3")
    tables_data_dict = get_data_from_db(tables_to_import)
    tables_data_json = json.dumps(tables_data_dict).encode("utf-8")
       
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") 
    
    for table in tables_data_dict.keys():
        if not tables_data_dict[table]:
            print(f"No new data for table '{table}', skipping.")
            continue

        try:
            s3_client.put_object(
                Bucket = os.getenv("s3_ingestion_bucket"),
                Key = f"{table}_{timestamp}.json",
                Body = tables_data_json,
                ContentType = "application/json"
                )
            return {"status": "Json data successfully uploaded to s3"}
        
        except Exception as e:
            logger.error(e)
            print (f"Could not upload to S3")
            return {
                "statusCode": 500, #find the right code
                "body": json.dumps(f"Error occured: {str(e)}")
            }
            
def extract_lambda_handler(event, context):
    """
    lambda function runs previous functions to get the data from the DB, convert to json and upload to the S3 bucket. 

    args: 
    event - will be invoked every 15 mins (eventbridge/stepfunction)

    returns: 
    currently returns a dict listing the response code, and list of csv files uploaded. 

    TODO: check this return output (will be the event for next lambda function) 
    TODO: event for next lambda function will be triggered by the object being added to the s3. 

    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") 


     #calling the first function to get the data
    tables_data_dict = get_data_from_db(tables_to_import)
    
    #calling the second function to convert the data we got from the first function into json and upload to s3
    convert_to_json_and_upload_to_s3()

    s3_bucket=os.getenv("s3_ingestion_bucket")
    
    s3_client=boto3.client("s3")

    updated_tables = []

    # for table in tables_data.keys():
    #     if tables_data[table]: 
    #         updated_tables.append(table)

    # for table in tables_data:
    #     if not tables_data[table]:
    #         continue
    #     file_name=f"data/extract/{table}_{timestamp}.csv"
      #  updated_tables.append(file_name)

    # upload_csv_to_ingestion_bucket(file_name,s3_bucket,s3_client,object_name=None)

    return {
        'statusCode': 200,
        'body': {'csv_files_uploaded': [updated_tables]}
    }
