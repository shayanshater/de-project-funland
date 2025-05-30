import os
from dotenv import load_dotenv
from pg8000.native import Connection, identifier, literal, DatabaseError
from pprint import pprint
import csv
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)




load_dotenv()

def connect_to_db():
    return Connection(
        user=os.getenv("totesys_user"),
        password=os.getenv("totesys_password"),
        database=os.getenv("totesys_database"),
        host=os.getenv("totesys_host"),
        port=int(os.getenv("totesys_port"))
    )



tables_to_import = ["counterparty", "currency", "department", "design", "staff",
                    "sales_order", "address", "payment", "purchase_order",
                    "payment_type", "transaction"]

    
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

    if os.path.exists("last_ingested.txt"):
        with open("last_ingested.txt", "r") as f:
            last_ingested = f.read().strip()
    else:
        last_ingested = None


    
    #We’ll use this time at the end to say: "This is the last time I checked."
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")            
    

    try:
        for table in tables_to_import:
            if last_ingested is None:  
                table_data = conn.run(
                    f"SELECT * FROM {identifier(table)};")
            else:
                table_data = conn.run(
                    f"SELECT * FROM {identifier(table)} WHERE last_updated > {literal(last_ingested)}")                
            column_names = [column['name'] for column in conn.columns]
            formatted_tables_data = {
                'tables' : [dict(zip(column_names, t)) for t in table_data]
                }
            tables_data[table] = formatted_tables_data["tables"]
        
        #This updates our file so that next time we run the code, we know where we left off.
        with open("last_ingested.txt", "w") as f:
                f.write(timestamp)
    
        
        return tables_data
    
    except DatabaseError as err:
        return "database error found"
    finally:  
        conn.close()
    
tables_data=get_data_from_db(tables_to_import)


def convert_data_to_csv_files(tables_data):
    """
    This function will take a Python dictionary of data in multiple tables 
    (including tablename, column names within each table and the values), 
    and convert it to CSV file that will be stored on the landing bucket
    with the specified file format.    
    
    Args:
    boto3 s3 client
    output from "get_data_from_db" fuction
        
    returns:
    string stating what's been done: "file {filename} has been uploaded to landing bucket"
    """

    print(tables_data)
    
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") 
    
    for table in tables_data.keys():
        if not tables_data[table]:
            print(f"No new data for table '{table}', skipping.")
            continue
    
        header = tables_data[table][0].keys()
        with open(f"data/extract/{table}_{timestamp}.csv",'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=header)
            writer.writeheader()
            for row in tables_data[table]:
                writer.writerow(row)

convert_data_to_csv_files(tables_data)




def upload_csv_to_ingestion_bucket(file_name, bucket_name, s3_client, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)
    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(file_name,bucket_name,object_name)
    except ClientError as ce:
        logging.error(ce)
        print(f"Failed to upload '{file_name}' to bucket '{bucket_name}'.")
        return False
    print(f"Succesfully uploaded '{file_name}' to bucket '{bucket_name}'.")
    return True

# upload_csv_to_ingestion_bucket("data/extract/address.csv",
#                                "funland-ingestion-bucket-20250529092926665800000002",
#                                "s3_client")


def extract_lambda_handler(context):
    
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") 


     #calling the first function to get the data
    tables_data=get_data_from_db(tables_to_import)
    
    #calling the second function to convert the data we got from the first function as csv file
    convert_data_to_csv_files(tables_data)

    s3_bucket=os.getenv("s3_ingestion_bucket")
    
    s3_client=boto3.client("s3")

    for table in tables_data:
        if not tables_data[table]:
            continue
        file_name=f"data/extract/{table}_{timestamp}.csv"

    upload_csv_to_ingestion_bucket(file_name,s3_bucket,s3_client,object_name=None)

    return {
        'statusCode': 200,
        'body': 'Data extracted and uploaded'
    }

