import os
from dotenv import load_dotenv
from pg8000.native import Connection, identifier, literal
from pprint import pprint
import csv


load_dotenv()

def connect_to_db():
    return Connection(
        user=os.getenv("totesys_user"),
        password=os.getenv("totesys_password"),
        database=os.getenv("totesys_database"),
        host=os.getenv("totesys_host"),
        port=int(os.getenv("totesys_port"))
    )

tables_to_import = ["counterparty", "currency", "department", "design", "staff", "sales_order",
                    "address", "payment", "purchase_order", "payment_type", "transaction"]

def get_data_from_db(tables_to_import):
    """
    connect to totesys db and query to extract the data from the relevant tables. 
    ARGS:
    db_name:
    password:
    name
      
    Returns:
    Python dictionary
    
    """
    conn = connect_to_db()
    tables_data =  {}

    for table in tables_to_import:
        table_data = conn.run(
            f"SELECT * FROM {identifier(table)} LIMIT 2;"
        )
        column_names = [column['name'] for column in conn.columns]
        formatted_tables_data = {
            'tables' : [dict(zip(column_names, t)) for t in table_data]
            }
        tables_data[table] = formatted_tables_data["tables"]
        
    conn.close()

    return tables_data

    
# get_data_from_db(tables_to_import)


def convert_data_to_csv_files():
    """
    This function will take a Python dictionary of data in multiple tables 
    (including tablename, column names within each table and the values), 
    and convert it to CSV file that will be stored on the landing bucket
    with the specified file format.
    
    file format year/month/day/hour/minute/seconds.json
    
    
    Args:
    boto3 s3 client
    output from "get_data_from_db" fuction
        
    returns:
    string stating what's been done: "file {filename} has been uploaded to landing bucket"
    """
    tables_data = get_data_from_db(tables_to_import)
    header = ['list of column names']
    writer = csv.DictWriter(file, fieldnames=header)
    writer.writeheader()
    """
    iterate through each k:v of the tables_data dict, where k is a table from totesys db.
    
    """
    for values in tables_data["outer key"]["inner keys"]:
        writer.writerow(values)


    pass