def get_data_from_db():
    """
    connect to totesys db and query to extract the data from the relevant tables.
    
    
    ARGS:
    db_name:
    password:
    name
    
    
    Returns:
    JSON string
    {
        table_1: [{.... all, the, data, from, rows}]
    }
    """
    

def convert_data_to_json_files():
    """
    This function will take a json string of data in multiple tables, and convert it to Json file that will be 
    stored on the landing bucket with the specified file format.
    
    file format year/month/day/hour/minute/seconds.json
    
    
    Args:
    boto3 s3 client
    JSON string
    
    
    returns:
    string: "file {filename} has been uploaded to landing bucket"
    """ 
    