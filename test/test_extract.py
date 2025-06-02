from src.extract import get_data_from_db, convert_data_to_csv_files, connect_to_db, upload_csv_to_ingestion_bucket, extract_lambda_handler 
import pytest
from pg8000.native import DatabaseError

@pytest.fixture(scope="module")
def conn():
    '''Runs seed before starting tests, yields, runs tests,
       then closes connection to db'''
    conn = connect_to_db()
    yield conn
    conn.close()

@pytest.fixture(scope="module")
def tables_to_import():
    '''Runs seed before starting tests, yields, runs tests,
       then closes connection to db'''
    return ["counterparty", "currency", 
                    "department", "design", "staff", "sales_order",
                    "address", "payment", "purchase_order", 
                    "payment_type", "transaction"]


class TestGetDataFromDB:
    def test_all_tables_exist(self, conn):
        tables_to_check = ["counterparty", "currency", 
                    "department", "design", "staff", "sales_order",
                    "address", "payment", "purchase_order", 
                    "payment_type", "transaction"]
        
        
        for table_name in tables_to_check:
            base_query = f"""SELECT EXISTS (SELECT FROM information_schema.tables \
                        WHERE table_name = '{table_name}')"""
            expect = conn.run(base_query)
            assert expect == [[True]]


    def test_get_data_from_db_returns_dictionary_with_correct_tables_and_row_data(self, tables_to_import):

        tables_data = get_data_from_db(tables_to_import)

        
        #assert isinstance(tables_data, dict)
        assert list(tables_data.keys()) == ["counterparty", "currency", 
                    "department", "design", "staff", "sales_order",
                    "address", "payment", "purchase_order", 
                    "payment_type", "transaction"]
        
        for table in tables_data.keys():
            for row in tables_data[table]:
                assert isinstance(row,dict)
                
                
    def test_get_data_from_db_gives_error(self, tables_to_import):
        tables_to_import[0] = 'ounterparty'
        assert get_data_from_db(tables_to_import) == "database error found"
    

        

            

        
        
        
        
    
            

