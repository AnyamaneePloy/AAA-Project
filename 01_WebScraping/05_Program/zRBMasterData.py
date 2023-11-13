import pandas as pd
import pyodbc
from datetime import datetime

class RBMasterData:
    def __init__(self):
        # Database connection string
        self.conn_str = '''Driver={SQL Server};
        Server=mbk-sqlsrv4;
        Database=MBKGROUPDataMart; 
        UID=user_di_aaa;
        PWD=Rpt!2023#;'''
        self.today = datetime.now()

    def get_data_from_db(self):
        # Connect to the database and fetch data, then convert the data to a pandas DataFrame
        try:
            with pyodbc.connect(self.conn_str) as conn:
                # Your SQL query
                query = '''SELECT * FROM [dbo].[AAA_STG_MasterRedBook]'''  
                self.db_data = pd.read_sql(query, conn)
                return self.db_data 
        except pyodbc.Error as e:
            print("Database error:", e)
        except Exception as e:
            print("Error:", e)

    def count_duplicates(self, subset=None):
        # Count duplicate rows based on the subset of columns
        if hasattr(self, 'db_data'):
            return self.db_data.duplicated(subset=subset).sum()
        else:
            print("Data not loaded. Run get_data_from_db first.")
            return 0

    def drop_duplicates(self, subset=None, keep='last', inplace=False):
        # Removes duplicate rows from the DataFrame self.db_data
        if hasattr(self, 'db_data'):
            if inplace:
                self.db_data.drop_duplicates(subset=subset, keep=keep, inplace=True)
                return self.db_data
            else:
                return self.db_data.drop_duplicates(subset=subset, keep=keep)
        else:
            print("Data not loaded. Run get_data_from_db first.")
            return None
        
