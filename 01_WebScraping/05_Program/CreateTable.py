import pyodbc
import pandas as pd
from datetime import datetime

# Path to the CSV file
csv_file_path = r'D:\Users\anyamanee\Anyamanee_Work\01_AppleAutoAuction\07_Application\01_PriceMatching_Program_V2.0\_Data\Input\01_Database\20231019\03_One2Car_20231019.csv'

# Set up a connection to the SQL Server
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=mbk-sqlsrv4;'
                      'Database=MBKGROUPDataMart;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()

# Create the table structure
# cursor.execute("""
# CREATE TABLE AAA_STG_One2Car (
#                 Last_updated DATETIME NULL,
#                 Brand nvarchar(255) NULL,
#                 Model nvarchar(255) NULL,
#                 SubModel nvarchar(255) NULL,
#                 Year numeric(4, 0) NULL,
#                 Gear nvarchar(100) NULL,
#                 Fuel nvarchar(100) NULL,
#                 Color_flg nvarchar(20) NULL,
#                 CarTypes nvarchar(100) NULL,
#                 Count bigint NULL,
#                 Mean_Price numeric(18, 2) NULL,
#                 Median_Price numeric(18, 2) NULL,
#                 Min_Price numeric(18, 2) NULL,
#                 Max_Price numeric(18, 2) NULL,
#                 Diff_Price numeric(18, 2) NULL
#             )""")
conn.commit()

# Read the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Add a column to the DataFrame for the current date and time
current_time = datetime.now()
df['Last_updated'] = current_time

# Move the 'Last_updated' column to the beginning of the DataFrame
cols = df.columns.tolist()
cols = [cols[-1]] + cols[:-1]
df = df[cols]
df = df.fillna("")
# Convert relevant columns to appropriate data types
for col in ['Year', 'Count', 'Mean_Price', 'Median_Price', 'Min_Price', 'Max_Price', 'Diff_Price']:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Manually insert data into the SQL Server table
placeholders = ",".join("?" * len(df.columns))
columns = ",".join(df.columns)
for index, row in df.iterrows():
    sql = f"INSERT INTO AAA_STG_One2Car ({columns}) VALUES ({placeholders})"
    cursor.execute(sql, row.tolist())
conn.commit()

# Verify the insertion
cursor.execute('SELECT * FROM AAA_STG_One2Car')
for row in cursor.fetchall():
    print(row)

conn.close()
