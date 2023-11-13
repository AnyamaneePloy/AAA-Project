import pyodbc
import pandas as pd
from datetime import datetime

# Set up a connection to the SQL Server
connDev = pyodbc.connect('Driver={SQL Server};Server=mbk-sqlsrv4;Database=MBKGROUPDataMart;Trusted_Connection=yes;')
connProd= pyodbc.connect("Driver={SQL Server};Server=AAA-DBSRV1;Database=AAAeAuction;Trusted_Connection=yes;")
curDev = connDev.cursor()
curProd = connProd.cursor()

# Path to the CSV file
# csv_file_path = r'D:\Users\anyamanee\Anyamanee_Work\01_AppleAutoAuction\07_Application\01_PriceMatching_Program_V2.0\_Data\Input\01_Database\20231019\03_One2Car_20231019.csv'

# Your SQL query from AAA production (read only)
queryProd = '''SELECT t1.VinNo, t1.ContractNo,
t1.ItemCode, t1.CompanyCode,
t1.BrandCode, b.BrandNameEng,
t1.ModelCode, im.ModelName, 
t1.SubModelCode,ism.SubModelName,
t1.ManufactureYear AS Year,
q.Quality AS Grade,
iq.Quality AS InQuality,
icc.CcName,
ig.GearName,
igt.GearTypeName AS drive,
dc.Color,
t1.ReceiveMiles AS MilesNo,
t1.ReceivedDate,
t1.UpdatedDate
FROM InGoods t1
LEFT JOIN DbBrand    b  WITH(NOLOCK) ON b.BrandCode = t1.BrandCode 
LEFT JOIN InModel    im  WITH(NOLOCK) ON im.BrandCode = t1.BrandCode   
                                    AND im.ModelCode = t1.ModelCode
LEFT JOIN InSubModel ism WITH(NOLOCK) ON ism.BrandCode = t1.BrandCode   
                    AND ism.ModelCode = t1.ModelCode
                    AND ism.SubModelCode = t1.SubModelCode
LEFT JOIN InQuality  q WITH(NOLOCK) ON q.CompanyCode = t1.CompanyCode  
                                    AND q.QualityCode = t1.QualityCode
LEFT JOIN InQuality  iq  WITH(NOLOCK) ON iq.CompanyCode = t1.CompanyCode
                                    AND iq.QualityCode = t1.InteriorQualityCode
LEFT JOIN InCc       icc WITH(NOLOCK) ON icc.CompanyCode = t1.CompanyCode 
                                    AND icc.CcID = t1.CcID
LEFT JOIN InGear     ig WITH(NOLOCK) ON ig.GearCode = t1.GearCode
INNER JOIN InGearType igt with(nolock) on t1.GearType = igt.GearType 
LEFT JOIN DbColor    dc WITH(NOLOCK) ON dc.ColorCode = t1.ColorCode'''  # abbreviated for clarity
curProd.execute(queryProd)
db_data = curProd.fetchall()
db_data = pd.DataFrame.from_records(db_data, columns=[x[0] for x in curProd.description])
connProd.close()

#%% Create New Table internal database for development
# Drop Table
# SQL command to drop a table
drop_table_command = "DROP TABLE IF EXISTS AAA_STG_ReceivedCarInfo"
curDev.execute(drop_table_command)
connDev.commit()
print("Table dropped successfully.")

# Create the table structure
curDev.execute(""" CREATE TABLE AAA_STG_ReceivedCarInfo(
	LoadDate datetime NULL,
	CompanyCode nvarchar(10) NULL,
	ItemCode nvarchar(20) NULL,
	VinNo nvarchar(50) NULL,
	ContractNo nvarchar(20) NULL,
	BrandCode nvarchar(10) NULL,
	BrandNameEng nvarchar(100) NULL,
	ModelCode nvarchar(10) NULL,
	ModelName nvarchar(100) NULL,
	SubModelCode nvarchar(10) NULL,
	SubModelName nvarchar(100) NULL,
	Year numeric(4, 0) NULL,
	Grade nvarchar(10) NULL,
	InQuality nvarchar(10) NULL,
	CcName nvarchar(100) NULL,
	GearName nvarchar(100) NULL,
	drive nvarchar(100) NULL,
	Color nvarchar(1000) NULL,
	MilesNo numeric(18, 2) NULL,
	ReceivedDate nvarchar(10) NULL,
	UpdatedDate datetime NULL)""")
curDev.commit()

# Read the DataFrame
df = db_data

# Add a column to the DataFrame for the current date and time
current_time = datetime.now()
df['LoadDate'] = current_time

# Move the 'Last_updated' column to the beginning of the DataFrame
cols = df.columns.tolist()
cols = [cols[-1]] + cols[:-1]
df = df[cols]
df.dropna(subset=['ReceivedDate'], inplace=True)
df['ReceivedDate'] = pd.to_datetime(df['ReceivedDate'], errors='coerce')
# df = df.fillna("")
# # Convert relevant columns to appropriate data types
# for col in ['Year', 'Count', 'Mean_Price', 'Median_Price', 'Min_Price', 'Max_Price', 'Diff_Price']:
#     df[col] = pd.to_numeric(df[col], errors='coerce')

# Manually insert data into the SQL Server table
placeholders = ",".join("?" * len(df.columns))
columns = ",".join(df.columns)
# Convert DataFrame to a list of tuples
data_to_insert = df.values.tolist()

# Execute batch insert with executemany
batch_size = 10000  # Adjust batch size according to your needs
for i in range(0, len(data_to_insert), batch_size):
    batch = data_to_insert[i:i + batch_size]
    sql = f"INSERT INTO AAA_STG_ReceivedCarInfo ({columns}) VALUES ({placeholders})"
    curDev.executemany(sql, batch)
    connDev.commit()  # Commit after each batch

# Verify the insertion
curDev.execute('SELECT * FROM AAA_STG_ReceivedCarInfo')
for row in curDev.fetchall():
    print(row)

connDev.close()
