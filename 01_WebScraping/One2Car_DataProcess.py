## Developement Description
'''
1. import all necessary package
2.
'''

#%% import all necessary package
import tkinter
from tkinter import filedialog
import pandas as pd
import numpy as np

import glob
import sys
import os
import re
import seaborn as sns
import matplotlib.pyplot as plt

from pathlib import Path
from datetime import date, datetime
import warnings
# Ignore all warnings
warnings.filterwarnings("ignore")

import pkg_resources
installed_packages = pkg_resources.working_set
# Create an empty DataFrame with columns
columns = ["Package", "Version"]
data_pkg = pd.DataFrame(columns=columns)

with open('requirements1.txt', 'w') as f:
    for package in installed_packages:
        f.write(f"{package.key}=={package.version}\n")
        entry = [package.key, package.version]
        row = pd.Series(entry, index=data_pkg.columns)
        data_pkg = data_pkg.append(row, ignore_index=True)
    print(data_pkg)

#%% Define column name
# To drop data column
col_drop = ["item.offers.seller.homeLocation.address.url"
,"item.offers.itemCondition"	
,"item.offers.availability"
,"item.offers.seller.@type"
,"item.offers.seller.homeLocation.@type"
,"item.offers.seller.homeLocation.address.@type"
,"item.offers.@type"
,"item.mainEntityOfPage"
,"item.url"        
,"item.author.@type"       
,"item.author.name"        
,"item.brand.@type"        
,"item.@type"           
,"item.additionalType"        
,"@type"        
,"Unnamed: 1"        
,"item.image"]
col_drop2 = ['n_Color',
 'Gear_Type',
 'Description_2',
 'Tmpsub_Model'
]
# To change column name 
col_name = ['PageNo',
 'Position',
 'Name',
 'Description',
 'Brand',
 'Model',
 'n_CarTypes',
 'fuelType',
 'seatingCapacity',
 'n_Color',
 'n_Price',
 'Currency',
 'addressLocality',
 'addressRegion']

#%% Define Function

def check_and_create_directory(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created.")
    else:
        print(f"Directory '{directory_path}' already exists.")

# to flag car color 
def color_flag(value):
	ls_colorhit = ['ขาว', 'ดำ']#'เทา', 'เงิน'
	if value in ls_colorhit:
		return "1"
	else:
		return "0"

root = tkinter.Tk()
root.withdraw() #use to hide tkinter window
def search_for_file_path ():
    currdir = os.getcwd()
    tempdir = filedialog.askdirectory(parent=root, initialdir=currdir, title='Please select a directory')
    if len(tempdir) > 0:
        print ("Path: %s" % tempdir)
    return tempdir

def calculate_price_statistics(df, lst_group, price_column='n_Price'):
    # Define a custom aggregation function for price difference
    def price_difference(x):
        return x.max() - x.min()
    # Group the DataFrame and calculate count, mean, min, max, and price differences
    agg_functions = {
        'Count': ('size'),
        'Mean_Price': ('mean'),
        'Min_Price': ('min'),
        'Max_Price': ('max'),
        'Diff_Price': (price_difference)
    }
    df_result = df.groupby(lst_group)[price_column].agg(**agg_functions).reset_index()
    df_result.columns = lst_group + list(agg_functions.keys())
    return df_result

#%%
# Determine date to save file
now = datetime.now() # current date and time
date_time = now.strftime("%Y%m%d")
print(date_time)

#%% Import Data from Web scraping on .CSV file
print('**************** Data Loading **********************')
root = search_for_file_path()
ls_path = [os.path.join(path, name) 
             for path, subdirs, files in os.walk(root) 
             for name in files if name.endswith(".csv")]
print(f'File Number: {len(ls_path)}')
print('**************************************')

# Load to dataframe
df_csv = [pd.read_csv(x) for x in ls_path]
df_noclean = pd.concat(df_csv)

# Drop colunm in dataframe and change column name
df_drop = df_noclean.drop(columns= col_drop)
df_drop.columns = col_name

#Save path setup
save_path = os.path.dirname(os.path.dirname(root))
save_path_target = f"03_DataSave/{date_time}_webone2car_original_noclean.csv"
save_path_result = os.path.dirname(os.path.join(save_path,save_path_target))
#Check directory folder
check_and_create_directory(save_path_result)
#Save Data No Cleaned
df_drop.to_csv(os.path.join(save_path,save_path_target), index= False, encoding='utf-8-sig')
print('***************** Check Data Amount *********************')
print(f'Raw Data Amount: {df_drop.shape[0]} rows : {df_drop.shape[1]} columns')

#%% Data Extraction to important features
df_extract= df_drop
# Columns to consider for duplicate detection (exclude "ignore_column")
columns_to_check = df_extract.columns.difference(['PageNo', 'Position'])
# Count duplicate rows based on the selected columns
duplicate_count = df_extract.duplicated(subset=columns_to_check).sum()
print("Total duplicate rows:", duplicate_count)
# Drop duplicated
df_extract = df_extract.drop_duplicates(subset=['PageNo', 'Position'])
# Data list to extract data from web scraping 
ls_WheelDrive = ['FWD' , 'RWD', '4WD' , 'AWD' ]
# Fuel pairing list
ls_oldFuel = list(df_extract['fuelType'].unique())
ls_newFuel = ['ดีเซล', 'เบนซิน', 'เบนซิน', 'ไฟฟ้า', 'Hybrid', 'เบนซิน + CNG/NGV']

# Extract data from description column
df_extract['n_Year'] = df_extract['Name'].str.extract(r'(\d{4})')
df_extract['Gear_Type']  = df_extract['Name'].str.extract(pat = r'([AM]T)')
# Replace data with a specific value
df_extract['Description_2'] = df_extract['Description'].str.replace('xxx', '000')
df_extract['Mile_km']  = df_extract['Description_2'].str.extract(r'(\d{1,3}(?:,\d{3}\s*)km)')
df_extract['Mile_km'] = df_extract['Mile_km'].str.replace('km', '')
df_extract['Color'] = df_extract['n_Color'].str.replace('สี', '')
df_extract['Gear'] = df_extract['Gear_Type'].str.replace('T', '')
#Engine Capacity
df_extract['CC'] = df_extract['Name'].str.extract(r'(\d+\.\d+)')
df_extract['n_WheelDrive'] = df_extract['Name'].str.extract("(" + "|".join(ls_WheelDrive) +")", expand=False)
df_extract['n_WheelDrive'] = df_extract['n_WheelDrive'].str.upper()
df_extract['Fuel'] = df_extract['fuelType'].replace(ls_oldFuel, ls_newFuel)
# Replace the matched pattern with an empty string in the 'Car Model' column
df_extract['Tmpsub_Model'] = df_extract['Name'].str.replace(r'\(ปี \d{2}-\d{2}\)', '')
df_extract['Tmpsub_Model'] = df_extract['Tmpsub_Model'].str.extract(r'(\d+\.\d+\s.*)')
df_extract['Gear_Type'] = df_extract['Gear_Type'].astype(str)
df_extract['n_CarTypes'] = df_extract['n_CarTypes'].astype(str)
df_extract['n_WheelDrive'] = df_extract['n_WheelDrive'].astype(str)
df_extract['Tmpsub_Model'] = df_extract['Tmpsub_Model'].astype(str)
df_extract['sub_Model'] = df_extract.apply(lambda row: row['Tmpsub_Model'].replace(row['Gear_Type'], '').replace(row['n_CarTypes'], '').replace(row['n_WheelDrive'], ''), axis=1)
df_extract['sub_Model'] = df_extract['sub_Model'] .str.replace(r'  ', ' ')
# Remove non-numeric characters (comma) and convert column A to float
df_extract['Mile_km'] = df_extract['Mile_km'].str.replace(',', '').astype(float)
# Determine color to favor( flag = 1) ['ขาว', 'ดำ'] color
df_extract['Color_flg'] = df_extract['Color'].map(color_flag)

#%% Data Cleaning 
# Determine Threshold to exclude data 
# Explore from data One2Car
Th_price = 1000 
Th_currency = ["THB"]
Th_cc = 0.0

# Exculde Data by Threshold
df_cleaned = df_extract[df_extract['n_Price'] >= Th_price]
df_cleaned = df_cleaned[df_cleaned['Currency'].isin(Th_currency)] 
df_cleaned = df_cleaned[df_cleaned['CC'].astype(float) > Th_cc] 
print(f'Number of All Data : {df_extract.shape[0]}')
print(f'Number of After Drop by Price_Th = {Th_price}: {df_cleaned.shape[0]}')
print(f'Number of After Drop by Currency : {df_cleaned.shape[0]}')
print(f'Number of After Drop by Mile_Th : {df_cleaned.shape[0]}')

df_cleaned['CC']= df_cleaned['CC'].astype(float) 
df_cleaned = df_cleaned.drop(columns= col_drop2)

#Save Data Cleaned
save_path_target = f"03_DataSave/{date_time}_webone2car_final_cleaned.csv"
df_cleaned.to_csv(os.path.join(save_path,save_path_target), index= False, encoding='utf-8-sig')

#%% Pricing Data Range
lst_group = ['Brand', 'Model','sub_Model', 'n_Year','Fuel']
lst_group2 = ['Brand', 'Model','sub_Model', 'n_Year','Fuel', 'Gear']

# Group the DataFrame and calculate count, min, and max values
df_price_o2c = calculate_price_statistics(df_cleaned, lst_group)

# col_name2 = ['Brand', 'Model','sub_Model', 'n_Year', 'Gear','Fuel']
# df_price_o2c.columns = col_name2

#Save Data Pricing List from One2Car
save_path_target = f"03_DataSave/{date_time}_webone2car_Final.csv"
df_price_o2c.to_csv(os.path.join(save_path,save_path_target), index= False, encoding='utf-8-sig')

#%% Data Summary
df_summ = df_price_o2c
# Top5 Car Amount by Brand
brand_counts = df_summ.groupby(['Brand']).size().reset_index(name='Count')
# Sort by Count in descending order to get the top brand
sorted_brands = brand_counts.sort_values(['Count'], ascending=[False])
# Get the top 5 models for each brand
top5_by_brand = sorted_brands.head(5)
print('***************** Top 5 By Brand *********************')
print(top5_by_brand)

# Top5 Car Amount by Brand and Model
brand_model_counts = df_summ.groupby(['Brand', 'Model']).size().reset_index(name='Count')
# Sort by Count in descending order to get the top models for each brand
sorted_brands = brand_model_counts.sort_values(['Brand', 'Count'], ascending=[True, False])
# Get the top 5 models for each brand
top5_models_by_brand = sorted_brands.groupby('Brand').head(5)
print('****************** Top 5 By Brand and Model ********************')
print(top5_models_by_brand)
