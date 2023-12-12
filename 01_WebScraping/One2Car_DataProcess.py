## Developement Description
'''
1. import all necessary package
2.
'''

#%% import all necessary package
import tkinter as tk
from tkinter import filedialog, ttk
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

import glob
import sys
import os
import re
import seaborn as sns

from pathlib import Path
from datetime import date, datetime
import warnings
# Ignore all warnings
warnings.filterwarnings("ignore")

# import pkg_resources
# installed_packages = pkg_resources.working_set
# # Create an empty DataFrame with columns
# columns = ["Package", "Version"]
# data_pkg = pd.DataFrame(columns=columns)

# with open('requirements1.txt', 'w') as f:
#     for package in installed_packages:
#         f.write(f"{package.key}=={package.version}\n")
#         entry = [package.key, package.version]
#         row = pd.Series(entry, index=data_pkg.columns)
#         data_pkg = data_pkg.append(row, ignore_index=True)
#     print(data_pkg)

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

col_drop2 = ['Gear_Type',
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
 'CarTypes',
 'fuelType',
 'seatingCapacity',
 'Color',
 'Price',
 'Currency',
 'addressLocality',
 'addressRegion',
 'FolderName',
 'CreatedDate']

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

# Select Folder Function
def save_selected_path(selected_path):
    with open("selected_path.txt", "w") as file:
        file.write(selected_path)

def load_selected_path():
    if os.path.exists("selected_path.txt"):
        with open("selected_path.txt", "r") as file:
            return file.read()
    return None

def search_for_file_path ():
    try:
        prev_selected_path = load_selected_path()
        root = tk.Tk()
        root.withdraw() # use to hide tkinter window
        tempdir = filedialog.askdirectory(parent=root, initialdir=prev_selected_path, 
                                          title='Please select directory of Master file')
        if len(tempdir) > 0:
            print("Path: %s" % tempdir)
        save_selected_path(tempdir)
        return tempdir
    except Exception as e:
        print("An error occurred while selecting the directory. Please ensure it's a valid directory.")
        print(str(e))
        return None

def calculate_price_statistics(df, lst_group, price_column='Price'):
    # Define a custom aggregation function for price difference
    def price_difference(x):
        return x.max() - x.min()
    # Define a custom aggregation function for mode
    def mode(x):
        mode_series = x.mode()
        # If there's only one mode, return it. Otherwise, return the entire series as a list.
        if len(mode_series) == 1:
            return mode_series.iloc[0]
        else:
            return mode_series.tolist()

    # Group the DataFrame and calculate count, mean, min, max, price differences, and mode
    agg_functions = {
        'Count': 'size',
        'Mean_Price': lambda x: int(x.mean()),
        'Median_Price': lambda x: int(x.median()),
        'Min_Price': 'min',
        'Max_Price': 'max',
        'Diff_Price': price_difference
    }

    df_result = df.groupby(lst_group)[price_column].agg(**agg_functions).reset_index()
    df_result.columns = lst_group + list(agg_functions.keys())
    return df_result

#%%
# Determine date to save file
now = datetime.now() # current date and time
date_time = now.strftime("%Y%m%d") #_%H%M")
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
df_noclean = []
# Add folder name column to each dataframe in df_csv
for idx, df in enumerate(df_csv):
    folder_name = os.path.basename(os.path.dirname(os.path.dirname(ls_path[idx])))
    df['folder_name'] = folder_name
    df_noclean.append(df)

df_noclean = pd.concat(df_noclean)
grouped_counts = df_noclean.groupby('folder_name').size().reset_index(name='counts')
print(grouped_counts)

# Convert the 'date_string' column to datetime
df_noclean['CreatedDate'] = pd.to_datetime(df_noclean['folder_name'], format='%Y%m%d')
# Drop colunm in dataframe and change column name
df_drop = df_noclean.drop(columns= col_drop)
df_drop.columns = col_name

df_noclean = df_drop.sort_values(by='FolderName')
cols_to_exclude = ['FolderName', 'PageNo', 'Position','CreatedDate']# Exclude 'folder_name', 'PageNo', and 'Position' when checking for duplicates
cols_for_duplicate_check = [col for col in df_noclean.columns if col not in cols_to_exclude]

results = []
previous_group_data = None
# Iterate over each folder
for folder, group in df_noclean.groupby('FolderName'):
    if previous_group_data is not None:
        merged = pd.concat([previous_group_data, group]).reset_index(drop=True)  # reset index here
        duplicates_in_group = merged[merged.duplicated(subset=cols_for_duplicate_check)]
        results.append({'FolderName': folder, 'duplicate_counts': len(duplicates_in_group)})
        previous_group_data = merged
    else:
        previous_group_data = group
# Convert results to DataFrame
df_results = pd.DataFrame(results)
print(df_results)

#Save path setup
save_path = os.path.dirname(os.path.dirname(root))
save_path_target = f"03_DataSave/03_One2Car_{date_time}_original.csv"
save_path_result = os.path.dirname(os.path.join(save_path,save_path_target))
#Check directory folder
check_and_create_directory(save_path_result)
#Save Data No Cleaned
df_drop.to_csv(os.path.join(save_path,save_path_target), index= False, encoding='utf-8-sig')
print('***************** Check Data Amount *********************')
print(f'Raw Data Amount: {df_drop.shape[0]} rows : {df_drop.shape[1]} columns')

#%% Data Extraction to important features
df_extract= df_drop
columns_to_check = df_extract.columns.difference(['PageNo', 'Position', 'FolderName','CreatedDate'])
# Count duplicate rows based on the selected columns
duplicate_count = df_extract.duplicated(subset=columns_to_check).sum()
print("Total duplicate rows:", duplicate_count)
# Drop duplicated
# Columns to consider for duplicate detection (exclude "ignore_column")
columns_to_consider = [col for col in df_extract.columns if col not in ['PageNo', 'Position','FolderName']]
df_extract = df_extract.drop_duplicates(subset=columns_to_consider)
# Data list to extract data from web scraping 
ls_WheelDrive = ['FWD' , 'RWD', '4WD' , 'AWD' ]
# Fuel pairing list
ls_oldFuel = list(df_extract['fuelType'].unique())
ls_newFuel = ['ดีเซล', 'เบนซิน', 'เบนซิน', 'ไฟฟ้า', 'Hybrid', 'เบนซิน + CNG/NGV']

# Extract data from description column
df_extract['Year'] = df_extract['Name'].str.extract(r'(\d{4})')
df_extract['Gear_Type']  = df_extract['Name'].str.extract(pat = r'([AM]T)')
# Replace data with a specific value
df_extract['Description_2'] = df_extract['Description'].str.replace('xxx', '000')
df_extract['Mile_km']  = df_extract['Description_2'].str.extract(r'(\d{1,3}(?:,\d{3}\s*)km)')
df_extract['Mile_km'] = df_extract['Mile_km'].str.replace('km', '')
df_extract['Color'] = df_extract['Color'].str.replace('สี', '')
#Engine Capacity
df_extract['CC'] = df_extract['Name'].str.extract(r'(\d+\.\d+)')
df_extract['WheelDrive'] = df_extract['Name'].str.extract("(" + "|".join(ls_WheelDrive) +")", expand=False)
df_extract['WheelDrive'] = df_extract['WheelDrive'].str.upper()
df_extract['Fuel'] = df_extract['fuelType'].replace(ls_oldFuel, ls_newFuel)
# Replace the matched pattern with an empty string in the 'Car Model' column
df_extract['Tmpsub_Model'] = df_extract['Name'].str.replace(r'\(ปี \d{2}-\d{2}\)', '')
df_extract['Tmpsub_Model'] = df_extract['Tmpsub_Model'].str.extract(r'(\d+\.\d+\s.*)')
df_extract['Gear_Type'] = df_extract['Gear_Type'].astype(str)
df_extract['Gear'] = df_extract['Gear_Type']
df_extract['CarTypes'] = df_extract['CarTypes'].astype(str)
df_extract['WheelDrive'] = df_extract['WheelDrive'].astype(str)
df_extract['Tmpsub_Model'] = df_extract['Tmpsub_Model'].astype(str)
df_extract['SubModel'] = df_extract.apply(lambda row: row['Tmpsub_Model'].replace(row['Gear_Type'], '').replace(row['CarTypes'], '').replace(row['WheelDrive'], ''), axis=1)
from collections import OrderedDict  
# Using OrderedDict to maintain order of words while removing duplicates
def remove_duplicate_words(s):
    return ' '.join(OrderedDict.fromkeys(s.split()))
df_extract['SubModel'] = df_extract['SubModel'].str.replace(r'[\u0E00-\u0E7F]+', '')# 1. Remove Thai words
df_extract['SubModel'] = df_extract['SubModel'].str.replace('null', '')# 2. Replace "null"
df_extract['SubModel'] = df_extract['SubModel'].str.replace(r'  ', ' ')# 3. Replace double spaces
df_extract['SubModel'] = df_extract['SubModel'].str.replace(r'(\d+\.\d+) ([a-zA-Z0-9\s]+) \1', r'\1 \2') # 4. Remove repeated decimal numbers with text in between
df_extract['SubModel'] = df_extract['SubModel'].str.replace(r'(\d+\.\d+) \1', r'\1')# 5. Remove direct repeated decimal numbers
df_extract['SubModel'] = df_extract['SubModel'].str.replace(r'(\d+\.\d+) +\1', r'\1')
df_extract['SubModel'] = df_extract['SubModel'].str.replace(r'\s+', ' ').str.strip()
df_extract['SubModel'] = df_extract['SubModel'].apply(remove_duplicate_words)
df_extract['SubModel'] = df_extract['SubModel'].str.replace(r'\(|\)| -', '', regex=True).str.strip()# Removing () and -
df_extract['SubModel'] = df_extract['SubModel'].str.replace(r'  ', ' ')# 3. Replace double spaces
df_extract['SubModel'] = df_extract['SubModel'].str.strip()
# Remove non-numeric characters (comma) and convert column A to float
df_extract['Mile_km'] = df_extract['Mile_km'].str.replace(',', '').astype(float)
df_extract['Color_flg'] = df_extract['Color'].map(color_flag)# Determine color to favor( flag = 1) ['ขาว', 'ดำ'] color
# Convert 'Year' column to integer
df_extract['Year'] = pd.to_numeric(df_extract['Year'], errors='coerce')
df_extract['CarAge'] = df_extract['CreatedDate'].dt.year - df_extract['Year']

#%% Data Cleaning 
# Explore from data One2Car
Th_price = 100000 # based on data
Th_currency = ["THB"]
Th_cc = 0.0

# Exculde Data by Threshold
df_cleaned = df_extract[df_extract['Price'] >= Th_price]
df_cleaned = df_cleaned[df_cleaned['Currency'].isin(Th_currency)] 
df_cleaned = df_cleaned[df_cleaned['CC'].astype(float) > Th_cc] 
print(f'Number of All Data : {df_extract.shape[0]}')
print(f'Number of After Drop by Price_Th = {Th_price}: {df_cleaned.shape[0]}')
print(f'Number of After Drop by Currency : {df_cleaned.shape[0]}')
print(f'Number of After Drop by Mile_Th : {df_cleaned.shape[0]}')

df_cleaned['CC']= df_cleaned['CC'].astype(float) 
df_cleaned = df_cleaned.drop(columns= col_drop2)

#Save Data Cleaned
save_path_target = f"03_DataSave/03_One2Car_{date_time}_cleaned.csv"
df_cleaned.to_csv(os.path.join(save_path,save_path_target), index= False, encoding='utf-8-sig')

#%% Pricing Data Range
lst_group = ['Brand', 'Model', 'SubModel', 'Year', 'CarAge', 'Gear', 'Fuel', 'Color_flg', 'CarTypes']

# Group the DataFrame and calculate count, min, and max values
df_price_o2c = calculate_price_statistics(df_cleaned, lst_group)

#Save Data Pricing List from One2Car
save_path_target = f"03_DataSave/03_One2Car_{date_time}.csv"
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
