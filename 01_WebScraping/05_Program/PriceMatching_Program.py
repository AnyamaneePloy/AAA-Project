#%% Import Library
import pandas as pd
import numpy as np
import os
import re
import tkinter as tk
import warnings
warnings.filterwarnings("ignore") # Ignore all warnings

from itertools import combinations
from fuzzywuzzy import fuzz as fw_fuzz, process as fw_process
from rapidfuzz import fuzz, process
from multiprocessing import Pool
from datetime import datetime
from tkinter import filedialog, messagebox

from zDataMatcher import DataMatcher
from zDataMerger import DataMerger
from zDMSDataMerger import DMSDataMerger
from zDMSMasterData import DMSMasterData
from zRBMasterData import RBMasterData
from zDMSAggPriceResult import DMSAggPriceResult
from zDataSellerMapping import DataSellerMapping
from zO2CPriceData import O2CPriceData

#%% S1: Function Defined
#%% S1.1: Folder and File Selection Function
def save_selected_path(selected_path):
    with open("selected_path.txt", "w") as file:
        file.write(selected_path)

def load_selected_path():
    if os.path.exists("selected_path.txt"):
        with open("selected_path.txt", "r") as file:
            return file.read()
    return None

def check_and_create_directory(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created.")
    else:
        print(f"Directory '{directory_path}' already exists.")

def search_for_file_path():
    try:
        prev_selected_path = load_selected_path()
        root = tk.Tk()
        root.withdraw() # use to hide tkinter window
        tempdir = filedialog.askdirectory(parent=root, initialdir=prev_selected_path, 
                                          title='Please select directory of Master file')
        if len(tempdir) > 0:
            print("Path: %s" % tempdir)
        
        if not tempdir:
            return None
        save_selected_path(tempdir)
        return tempdir
    except Exception as e:
        print("An error occurred while selecting the directory. Please ensure it's a valid directory.")
        print(str(e))
        return None

def select_files():
    try:
        prev_selected_path = load_selected_path()
        root = tk.Tk()
        root.withdraw()  # use to hide tkinter window
        file_paths = filedialog.askopenfilenames(parent=root, initialdir=prev_selected_path, 
                                                 title='Please select files', 
                                                 filetypes=(("Excel files", "*.xlsx"), 
                                                            ("CSV files", "*.csv"), 
                                                            ("All files", "*.*")))   
        
        if not file_paths or not all([file_path.endswith(('.xlsx', '.csv')) for file_path in file_paths]):
            raise ValueError("Invalid file format selected. Please select .xlsx or .csv files only.")
        
        if len(file_paths) > 0:
            directory = os.path.dirname(file_paths[0])
            print("Selected Files:")
            for file_path in file_paths:
                print(file_path)
            save_selected_path(directory)  
        return file_paths
    except Exception as e:
        print("An error occurred while selecting the files. Please ensure you've selected the correct format.")
        print(str(e))
        return []
        
#%% S1.2: Fuzzy Matching Text Function
def find_best_match(target_string, choices):
    # This function finds the best match for a given target_string from a list of choices
    return max(choices, key=lambda choice: fuzz.ratio(target_string, choice))

def parallel_fuzzy_matching(data, choices, n_processes):
    with Pool(n_processes) as pool:
        results = pool.map(find_best_match, [(row, choices) for row in data])
    return results

def fuzzy_match_with_lib_choice(source_df, target_df, source_col, target_col, colname, library="rapidfuzz", threshold=100, limit=1):
    s = source_df[source_col].tolist()
    if library == "rapidfuzz":
        m = target_df[target_col].apply(lambda x: process.extract(x, s, limit=limit, scorer=fuzz.ratio))
    elif library == "fuzzywuzzy":
        m = target_df[target_col].apply(lambda x: fw_process.extract(x, s, limit=limit, scorer=fw_fuzz.ratio))
    else:
        raise ValueError("Invalid library choice. Choose either 'rapidfuzz' or 'fuzzywuzzy'.")
    
    target_df[f'{colname}'] = m.apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))  
    no_matches_count = target_df[target_df[f'{colname}'] == ""].shape[0]
    percent_data_loss = (no_matches_count / target_df.shape[0]) * 100
    print(f"Percent data completed for {colname}: {100-percent_data_loss:.2f}%")
    
    return target_df, percent_data_loss

def run_fuzzy_match_with_lib_choice(df, source, target, columns, library="rapidfuzz", thresholds=100, limit=1):
    for source_df, target_df, source_col, target_col, colname in columns:
        df, _ = fuzzy_match_with_lib_choice(source_df, target_df, source_col, target_col, colname, library, thresholds, limit)
    return df

#%% S1.3: Find Unique Data Function
def create_unique_brands_dataframe(dataframe, column_names, new_column_suffixes, new_column_prefix):
    unique_data = {}
    # Determine the combinations of unique values for brand and model columns
    unique_combinations = dataframe[column_names].drop_duplicates().reset_index(drop=True)
    
    for col, suffix, prefix in zip(column_names, new_column_suffixes, new_column_prefix):
        unique_data[f'{prefix}_{suffix}'] = unique_combinations[col].tolist()
        
    return pd.DataFrame(unique_data)

def process_dataframes_and_print(df_list, col_lists, suffix_lists, prefix_lists):
    if not (len(df_list) == len(col_lists) == len(suffix_lists) == len(prefix_lists)):
        raise ValueError("All input lists should have the same length.")
    
    result_dfs = []

    for df, cols, suffixes, prefix in zip(df_list, col_lists, suffix_lists, prefix_lists):
        unique_df = create_unique_brands_dataframe(df, cols, suffixes, prefix)
        result_dfs.append(unique_df)
        # Print the results for visual check
        # print(unique_df.head(3))
        # print(f'{prefix[-1]}_{suffixes[-1]} :\t', len(unique_df))

    return result_dfs
#%% S1.4: Save File Function
def save_master_list_to_csv(master_df, category, save_path_base, current_date_time, datatype, namefile=None):
    if datatype == "Master":
        save_path_target = f"Input/04_Master/{category}_MasterLst.csv"
    else:
        save_path_target = f"Output/{current_date_time}_{namefile}.csv"
    
    full_save_path = os.path.join(save_path_base, save_path_target)
    path_target = os.path.dirname(full_save_path)
    check_and_create_directory(path_target)
    master_df.to_csv(full_save_path, index=False, encoding='utf-8-sig')
    
    return full_save_path

def process_and_print(df_list, col_lists, suffix_lists, prefix_lists):
    return process_dataframes_and_print(df_list, col_lists, suffix_lists, prefix_lists)

def match_table(df_list, col_lists, suffix_lists, prefix_lists):
    return process_dataframes_and_print(df_list, col_lists, suffix_lists, prefix_lists)

#%% S1.5: Main Function to match Brand, Model and Submodel (DMS, RB, One2Car)
def main():
    while True:  
        # Load Data from Database
        try:
            #------------------------------------------------------------------------------
            print("===== Loading Data from Database =====")
            ## One2Car MASTER -----------------------------------------
            print("=============================================")
            print("Loading One2Car Data")
            df_O2Cprocessor = O2CPriceData()
            df_O2C = df_O2Cprocessor.get_data_from_db()  # Call the method with parentheses
            print("One2Car Master Data:", df_O2C.shape)
            # print(df_O2C.head(3))
            # Define the columns to check for duplicates
            columns = ['Brand', 'Model', 'SubModel', 'Year', 'Gear', 'Fuel', 'Color_flg', 'CarTypes', 'Count',
                       'Mean_Price', 'Median_Price', 'Min_Price', 'Max_Price', 'Diff_Price']     
            print("Number of duplicate rows before removal:", df_O2Cprocessor.count_duplicates(subset=columns))
            df_O2C = df_O2Cprocessor.drop_duplicates(subset=columns, inplace=True)
            print("Number of duplicate rows after removal:", df_O2Cprocessor.count_duplicates(subset=columns))
            print()

            ## DMS MASTER -----------------------------------------
            print("=============================================")
            print("Loading DMS MASTER Data")
            df_DMSprocessor = DMSMasterData()
            df_DMS = df_DMSprocessor.get_data_from_db()  # Call the method with parentheses
            print("DMS Master Data:", df_DMS.shape )
            # print( df_DMS.head(3))
            # Define the columns to check for duplicates
            columns = ['BrandCode', 'BrandNameEng', 'ModelCode', 'ModelName', 'SubModelCode', 'SubModelName']     
            print("Number of duplicate rows before removal:", df_DMSprocessor.count_duplicates(subset=columns))
            df_DMS = df_DMSprocessor.drop_duplicates(subset=columns, inplace=True)
            print("Number of duplicate rows after removal:", df_DMSprocessor.count_duplicates(subset=columns))
            print()

            ## RedBook MASTER -----------------------------------------
            print("=============================================")
            print("Loading RedBook MASTER Data")
            df_RBprocessor = RBMasterData()
            df_RB = df_RBprocessor.get_data_from_db()
            # Define the columns to check for duplicates
            columns = ['RBBandCode', 'RBBrand', 'RBModel', 'RBSubModel']
            print("RedBook Master Data:", df_RB.shape)
            # print(df_RB.head(3))
            print("Number of duplicate rows before removal:", df_RBprocessor.count_duplicates(subset=columns))
            df_RB = df_RBprocessor.drop_duplicates(subset=columns, inplace=True)
            print("Number of duplicate rows after removal:", df_RBprocessor.count_duplicates(subset=columns))
            print()

            ## DMS Price -----------------------------------------
            print("=============================================")
            print("Loading Price of DMS Database")
            df_DMSPriceprocessor = DMSAggPriceResult()
            df_DMSPrice = df_DMSPriceprocessor.run()
            print("Price of DMS Database:", df_DMSPrice.shape)
            print()
            print("=============================================")
            print("Display Matching Percentage of Brand, Model and Submodel")
            print()
            # print( df_DMSPrice.head(3))
        except Exception as e:
            print(f"Error reading CSV files: {e}")
            continue
        ## Start to Matching Data
        # Brands
        uniBrands_df_DMS, uniBrands_df_O2C, uniBrands_df_RB = process_dataframes_and_print(
            [df_DMS, df_O2C, df_RB], 
            [['BrandCode','BrandNameEng'], ['Brand'], ['RBBrand']], 
            [['DMS','DMS'], ['O2C'], ['RB']],
            [['BrandCode','BrandNameEng'], ['BrandNameEng'], ['BrandNameEng']], 
        )
        columns_brands = [
            (uniBrands_df_O2C, uniBrands_df_DMS, 'BrandNameEng_O2C', 'BrandNameEng_DMS', 'BrandNameEng_O2C'),
            (uniBrands_df_RB, uniBrands_df_DMS, 'BrandNameEng_RB', 'BrandNameEng_DMS', 'BrandNameEng_RB')
        ]
        MasterLst_Brand = run_fuzzy_match_with_lib_choice(df_DMS, df_O2C, df_RB, columns_brands,library="fuzzywuzzy", thresholds=90)
        # save_master_list_to_csv(MasterLst_Brand, 'Brand', save_path, date_time, "Master")

        # Models
        uniModel_df_DMS, uniModel_df_O2C, uniModel_df_RB = process_dataframes_and_print(
            [df_DMS, df_O2C, df_RB], 
            [['BrandCode', 'ModelCode','ModelName'], ['Brand','Model'], ['RBBrand','RBModel']], 
            [['DMS', 'DMS', 'DMS'], ['O2C', 'O2C'], ['RB', 'RB']],
            [['BrandCode', 'ModelCode', 'ModelName'], ['BrandCode','ModelName'], ['BrandCode','ModelName']]
            )
        columns_models = [
            (uniModel_df_O2C, uniModel_df_DMS, 'ModelName_O2C', 'ModelName_DMS', 'ModelName_O2C'),
            (uniModel_df_RB, uniModel_df_DMS, 'ModelName_RB', 'ModelName_DMS', 'ModelName_RB')
        ]
        MasterLst_Model = run_fuzzy_match_with_lib_choice(df_DMS, df_O2C, df_RB, columns_models,library="fuzzywuzzy", thresholds=90)
        # save_master_list_to_csv(MasterLst_Model, 'Model', save_path, date_time,"Master")

        # Sub-Model
        uniSubModel_df_DMS, uniSubModel_df_O2C, uniSubModel_df_RB = process_dataframes_and_print(
            [df_DMS, df_O2C, df_RB], 
            [['BrandCode','ModelCode', 'SubModelCode','SubModelName'], ['Brand','Model','SubModel'], ['RBBrand','RBModel','RBSubModel']], 
            [['DMS', 'DMS', 'DMS', 'DMS'], ['O2C', 'O2C', 'O2C'], ['RB', 'RB', 'RB']],
            [['BrandCode', 'ModelCode', 'SubModelCode','SubModelName'],
            ['BrandCode', 'ModelCode','SubModelName'], ['BrandCode', 'ModelCode','SubModelName']]
            )
        columns_submodels = [
            (uniSubModel_df_O2C, uniSubModel_df_DMS, 'SubModelName_O2C', 'SubModelName_DMS', 'SubModelName_O2C'),
            (uniSubModel_df_RB, uniSubModel_df_DMS, 'SubModelName_RB', 'SubModelName_DMS', 'SubModelName_RB')
        ]
        MasterLst_SubModel = run_fuzzy_match_with_lib_choice(df_DMS, df_O2C, df_RB, columns_submodels,library="rapidfuzz", thresholds=90)
        # save_master_list_to_csv(MasterLst_SubModel, 'SubModel', save_path, date_time,"Master")
        break
    return df_DMS, df_O2C, df_RB, df_DMSPrice, MasterLst_Brand, MasterLst_Model, MasterLst_SubModel
#%% S1.6: Price Calculation Function (One2Car)
def calculate_price(df, lst_group):
    agg_functions = {
        'Count': 'sum',  # Assuming Count is a numeric field you want to sum
        'Mean_Price': lambda x: int(x.median()),
        'Min_Price': 'min',
        'Max_Price': lambda x: int(x.median()),
    }
    df_result = df.groupby(lst_group).agg(agg_functions).reset_index()  
    # Round the values and convert to integer
    for col in ['Count', 'Mean_Price', 'Min_Price', 'Max_Price']:
        if col in df_result.columns:  # Check if the column exists in the result
            df_result[col] = df_result[col].round().astype(int)
    
    # Flatten the column names
    df_result.columns = [col[0] if isinstance(col, tuple) and col[1] == '' else col for col in df_result.columns.values]
    return df_result
#%% S1.7: Generate Key and Tokens
def generate_keys(df, columns_tokens):
    # Create the 'Keys' column based on the number of selected columns
    df['Keys'] = df[columns_tokens[0]]
    for col in columns_tokens[1:]:
        df['Keys'] += ' ' + df[col].str.upper()
    df['Keys'] = df['Keys'].str.replace(r'[\u0E00-\u0E7F]+', '')# 1. Remove Thai words
    df['Keys'] = df['Keys'].str.replace(r'\(|\)', '', regex=True).str.strip()# Removing '()'
    df['Tokens'] = df['Keys'].apply(generate_tokens)  # Apply generate_tokens directly here
    return df

def generate_tokens(model_name):
    alphanumeric_pattern = re.compile(r'^[a-zA-Z0-9.-]+$')  # Include numbers, period, and hyphen
    return {word for word in str(model_name).split() if alphanumeric_pattern.match(word)}

#%% S2: Import Data from Database or .CSV file
# Import from database 
if __name__ == "__main__":
    df_DMS, df_O2C, df_RB, df_DMSPrice, MasterLst_Brand, MasterLst_Model, MasterLst_SubModel = main()

# Brand
blank_count = (MasterLst_Brand.astype(str).applymap(lambda x: x.strip()) == '').sum()
MasterLst_Brand = MasterLst_Brand[MasterLst_Brand['BrandNameEng_O2C'].astype(str).str.strip() != '']
result_BrandCode= MasterLst_Brand.set_index('BrandNameEng_O2C')['BrandCode_DMS'].to_dict()
# print(blank_count)

# Model
blank_count = (MasterLst_Model.astype(str).applymap(lambda x: x.strip()) == '').sum()
MasterLst_Model = MasterLst_Model[MasterLst_Model['ModelName_O2C'].astype(str).str.strip() != '']
result_ModelCode= MasterLst_Model.set_index(['BrandCode_DMS','ModelName_O2C'])['ModelCode_DMS'].to_dict()
# print(blank_count)

# Submodel
blank_count = (MasterLst_SubModel.astype(str).applymap(lambda x: x.strip()) == '').sum()
MasterLst_SubModel = MasterLst_SubModel[MasterLst_SubModel['SubModelName_O2C'].astype(str).str.strip() != '']
result_SubModelCode= MasterLst_SubModel.set_index(['BrandCode_DMS','ModelCode_DMS','SubModelName_O2C'])['SubModelCode_DMS'].to_dict()
# print(blank_count)

#%% Data Matching
result_ModelCode2 = {(k1, str(k2).upper()): v for (k1, k2), v in result_ModelCode.items()}
result_SubModelCode2 = {(k1, k2, str(k3).upper()): v for (k1, k2, k3), v in result_SubModelCode.items()}
# Brand
df_price = df_O2C
df_price['BrandCode'] = df_price['Brand'].map(result_BrandCode)
# Model
tuple_model= df_price[['BrandCode', 'Model']].apply(lambda x:(x[0], x[1].upper()), axis=1)
df_price['ModelCode'] = tuple_model.map(result_ModelCode2)
# Submodel
tuple_submodel = df_price[['BrandCode', 'ModelCode', 'SubModel']].apply(lambda x: (x[0], x[1], str(x[2]).upper()), axis=1)
df_price['SubModelCode'] = tuple_submodel.map(result_SubModelCode2)
# Convert only strings in the DataFrame to uppercase
df_price = df_price.applymap(lambda s: s.upper() if isinstance(s, str) else s)

#%% One2Car Data Processing  ---------------------------------------------
# app_o2cprice = ColumnSelector(df_price, dialog_title="Select Column to Group Price from One2Car DATA")
# app_o2cprice.select_columns(dialog_title="Select Column to Group Price from One2Car DATA")
# app_o2cprice.mainloop()
# lst_group = app_o2cprice.selected_columns

# Group and Calculate Price Data from One2Car
print('\n**************** One2Car Data Processing **********************')
lst_group = ['Brand', 'BrandCode', 'Model', 'ModelCode', 'Year', 'CarAge']
df_price['CarAge'] = df_price['CarAge'].astype('int')
mapp_price = calculate_price(df_price, lst_group)
mapp_price['Year'] = mapp_price['Year'].astype(object) # Convert 'Year' and 'CarAge' columns to object type
mapp_price['CarAge'] = mapp_price['CarAge'].astype(object)
print(mapp_price.head(3))

#%% Vendor Data Processing ---------------------------------------------
while True:
    # Load Vendor Data (Add Exception)
    print('\n**************** Seller Data Loading **********************')
    df_vendorprocessor = DataSellerMapping()
    df_vendor, df_vendor_tmp, original_filename, full_save_path= df_vendorprocessor.run()# Run the process
    print(f'Number of Original File: {df_vendor_tmp.shape[0]}')
    print(f'Number of DMS Mapping File: {df_vendor.shape[0]}\n')
    # Check if there are any null values in the target column
    target_columns = ["BrandCode", "ModelCode", "ManufactureYear"]
    if df_vendor[target_columns].isnull().any().any():  # The second 'any()' checks across columns
        print()
        print(f"At least one of the columns {target_columns} contains null data.")
        null_data_rows = df_vendor[df_vendor[target_columns].isnull().any(axis=1)]
        print(null_data_rows)
        year_columns = [column for column in df_vendor_tmp.columns if 'Year' in column]
        if df_vendor["ManufactureYear"].isnull().any() & len(year_columns) >= 1:
            print("List Column of Manufaturing Year in Seller's File (Excel or CSV)")
            [print(str(i+1)+": " + x) for i, x in enumerate(year_columns)]
            n_Select = int(input(f'SELECT Number of Column to Mapping: '))
            colName = year_columns[n_Select-1]
            # Perform a left join
            result = pd.merge(df_vendor, df_vendor_tmp[['VinNo', colName]], left_on='VinNo', right_on='VinNo', how='left')
            comparison_df = result['Year']==result[colName]
            mismatches = df_vendor[comparison_df]
            df_vendor['Year'] = df_vendor['Year'].combine_first(result[colName])
    else:
        print(f"==== None of the columns {target_columns} have null data. ====")

    # Now you can use df, original_filename, and full_save_path as needed
    print(f"DataFrame:\n{df_vendor.head()}")
    print(f"Original Filename: {original_filename}")
    print(f"Full Save Path: {full_save_path}")

    df_vendor_curr = df_vendor
    # Select column 
    # app_vendor = ColumnSelector(df_vendor_curr, dialog_title="Select Column to Mapping from Leasing or Vendor DATA" )
    # app_vendor.select_columns(dialog_title="Select Column to Mapping from Leasing or Vendor DATA")
    # app_vendor.mainloop()
    # print("Mapping Columns:", app_vendor.selected_columns)

    while True:    
        columns_to_keep = ['BrandCode','BrandNameEng', 'ModelCode', 'ModelName', 'SubModelCode', 'SubModelName', 'Year', 'CarAge', 'Grade']
        df_vendor_filt = df_vendor_curr[columns_to_keep]
        df_vendor_filt_tmp = df_vendor_filt
        # Select Column to Mapping Text Data
        # app_key = ColumnSelector(df_vendor_filt_tmp, dialog_title="Select KEYS Column for Text Mapping from Vendor")
        # app_key.select_columns(dialog_title="Select KEYS Column for Text Mapping from Vendor")
        # app_key.mainloop()
        columns_tokens =['BrandNameEng', 'ModelName']
        try:
            if not columns_tokens:
                raise ValueError("No columns were selected.")
            df_vendor_filt = generate_keys(df_vendor_filt_tmp, columns_tokens)
            df_DMS = generate_keys(df_DMS, ['BrandNameEng', 'ModelName'])
            print("Mapping Successful!")
            break

        except KeyError as ke:
            # This will catch if one of the columns in columns_tokens doesn't exist in df_vendor_filt or df_DMS.
            print(f"Error: Column '{ke}' not found in the DataFrame. Try again.")
            
        except ValueError as ve:
            # This will catch the custom exception we raised if no columns were selected.
            print(f"Error: {ve}. Try again.")
            
        except Exception as e:
            # This will catch any other general exceptions and errors.
            print(f"An unexpected error occurred: {e}. Try again.")
        
        # Optionally, add a way for users to exit the loop
        choice = input("Do you want to continue? (yes/no): ").strip().lower()
        if choice == 'no':
            break
    #%%  Database
    df_dmsprocess = df_DMS
    # Select DMS Column
    while True:
        columns_keys = ['BrandCode','BrandNameEng', 'ModelCode', 'ModelName']
        # Check for odd number of columns
        if len(columns_keys) % 2 != 0:
            messagebox.showwarning("Warning", "Please select an even number of columns!")
            continue
        else:
            print("Selected Columns:", columns_keys)
            break

    app = DataMatcher(df_vendor_filt, df_dmsprocess, columns_keys)
    app.run()
    df_vendor_filt = app.df_vendor_filt

    #%% Filter Mapping
    if __name__ == '__main__':
        root = tk.Tk()
        app_match = DataMerger(root, df_vendor_curr, df_vendor_filt, mapp_price)
        root.mainloop()
        df_vendor = app_match.get_df_vendor() 
        df_aaprice = app_match.result
    # save_master_list_to_csv(df_vendor, '', save_path, date_time,"","vendor_adeddprice" )

    #%% Merge DMS price Data 
    # Grouping by and aggregating the median
    lst = ['BrandCode', 'ModelCode', 'Grade', 'CarAge']
    app_aaamatch = DMSDataMerger(df_vendor, df_DMSPrice, lst)
    merged_df = app_aaamatch.run()
    # Rounding and converting MeanOpenPrice and MeanSoldPrice columns to integers
    merged_df['AAA_OpenPrice'] = merged_df['AAA_OpenPrice'].round(0).astype('Int64')

    merged_df['AAA_SoldPrice'] = merged_df['AAA_SoldPrice'].round(0).astype('Int64')

    #%% Adjust Price ------------------------------------------------------------------------
    df = merged_df
    # Load Adjust Price Data (Add Exception)
    print('**************** Data to Adjust Price Loading **********************')
    selected_files = select_files()
    root = os.path.dirname(selected_files[0])
    print(f'File Number: {len(selected_files)}')
    print('**********************************************************')
    df_adjprice = []
    for file in selected_files:
        if file.endswith('.csv'):
            df_tmp = pd.read_csv(file)
        elif file.endswith('.xlsx'):
            df_tmp = pd.read_excel(file)
        else:
            continue
        df_adjprice.append(df_tmp)

    df_adjprice = df_adjprice[0]
    df_adjprice['Grade']=df_adjprice['Grade'].astype(str)
    df['MarketPrice'] = df['MarketPrice'].replace('', np.nan).astype(float)

    def get_non_nan_columns(df, idx):
        row = df.loc[idx]
        return [col for col in df.columns if pd.notna(row[col]) and col != "Discount/Addition"]
    # Extract non-NaN columns for each row in the DataFrame and store in a list
    output = [get_non_nan_columns(df_adjprice, idx) for idx in df_adjprice.index]

    def apply_discounts(df, df_adjprice):
        # Initializations
        df['Discount'] = np.nan
        df['AdjustedPrice'] = df['MarketPrice']
        df['AdjustedPrice_AAA'] = df['AAA_AdjOpenPrice']
        df['Matched'] = False  # Column to indicate if a match was found
        df['MatchedStatus'] = None  
        df['Matched_AAA'] = False  # Column to indicate if a match was found
        df['MatchedStatus_AAA'] = None  

        # Dynamically generate filter_list
        filter_list = list(set(df_adjprice.columns).intersection(set(df.columns)))

        for idx, main_row in df.iterrows():
            matched = False
            # Dynamically generate perfect match condition
            conditions = [df_adjprice[col] == main_row[col] for col in filter_list if col in df.columns]
            perfect_match_condition = np.logical_and.reduce(conditions, axis=0)
            perfect_match = df_adjprice[perfect_match_condition]
            # matchCol = [get_non_nan_columns(perfect_match, idx) for idx in perfect_match.index]

            if not perfect_match.empty:
                matched = True
                discount = perfect_match['Discount/Addition'].values[0]
                matchCol = [get_non_nan_columns(perfect_match, idx) for idx in perfect_match.index]

            # Generate conditions for partial matches
            for i in range(len(filter_list), 0, -1):
                if matched: 
                    break
                for subset in combinations(filter_list, i):  # Consider all combinations of i elements
                    conditions = [df_adjprice[col] == main_row[col] for col in subset]
                    null_conditions = [pd.isnull(df_adjprice[col]) for col in filter_list if col not in subset]
                    all_conditions = conditions + null_conditions
                    combined_condition = np.logical_and.reduce(all_conditions, axis=0)

                    partial_match = df_adjprice[combined_condition]
                    if not partial_match.empty:
                        matched = True
                        discount = partial_match['Discount/Addition'].values[0]
                        matchCol = [get_non_nan_columns(partial_match, idx) for idx in partial_match.index]
                        break  # If match found, exit the inner loop

            # Apply discount if matched
            if matched:
                df.at[idx, 'Discount'] = float(discount)
                df.at[idx, 'AdjustedPrice'] = df.at[idx, 'MarketPrice'] + (df.at[idx, 'MarketPrice'] * float(discount) / 100)
                df.at[idx, 'AdjustedPrice_AAA'] = df.at[idx, 'AAA_AdjOpenPrice'] + (df.at[idx, 'AAA_AdjOpenPrice'] * float(discount) / 100)
                df.at[idx, 'Matched'] = True
                df.at[idx, 'AdjustedStatus'] = matchCol[0]
                df.at[idx, 'Matched_AAA'] = True
                df.at[idx, 'AdjustedStatus_AAA'] = matchCol[0]
            else:
                print(f"Data in row {idx} of df does not match with df_adjprice for filters: {filter_list}")
        return df

    updated_df = apply_discounts(df, df_adjprice)
    # print(updated_df[['Grade', 'BrandCode', 'ModelCode', 'MarketPrice', 'Discount', 'AdjustedPrice', 'Matched']])
    # print(updated_df[['Grade', 'BrandCode', 'ModelCode', 'AAA_OpenPrice', 'Discount', 'AdjustedPrice_AAA', 'Matched_AAA']])

    #%% Save File
    now = datetime.now()
    date_time = now.strftime("%Y%m%d_%H%M%S")
    root = search_for_file_path()
    save_path = os.path.dirname(root)

    def round_to_3_sig_figs(num):
        # If the value is NaN, return NaN
        if pd.isna(num):
            return np.nan
        
        if isinstance(num, int):
            return round(num, -int(len(str(abs(num))) - 3))
        else:
            count = 0
            while abs(num) < 100:
                num *= 10
                count += 1
            rounded = round(num, -int(len(str(int(abs(num)))) - 3))
            return rounded / (10**count)

    df_vendor['MeanOpenPrice'] = updated_df['AAA_OpenPrice'].apply(round_to_3_sig_figs)
    df_vendor['MeanOpenPrice'] = updated_df['AAA_OpenPrice'].apply(round_to_3_sig_figs)
    df_vendor['MeanSoldPrice'] = updated_df['AAA_SoldPrice'].apply(round_to_3_sig_figs)
    df_vendor['PriceStatus_AAA'] = updated_df['AAA_PriceStatus']
    df_vendor['AdjustedPrice'] = updated_df['AdjustedPrice']
    df_vendor['AAA_AdjOpenPrice'] = updated_df['AdjustedPrice_AAA']
    df_vendor['Discount/Addition'] = updated_df['Discount']
    df_vendor['AdjustedStatus'] = updated_df['AdjustedStatus']

    listCol = ['ItemCode', 'VinNo', 'ContractNo', 'BrandCode', 'BrandNameEng', 'ModelCode', 'ModelName', 'SubModelCode', 'SubModelName', 
            'ManufactureYear', 'Grade', 'InQuality', 'CcName', 'GearName', 'drive', 'Color', 'MilesNo', 'ReceivedDate', 'UpdatedDate', 
            'Year','CarAge', 'MarketPrice', 'AdjustedPrice', 'PriceStatus',  'MeanOpenPrice', 'AAA_AdjOpenPrice', 'MeanSoldPrice', 
            'PriceStatus_AAA', 'Discount/Addition']
            #    'Discount/Addition', 'AdjustedStatus']
    df_result = df_vendor[listCol]
    
    # Add to Mapping data Contractno
    lst_header1 = list(df_vendor_tmp.columns)
    lst_header2 = ['VinNo','CarAge', 'MarketPrice', 'AdjustedPrice', 'PriceStatus',  'MeanOpenPrice', 'AAA_AdjOpenPrice', 'MeanSoldPrice', 'PriceStatus_AAA', 'Discount/Addition']
    df_result_tmp = df_vendor_tmp.merge(df_result[lst_header2], on=['VinNo'], how='left')
    df_result_tmp['Year'] = df_result['Year']

    # String Type
    columnsList = ['Account','ContractNo']#(or specific columns that you need)
    for col in columnsList:
        df_result_tmp[col] =  df_result_tmp[col].apply(lambda x: "" + str(x) + "")

    filenameSave = original_filename + "_estprice" 
    save_master_list_to_csv(df_result_tmp, '', save_path, date_time,'',filenameSave)

    try:
        X_program = input(f'Please enter (exit) to end the program: ').lower()
        if X_program == "exit":
            break
    except:
        print("Please check your input")

