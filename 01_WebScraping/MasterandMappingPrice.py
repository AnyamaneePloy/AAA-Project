#%%
import pandas as pd
from fuzzywuzzy import fuzz as fw_fuzz, process as fw_process
from rapidfuzz import fuzz, process
from multiprocessing import Pool
from multiprocessing import Pool

import os
import tkinter as tk
from tkinter import filedialog, messagebox

from datetime import date, datetime
import warnings
# Ignore all warnings
warnings.filterwarnings("ignore")

#%%
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
    prev_selected_path = load_selected_path()
    root = tk.Tk()
    root.withdraw() #use to hide tkinter window
    currdir = os.getcwd()
    tempdir = filedialog.askdirectory(parent=root, initialdir=prev_selected_path, title='Please select a directory')
    if len(tempdir) > 0:
        print ("Path: %s" % tempdir)
    save_selected_path(tempdir)
    return tempdir

def select_files():
    prev_selected_path = load_selected_path()

    root = tk.Tk()
    root.withdraw()  # use to hide tkinter window
    file_paths = filedialog.askopenfilenames(parent=root, initialdir=prev_selected_path, title='Please select files', filetypes=(("PDF files", "*.pdf"), ("All files", "*.*")))
    
    if len(file_paths) > 0:
        directory = os.path.dirname(file_paths[0])
        print("Selected Files:")
        for file_path in file_paths:
            print(file_path)
        save_selected_path(directory)
    
    return file_paths

def find_best_match(target_string, choices):
    # This function finds the best match for a given target_string from a list of choices
    return max(choices, key=lambda choice: fuzz.ratio(target_string, choice))

def parallel_fuzzy_matching(data, choices, n_processes):
    with Pool(n_processes) as pool:
        results = pool.map(find_best_match, [(row, choices) for row in data])
    return results


def fuzzy_match_with_lib_choice(source_df, target_df, source_col, target_col, colname, library="rapidfuzz", threshold=100, limit=2):
    """
    Match strings in a target dataframe column to strings in a source dataframe column using fuzzy matching.
    Parameters:
        library: either 'rapidfuzz' or 'fuzzywuzzy'
    """
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

def run_fuzzy_match_with_lib_choice(df, source, target, columns, library="rapidfuzz", thresholds=100, limit=2):
    """Run the fuzzy match operations with library choice and return the modified dataframe."""
    for source_df, target_df, source_col, target_col, colname in columns:
        df, _ = fuzzy_match_with_lib_choice(source_df, target_df, source_col, target_col, colname, library, thresholds, limit)
    return df


# Find unique
def create_unique_brands_dataframe(dataframe, column_names, new_column_suffixes, new_column_prefix):
    unique_data = {}
    
    # Determine the combinations of unique values for brand and model columns
    unique_combinations = dataframe[column_names].drop_duplicates().reset_index(drop=True)
    
    for col, suffix, prefix in zip(column_names, new_column_suffixes, new_column_prefix):
        unique_data[f'{prefix}_{suffix}'] = unique_combinations[col].tolist()
        
    return pd.DataFrame(unique_data)

def process_dataframes_and_print(df_list, col_lists, suffix_lists, prefix_lists):
    """
    Process a list of dataframes, extract unique values, and print the results.
    
    Parameters:
    - df_list: List of source dataframes.
    - col_lists: List of lists of columns from which to extract unique values.
    - suffix_lists: List of lists of suffixes to append to the new dataframe's column names.
    """
    
    if not (len(df_list) == len(col_lists) == len(suffix_lists) == len(prefix_lists)):
        raise ValueError("All input lists should have the same length.")
    
    result_dfs = []

    for df, cols, suffixes, prefix in zip(df_list, col_lists, suffix_lists, prefix_lists):
        unique_df = create_unique_brands_dataframe(df, cols, suffixes, prefix)
        result_dfs.append(unique_df)
        
        # Print the results for visual check
        # print(unique_df.head(3))
        print(f'{prefix[-1]}_{suffixes[-1]} :\t', len(unique_df))

    return result_dfs

def save_master_list_to_csv(master_df, category, save_path_base, current_date_time):
    """
    Save a provided master dataframe to a CSV file.
    """
    save_path_target = f"03_DataSave/{current_date_time}_MasterLst_{category}.csv"
    full_save_path = os.path.join(save_path_base, save_path_target)

    master_df.to_csv(full_save_path, index=False, encoding='utf-8-sig')
    
    return full_save_path


def process_and_print(df_list, col_lists, suffix_lists, prefix_lists):
    """Process and print the dataframes, and return them."""
    return process_dataframes_and_print(df_list, col_lists, suffix_lists, prefix_lists)

def match_table(df_list, col_lists, suffix_lists, prefix_lists):
    """Process and print the dataframes, and return them."""
    return process_dataframes_and_print(df_list, col_lists, suffix_lists, prefix_lists)

def main():
    # Load Data
    root = search_for_file_path()
    ls_path = [os.path.join(path, name) 
             for path, subdirs, files in os.walk(root) 
             for name in files if name.endswith(".csv")]
    print(f'File Number: {len(ls_path)}')
    print(ls_path)

    df_DMS = pd.read_csv(ls_path[0])
    df_RB = pd.read_csv(ls_path[1])
    df_O2C = pd.read_csv(ls_path[2])

    now = datetime.now()
    date_time = now.strftime("%Y%m%d")
    save_path = os.path.dirname(os.path.dirname(root))
    print()

    # Brands
    # Brands
    uniBrands_df_DMS, uniBrands_df_O2C, uniBrands_df_RB = process_dataframes_and_print(
        [df_DMS, df_O2C, df_RB], 
        [['BrandCode','BrandNameEng'], ['Brand'], ['RBBrand']], 
        [['DMS','DMS'], ['O2C'], ['RB']],
        [['BrandCode','BrandNameEng'], ['BrandNameEng'], ['BrandNameEng']], 
    )
    print()
    columns_brands = [
        (uniBrands_df_O2C, uniBrands_df_DMS, 'BrandNameEng_O2C', 'BrandNameEng_DMS', 'BrandNameEng_O2C'),
        (uniBrands_df_RB, uniBrands_df_DMS, 'BrandNameEng_RB', 'BrandNameEng_DMS', 'BrandNameEng_RB')
    ]
    MasterLst_Brand = run_fuzzy_match_with_lib_choice(df_DMS, df_O2C, df_RB, columns_brands,library="fuzzywuzzy", thresholds=90)
    save_master_list_to_csv(MasterLst_Brand, 'Brand', save_path, date_time)

    # Models

    uniModel_df_DMS, uniModel_df_O2C, uniModel_df_RB = process_dataframes_and_print(
        [df_DMS, df_O2C, df_RB], 
        [['BrandNameEng', 'ModelCode','ModelName'], ['Brand','Model'], ['RBBrand','RBModel']], 
        [['DMS', 'DMS', 'DMS'], ['O2C', 'O2C'], ['RB', 'RB']],
        [['BrandNameEng', 'ModelCode', 'ModelName'], ['BrandNameEng','ModelName'], ['BrandNameEng','ModelName']]
    )
    print()
    columns_models = [
        (uniModel_df_O2C, uniModel_df_DMS, 'ModelName_O2C', 'ModelName_DMS', 'ModelName_O2C'),
        (uniModel_df_RB, uniModel_df_DMS, 'ModelName_RB', 'ModelName_DMS', 'ModelName_RB')
    ]
    MasterLst_Model = run_fuzzy_match_with_lib_choice(df_DMS, df_O2C, df_RB, columns_models,library="fuzzywuzzy", thresholds=90)
    save_master_list_to_csv(MasterLst_Model, 'Model', save_path, date_time)

    # Sub-Model
    uniSubModel_df_DMS, uniSubModel_df_O2C, uniSubModel_df_RB = process_dataframes_and_print(
        [df_DMS, df_O2C, df_RB], 
        [['BrandNameEng','ModelName', 'SubModelCode','SubModelName'], ['Brand','Model','sub_Model'], ['RBBrand','RBModel','RBSubModel']], 
        [['DMS', 'DMS', 'DMS', 'DMS'], ['O2C', 'O2C', 'O2C'], ['RB', 'RB', 'RB']],
        [['BrandNameEng', 'ModelName', 'SubModelCode','SubModelName'], ['BrandNameEng','ModelName','SubModelName'], ['BrandNameEng','ModelName','SubModelName']]
    )
    print()
    columns_submodels = [
        (uniSubModel_df_O2C, uniSubModel_df_DMS, 'SubModelName_O2C', 'SubModelName_DMS', 'SubModelName_O2C'),
        (uniSubModel_df_RB, uniSubModel_df_DMS, 'SubModelName_RB', 'SubModelName_DMS', 'SubModelName_RB')
    ]
    MasterLst_SubModel = run_fuzzy_match_with_lib_choice(df_DMS, df_O2C, df_RB, columns_submodels,library="rapidfuzz", thresholds=90)
    save_master_list_to_csv(MasterLst_SubModel, 'SubModel', save_path, date_time)

#%% Import Data from Web scraping on .CSV file
if __name__ == "__main__":
    main()