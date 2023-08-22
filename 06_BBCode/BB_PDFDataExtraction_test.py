#%% Description
print(''' Code Description
      Created Date: 4 August 2023
      Creator: Anyamanee P.\n
      ******** Revision ***********
      1.0.0 = Newly Created
      ''')

#%% Import 
import os
import pdfplumber
import pandas as pd
import tkinter
from tkinter import filedialog
from tqdm import tqdm
from datetime import datetime
# Ignore warning messages
import warnings
warnings.filterwarnings("ignore")

#%% Define Function
def brand(text):
    lines = text.split('\n')
    brand_line = [line for line in lines if "ยหี่ ้อรถยนต์" in line]
    if brand_line:
        brand = brand_line[0].split("รถยนต์ : ")[-1]
        return str(brand)
    return None

def select_directory():
    root = tkinter.Tk()
    root.withdraw()  # use to hide tkinter window
    currdir = os.getcwd()
    tempdir = filedialog.askdirectory(parent=root, initialdir=currdir, title='Please select a directory')
    if len(tempdir) > 0:
        print("Path: %s" % tempdir)
    return tempdir

def check_and_create_directory(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created.")
    else:
        print(f"Directory '{directory_path}' already exists.")

def save_selected_path(selected_path):
    with open("selected_path.txt", "w") as file:
        file.write(selected_path)

def load_selected_path():
    if os.path.exists("selected_path.txt"):
        with open("selected_path.txt", "r") as file:
            return file.read()
    return None

def select_files():
    prev_selected_path = load_selected_path()

    root = tkinter.Tk()
    root.withdraw()  # use to hide tkinter window
    file_paths = filedialog.askopenfilenames(parent=root, initialdir=prev_selected_path, title='Please select files', filetypes=(("PDF files", "*.pdf"), ("All files", "*.*")))
    
    if len(file_paths) > 0:
        directory = os.path.dirname(file_paths[0])
        print("Selected Files:")
        for file_path in file_paths:
            print(file_path)
        save_selected_path(directory)
    
    return file_paths

#%% Import Data
now = datetime.now()
date_time = now.strftime("%Y%m%d")
print(date_time)

print('**************** Data Loading **********************')
# ls_path = [os.path.join(root, name) for name in os.listdir(root) if name.endswith(".pdf")]
selected_files = select_files()
root = os.path.dirname(selected_files[0])
print(f'File Number: {len(selected_files)}')
print('***************************************************')

#%% Main Code
BB_columns = ['Model_SubModel', 'Year', 'AUTO', 'Temp', 'MANUAL']
dfs = []

# Progress bar
pbar = tqdm(total=len(selected_files), desc="Processing", bar_format="{l_bar}{bar} [ time left: {remaining}, elapsed: {elapsed} ]")

for filename in selected_files:
    with pdfplumber.open(filename) as pdf:
        for id_page in range(len(pdf.pages)):
            table = pdf.pages[id_page].extract_table()
            text = pdf.pages[id_page].extract_text()
            if table is not None:
                temp = pd.DataFrame(table[2:], columns=BB_columns)  # Skip the header row
                temp['Brand'] = brand(text)
                temp.fillna(method='ffill', inplace=True)  # Replace None with the value from forward
                temp.drop(['Temp'], axis=1, inplace=True)
                dfs.append(temp)
    
    pbar.update(1)  # Update progress bar after processing each file

# Concatenate all dataframes in the list into a single dataframe
df_BB = pd.concat(dfs, ignore_index=True)

# Combine 'AUTO' and 'MANUAL' columns into column 'GearType'
df = pd.melt(df_BB, id_vars=['Model_SubModel', 'Year', 'Brand'], 
             var_name='GearType', value_name='Price')
# Replace '-' with NaN
df.replace('-', pd.NA, inplace=True)
df['Price'].replace(' ', '', inplace=True)
df = df.dropna()
df = df.reset_index(drop=True)
# Reindex columns in the desired order
desired_order = ['Brand', 'Model_SubModel', 'Year', 'GearType', 'Price']
df = df[desired_order]
print(df.head())
print()

# Count the occurrences of each Model_SubModel in each Brand
model_count = df.groupby(['Brand', 'Model_SubModel']).size().reset_index(name='Count')
# Find the minimum and maximum year for each brand
df['Year'] = pd.to_numeric(df['Year'])
min_max_years = df.groupby('Brand')['Year'].agg(['min', 'max'])

print(f'''Bluebook Data Checking
Amount of Brand : {len(df.groupby(['Brand'])['Year'].count())}
Amount of Model/SubModel in each Brand : {model_count}
''')


#%% Save Data File to CSV
date_time = now.strftime("%Y%m%d_%H%M%S")
print(date_time)
save_path = root
save_path_target = f"03_DataSave/{date_time}_BlueBook.csv"
save_path_result = os.path.dirname(os.path.join(save_path,save_path_target))
#Check directory folder
check_and_create_directory(save_path_result)
df.to_csv(os.path.join(save_path,save_path_target), index= False, encoding='utf-8-sig')

print("\nCSV file saved successfully.\n")
