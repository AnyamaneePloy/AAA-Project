#%% Import Library
import os
import pandas as pd
import numpy as np
import pyodbc
import datetime
import tkinter as tk
from tkinter import filedialog

from zColumnSelector import ColumnSelector
from zColumnMapping import ColumnMapping

class DataSellerMapping:
    def __init__(self):
        self.selected_files = []
        self.df_vendor = []
        self.df_vendor_curr = None
        self.df_vendor_filt = None
        self.df_vendor_filt_tmp = None
        self.tp_mapping = {}
        self.transformed_data = []

    @staticmethod
    def save_selected_path(selected_path):
        with open("selected_path.txt", "w") as file:
            file.write(selected_path)

    @staticmethod
    def load_selected_path():
        if os.path.exists("selected_path.txt"):
            with open("selected_path.txt", "r") as file:
                return file.read()
        return None

    @staticmethod
    def check_and_create_directory(directory_path):
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            print(f"Directory '{directory_path}' created.")
        else:
            print(f"Directory '{directory_path}' already exists.")

    @staticmethod
    def search_for_file_path():
        prev_selected_path = DataSellerMapping.load_selected_path()
        root = tk.Tk()
        root.withdraw()  # use to hide tkinter window
        tempdir = filedialog.askdirectory(parent=root, initialdir=prev_selected_path, 
                                          title='Please select Seller file')
        if not tempdir:
            return None
        DataSellerMapping.save_selected_path(tempdir)
        return tempdir

    def select_files(self):
        prev_selected_path = DataSellerMapping.load_selected_path()
        root = tk.Tk()
        root.withdraw()  # use to hide tkinter window
        file_paths = filedialog.askopenfilenames(parent=root, initialdir=prev_selected_path, 
                                                 title='Please select files', 
                                                 filetypes=(("Excel files", "*.xlsx"), 
                                                            ("CSV files", "*.csv"), 
                                                            ("All files", "*.*")))
        
        if len(file_paths) > 0:
            directory = os.path.dirname(file_paths[0])
            print("Selected Files:")
            for file_path in file_paths:
                print(file_path)
            DataSellerMapping.save_selected_path(directory)
        self.selected_files = file_paths

    def load_vendor_data(self):
        root = os.path.dirname(self.selected_files[0])
        dtype_dict = {"Contract No": str}
        for file in self.selected_files:
            if file.endswith('.csv'):
                df = pd.read_csv(file)
            elif file.endswith('.xlsx'):
                df = pd.read_excel(file, dtype=dtype_dict)
            else:
                continue
            self.df_vendor.append(df)
        self.df_vendor_curr = self.df_vendor[0]

    def select_column(self):
        app_vendor = ColumnSelector(self.df_vendor_curr, dialog_title="Select Column to Mapping from Seller DATA")
        app_vendor.select_columns(dialog_title="Select Column to Mapping from Seller DATA")
        app_vendor.mainloop()
        columns_to_keep = app_vendor.selected_columns
        self.df_vendor_filt = self.df_vendor_curr[columns_to_keep]
    
    def map_with_text_analytics(self):
        columns = ["ContractNo", "VinNo"]
        app_mapp = ColumnMapping(self.df_vendor_filt.columns, columns)
        try:
            app_mapp.mainloop()
        except AttributeError:
            pass
        self.tp_mapping = app_mapp.mappings
        self.df_vendor_filt_tmp = self.df_vendor_filt
        self.df_vendor_filt_tmp.rename(columns=self.tp_mapping, inplace=True)
        self.df_vendor_curr.rename(columns=self.tp_mapping, inplace=True)
    
    def fill_missing_from_others(self, latest_row, other_rows):
        for col in latest_row.index:
            if pd.isnull(latest_row[col]) and not other_rows[col].isnull().all():
                latest_row[col] = other_rows.sort_values('UpdatedDate', ascending=False)[col].dropna().head(1).values[0]
        return latest_row

    def connect_db_and_transform(self):
        # Determined Database and Server Name
        conn = pyodbc.connect('''Driver={SQL Server};
                                Server=mbk-sqlsrv4;
                                Database=MBKGROUPDataMart; 
                                UID=user_di_aaa;
                                PWD=Rpt!2023#;''')
        self.cursor = conn.cursor()
        self.transformed_data = pd.DataFrame()

        for _, row in self.df_vendor_filt_tmp.iterrows():
            # Mapping data between Seller's file with Data from database 
            contract_no, vin_no = str(row['ContractNo']), str(row['VinNo']) 
            sql  = '''SELECT * FROM [dbo].[AAA_STG_ReceiveCarTransaction] as t1
                       WHERE t1.VinNo = ? OR t1.ContractNo = ? '''
            
            self.cursor.execute(sql, vin_no, contract_no)
            rows = self.cursor.fetchall()
            if rows:
                # Convert fetched data to a DataFrame
                db_data = pd.DataFrame.from_records(rows, columns=[desc[0] for desc in self.cursor.description])
                self.transformed_data = pd.concat([self.transformed_data, db_data], ignore_index=True)
            else:
                # Create a DataFrame row with the original VinNo and ContractNo and NaN for all other columns
                columns=[desc[0] for desc in self.cursor.description]
                no_data = {col: np.nan for col in columns}
                no_data['VinNo'] = vin_no            
                no_data['ContractNo'] = contract_no  
                no_data_df = pd.DataFrame([no_data], columns=columns)
                # Add the NaN data to the transformed_data DataFrame
                self.transformed_data = pd.concat([self.transformed_data, no_data_df], ignore_index=True)
        conn.close()
        self.transformed_data['UpdatedDate'] = pd.to_datetime(self.transformed_data['UpdatedDate'])
        self.transformed_data.sort_values(by=['VinNo', 'UpdatedDate'], ascending=[True, False], inplace=True)

        # Group by 'VinNo' and process each group
        self.filled_data = pd.DataFrame()
        countD = 0
        for _, group in self.transformed_data.groupby('VinNo'):
            if len(group) > 1:
                # Sort group by 'UpdatedDate' so that the latest entry is first
                group.sort_values(by='UpdatedDate', ascending=False, inplace=True)
                # Take the latest row and fill its missing data from other rows
                latest_row = group.iloc[0]
                other_rows = group.iloc[1:]
                filled_latest_row = self.fill_missing_from_others(latest_row, other_rows)
                self.filled_data = self.filled_data.append(filled_latest_row, ignore_index=True)
                countD +=1
            else:
                self.filled_data = self.filled_data.append(group, ignore_index=True)

        # Remove any potential duplicates after filling
        self.filled_data.drop_duplicates(subset=['VinNo'], keep='first', inplace=True)
        self.filled_data.reset_index(drop=True, inplace=True)
        self.filled_data["Year"] = self.filled_data["ManufactureYear"]
        return self.filled_data

    def process_dates(self, dataframe):
        dataframe.loc[:, 'date_Received'] = dataframe['ReceivedDate'].dt.date
        dataframe.loc[:, 'year_Received'] = dataframe['ReceivedDate'].dt.year
        dataframe.loc[:, 'month_Received'] = dataframe['ReceivedDate'].dt.month
        dataframe.loc[:, 'week_Received'] = dataframe['ReceivedDate'].dt.isocalendar().week
        
        # Assuming 'CreatedDate' and 'ManuYear' columns exist in the dataframe:
        dataframe.loc[:, 'CarAge'] = dataframe['year_Received'] - dataframe['Year'] # changing year to age
        dataframe['Grade'] = dataframe['Grade'].replace('A', 'R')      
        return dataframe
    
    def save_with_datetime(self, master_df, category=None, ori_filename=None ,path_base=None, datatype='', namefile=None):
        # Extract the current date and time, and format it as a string
        current_datetime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        if datatype == "Master":
            save_path_target = f"Input/04_Master/{category}_MasterLst.csv"
        else:
            save_path_target = f"DMS/{ori_filename}_{current_datetime}.xlsx"
        
        full_save_path = os.path.join(path_base, save_path_target)
        path_target = os.path.dirname(full_save_path)
        
        self.check_and_create_directory(path_target)      
        if save_path_target.endswith('.csv'):
            master_df.to_csv(full_save_path, index=False, encoding='utf-8-sig')
        else:
            master_df.to_excel(full_save_path, index=False)
        
        print(f'Save Direction: {full_save_path}')         
        return full_save_path
    
    def run(self):
        # Select files amd map column name
        self.select_files()
        self.load_vendor_data()# Load vendor data
        self.select_column()# Select columns
        self.map_with_text_analytics()# Map with text analytics
        self.connect_db_and_transform()# Connect to the database and transform the data

        # Convert to pandas DataFrame using processor's transformed_data 
        df = self.filled_data
        df['ReceivedDate'] = pd.to_datetime(df['ReceivedDate'])
        print(df.info())
        # Drop the duplicates 
        df = df.drop_duplicates(subset=['VinNo', 'ContractNo'], keep='last')
        df = df.drop(['LoadDate','CompanyCode'], axis=1)
        df = self.process_dates(df)
        print(df.info())
        print(df.head())
        # Save file that mapping data from database:
        original_filename = os.path.splitext(os.path.basename(self.selected_files[0]))[0]
        path_base = os.path.dirname(os.path.dirname(self.selected_files[0]))
        full_save_path = self.save_with_datetime(df, ori_filename=original_filename, path_base=path_base)
        return df, self.df_vendor_curr, original_filename, full_save_path
