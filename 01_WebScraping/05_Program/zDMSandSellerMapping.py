#%% Import 
import os
import pandas as pd
import pyodbc
import datetime
import tkinter as tk
from tkinter import filedialog
from zColumnSelector import ColumnSelector
from zColumnMapping import ColumnMapping

class DataProcessor:
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
        prev_selected_path = DataProcessor.load_selected_path()
        root = tk.Tk()
        root.withdraw()  # use to hide tkinter window
        tempdir = filedialog.askdirectory(parent=root, initialdir=prev_selected_path, 
                                          title='Please select directory of Master file')
        if not tempdir:
            return None
        DataProcessor.save_selected_path(tempdir)
        return tempdir

    def select_files(self):
        prev_selected_path = DataProcessor.load_selected_path()
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
            DataProcessor.save_selected_path(directory)
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
        app_vendor = ColumnSelector(self.df_vendor_curr, dialog_title="Select Column to Mapping from Leasing or Vendor DATA")
        app_vendor.select_columns(dialog_title="Select Column to Mapping from Leasing or Vendor DATA")
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

    def connect_db_and_transform(self):
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=AAA-DBSRV1;'
                              'Database=AAAeAuction;'
                              'Trusted_Connection=yes;')
        self.cursor = conn.cursor()

        for _, row in self.df_vendor_filt_tmp.iterrows():
            contract_no, vin_no = str(row['ContractNo']), str(row['VinNo']) 
            sql = """
                SELECT t1.VinNo, t1.ContractNo,
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
                t1.ReceivedDate
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
                LEFT JOIN DbColor    dc WITH(NOLOCK) ON dc.ColorCode = t1.ColorCode
                WHERE t1.VinNo = ? AND t1.ContractNo = ?                
            """
            self.cursor.execute(sql, vin_no, contract_no)
            db_data = self.cursor.fetchall()
            self.transformed_data.extend(db_data)
        conn.close()

    def process_dates(self, dataframe):
        """
        Process the date-related columns of the dataframe.
        """
        dataframe.loc[:, 'date_Received'] = dataframe['ReceivedDate'].dt.date
        dataframe.loc[:, 'year_Received'] = dataframe['ReceivedDate'].dt.year
        dataframe.loc[:, 'month_Received'] = dataframe['ReceivedDate'].dt.month
        dataframe.loc[:, 'week_Received'] = dataframe['ReceivedDate'].dt.isocalendar().week
        
        # Assuming 'CreatedDate' and 'ManuYear' columns exist in the dataframe:
        dataframe.loc[:, 'CarAge'] = dataframe['year_Received'] - dataframe['Year'] # changing year to age
        dataframe['Grade'] = dataframe['Grade'].replace('A', 'R')
        
        return dataframe
    
    def save_with_datetime(self, dataframe, original_filename):
        # Extract the current date and time, and format it as a string
        current_datetime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        new_filename = f"{original_filename}_{current_datetime}.xlsx"
        dataframe.to_excel(new_filename, index=False)

        print(f"Data saved to: {new_filename}")
        return new_filename

if __name__ == "__main__":
    processor = DataProcessor()
    processor.select_files()
    processor.load_vendor_data()
    processor.select_column()
    processor.map_with_text_analytics()
    processor.connect_db_and_transform()

    # Convert to pandas DataFrame using processor's transformed_data and cursor  
    df = pd.DataFrame.from_records(processor.transformed_data, columns=[x[0] for x in processor.cursor.description])
    print(df.info())

    df = df.drop_duplicates(subset=['VinNo', 'ContractNo'], keep='last')# Drop the duplicates from the original DataFrame
    df = processor.process_dates(df)
    print(df.info())
    print(df.head())

    # Assuming the original filename is one of the selected files:
    original_filename = os.path.splitext(os.path.basename(processor.selected_files[0]))[0]
    processor.save_with_datetime(df, original_filename)
