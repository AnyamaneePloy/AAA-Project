import pyodbc
import os
import pandas as pd
from datetime import datetime
import tkinter as tk
from tkinter import filedialog


class DMSDataProcessor:
    def __init__(self):
        # Database connection string
        self.conn_str = 'Driver={SQL Server};Server=AAA-DBSRV1;Database=AAAeAuction;Trusted_Connection=yes;'
        self.today = datetime.now()

    def get_data_from_db(self):
        # Connect to the database and fetch data, then convert the data to a pandas DataFrame
        with pyodbc.connect(self.conn_str) as conn:
            cursor = conn.cursor()
            # Your SQL query
            query = '''SELECT t1.CompanyCode,
                    t1.ItemCode,
                    t1.BrandCode,b.BrandNameEng, 
                    t1.ModelCode,im.ModelName, 
                    t1.SubModelCode,ism.SubModelName, 
                    t1.CustSubModelCode,CSM.CustSubModelName, 
                    ISNULL(t1.LicensePlateNo,'') AS LicensePlateNo,
                    t1.ReceivedWareHouseCode AS WareHouse,
                    t1.ColorCode,dc.Color, 
                    t1.EngineCode,
                    t1.GearCode,ig.GearName, 
                    t1.GearType,igt.GearTypeName AS drive, 
                    t1.ManufactureYear AS Year,
                    t1.ReceiveMiles AS MilesNo,
                    q.Quality AS Grade,
                    iq.Quality AS InQuality,
                    p.ItemAuctionStatus,
                    p.OpeningPrice,
                    p.SoldPrice,
                    p.UpdatedDate,
                    ibd.BodyName,
                    t1.CcID,
                    icc.CcName
                    FROM InGoods t1
                    INNER JOIN AuAuctionItem p WITH(NOLOCK) ON p.CompanyCode = t1.CompanyCode   
                                                            AND p.ItemCode = t1.ItemCode
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
                    LEFT JOIN InBody     ibd WITH(NOLOCK) ON ibd.CompanyCode = t1.CompanyCode   
                                                        AND ibd.BodyCode = t1.BodyCode
                    LEFT JOIN InCc       icc WITH(NOLOCK) ON icc.CompanyCode = t1.CompanyCode 
                                                        AND icc.CcID = t1.CcID
                                                        AND icc.ProductCategoryCode = t1.ProductCategoryCode
                    LEFT JOIN InGear     ig WITH(NOLOCK) ON ig.GearCode = t1.GearCode
                    LEFT JOIN DbColor    dc WITH(NOLOCK) ON dc.ColorCode = t1.ColorCode
                    INNER JOIN InGearType igt with(nolock) on t1.GearType = igt.GearType 
                    INNER JOIN InCustomerSubModel CSM WITH(NOLOCK) ON t1.CustSubModelCode = CSM.CustSubModelCode
                    INNER JOIN ByBuyer    BB WITH(NOLOCK) ON p.SoldToBuyerCode = BB.BuyerCode'''  # abbreviated for clarity
            cursor.execute(query)
            self.db_data = cursor.fetchall()
            self.db_data = pd.DataFrame.from_records(self.db_data, columns=[x[0] for x in cursor.description])
        return self.db_data

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
    def search_for_file_path():
        # Use tkinter to open a directory selection dialog
        prev_selected_path = DMSDataProcessor.load_selected_path()
        root = tk.Tk()
        root.withdraw()
        tempdir = filedialog.askdirectory(parent=root, initialdir=prev_selected_path, title='Please select directory of Master file')
        root.destroy()
        if tempdir:
            print("Path:", tempdir)
            DMSDataProcessor.save_selected_path(tempdir)
        return tempdir

    def process_data(self, df):
        # Process the data based on your requirements
        df['UpdatedDate'] = pd.to_datetime(df['UpdatedDate'])
        df['date_update'] = df['UpdatedDate'].dt.date
        df['year_update'] = df['UpdatedDate'].dt.year
        df['week_update'] = df['UpdatedDate'].dt.isocalendar().week
        df['CarAge'] = self.today.year - df['Year']

        df = df[df['year_update'] >= 2023]
        df = df[df['ItemAuctionStatus'] == "03"]   
        df = df[(df['Year'] <= self.today.year) & (df['Year'] >= 1990)]   
        df['Grade'] = df['Grade'].replace('A', 'R')
        return df

    @staticmethod
    def get_aggregated_data(df, lst_group):
        # Aggregate the data based on the given group list
        df_agg = df.groupby(lst_group).agg(
            Count=('BrandCode', 'count'),
            MeanOpenPrice=('OpeningPrice', 'mean'),
            MedianOpenPrice=('OpeningPrice', 'median'),
            MeanSoldPrice=('SoldPrice', 'mean'),
            MedianSoldPrice=('SoldPrice', 'median')
        ).reset_index().assign(DiffOpenAndSold=lambda df: df['MeanSoldPrice'] - df['MeanOpenPrice'])
        return df_agg

    def save_to_csv(self, df_agg):
        date_str = self.today.strftime("%Y%m%d")
        root = self.search_for_file_path()
        save_path_target = os.path.join("01_Database", f"04_DMSPrice_{date_str}.csv")
        df_agg.to_csv(os.path.join(root, save_path_target), index=False, encoding='utf-8-sig')

    def run(self):
        raw_data = self.get_data_from_db()
        processed_data = self.process_data(raw_data)
        lst_group = ['BrandCode', 'ModelCode', 'SubModelCode', 'CarAge', 'GearName', 'BodyName', 'Grade']
        agg_result = self.get_aggregated_data(processed_data, lst_group)
        self.save_to_csv(agg_result)

if __name__ == "__main__":
    processor = DMSDataProcessor()
    processor.run()
