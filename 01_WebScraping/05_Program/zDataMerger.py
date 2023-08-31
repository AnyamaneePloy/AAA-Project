import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox

permanent_columns = ["Count", "Min_Price", "Mean_Price"]

# def check_datatype(df, columns):
#     """Check if the columns in df have the correct datatypes."""
#     for col in columns:
#         if df[col].dtype not in [int, float]:
#             return False
#     return True

class DataMerger:
    def __init__(self, master,df_vendor_curr, df_vendor_filt, mapp_price):
        self.root = master
        self.df_vendor_filt = df_vendor_filt
        self.mapp_price = mapp_price
        self.df_vendor_curr = df_vendor_curr

        self.root.title("Select Columns for Merging")
        self.root.geometry("400x400")
        
        self.initUI()
        
    def initUI(self):
        self.listbox = tk.Listbox(self.root, selectmode=tk.MULTIPLE)
        self.listbox.pack(pady=20)
        
        # All columns that the user can select from
        available_columns = ["BrandCode", "ModelCode", "SubModelCode", "Year"] 
        for column in available_columns:
            self.listbox.insert(tk.END, column)
        
        btn_submit = ttk.Button(self.root, text="Merge with Selected Columns", command=self.get_selected_columns)
        btn_submit.pack(pady=20)
        
        ttk.Button(self.root, text="Close", command=self.close_app).pack(pady=10)

    
    def get_selected_columns(self):
        selected = self.listbox.curselection()
        cols_to_use = [self.listbox.get(i) for i in selected]

        # global df_vendor_curr  # Add this line
        
        if not cols_to_use:
            messagebox.showerror("Error", "No columns selected!")
            return
        
        try:
            # Add the selected columns to the permanent columns
            merge_cols = cols_to_use + permanent_columns
            
            self.result = self.df_vendor_filt.merge(self.mapp_price[merge_cols], left_on=cols_to_use, right_on=cols_to_use, how='left')
            
            print(self.df_vendor_filt.shape[0])
            print( self.result.shape[0])

            cols_to_drop = ['Keys', 'Tokens', 'Score']
            df_aaprice =  self.result.drop(columns=cols_to_drop)
            df_vendor = pd.DataFrame(self.df_vendor_curr)
            df_vendor['MarketPrice'] = df_aaprice['Min_Price']
            
            # Use a Toplevel widget to display outputs
            new_window = tk.Toplevel(self.root)
            output_label = tk.Label(new_window, text=f"Selected Columns: {cols_to_use}\n\nMerged Data:\n{df_vendor}")
            output_label.pack(pady=20)
            
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def get_df_vendor(self):
        return self.df_vendor

    def get_df_aaprice(self):
        return self.df_aaprice
    
    def close_app(self):
        self.root.destroy()
        self.root.quit()

