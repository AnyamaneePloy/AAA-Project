import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox

permanent_columns = ["Count", "Min_Price", "Mean_Price"]

class DataMerger:
    def __init__(self, master, df_vendor_curr, df_vendor_filt, mapp_price):
        self.root = master
        self.df_vendor_filt = df_vendor_filt
        self.mapp_price = mapp_price
        self.df_vendor_curr = df_vendor_curr

        # Priority dict: lower values have higher priority
        self.column_priority = {
            'BrandCode': 1,
            'ModelCode': 2,
            'SubModelCode': 5,
            'Year': 4,
        }

        self.root.title("Select Columns for Merging")
        self.root.geometry("500x500")

        self.initUI()
        
    def initUI(self):
        self.listbox = tk.Listbox(self.root, selectmode=tk.MULTIPLE)
        self.listbox.pack(pady=20)

        # All columns that the user can select from
        available_columns = list(self.column_priority.keys())
        for column in available_columns:
            self.listbox.insert(tk.END, column)

        # Set default columns based on priority
        self.set_default_selections()

        btn_submit = ttk.Button(self.root, text="Merge Price", command=self.get_selected_columns)
        btn_submit.pack(pady=20)

        ttk.Button(self.root, text="Next", command=self.close_app).pack(pady=10)

    # Function Calculated
    def calculate_price(self, df, lst_group):

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
    
    def set_default_selections(self):
        # Set, let's say, the top 2 columns as default based on priority
        default_cols = sorted(self.column_priority, key=self.column_priority.get)[:2]

        for col in default_cols:
            index = self.listbox.get(0, "end").index(col)  # Get index of the column in listbox
            self.listbox.select_set(index)  # Set the column as selected

    def get_selected_columns(self):
        selected = self.listbox.curselection()
        cols_to_use = [self.listbox.get(i) for i in selected]

        if not cols_to_use:
            messagebox.showerror("Error", "No columns selected!")
            return
    
        try:
            # Add the selected columns to the permanent columns
            merge_cols = cols_to_use + permanent_columns
            # Drop duplicates from the right table based on the merge columns
            self.mapp_price.drop_duplicates(subset=cols_to_use, inplace=True)

            # Perform the merge
            self.result = self.df_vendor_filt.merge(self.mapp_price[merge_cols], 
                                                    left_on=cols_to_use, 
                                                    right_on=cols_to_use, 
                                                    how='left', indicator=True)
            # Stamp the columns used for rows that matched from both data sources
            self.result.loc[self.result['_merge'] == 'both', 'PriceStatus'] = str(cols_to_use)
            next_cols = cols_to_use
            while True:
                if self.result['Min_Price'].isna().any():
                    next_cols = next_cols[:-1]

                    

                    merge_cols = next_cols + permanent_columns
                    mapp_price_new = self.calculate_price(self.mapp_price, next_cols)
                    df_reduced = self.df_vendor_filt.merge(mapp_price_new[merge_cols],
                                                           left_on = next_cols, 
                                                           right_on = next_cols, how = 'left')
                    # Update only NaN values in df from df_reduced
                    mask = self.result['Min_Price'].isna()
                    self.result.loc[mask, 'Min_Price'] = df_reduced.loc[mask, 'Min_Price']

                    # Stamp the columns used for rows that matched from both data sources in this iteration
                    matched_rows = mask & ~self.result['Min_Price'].isna()
                    self.result.loc[matched_rows, 'PriceStatus'] = str(next_cols)
                    
                if len(next_cols) < 2:
                    break

            # Check if the result has the same shape as the left table
            assert self.result.shape[0] == self.df_vendor_filt.shape[0], "The result size does not match the left table size!"
            
            print(self.df_vendor_filt.shape[0])
            print( self.result.shape[0])

            cols_to_drop = ['Keys', 'Tokens', 'Score']
<<<<<<< HEAD
            df_aaprice =  self.result.drop(columns=cols_to_drop)
            df_vendor = pd.DataFrame(self.df_vendor_curr)
            df_vendor['MarketPrice'] = df_aaprice['Min_Price']
            df_vendor['AdjustedPrice'] = df_aaprice['Min_Price']
            df_vendor['PriceStatus'] = df_aaprice['PriceStatus']
=======
            self.df_aaprice =  self.result.drop(columns=cols_to_drop)
            self.df_vendor = pd.DataFrame(self.df_vendor_curr)
            self.df_vendor['MarketPrice'] = self.df_aaprice['Min_Price']
            self.df_vendor['AdjustedPrice'] = self.df_aaprice['Min_Price']
            self.df_vendor['PriceStatus'] = self.df_aaprice['PriceStatus']
>>>>>>> Development
            
            # Use a Toplevel widget to display outputs
            new_window = tk.Toplevel(self.root)
            output_label = tk.Label(new_window, text=f"Selected Columns: {cols_to_use}\n\nMerged Data:\n{self.df_vendor}")
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

