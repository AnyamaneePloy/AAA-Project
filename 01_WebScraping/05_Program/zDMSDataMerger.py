import pandas as pd

permanent_columns = ['Count', 'MedianOpenPrice', 'MedianSoldPrice']

class DMSDataMerger:
    def __init__(self, df_vendor, df_DMSPrice, cols_to_use):
        self.df_vendor = df_vendor
        self.mapp_price = df_DMSPrice
        self.cols_to_use =cols_to_use

        # Priority dict: lower values have higher priority
        self.column_priority = {
            'BrandCode': 1,
            'ModelCode': 2,
            'CarAge': 3,
            'Year': 4, 
        }

    # Function Calculated
    def calculate_price(self, df, lst_group):
        agg_functions = {
            'Count': 'sum',  # Assuming Count is a numeric field you want to sum
            'MedianOpenPrice': lambda x: int(x.median()),
            'MedianSoldPrice': lambda x: int(x.median())}

        df_result = df.groupby(lst_group).agg(agg_functions).reset_index()  
        # Round the values and convert to integer
        for col in ['Count', 'MedianOpenPrice ', 'MedianSoldPrice']:
            if col in df_result.columns:  # Check if the column exists in the result
                df_result[col] = df_result[col].round().astype(int)

        # Flatten the column names
        df_result.columns = [col[0] if isinstance(col, tuple) and col[1] == '' else col for col in df_result.columns.values]       
        return df_result

    def get_selected_columns(self, colName):
        # Add the selected columns to the permanent columns
        merge_cols = self.cols_to_use + permanent_columns
        # Drop duplicates from the right table based on the merge columns
        self.mapp_price = self.calculate_price(self.mapp_price, self.cols_to_use)
        self.mapp_price.drop_duplicates(subset= self.cols_to_use, inplace=True)

        # Perform the merge
        self.result = self.df_vendor.merge(self.mapp_price[merge_cols], 
                                                left_on=self.cols_to_use, 
                                                right_on=self.cols_to_use, 
                                                how='left', indicator=True)
        # Stamp the columns used for rows that matched from both data sources
        self.result.loc[self.result['_merge'] == 'both', 'PriceStatus'] = str(self.cols_to_use)
        next_cols = self.cols_to_use
        while True:
            if self.result[colName].isna().any().any():
                next_cols = next_cols[:-1]
                merge_cols = next_cols + permanent_columns
                mapp_price_new = self.calculate_price(self.mapp_price, next_cols)
                df_reduced = self.df_vendor.merge(mapp_price_new[merge_cols],
                                                        left_on = next_cols, 
                                                        right_on = next_cols, how = 'left')
                # Update only NaN values in df from df_reduced
                mask = self.result[colName].isna()
                for i in range(len(colName)):
                    self.result.loc[mask[colName[i]], colName[i]] = df_reduced.loc[mask[colName[i]], colName[i]]

                # Stamp the columns used for rows that matched from both data sources in this iteration
                matched_rows = mask & ~self.result[colName].isna()
                self.result.loc[matched_rows[colName[0]], 'PriceStatus'] = str(next_cols)
                
            if len(next_cols) < 2:
                break

        # Check if the result has the same shape as the left table
        assert self.result.shape[0] == self.df_vendor.shape[0], "The result size does not match the left table size!"
        
        print(self.df_vendor.shape[0])
        print( self.result.shape[0])

        self.df_vendor = pd.DataFrame(self.df_vendor)
        self.df_vendor['AAA_OpenPrice'] = self.result['MedianOpenPrice']
        self.df_vendor['AAA_AdjOpenPrice'] = self.result['MedianOpenPrice']
        self.df_vendor['AAA_SoldPrice'] = self.result['MedianSoldPrice']
        self.df_vendor['AAA_PriceStatus'] = self.result['PriceStatus']
    

    def run(self):
        # Select files amd map column name
        self.get_selected_columns(['MedianOpenPrice','MedianSoldPrice'] )# Load vendor data
        return self.df_vendor 


            



