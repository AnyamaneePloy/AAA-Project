import tkinter as tk
import pandas as pd
import re
from tkinter import ttk, messagebox

class DataMatcher:
    def __init__(self, df_vendor_filt, df_DMS, columns_keys):
        self.df_vendor_filt = df_vendor_filt
        self.df_DMS = df_DMS
        self.columns_keys = columns_keys
        
        self.root = tk.Tk()
        self.root.title("Data Matching")
        self.root.geometry("500x500")
        
        self.setup_ui()
    
    def setup_ui(self):
        label = tk.Label(self.root, text="Click to Match DataFrames")
        label.pack(pady=20)
        
        self.btn_vendor = ttk.Button(self.root, text="Preview Seller Data", 
                                     command=self.display_vendor_dataframe)
        self.btn_vendor.pack(pady=10)

        self.btn_dms = ttk.Button(self.root, text="Preview DMS Data", 
                                  command=self.display_dms_dataframe)
        self.btn_dms.pack(pady=10)

        self.btn_submit = ttk.Button(self.root, text="Click to Match Price Data", 
                                     command=self.on_submit)
        self.btn_submit.pack(pady=20)

    def display_dataframe(self, df, title="DataFrame Preview"):
        window = tk.Toplevel(self.root)
        window.title(title)
        
        text = tk.Text(window, wrap=tk.NONE)
        text.pack(fill=tk.BOTH, expand=True)
        
        # Convert the DataFrame to a string and insert it into the Text widget
        data_string = df.to_string()
        text.insert(tk.END, data_string)

        scroll_x = ttk.Scrollbar(window, orient=tk.HORIZONTAL, command=text.xview)
        scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
        
        scroll_y = ttk.Scrollbar(window, orient=tk.VERTICAL, command=text.yview)
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)

        text.config(xscrollcommand=scroll_x.set, yscrollcommand=scroll_y.set)

    def display_vendor_dataframe(self):
        cols_to_drop = ['Keys']
        self.display_dataframe(self.df_vendor_filt.drop(columns=cols_to_drop), 
                               "Vendor DataFrame")

    def display_dms_dataframe(self):
        cols_to_drop = ['Keys']
        self.display_dataframe(self.df_DMS.drop(columns=cols_to_drop), 
                               "DMS DataFrame")

    # Function to tokenize strings
    def tokenize_model(self, model_name):
        if isinstance(model_name, float):  # Check if it's NaN
            return set()
        model_name = re.sub(r'[^\w\s]', '', model_name).upper()
        return set([token for token in model_name.split() if token.isalnum()])


    # Function to compute similarity between two token sets
    def compute_similarity(self, tokens1, tokens2):
        intersection_len = len(tokens1.intersection(tokens2))
        union_len = len(tokens1.union(tokens2))
        if union_len == 0:
            return 0
        # Use Jaccard similarity for set-based token similarity
        return intersection_len / float(union_len)
    
    # Function to find the best matching row
    def find_best_match(self, row, df_target, output_cols):
        tokens = row['Tokens']
        similarity_scores = df_target['Tokens'].apply(lambda x: self.compute_similarity(tokens, x))
        
        best_match_index = similarity_scores.idxmax()
        best_score = similarity_scores[best_match_index]
        matched_row = df_target.iloc[best_match_index]
        
        results = [matched_row[col] for col in output_cols]
        return pd.Series(results + [best_score], index=output_cols + ['Score'])

    def perform_matching(self,selected_columns):
        try:
            self.df_DMS["Tokens"] = self.df_DMS["Keys"].apply(self.tokenize_model)
            self.df_vendor_filt["Tokens"] = self.df_vendor_filt["Keys"].apply(self.tokenize_model)

            self.df_vendor_filt[selected_columns + ['Score']] = self.df_vendor_filt.apply(self.find_best_match, 
                                                                                df_target=self.df_DMS, output_cols=selected_columns, axis=1)
            print(self.df_vendor_filt[['Keys'] + selected_columns + ['Score']])
            messagebox.showinfo("Success", "Matching completed. Check console for results.")
        except Exception as e:
            messagebox.showerror("Error", str(e))
            
        self.root.destroy()
        self.root.quit()


    def on_submit(self):
        selected_columns = ["Keys"]
        self.perform_matching(self.columns_keys)

    def run(self):
        self.root.mainloop()