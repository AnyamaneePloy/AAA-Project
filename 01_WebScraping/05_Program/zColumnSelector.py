import tkinter as tk
from tkinter import ttk
# Column Selection Class
class ColumnSelector(tk.Tk):
    def __init__(self, df, Title = "Column Selector Main Window"):
        super().__init__()
        
        self.geometry("500x500")
        self.title(Title)  # Title for the main window
        self.df = df
        self.selected_columns = []
        
        ttk.Button(self, text="Select Columns", command=self.select_columns).pack(pady=20)

    def select_columns(self, dialog_title="Column Selection Dialog"):
        dialog = tk.Toplevel(self)
        dialog.title(dialog_title)
        dialog.geometry("500x500")

        frame = ttk.Frame(dialog)
        frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(frame, orient=tk.VERTICAL)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        listbox = tk.Listbox(frame, selectmode=tk.MULTIPLE, yscrollcommand=scrollbar.set)
        listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=listbox.yview)

        for col in self.df.columns:
            listbox.insert(tk.END, col)
            if col in self.selected_columns:
                listbox.select_set(listbox.size()-1) 

        confirm_button = ttk.Button(dialog, text="Confirm", command=lambda: self.finalize_selection(dialog, listbox))
        confirm_button.pack(pady=20)

    def finalize_selection(self, dialog, listbox):
        self.selected_columns = [listbox.get(i) for i in listbox.curselection()]
        self.df_filtered = self.df[self.selected_columns]

        dialog.destroy()
        self.destroy()
        self.quit()

