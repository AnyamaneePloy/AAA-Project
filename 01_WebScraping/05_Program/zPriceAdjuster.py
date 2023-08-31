import tkinter as tk
from tkinter import ttk, messagebox
from pandastable import Table
import pandas as pd

class PriceAdjuster:
    def __init__(self, master, df):
        self.root = master
        self.df = df.copy()
        self.df['Adjusted_Price'] = self.df['MarketPrice']
        
        self.initUI()

    def initUI(self):
        self.frame = tk.Frame(self.root)
        self.frame.pack(fill='both', expand=True)

        self.pt = Table(self.frame, dataframe=self.df)
        self.pt.show()

        self.create_filters()
        
        self.label_adjustment = ttk.Label(self.root, text="Adjustment % (discount: -5, addition: 5):")
        self.label_adjustment.pack(pady=10)

        self.entry_adjustment = ttk.Entry(self.root)
        self.entry_adjustment.pack(pady=10)

        self.btn_apply = ttk.Button(self.root, text="Apply Adjustment", command=self.apply_adjustment)
        self.btn_apply.pack(pady=10)

        self.btn_quit = ttk.Button(self.root, text="Quit", command=self.root.quit)
        self.btn_quit.pack(pady=20)

    def create_filters(self):
        self.n_column = tk.StringVar()
        self.combobox_column = ttk.Combobox(self.root, width=27, textvariable=self.n_column)
        self.combobox_column.pack(pady=10)

        self.combobox_column['values'] = ['- select column -'] + list(self.df.columns)
        self.combobox_column.bind("<<ComboboxSelected>>", self.update_values)

        self.n_value = tk.StringVar()
        self.combobox_value = ttk.Combobox(self.root, width=27, textvariable=self.n_value)
        self.combobox_value.pack(pady=10)
        self.combobox_value['values'] = ['- select value -']
        self.combobox_value.bind("<<ComboboxSelected>>", self.selection)

    def update_values(self, event):
        selected_column = self.combobox_column.get()
        unique_values = ['- select value -'] + sorted(self.df[selected_column].unique())
        self.combobox_value['values'] = unique_values
        self.combobox_value.current(0)

    def selection(self, event):
        selected_column = self.combobox_column.get()
        selected_value = self.combobox_value.get()
        
        if selected_column == '- select column -' or selected_value == '- select value -':
            dfx = self.df
        else:
            dfx = self.df[self.df[selected_column] == selected_value]
        
        self.pt.model.df = dfx
        self.pt.redraw()

    def apply_adjustment(self):
        try:
            adjustment = float(self.entry_adjustment.get()) / 100

            selected_column = self.combobox_column.get()
            selected_value = self.combobox_value.get()
            
            if selected_column == '- select column -' or selected_value == '- select value -':
                dfx = self.df
            else:
                dfx = self.df[self.df[selected_column] == selected_value]

            dfx['Adjusted_Price'] = dfx['Adjusted_Price'] * (1 + adjustment)
            self.df.update(dfx)

            self.pt.model.df = dfx
            self.pt.redraw()
        except ValueError:
            messagebox.showerror("Error", "Please enter a valid adjustment percentage.")