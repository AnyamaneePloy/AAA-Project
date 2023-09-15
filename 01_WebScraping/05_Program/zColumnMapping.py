
import tkinter as tk
from tkinter import ttk

class ColumnMapping(tk.Toplevel):
    def __init__(self, columns, columns_mapp):
        super().__init__()

        self.geometry("500x500")
        self.title("Column Mapping")

        self.columns = columns
        self.mappings = {}

        self.possible_names = columns_mapp

        # Left listbox for columns
        self.column_listbox = tk.Listbox(self, selectmode=tk.SINGLE)
        for col in self.columns:
            self.column_listbox.insert(tk.END, col)
        self.column_listbox.bind("<<ListboxSelect>>", self.on_column_select)
        self.column_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Right panel for renaming
        self.right_panel = ttk.Frame(self)
        self.right_panel.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        self.selected_column_var = tk.StringVar()
        ttk.Label(self.right_panel, textvariable=self.selected_column_var).pack(pady=10)

        self.new_name_var = tk.StringVar()
        self.new_name_dropdown = ttk.Combobox(self.right_panel, textvariable=self.new_name_var, values=self.possible_names)
        self.new_name_dropdown.pack(pady=10)

        ttk.Button(self.right_panel, text="Map", command=self.map_column).pack(pady=10)
        ttk.Button(self.right_panel, text="Next", command=self.close_app).pack(pady=10)

        # Display mappings using Treeview
        self.mapping_tree = ttk.Treeview(self.right_panel, columns=('Original', 'Mapped'), show='headings')
        self.mapping_tree.heading('Original', text='Original Name')
        self.mapping_tree.heading('Mapped', text='Mapped Name')
        self.mapping_tree.pack(fill=tk.BOTH, expand=True, pady=20)

    def on_column_select(self, event):
        selected_index = self.column_listbox.curselection()
        if selected_index:  # Check if a selection was made
            self.selected_column = self.column_listbox.get(selected_index[0])
            self.selected_column_var.set(f"Map: {self.selected_column}")

    def map_column(self):
        new_name = self.new_name_var.get()
        if new_name:
            self.mappings[self.selected_column] = new_name
            self.refresh_mappings_tree()

    def refresh_mappings_tree(self):
        # Clear current items
        for item in self.mapping_tree.get_children():
            self.mapping_tree.delete(item)
        # Insert new mappings
        for orig, mapped in self.mappings.items():
            self.mapping_tree.insert("", "end", values=(orig, mapped))
        self.mapping_tree.update()  # Add this line

    def close_app(self):
        self.destroy()
        self.quit()

