import tkinter as tk
from tkinter import filedialog

def browse_file():
    file_path = filedialog.askopenfilename()
    entry_file_path.delete(0, tk.END)
    entry_file_path.insert(tk.END, file_path)

def submit_link():
    link = entry_link.get()
    print("Selected file:", entry_file_path.get())
    print("Entered link:", link)

# Create the Tkinter window
window = tk.Tk()

# Create the file path label and entry
label_file_path = tk.Label(window, text="File Path:")
label_file_path.pack()

entry_file_path = tk.Entry(window, width=50)
entry_file_path.pack()

button_browse = tk.Button(window, text="Browse", command=browse_file)
button_browse.pack()

# Create the link label and entry
label_link = tk.Label(window, text="Website Link:")
label_link.pack()

entry_link = tk.Entry(window, width=50)
entry_link.pack()

# Create the submit button
button_submit = tk.Button(window, text="Submit", command=submit_link)
button_submit.pack()

# Run the Tkinter event loop
window.mainloop()
