import os
import pickle
import sys
import time
import tkinter as tk
import traceback

import pandas as pd
from tkinter import ttk
from tkinter import messagebox
import loggingvar
from smbprotocol.connection import Connection
# from smbprotocol.exceptions import InvalidStatusCode, UnauthorizedAccess, ItemNotFound

# Function to update the output box based on the selected item
def update_output(*args):
    selected_item = combo_var.get()
    if selected_item == 'Overall_info':
        output_text.delete(1.0, tk.END)  # Clear previous text
        output_text.insert(1.0, f"Selected item: {selected_item}")
        text = """
                        ACTIVE CONNECTIONS : 
                """
        output_text.insert(2.0, text)
        with open('connections.pkl', 'rb') as file:
            data = pickle.load(file)
        dfdata = {"IP Addresses":[], 'last_contact':[], 'Connection no.':[], 'info':[]}
        for ip in data['ACTIVE_CONNECTIONS'].keys():
            for index,inst in enumerate(data['ACTIVE_CONNECTIONS'][ip].keys()):
                dfdata["IP Addresses"].append(ip)
                dfdata["Connection no."].append(index)
                # print(data['ACTIVE_CONNECTIONS'][ip])
                dfdata["last_contact"].append(data['ACTIVE_CONNECTIONS'][ip][inst]["Last Contact"])
                if 'info' in data['ACTIVE_CONNECTIONS'][ip][inst].keys():
                    dfdata["info"].append(str(data['ACTIVE_CONNECTIONS'][ip][inst]['info']))
                else:
                    dfdata["info"].append(pd.NA)

        # print(dfdata)

        df = pd.DataFrame(data=dfdata)
        dfs = df.to_markdown(index=False, tablefmt="grid")
        output_text.insert("3.0", '\n')
        output_text.insert("4.0",dfs)
    else:
        output_text.delete(1.0, tk.END)  # Clear previous text
        output_text.insert(1.0, f"Selected item: {selected_item}\n")
        with open(f'{selected_item}.log','r',encoding='utf-8') as file:
            # output_text.delete(1.0, tk.END)  # Clear previous text
            # output_text.insert(tk.END, f"Selected item: {selected_item}")
            text = file.readlines()
            text.reverse()
            text = '\n'.join(text)
            # output_text.config(state="normal")  # Enable Text widget for editing
            # output_text.delete("1.0", "end")  # Clear existing text
            output_text.insert("2.0", text)  # Insert new text
            # output_text.config(state="disabled")


# Create the main window
root = tk.Tk()
project_name = 'Rappi'
root.title("Rappi Running stats")
root.geometry("900x400")

# Create a dropdown list (combobox)
try:
    os.chdir(rf'{loggingvar.network_drive_char}:\\')
except:
    messagebox.showerror("File not found","File not found, make sure to map the network logging drive")
    sys.exit()

try:
    options = []
    options.append('Overall_info')
    # print(os.listdir())
    options.extend([x.split('.log')[0] for x in os.listdir() if x.endswith('.log')])
    # print(options)
    combo_var = tk.StringVar()
    combo_var.set('Overall_info')

    combo_var.trace('w',update_output)
    combo_box = ttk.Combobox(root, textvariable=combo_var, values=options)
    combo_box.pack(pady=20)
    # update_button = tk.Button(root, text="Update Output", command=update_output, width=20, height=1)
    # update_button.pack()
    # Create an output text widget
    output_text = tk.Text(root, height=100, width=300)
    output_text.pack()
    update_output()

    # Create a button to trigger the output update

    # update_button.size()
    # update_button.place(x=550, y=20)


    # Start the Tkinter main event loop
    root.mainloop()
except:
    messagebox.showerror("Error",str(traceback.format_exc()))