#%%
from bs4 import BeautifulSoup
from requests import get
import requests
import os
import pandas as pd
import time
from datetime import datetime
from time import sleep
import random
import re
import json
from tqdm import tqdm
from progress.bar import Bar
import tkinter as tk
from tkinter import filedialog
import pkg_resources

# installed_packages = pkg_resources.working_set
# # Create an empty DataFrame with columns
# columns = ["Package", "Version"]
# data_pkg = pd.DataFrame(columns=columns)

# with open('requirements.txt', 'w') as f:
#     for package in installed_packages:
#         f.write(f"{package.key}=={package.version}\n")
#         entry = [package.key, package.version]
#         row = pd.Series(entry, index=data_pkg.columns)
#         data_pkg = data_pkg.append(row, ignore_index=True)
#     print(data_pkg)
#%%
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}

#%% Folder and File Selection
# Select Folder Function
def save_selected_path(selected_path):
    with open("selected_path.txt", "w") as file:
        file.write(selected_path)

def load_selected_path():
    if os.path.exists("selected_path.txt"):
        with open("selected_path.txt", "r") as file:
            return file.read()
    return None

def search_for_file_path():
    
    try:
        prev_selected_path = load_selected_path()
        root = tk.Tk()
        root.withdraw() # use to hide tkinter window
        tempdir = filedialog.askdirectory(parent=root, initialdir=prev_selected_path, 
                                          title='Please select directory of Master file')
        if len(tempdir) > 0:
            print("Path: %s" % tempdir)
        save_selected_path(tempdir)
        return tempdir
    except Exception as e:
        print("An error occurred while selecting the directory. Please ensure it's a valid directory.")
        print(str(e))
        return None

def scrape_one2car_data(start_page, end_page, url_pattern):
    data_lst = []
    key_lst = []
    based_path = search_for_file_path ()

    page_range = (end_page - start_page) + 1
    pbar = tqdm(total=page_range, desc="Loading", bar_format="{l_bar}{bar} [ time left: {remaining}, elapsed: {elapsed} ]")

    MAX_RETRIES = 5  # Maximum number of times to retry a request
    for page_number in range(start_page, end_page + 1):
        pbar.update(1)
        time.sleep(0.1)
        retry_count = 0  # Reset the retry count for each page
        
        while retry_count < MAX_RETRIES:
            try:
                urls = url_pattern.format(page_number)
                response = get(urls, headers=headers,  timeout=60) # Setting a timeout of 10 seconds

                if response.status_code == 200:
                    pass
                elif response.status_code == 404:
                    print(f"Page {page_number}: Error 404 page not found")
                    continue
                else:
                    print(f"Page {page_number}: Not both 200 and 404")
                    continue

                html_soup = BeautifulSoup(response.text, 'html.parser')

                # Get .json data process
                content_list = html_soup.find_all('script')[2].text.strip()
                middle = len(content_list) // 2
                under = content_list.rfind("itemListElement", 0, middle)
                content_list1 = html_soup.find_all('script')[2].text.strip()[under:]
                over = content_list1.find("[", 0, middle)
                content_list2 = html_soup.find_all('script')[2].text.strip()[under + over:-2]

                data_js = json.loads(content_list2)
                df = pd.json_normalize(data_js)

                data_lst.append(df)
                key_lst.append('Page' + str(page_number))

                if (page_number % 10 == 0) or (page_number == end_page):
                    data_result = pd.concat(data_lst, keys=key_lst)
                    now = datetime.now()
                    date_time = now.strftime("%Y%m%d_%H%M%S")
                    file_path = f"{date_time}_P{page_number}_one2car_data_noclean.csv"
                    data_result.to_csv(os.path.join(based_path, file_path) , encoding="utf-8-sig")
                    data_lst, key_lst = [], []
                    
                if page_number == end_page:
                    print(f">>>>>>>>>>> End Process at Page no.{end_page} <<<<<<<<<<<<")
                    break
                break
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                print(f"Timeout or connection error on page {page_number}. Retrying...")
                retry_count += 1
                sleep(2 * retry_count)  # Exponential backoff: wait longer after each retry

            except ValueError:
                if err_count == 5:
                    break
                print(">>>>>>>>>>> Process Error <<<<<<<<<<<<")
                err_count += 1
                continue

        if retry_count == MAX_RETRIES:
            print(f"Failed to retrieve page {page_number} after {MAX_RETRIES} retries. Skipping...")

    pbar.close()
    print(">>>>>>>>>>> Task completed! <<<<<<<<<<<<")
    return data_result
#%%
# Example usage:
start_page_number = int(input('Enter Start Number Page:\t '))
end_page_number = int(input('''Enter End Number Page:\t '''))

# Update the URL pattern according to your requirements
url_pattern = "https://www.one2car.com/%E0%B8%A3%E0%B8%96-%E0%B8%AA%E0%B8%B3%E0%B8%AB%E0%B8%A3%E0%B8%B1%E0%B8%9A-%E0%B8%82%E0%B8%B2%E0%B8%A2?page_number={}&page_size=26"

# Call the function with start and end page numbers and the URL pattern
scraped_data = scrape_one2car_data(start_page_number, end_page_number, url_pattern)
