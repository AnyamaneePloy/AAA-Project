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
import tkinter 
from tkinter import filedialog
import pkg_resources

installed_packages = pkg_resources.working_set
# Create an empty DataFrame with columns
columns = ["Package", "Version"]
data_pkg = pd.DataFrame(columns=columns)

with open('requirements.txt', 'w') as f:
    for package in installed_packages:
        f.write(f"{package.key}=={package.version}\n")
        entry = [package.key, package.version]
        row = pd.Series(entry, index=data_pkg.columns)
        data_pkg = data_pkg.append(row, ignore_index=True)
    print(data_pkg)
#%%
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}

root = tkinter.Tk()
root.withdraw() #use to hide tkinter window
def search_for_file_path ():
    currdir = os.getcwd()
    tempdir = filedialog.askdirectory(parent=root, initialdir=currdir, title='Please select a directory')
    if len(tempdir) > 0:
        print ("Path: %s" % tempdir)
    return tempdir

def scrape_one2car_data(start_page, end_page, url_pattern):
    data_lst = []
    key_lst = []
    based_path = search_for_file_path ()

    page_range = (end_page - start_page) + 1
    pbar = tqdm(total=page_range, desc="Loading", bar_format="{l_bar}{bar} [ time left: {remaining}, elapsed: {elapsed} ]")

    for page_number in range(start_page, end_page + 1):
        pbar.update(1)
        time.sleep(0.1)

        try:
            urls = url_pattern.format(page_number)
            response = get(urls, headers=headers)

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
        except ValueError:
            if err_count == 5:
                break

            print(">>>>>>>>>>> Process Error <<<<<<<<<<<<")
            err_count += 1
            continue

    pbar.close()
    print(">>>>>>>>>>> Task completed! <<<<<<<<<<<<")
    return data_result
#%%
# Example usage:
start_page_number = int(input('Enter Start Number Page: '))
end_page_number = int(input('''Enter End Number Page (at July, 2023 end number is 1821 pages): '''))

# Update the URL pattern according to your requirements
url_pattern = "https://www.one2car.com/%E0%B8%A3%E0%B8%96-%E0%B8%AA%E0%B8%B3%E0%B8%AB%E0%B8%A3%E0%B8%B1%E0%B8%9A-%E0%B8%82%E0%B8%B2%E0%B8%A2?page_number={}&page_size=26"

# Call the function with start and end page numbers and the URL pattern
scraped_data = scrape_one2car_data(start_page_number, end_page_number, url_pattern)
