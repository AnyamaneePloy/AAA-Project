from bs4 import BeautifulSoup
from requests import get
import requests

import pandas as pd
import time
from datetime import datetime
from time import sleep
import random
import re
import json
from tqdm import tqdm
from progress.bar import Bar

headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}

# Parameters setup
data_lst = []
key_lst = []
err_count = 0

page_number = int(input('Enter Start Number Page: '))#Start page
page_number_end = int(input('''Enter End Number Page (at June, 2023 end number is 1774 pages): ''')) #End page last page at 1774

page_range = (page_number_end-page_number)+1
pbar = tqdm(total=page_range, desc="Loading", bar_format="{l_bar}{bar} [ time left: {remaining} ]")
url_page = input('''Enter URL 
(Example: "https://www.one2car.com/%E0%B8%A3%E0%B8%96-%E0%B8%AA%E0%B8%B3%E0%B8%AB%E0%B8%A3%E0%B8%B1%E0%B8%9A-%E0%B8%82%E0%B8%B2%E0%B8%A2?page_number={}&page_size=26" "https://www.one2car.com/%E0%B8%A3%E0%B8%96-%E0%B8%AA%E0%B8%B3%E0%B8%AB%E0%B8%A3%E0%B8%B1%E0%B8%9A-%E0%B8%82%E0%B8%B2%E0%B8%A2?page_number={}&page_size=26"
Enter: ''')
while True:
    # Update the progress bar
    pbar.update(1)
    time.sleep(0.1)
    try :
        #One2Car Website:
        urls =url_page.format(page_number)
        response = get(urls, headers=headers)
        if response.status_code == 200:
            pass
            #print(f"Page{page_number}: Connect with URL Successful")
        elif response.status_code == 404:
            print(f"Page{page_number}: Error 404 page not found")
            continue
        else:
            print(f"Page{page_number}: Not both 200 and 404")
            continue

        html_soup = BeautifulSoup(response.text, 'html.parser')

        # Get .json data process
        content_list = html_soup.find_all('script')[2].text.strip()
        middle = len(content_list)//2
        under = content_list.rfind("itemListElement", 0, middle)
        content_list1 = html_soup.find_all('script')[2].text.strip()[under:]
        over = content_list1.find("[",0, middle)
        content_list2 = html_soup.find_all('script')[2].text.strip()[under+over:-2]
        #print(content_list)
        #print(content_list1)

        data_js = json.loads(content_list2)
        # print(data_js)

        df = pd.json_normalize(data_js)
        #print(df.head(10))

        data_lst.append(df)
        key_lst.append('Page'+str(page_number))
        
        #Write Data
        if (page_number % 10 == 0) | (page_number == page_number_end):
            data_result = pd.concat(data_lst, keys=key_lst)
            #set filename 
            now = datetime.now() # current date and time
            date_time = now.strftime("%Y%m%d_%H%M%S")
            #print("date and time:",date_time)
            #print(f'EndPage {page_number} Data Number: {len(data_result)}\n')
           
            data_result.to_csv(f"{date_time}_P{page_number}_one2car_data_noclean.csv", encoding="utf-8-sig") # CSV file           
            data_result.to_json(f"{date_time}_P{page_number}_one2car_data_noclean.json", orient='records')# JSON file
            data_lst,key_lst = [],[]

        if page_number == page_number_end:
            print(f">>>>>>>>>>> End Process at Page no.{page_number_end} <<<<<<<<<<<<")
            break
        page_number+=1
        
    except ValueError:
        if err_count== 5:
            break

        print(">>>>>>>>>>> Process Error <<<<<<<<<<<<")
        err_count+=1
        continue
              
pbar.close()
print("Task completed!")

