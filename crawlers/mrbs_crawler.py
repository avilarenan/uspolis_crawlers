import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import date, timedelta
import calendar

days_of_week = ["seg", "ter", "qua", "qui", "sex", "sab", "dom",]

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def get_rooms_capacities_and_class_times(table):
    headers = []
    for i in table.find_all("th"):
        title = i.text
        headers.append(title)

    i = 0
    for element in headers:
        if len(element.split(":")) > 1:
            break
        i += 1

    rooms = headers[1:i]

    rooms_capacity = {}
    for room in rooms:
        if "-" in room:
            rooms_capacity[room[:5]] = room[5:]
        else:
            rooms_capacity[room[:-2]] = room[-2:]
    
    class_times = headers[i:]

    return rooms_capacity, class_times

def get_day_allocs(page, single_date):
    soup = BeautifulSoup(page.text, 'lxml')
    table1 = soup.find("table", id="day_main")

    rooms_capacity, class_times = get_rooms_capacities_and_class_times(table1)

    list_of_rows = []
    for j, j_content in enumerate(table1.find_all("tr")[1:]):
        row_data = j_content.find_all("td")
        row = [
            (
                i_content.text,
                i_content.attrs.get("rowspan"),
                list(rooms_capacity.keys())[i],
                class_times[j],
                class_times[j + int(i_content.attrs.get("rowspan", 0)) - 1]
            ) for i, i_content in enumerate(row_data)
        ]
        list_of_rows += row
        
    df = pd.DataFrame().from_records(list_of_rows)
    df.columns = ["code", "length", "room", "start_time", "end_time"]
    df = df.drop("length", axis=1)
    df = df.replace('', np.nan)
    df = df.dropna(axis=0, how="any").reset_index().drop("index", axis=1)
    df["date"] = single_date.strftime("%Y-%m-%d")
    df["day_of_week"] = days_of_week[single_date.weekday()]

    return df

def get_mrbs_data(start_date=date(2023, 8, 6), end_date=date(2023, 8, 12), mrbs_endpoint="main.lcs.poli.usp.br"):
    start_date = date(2023, 8, 6)
    end_date = date(2023, 8, 12)

    list_of_dfs = []

    for single_date in daterange(start_date, end_date):
        date_string = single_date.strftime("%Y-%m-%d")

        url = f'https://{mrbs_endpoint}/mrbs/index.php?view=day&view_all=1&page_date={date_string}'
        page = requests.get(url)
        list_of_dfs += [get_day_allocs(page, single_date)]

    final_allocs = pd.concat(list_of_dfs).reset_index().drop("index", axis=1)
    final_allocs.to_csv(f"mrbs_eletrica_{start_date}_to_{end_date}.csv")
    return final_allocs