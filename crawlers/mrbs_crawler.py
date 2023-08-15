import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import date, timedelta
import re

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

def get_day_allocs(page):
    """Returns allocs as matrix"""
    soup = BeautifulSoup(page.text, 'lxml')
    table1 = soup.find("table", id="day_main")

    rooms_capacity, class_times = get_rooms_capacities_and_class_times(table1)
    
    matrix = [[None for column in range(len(rooms_capacity))] for row in range(len(class_times))]

    for j, j_content in enumerate(table1.find_all("tr")[1:]):

        row_data = j_content.find_all("td")

        real_indexes = []
        for m in range(len(rooms_capacity)):
            if matrix[j][m] is None:
                real_indexes += [m]

        for i, i_content in enumerate(row_data):
            actual_index = real_indexes[i]
            duration = int(i_content.attrs.get("rowspan", 0))
            for k in range(duration):
                matrix[j+k][actual_index] = i_content.text
            
    return pd.DataFrame(matrix, columns=rooms_capacity, index=class_times)

def get_day_allocs_straight(matrix_allocs, weekday):
    df = matrix_allocs

    result = []
    for sala in df.columns.to_list():

        s = df[[sala]].reset_index()
        s = s.rename({"index" : "start_time"}, axis=1)
        ts = s.groupby(sala).first()

        s = df[[sala]].reset_index()
        s = s.rename({"index" : "end_time"}, axis=1)
        te = s.groupby(sala).last()

        fdf = pd.concat([ts, te], axis=1)
        fdf["room"] = fdf.index.name
        fdf.index.name = "code"
        fdf = fdf.reset_index()
        result += [fdf]

    df = pd.concat(result).reset_index().drop("index", axis=1)
    df["day_of_week"] = weekday
    return df

def get_mrbs_data(start_date, end_date, mrbs_endpoint="main.lcs.poli.usp.br"):

    list_of_dfs = []

    for single_date in daterange(start_date, end_date):
        date_string = single_date.strftime("%Y-%m-%d")

        url = f'https://{mrbs_endpoint}/mrbs/index.php?view=day&view_all=1&page_date={date_string}'
        page = requests.get(url)
        list_of_dfs += [get_day_allocs_straight(get_day_allocs(page), days_of_week[single_date.weekday()])]

    final_allocs = pd.concat(list_of_dfs).reset_index().drop("index", axis=1)
    final_allocs.to_csv(f"mrbs_eletrica_{start_date}_to_{end_date}.csv")

    pattern = r"([A-Z]{3}\d{4})|(\d+)"

    turmas_mrbs = final_allocs.to_dict(orient="records")

    filtered_turmas_mrbs = []
    for turma_mrbs in turmas_mrbs: # cleaning data
        if re.search(pattern, turma_mrbs["code"]):
            code = turma_mrbs["code"].strip()
            code = code.replace(" ", "")
            code = code[:7]
            if re.fullmatch(pattern, code):
                turma_mrbs["code"] = code
                filtered_turmas_mrbs += [turma_mrbs]
            else:
                print(f"""No match: {turma_mrbs["code"]}""")
        else:
            print(f"""No match: {turma_mrbs["code"]}""")

    return pd.DataFrame().from_records(filtered_turmas_mrbs)