from mrbs_crawler import get_mrbs_data
from jupiter_crawler import get_jupiter_class_infos
from datetime import datetime
import pandas as pd

def generate_eletrica_classes_file(start_date: str, end_date: str, BUILDING: str):

    turmas = get_mrbs_data(
        start_date=start_date,
        end_date=end_date,
        mrbs_endpoint="main.lcs.poli.usp.br"
    )
    turmas.to_csv("mrbs_crawled.csv")

    def get_possible_rooms_by_day_and_start_time(code:str, day: str, start_time: str):
        possible_rooms = []
        for item in turmas.to_dict(orient="records"):
            if item["code"] == code and item["day_of_week"] == day and item["start_time"] == start_time:
                possible_rooms += [item["room"]]

        return possible_rooms

    list_of_rows = []
    for code in list(turmas["code"].unique()):
        try:
            jupiter_turmas_info = get_jupiter_class_infos(code)
        except IndexError as e:
            print(f"Jupiter crawler failed for {code} with the following error: \n{e}")
            continue
        for jupiter_turma in jupiter_turmas_info:
            for day, ts, te in zip(jupiter_turma["dia_semana"], jupiter_turma["hora_inicio"], jupiter_turma["hora_fim"]):
                rooms = get_possible_rooms_by_day_and_start_time(code, day, ts)
                rooms_str = ','.join(rooms)
                

                list_of_rows += [{
                    "class_code" : jupiter_turma["cod_turma"],
                    "subject_code" : code,
                    "subject_name" : jupiter_turma["nome_disciplina"],
                    "professor" : jupiter_turma["prof"][0] if 0 in jupiter_turma["prof"] else "",
                    "start_period" : datetime.strptime(jupiter_turma["inicio"], "%d/%m/%Y").strftime("%Y-%m-%d"),
                    "end_period" : datetime.strptime(jupiter_turma["fim"], "%d/%m/%Y").strftime("%Y-%m-%d"),
                    "created_by" : f"{BUILDING}_crawler",
                    "week_day" : day,
                    "start_time" : ts,
                    "end_time" : te,
                    "building" : BUILDING,
                    "classroom" : rooms_str,
                    "floor" : 0
                }]

    df = pd.DataFrame().from_records(list_of_rows)
    df.to_csv("eletrica_ingestion.csv")
    return df