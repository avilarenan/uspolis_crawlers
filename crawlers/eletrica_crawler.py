from mrbs_crawler import get_mrbs_data
from jupiter_crawler import get_jupiter_class_infos
import pandas as pd
import re
from datetime import datetime

def generate_eletrica_classes_file(start_date, end_date, building):

    unmatched = []

    turmas = get_mrbs_data(
        start_date=start_date,
        end_date=end_date,
        mrbs_endpoint="main.lcs.poli.usp.br"
    ).to_dict(orient="records")

    pattern = r"([A-Z]{3}\d{4})|(\d+)"

    filtered_turmas = []
    for turma in turmas: # cleaning data
        if re.search(pattern, turma["code"]):
            code = turma["code"].strip()
            code = code.replace(" ", "")
            code = code[:7]
            if re.fullmatch(pattern, code):
                turma["code"] = code
                filtered_turmas += [turma]
            else:
                print(f"""No match: {turma["code"]}""")
        else:
            print(f"""No match: {turma["code"]}""")
            
    turmas = filtered_turmas

    list_of_rows = []
    for turma in turmas:

        try:
            turmas_info = get_jupiter_class_infos(turma["code"])
        except IndexError as e:
            print(e)
            print(turma["code"])
            
        actual_turma = []
        for turma_info in turmas_info:
            try:
                if turma_info["hora_inicio"].index(turma["start_time"]) == turma_info["dia_semana"].index(turma["day_of_week"]):
                    actual_turma += [turma_info]
            except Exception as e:
                pass
        
        if len(actual_turma) == 0:
            unmatched += [{
                "aloc": turma,
                "jupiter": turmas_info
            }]
            continue
        turma_info = actual_turma[0]



        list_of_rows += [{
            "class_code" : turma_info["cod_turma"],
            "subject_code" : turma["code"],
            "subject_name" : turma_info["nome_disciplina"],
            "professor" : turma_info["prof"][0] if 0 in turma_info["prof"] else "",
            "start_period" : datetime.strptime(turma_info["inicio"], "%d/%m/%Y").strftime("%Y-%m-%d"),
            "end_period" : datetime.strptime(turma_info["fim"], "%d/%m/%Y").strftime("%Y-%m-%d"),
            "created_by" : f"{building}_crawler",
            "week_day" : turma["day_of_week"],
            "start_time" : turma["start_time"],
            "end_time" : turma["end_time"],
            "building" : building,
            "classroom" : turma["room"],
            "floor" : 0
        }]

    return pd.DataFrame().from_records(list_of_rows)