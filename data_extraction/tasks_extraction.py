# tasks_extraction.py

from tenacity import retry, wait_exponential, stop_after_attempt
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm 
from datetime import datetime, timedelta
import time
import re
import json
import requests
import logging
import numpy as np 
import pandas as pd
import diskcache as dc

def extract_tasks_data():

    # Cache Persistente
    cache = dc.Cache('/tmp/mycache')

    cache.clear()

    # Configuração do Logger
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Configuração da Data Atual
    hoje = datetime.now()

    # Definição dos intervalos de datas e nomes das sprints
    sprints = [
    ("07-01", "20-01", "Sprint 1 - 2025"), ("21-01", "03-02", "Sprint 2 - 2025"), ("04-02", "17-02", "Sprint 3 - 2025"),
    ("18-02", "03-03", "Sprint 4 - 2025"), ("04-03", "17-03", "Sprint 5 - 2025"), ("18-03", "31-03", "Sprint 6 - 2025"),
    ("01-04", "14-04", "Sprint 7 - 2025"), ("15-04", "28-04", "Sprint 8 - 2025"), ("29-04", "12-05", "Sprint 9 - 2025"),
    ("13-05", "26-05", "Sprint 10 - 2025"), ("27-05", "09-06", "Sprint 11 - 2025"), ("10-06", "23-06", "Sprint 12 - 2025"),
    ("24-06", "07-07", "Sprint 13 - 2025"), ("08-07", "21-07", "Sprint 14 - 2025"), ("22-07", "04-08", "Sprint 15 - 2025"),
    ("05-08", "18-08", "Sprint 16 - 2025"), ("19-08", "01-09", "Sprint 17 - 2025"), ("02-09", "15-09", "Sprint 18 - 2025"),
    ("16-09", "29-09", "Sprint 19 - 2025"), ("30-09", "13-10", "Sprint 20 - 2025"), ("14-10", "27-10", "Sprint 21 - 2025"),
    ("28-10", "10-11", "Sprint 22 - 2025"), ("11-11", "24-11", "Sprint 23 - 2025"), ("25-11", "08-12", "Sprint 24 - 2025"),
    ("09-12", "22-12", "Sprint 25 - 2025")
]
    # Encontrar o sprint correspondente
    sprint_atual = None
    for inicio, fim, sprint in sprints:
        data_inicio = datetime.strptime(f"{inicio}-2025", "%d-%m-%Y")
        data_fim = datetime.strptime(f"{fim}-2025", "%d-%m-%Y")
        
        # Ajuste: comparar incluindo o dia de fim
        if data_inicio <= hoje <= (data_fim.replace(hour=23, minute=59, second=59)):
            sprint_atual = sprint
            break

    logger.info(f"O sprint atual é: {sprint_atual}")

    # Configuração da API
    team_id = "3061382"
    api_token = "pk_82087950_HMVU88P6USAAWJ6B9GQBXZ5Y5HRFBPHG"
    headers = {"Authorization": api_token}

    # Limites de taxa e configuração
    MAX_REQUESTS_PER_MINUTE = 100
    WAIT_TIME = 60 / MAX_REQUESTS_PER_MINUTE  # Tempo de espera entre requisições
    MAX_RETRIES = 5
    RETRY_DELAY = 10  # Tempo inicial de atraso em segundos

    def cached(func):
        def wrapper(*args, **kwargs):
            key = (func.__name__, args, frozenset(kwargs.items()))
            if key in cache:
                return cache[key]
            result = func(*args, **kwargs)
            cache[key] = result
            return result
        return wrapper

    @cached
    def get_all_spaces(team_id):
        url = f"https://api.clickup.com/api/v2/team/{team_id}/space"
        query = {"archived": "false"}
        response = requests.get(url, headers=headers, params=query, timeout=45)
        response.raise_for_status()
        return response.json()

    @cached
    def get_all_folders(space_id):
        url = f"https://api.clickup.com/api/v2/space/{space_id}/folder"
        query = {"archived": "false"}
        response = requests.get(url, headers=headers, params=query, timeout=45)
        response.raise_for_status()
        return response.json()

    @cached
    def get_all_lists(folder_id):
        url = f"https://api.clickup.com/api/v2/folder/{folder_id}/list"
        query = {"archived": "false"}
        response = requests.get(url, headers=headers, params=query, timeout=45)
        response.raise_for_status()
        return response.json()

    @retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(MAX_RETRIES))
    def get_all_tasks_in_list(list_id):
        tasks = []
        page = 0
        while True:
            url = f"https://api.clickup.com/api/v2/list/{list_id}/task"
            query = {"page": page, "subtasks": "true", "include_closed": "true"}
            response = requests.get(url, headers=headers, params=query, timeout=45)
            response.raise_for_status()
            data = response.json()
            tasks.extend(data['tasks'])
            if len(data['tasks']) < 100:
                break
            page += 1
            time.sleep(WAIT_TIME)  # Adicionando um pequeno atraso entre as requisições
        return tasks

    def get_all_tasks(team_id, list_name_filter):
        all_tasks = []
        spaces = get_all_spaces(team_id)['spaces']
        future_to_space = {}
        
        # Adicionando progresso de tarefas
        total_spaces = len(spaces)
        with ThreadPoolExecutor(max_workers=10) as executor:
            for space in spaces:
                space_id = space['id']
                space_name = space['name']
                
                if space_name in ["Leadership", "Operations"]:
                    continue
                
                if space_name == "Squads: Sprints":
                    folders = get_all_folders(space_id)['folders']
                    for folder in folders:
                        if folder['name'] == "Sprints 2024":
                            folder_id = folder['id']
                            lists = get_all_lists(folder_id)['lists']
                            for list_item in lists:
                                if list_item['name'] == list_name_filter:
                                    list_id = list_item['id']
                                    future_to_space[executor.submit(get_all_tasks_in_list, list_id)] = space_name
                else:
                    folders = get_all_folders(space_id)['folders']
                    for folder in folders:
                        folder_id = folder['id']
                        lists = get_all_lists(folder_id)['lists']
                        for list_item in lists:
                            list_id = list_item['id']
                            future_to_space[executor.submit(get_all_tasks_in_list, list_id)] = space_name
            
            with tqdm(total=len(future_to_space), desc="Fetching tasks") as pbar:
                for future in as_completed(future_to_space):
                    try:
                        tasks = future.result()
                        all_tasks.extend(tasks)
                    except Exception as e:
                        logger.error(f"Erro ao obter tarefas: {e}")
                    finally:
                        pbar.update(1)  # Atualiza a barra de progresso

        return all_tasks

    list_name_filter = sprint_atual

    try:
        all_tasks = get_all_tasks(team_id, list_name_filter)
        df_task = pd.json_normalize(all_tasks)
        logger.info("Tarefas obtidas com sucesso!")
    except Exception as e:
        logger.error(f"Falha ao obter as tarefas: {e}")

    target_name = list_name_filter

    def row_contains_target(row, target):
        return any(row.astype(str).str.contains(target))

    df_task['contains_target'] = df_task.apply(row_contains_target, axis=1, target=target_name)
    count_rows_with_target = df_task['contains_target'].sum()

    def extract_sprint_number(name):
        match = re.search(r"Sprint (\d+)", name)
        return int(match.group(1)) if match else None

    sprint_number = extract_sprint_number(target_name)
    df_task['Sprint'] = sprint_number

    tasks_req = df_task[df_task['contains_target']]

    return tasks_req
