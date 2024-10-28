# squads_extraction.py

import requests
import pandas as pd
from dotenv import load_dotenv
import os

def extract_squads_data():

    object_name = "squads"
    associated_object_name = "teams"

   # Carregar variáveis de ambiente do .env
    load_dotenv()

    # Pegar o token do arquivo .env
    hubspot_token = os.getenv('HUBSPOT_TOKEN')

    # Usar o token no header
    headers = {'Authorization': 'Bearer ' + hubspot_token}

    # Construa a URL da API para obter registros do objeto principal
    url = f"https://api.hubapi.com/crm/v3/objects/{object_name}/search"
    query = {
        "filters": [],
        "properties": ["capacity_points", "departure_date", "function", "hubspot_team", "manager", "member_email",
                    "member_name", "member_status", "merged_record_ids", "modality", "object_create_date", 
                    "object_last_modified_date", "owner", "owner_assigned_date", "record_source", "record_source_detail_1",
                    "record_source_detail_2", "record_source_detail_3", "squad", "start_date", "updated_by_user_id"
        ],
        "limit": 100  
    }

    response = requests.post(url, headers=headers, json=query)

    if response.status_code == 200:
        response_json = response.json()
        
        if isinstance(response_json, dict) and 'results' in response_json:
            df_records = pd.DataFrame(response_json['results'])
        else:
            print("Formato inesperado da resposta JSON")
            df_records = pd.DataFrame()
    else:
        print(f"Erro na requisição: {response.status_code}")
        df_records = pd.DataFrame()

    # Função para obter IDs dos objetos associados
    def get_associated_objects(record_id):
        url = f"https://api.hubapi.com/crm/v3/objects/{object_name}/{record_id}/associations/{associated_object_name}"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro ao obter objetos associados para {record_id}: {response.status_code}")
            return None

    # Função para obter informações do objeto associado
    def get_team_details(team_id):
        url = f"https://api.hubapi.com/crm/v3/objects/{associated_object_name}/{team_id}"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Erro ao obter detalhes do time {team_id}: {response.status_code}")
            return None

    # Obter detalhes dos objetos associados
    def get_team_details_for_records(record_ids):
        team_details = []
        for record_id in record_ids:
            associated_team_ids = get_associated_objects(record_id)
            if associated_team_ids and 'results' in associated_team_ids:
                for team in associated_team_ids['results']:
                    team_id = team['id']
                    details = get_team_details(team_id)
                    if details:
                        team_details.append({
                            'squad_id': record_id,
                            'team_id': team_id,
                        })
        return pd.DataFrame(team_details)

    # Obter IDs dos registros
    if not df_records.empty:
        record_ids = df_records['id'].tolist()
        # Obter detalhes dos teams
        df_team_details = get_team_details_for_records(record_ids)

        # Expandir os dados das propriedades
        df_expanded = pd.json_normalize(df_records['properties'])
        squads = df_records.drop(columns=['properties']).join(df_expanded)

        # Unir com dados dos teams
        df_combined = squads.merge(df_team_details, left_on='id', right_on='squad_id', how='left')

    else:
        print("Nenhum registro encontrado.")


    object_name2 = "teams"

    # URL da API para obter registros
    url = f"https://api.hubapi.com/crm/v3/objects/{object_name2}/search"

    query = {
        "filters": [],
        "properties": ["squad_name"
        ],
        "limit": 100  
    }

    response = requests.post(url, headers={'Authorization': 'Bearer ' + hubspot_token},json=query)

    if response.status_code == 200:
        response_json = response.json()
        
        if isinstance(response_json, dict) and 'results' in response_json:
            df_records = pd.DataFrame(response_json['results'])
        else:
            print("Formato inesperado da resposta JSON")
    else:
        print(f"Erro na requisição: {response.status_code}")

    team_expanded = pd.json_normalize(df_records['properties'])

    team = df_records.drop(columns=['properties']).join(team_expanded)

    team = team[['id','squad_name']]

    teams = team.rename(columns={'id': 'team_id'})

    squads = pd.merge(df_combined, teams, on='team_id', how='left')

    return squads,teams