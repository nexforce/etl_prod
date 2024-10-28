# squads_transformation.py

import pandas as pd

def transform_squads_data(squads):
    
    # Remover colunas desnecessárias
    squads = squads.drop(columns=['archived','createdAt','updatedAt','hs_object_id','manager','modality'])

    # Adicionar colunas novas
    squads['monthly_cost'] = 0
    squads['list'] = None
    squads['slack_channel'] = None

    # Atualizar status
    squads['member_status'] = squads['member_status'].replace({'Active': 'ON THE TEAM', 'Inactive': 'LEFT THE TEAM'})

    # Converter colunas de data para o formato dd/mm/yyyy
    squads['hs_createdate'] = pd.to_datetime(squads['hs_createdate']).dt.strftime('%d/%m/%Y')
    squads['hs_lastmodifieddate'] = pd.to_datetime(squads['hs_lastmodifieddate']).dt.strftime('%d/%m/%Y')

    # Selecionar e renomear colunas
    squads = squads[['id','member_name','function','member_email','capacity_points','monthly_cost','squad_name','slack_channel','member_status',
                    'list','start_date','departure_date','hs_createdate','hs_lastmodifieddate']]
    
    new_columns = ['task_id', 'task_name', 'position', 'email','capacity_points','monthly_cost','team','slack_channel',
                'status', 'list','start_date','due_date','date_created', 'date_updated']
    
    squads.columns = new_columns

    # Transformar os tipos das colunas para correspondência com o BigQuery
    squads['task_id'] = squads['task_id'].astype(str)
    squads['task_name'] = squads['task_name'].astype(str)
    squads['position'] = squads['position'].astype(str)
    squads['email'] = squads['email'].astype(str)
    squads['capacity_points'] = pd.to_numeric(squads['capacity_points'], errors='coerce')
    squads['monthly_cost'] = pd.to_numeric(squads['monthly_cost'], errors='coerce')
    squads['team'] = squads['team'].astype(str)
    squads['slack_channel'] = squads['slack_channel'].astype(str)
    squads['status'] = squads['status'].astype(str)
    squads['list'] = squads['list'].astype(str)
    
    # Converter colunas de data para o formato de string ou datetime adequado para BigQuery
    squads['date_created'] = pd.to_datetime(squads['date_created'], errors='coerce', dayfirst=True).dt.date  # Converte para formato de data
    squads['date_updated'] = pd.to_datetime(squads['date_updated'], errors='coerce', dayfirst=True).dt.date  # Converte para formato de data

    squads['start_date'] = squads['start_date'].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime('%d/%m/%Y') if pd.notnull(x) else x)
    squads['due_date'] = squads['due_date'].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime('%d/%m/%Y') if pd.notnull(x) else x)

    exp_squad = squads.copy()
    
    return exp_squad

