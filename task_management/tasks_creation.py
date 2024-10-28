#tasks_creation

from dotenv import load_dotenv
import requests
import pandas as pd
import os

def create_tasks_in_history(task_history, exp_squad):

    squadss = exp_squad.copy()

    task_history['Sprint'] = task_history['Sprint'].apply(lambda x: f"Sprint {int(x)}")

    json_data = {
        'id': '2256e0b7-3f05-440b-8f28-05b5b8f45704',
        'name': 'Nx Team',
        'type': 'drop_down',
        'type_config': {
            'default': 0,
            'placeholder': None,
            'new_drop_down': True,
            'options': [
                {'id': 'd6e583b1-c745-4257-9caf-40ebb0160e86', 'name': 'Altbier', 'color': None, 'orderindex': 0},
                {'id': '29df6fa7-e0bd-484f-9e5d-cc28bd1ad593', 'name': 'Aperol', 'color': None, 'orderindex': 1},
                {'id': '31ccd67d-8721-47f7-87c6-19e33d8e6fc9', 'name': 'Baden Baden', 'color': None, 'orderindex': 2},
                {'id': '7e54dec7-4845-43f7-8072-f15f215f5932', 'name': 'Blue Moon', 'color': None, 'orderindex': 3},
                {'id': 'e9f484b3-826c-480d-ad99-f0a5ead3a07d', 'name': 'Chimarrão', 'color': None, 'orderindex': 4},
                {'id': '78a59be6-1c5d-416a-a35c-efc522f84316', 'name': 'Dunkelweiz', 'color': None, 'orderindex': 5},
                {'id': 'b9788f9e-0b1b-4220-ae02-d61ad29996f9', 'name': 'Lager', 'color': None, 'orderindex': 6},
                {'id': '8338ebef-e774-480b-9145-92128f516eb6', 'name': 'Pilsen', 'color': None, 'orderindex': 7},
                {'id': '18212b76-a8fa-4505-b0b0-e13563593236', 'name': 'Malzbier', 'color': None, 'orderindex': 8},
                {'id': '7a926332-382a-48bc-94b5-af47cf4b92bc', 'name': 'Witbier', 'color': None, 'orderindex': 9},
                {'id': 'ab71cfb7-892b-494f-b048-1ae7ca32bd84', 'name': 'Stout', 'color': None, 'orderindex': 10},
                {'id': 'c1735a0d-2e8e-42a0-90d7-f34405d9fd98', 'name': 'Techila', 'color': None, 'orderindex': 11},
                {'id': '502a5c36-548b-414d-9a63-f4b8a9da7dfb', 'name': 'Weissbier', 'color': None, 'orderindex': 12},
                {'id': 'e828adea-a8da-48ca-b22f-1ec11039a2bd', 'name': 'Product', 'color': None, 'orderindex': 13},
                {'id': '6010dce2-15ec-46aa-8b96-2f837dc2ac56', 'name': 'Malbec', 'color': None, 'orderindex': 14},
                {'id': '413799ca-f332-4aca-9104-686757585a0c', 'name': 'Business Intelligence', 'color': None, 'orderindex': 15},
                {'id': '8c14753d-b497-4216-9a44-779040f70657', 'name': 'Developer', 'color': None, 'orderindex': 16},
                {'id': '516a19de-a502-458b-879a-617f49c5516b', 'name': 'Education', 'color': None, 'orderindex': 17},
                {'id': 'e2048562-52f8-4422-9d17-11d9cc235594', 'name': 'Support', 'color': None, 'orderindex': 18},
                {'id': '65c70817-345a-4307-9a8f-f8ab216e0d76', 'name': 'Other', 'color': None, 'orderindex': 19},
                {'id': 'd754d703-fb35-46b0-86b6-1ddbf603a561', 'name': 'Customer', 'color': None, 'orderindex': 20}
            ]
        },
        'date_created': '1673287867538',
        'hide_from_guests': False,
        'value': 0,
        'required': False
    }

    # Criar um dicionário para mapeamento nome -> valor
    team_mapping = {option['name']: option['orderindex'] for option in json_data['type_config']['options']}

    # Função para gerar o JSON com o valor atualizado
    def generate_updated_json(team_name):
        # Copia o JSON original
        updated_json = json_data.copy()
        # Atualiza o valor com base no nome do time
        updated_json['value'] = team_mapping.get(team_name, 'Unknown')
        return updated_json

    # Adicionar a nova coluna com os valores mapeados
    squadss['custom_fields'] = squadss['team'].apply(generate_updated_json)

    # Função para atualizar o campo 'custom_fields' em Sprint
    def update_custom_fields(row):
        sprint_value = row['Sprint']
        if not pd.isna(sprint_value):
            sprint_number = sprint_value.split(' ')[1]  # Extrai o número do sprint
            sprint_number = int(sprint_number)  # Converte para inteiro
            
            # Encontra o campo 'Custom Sprints' e atualiza o value
            for field in row['custom_fields']:
                if field['name'] == 'Custom Sprints':
                    for option in field['type_config']['options']:
                        if option['name'] == f'Sprint {sprint_number:02d}':  # Formata com dois dígitos
                            field['value'] = option['orderindex']
                            break
        return row

    # Função para extrair o ID do dicionário
    def extract_id(user_list):
        if len(user_list) > 0:
            return user_list[0].get('id', None)
        return None

    # Função para extrair o username do dicionário
    def extract_username(user_list):
        if len(user_list) > 0:
            return user_list[0].get('username', None)
        return None

    # Função para filtrar apenas o campo 'Nx Team'
    def filter_nx_team(custom_fields):
        return [field for field in custom_fields if field['name'] == 'Nx Team']

    # Função para combinar lista de dicionários com um dicionário único
    def combine_dicts(dict_list, single_dict):
        if isinstance(single_dict, list):
            if len(single_dict) == 0:
                return dict_list
        elif pd.isna(single_dict):
            single_dict = []
        else:
            single_dict = [single_dict]
        return dict_list + single_dict

    # Função para criar tarefa no ClickUp
    def create_task_in_clickup(row, list_id, api_token):
        url = f'https://api.clickup.com/api/v2/list/{list_id}/task'
        headers = {
            'Authorization': api_token,
            'Content-Type': 'application/json'
        }
        task = {
            'name': row['name'],
            'description': row['description'],
            'status': row['status.status'],
            'assignees': [row['assignee_id']],
            'due_date': row['due_date'],
            'custom_fields': row['custom_fields'],
            'priority': row['priority.id'],
            'points': row['points'],
            'notify_all': False,
           # 'notify': [] 
        }
        response = requests.post(url, headers=headers, json=task)
        if response.status_code == 200:
            print(f"Tarefa '{task['name']}' criada com sucesso!")
        else:
            print(f"Falha ao criar a tarefa '{task['name']}': {response.status_code} - {response.text}")

    # Aplica a função a cada linha do DataFrame
    task_history = task_history.apply(update_custom_fields, axis=1)

    # Extrair ID e username dos assignees
    task_history['assignee_username'] = task_history['assignees'].apply(extract_username)
    task_history['assignee_id'] = task_history['assignees'].apply(extract_id)

    # Filtrar apenas o campo 'Nx Team' e mesclar DataFrames
    aux2 = squadss.copy()
    #aux2['custom_fields'] = aux2['custom_fields'].apply(filter_nx_team)
    aux2 = aux2.rename(columns={'task_name': 'assignee_username'})
    aux2['assignee_username'] = aux2['assignee_username'].str.strip()
    task_history['assignee_username'] = task_history['assignee_username'].str.strip()
    task_history = task_history.merge(aux2[['assignee_username', 'custom_fields']], on='assignee_username', how='left')

    # Aplicar a função para combinar listas de dicionários
    task_history['custom_fields'] = task_history.apply(lambda row: combine_dicts(row['custom_fields_x'], row['custom_fields_y']), axis=1)

    # Atualizar status
    task_history.loc[task_history['status.status'] == 'complete', 'status.status'] = 'done'
    task_history = task_history.fillna(0)

    # Salvar DataFrame como JSON
    json_file_path = 'tasks.json'
    task_history.to_json(json_file_path, orient='records', lines=True)

    # Carregar o arquivo JSON em um DataFrame
    df = pd.read_json('tasks.json', lines=True)

    # Token e ID
    list_id = '901301447850'
    # Carregar variáveis do arquivo .env
    load_dotenv()

    api_token = os.getenv('API_TOKEN_SECONDARY') 

    # Iterar sobre as tarefas e criar cada uma no ClickUp
    df.apply(create_task_in_clickup, axis=1, list_id=list_id, api_token=api_token)

    return None