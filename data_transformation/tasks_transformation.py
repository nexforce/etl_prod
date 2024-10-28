import json
import pandas as pd
from shared_utils import convert_to_brazilian_date, date_columns

def transform_tasks_data(tasks, exp_squad):  # Passando o DataFrame de squads como argumento
    # Aplicando a conversão para cada coluna de data
    for col in date_columns:
        if col in tasks.columns:
            tasks[col] = tasks[col].apply(convert_to_brazilian_date)

    # Função para extrair Custom Fields
    def extract_fields(row):
        custom_fields = row['custom_fields']
        assignees = row['assignees']
        
        if isinstance(custom_fields, str):
            custom_fields_list = json.loads(custom_fields)
        else:
            custom_fields_list = custom_fields
        
        result = {
            'reviewer': None,
            'assignee': None,
            'email': None,
            'agile_category': None,
            'customer': 'Unknown Customer',
            'Quality Score': None
        }

        # Extracting custom fields
        for field in custom_fields_list:
            if field.get('name') == 'Reviewer':
                value = field.get('value')
                if value and isinstance(value, list):
                    result['reviewer'] = value[0].get('username')
            elif field.get('name') == 'Agile Category':
                selected_value = field.get('value')
                if selected_value is not None:
                    options = field.get('type_config', {}).get('options', [])
                    result['agile_category'] = next((option['name'] for option in options if options.index(option) == selected_value), None)
            elif field.get('name') == 'Customer':
                selected_value = field.get('value')
                if selected_value is not None:
                    options = field.get('type_config', {}).get('options', [])
                    result['customer'] = next((option['name'] for option in options if options.index(option) == selected_value), 'Unknown Customer')
            elif field.get('name') == 'Quality Score':
                result['Quality Score'] = field.get('value', None)

        # Extracting assignee
        if assignees and isinstance(assignees, list):
            result['assignee'] = assignees[0].get('username')

        # Extracting email
        if assignees and isinstance(assignees, list):
            result['email'] = assignees[0].get('email')
        
        return pd.Series(result)

    # Aplicando a função combinada
    tasks[['reviewer', 'assignee','email', 'agile_category', 'customer', 'Quality Score']] = tasks.apply(extract_fields, axis=1)

    # Retirando os espaços
    tasks['assignee'] = tasks['assignee'].str.strip()

    # Unindo os times e associando ao email
    df_aux = exp_squad[['email','team']]  # squads precisa ser passado como argumento para a função
    tasks = tasks.merge(df_aux, on='email', how='left')

    # Reorganização das Colunas
    tasks = tasks[['id','name','assignee','reviewer','priority.priority','agile_category','status.status','points','points','start_date',
                    'due_date','date_closed','date_done','customer','list.name','Sprint','team','date_created','date_updated','Quality Score','email']]

    new_columns = ['task_id', 'task_name', 'assignee', 'reviewer','priority','category', 'status','points_estimate','points_estimate_rolled_up',
                'start_date','due_date','date_closed','date_done','customer','list', 'sprint','team','date_created','date_updated',
                'quality_score','email'
                ]
                
    tasks.columns = new_columns

    # Colocando colunas em letra maiúscula
    tasks['status'] = tasks['status'].str.upper()
    tasks['priority'] = tasks['priority'].str.upper()

    # Colocando valores para inteiros sem casas decimais
    tasks['points_estimate'] = tasks['points_estimate'].apply(lambda x: round(x) if pd.notna(x) else x).astype('Int64')
    tasks['points_estimate_rolled_up'] = tasks['points_estimate_rolled_up'].apply(lambda x: round(x) if pd.notna(x) else x).astype('Int64')

    # Quando a categoria for "Customer task", o time será "Customer"
    tasks.loc[tasks['category'] == 'Customer task', 'team'] = 'Customer'

    # Formatando a coluna 'sprint' para o formato "Sprint X"
    tasks['sprint'] = tasks['sprint'].apply(lambda x: f"Sprint {int(x)}" if pd.notna(x) else None)
    
    # Convertendo colunas para tipos apropriados conforme a especificação
 
    tasks['points_estimate'] = tasks['points_estimate'].astype('Int64')  # points_estimate em INTEGER
    tasks['points_estimate_rolled_up'] = tasks['points_estimate_rolled_up'].astype('Int64')  # points_estimate_rolled_up em INTEGER
    tasks['start_date'] = pd.to_datetime(tasks['start_date'], format='%d/%m/%Y', errors='coerce').dt.date  # start_date em DATE
    tasks['due_date'] = pd.to_datetime(tasks['due_date'], format='%d/%m/%Y', errors='coerce').dt.date  # due_date em DATE
    tasks['date_created'] = pd.to_datetime(tasks['date_created'], format='%d/%m/%Y', errors='coerce').dt.date  # date_created em DATE
    tasks['date_updated'] = pd.to_datetime(tasks['date_updated'], format='%d/%m/%Y', errors='coerce').dt.date  # date_updated em DATE

    exp_task = tasks.copy()

    return exp_task
