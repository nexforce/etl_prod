# customers_transformation.py

import pandas as pd

def transform_customers_data(customer):

    def consolidate_rows(group):
        # Escolher a data mais antiga para "start_date" e "date_created"
        start_date = group['start_date'].min()
        date_created = group['date_created'].min()

        # Escolher a data mais recente para "due_date" e "date_updated"
        due_date = group['due_date'].max()
        date_updated = group['date_updated'].max()

        # Somar "plan_points" e "monthly_revenue" se o status for "active", seguindo as regras
        if (group['status'] == 'active').any():
            plan_points = group.loc[group['status'] == 'active', 'plan_points'].sum()
            monthly_revenue = group.loc[group['status'] == 'active', 'monthly_revenue'].sum()
        else:
            plan_points = 0
            monthly_revenue = 0

        # Consolidar colunas de strings diferentes, separando por ";", mas sem valores duplicados
        task_name = ';'.join(set(group['task_name'].dropna().astype(str)))
        customer = ';'.join(set(group['customer'].dropna().astype(str)))
        slack_channel = ';'.join(set(group['slack_channel'].dropna().astype(str)))
        team = ';'.join(set(group['team'].dropna().astype(str)))

        # Garantir que 'crm_id' contenha apenas valores únicos
        crm_id = ';'.join(set(group['crm_id'].dropna().astype(str)))

        # Status: Se qualquer uma das linhas for "active", o resultado será "active"
        status = 'active' if (group['status'] == 'active').any() else 'inactive'

        # Montar o dicionário de valores consolidados
        result = {
            'task_id': group['task_id'].iloc[0],
            'task_name': task_name,
            'customer': customer,
            'plan_points': plan_points,
            'currency': group['currency'].iloc[0],  # Manter o mesmo valor
            'monthly_revenue': monthly_revenue,
            'term': group['term'].dropna().iloc[0] if not group['term'].isna().all() else None,
            'start_date': start_date,
            'due_date': due_date,
            'has_growth_formula': group['has_growth_formula'].dropna().iloc[0] if not group['has_growth_formula'].isna().all() else None,
            'slack_channel': slack_channel,
            'team': team,
            'crm_id': crm_id,
            'date_created': date_created,
            'date_updated': date_updated,
            'status': status
        }

        return pd.Series(result)

    # Lista das colunas com datas
    colunas_datas = ['createdAt', 'updatedAt', 'service_due_date', 'service_end_date', 'service_start_date']  

    # Converter todas as colunas de data para datetime, sem formatar ainda
    for coluna in colunas_datas:
        customer[coluna] = pd.to_datetime(customer[coluna], utc=True, errors='coerce')

    # Calcular a coluna 'due_date' com a data mais recente entre 'service_end_date' e 'service_due_date'
    customer['due_date'] = customer[['service_end_date', 'service_due_date']].max(axis=1)

    # Agora, formatar todas as colunas de data para o formato DD/MM/YYYY
    for coluna in colunas_datas + ['due_date']:  # Inclui a nova coluna 'due_date'
        customer[coluna] = customer[coluna].dt.strftime('%d/%m/%Y')

    customer['amount'] = pd.to_numeric(customer['amount'], errors='coerce')
    # Verifica se a coluna 'payment_frequency' existe e se contém 'Yearly'
    if 'payment_frequency' in customer.columns:
        # Aplica a divisão por 12 nos valores de 'monthly_revenue' onde 'payment_frequency' for 'Yearly'
        customer.loc[customer['payment_frequency'] == 'Yearly', 'amount'] /= 12

    customer['term'] = None
    customer['service_currency'] = customer['service_currency'].str.upper()

    service = customer[["id","customer_name",'service_name','subscription_service_plan','sprint_points_plan','service_type','service_currency',
                        'payment_frequency', "mrr",'term','service_status','service_start_date','service_end_date', 'service_due_date']]

    new_columns_service = ["id","customer",'service_name','plan','plan_points','service_type','currency',
                        'payment_frequency', "mrr",'term','status','service_start_date','service_end_date', 'service_due_date']

    service.columns = new_columns_service

    # Converter as colunas para os tipos indicados
    service['plan_points'] = service['plan_points'].fillna(0).astype('int64')
    service['mrr'] = service['mrr'].astype('float')
    service.loc[:, 'mrr'] = service['mrr'].round(2)

    service['service_start_date'] = service['service_start_date'].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime('%d/%m/%Y') if pd.notnull(x) else x)
    service['service_due_date'] = service['service_due_date'].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime('%d/%m/%Y') if pd.notnull(x) else x)
    service['service_end_date'] = service['service_end_date'].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime('%d/%m/%Y') if pd.notnull(x) else x)

    # Criando múltiplas colunas com valores 'None'
    cols_to_add = ['go_live_date', 'last_renew_date', 'list', 'temperature', 'renewal_term', 'chances_to_renew', 'plan']
    for col in cols_to_add:
        customer[col] = None

    customer = customer[['id','customer_name','customer_name','plan','sprint_points_plan','service_currency','amount','term',
                        'service_start_date','go_live_date', 'last_renew_date','due_date', 'growth_formula_implemented','slack_channel',
                        'squad_name', 'subscription_crm_id','list', 'temperature','createdAt','updatedAt','chances_to_renew',
                        'renewal_term','service_status'
                        ]]

    new_columns = ['task_id', 'task_name', 'customer','plan','plan_points','currency', 'monthly_revenue','term','start_date',
                    'go_live_date', 'last_renew_date','due_date','has_growth_formula','slack_channel','team','crm_id',
                    'list', 'temperature','date_created', 'date_updated','chances_to_renew','renewal_term','status'
                ]

    customer.columns = new_columns

    # Converter colunas numéricas que estão no formato 'object' para numérico
    customer['plan_points'] = pd.to_numeric(customer['plan_points'], errors='coerce')
    customer['monthly_revenue'] = pd.to_numeric(customer['monthly_revenue'], errors='coerce')
    customer['start_date'] = customer[['start_date']].apply(lambda x: pd.to_datetime(x, errors='coerce'))
    customer['due_date'] = customer['due_date'].apply(lambda x: pd.to_datetime(x, errors='coerce'))

    # Identificar duplicatas e aplicar a função de consolidação
    df_duplicates = customer[customer.duplicated('task_id', keep=False)]
    df_consolidated = df_duplicates.groupby('task_id', group_keys=False).apply(consolidate_rows).reset_index(drop=True)

    # Remover as linhas duplicadas do DataFrame original
    df_non_duplicates = customer.drop_duplicates('task_id', keep=False)

    # Combinar o DataFrame de linhas únicas com o DataFrame consolidado
    df_final = pd.concat([df_non_duplicates, df_consolidated], ignore_index=True).sort_values('task_id').reset_index(drop=True)

    df_final['monthly_revenue'] = df_final['monthly_revenue'].round(2)

    # Converte as colunas para o tipo inteiro
    df_final['plan_points'] = df_final['plan_points'].astype('Int64')  # Inteiro (permite NaNs)
    df_final['term'] = df_final['term'].astype('Int64')
    df_final['renewal_term'] = df_final['renewal_term'].astype('Int64')           # Inteiro (permite NaNs)

    df_final['crm_id'] = pd.to_numeric(df_final['crm_id'], errors='coerce')               # Inteiro (permite NaNs)
    df_final['crm_id'] = df_final['crm_id'].astype('Int64')           # Inteiro (permite NaNs)

    # Converte as colunas para booleano
    df_final['has_growth_formula'] = df_final['has_growth_formula'].astype(bool)  # Booleano

    # Converte as colunas de data e verifica o tipo de cada coluna
    
    df_final['date_created'] = pd.to_datetime(df_final['date_created'], errors='coerce', dayfirst=True)
    df_final['date_updated'] = pd.to_datetime(df_final['date_updated'], errors='coerce', dayfirst=True)
    df_final['go_live_date'] = pd.to_datetime(df_final['go_live_date'], errors='coerce', dayfirst=True)
    df_final['last_renew_date'] = pd.to_datetime(df_final['last_renew_date'], errors='coerce', dayfirst=True)

    exp_customer = df_final.copy()

    return exp_customer, service



