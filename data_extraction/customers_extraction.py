# customers_extraction.py

import requests
import pandas as pd
from dotenv import load_dotenv
import os

def extract_customers_data(teams):

    # Defina os IDs dos objetos principais e os dois objetos associados
    object_id = "2-34192600"  # Objeto principal
    associated_object_1_id = "2-34344148"  # Primeiro objeto associado
    associated_object_2_id = "2-32628364"  # Segundo objeto associado

    # URL da API para buscar os registros do objeto principal
    url = f"https://api.hubapi.com/crm/v3/objects/{object_id}/search"

    # Carregar variáveis de ambiente do .env
    load_dotenv()

    # Pegar o token do arquivo .env
    hubspot_token = os.getenv('HUBSPOT_TOKEN')

    # Usar o token no header
    headers = {'Authorization': 'Bearer ' + hubspot_token}

    # Função para realizar a requisição e processar os registros do objeto principal
    def fetch_records(query):
        response = requests.post(url, headers=headers, json=query)
        return response.json() if response.status_code == 200 else None

    # Função para obter IDs dos objetos associados para um registro específico do objeto principal
    def get_associated_objects(record_id, associated_object_id):
        assoc_url = f"https://api.hubapi.com/crm/v3/objects/{object_id}/{record_id}/associations/{associated_object_id}"
        response = requests.get(assoc_url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro ao obter objetos associados para {record_id} com o objeto {associated_object_id}: {response.status_code}")
            return None

    # Requisição: Obter registros e todas as propriedades do objeto principal
    query_initial = {
        "properties": [
        "customer_name", "customer_status", "hs_createdate", "growth_formula_implemented", 
        "subscription_crm_id",  "hs_lastmodifieddate", 
        ],
        "limit": 100  
    }

    # Buscar os registros do objeto principal
    result = fetch_records(query_initial)

    # Processar os registros
    if result and 'results' in result:
        all_records = result['results']

        # Criar DataFrame para registros do objeto principal
        df_records = pd.DataFrame(all_records)
        customers = pd.json_normalize(df_records['properties']) if 'properties' in df_records.columns else pd.DataFrame()
        
        # Obter IDs dos registros do objeto principal
        record_ids = df_records['id'].tolist()
        
       # Buscar detalhes dos objetos associados ao objeto principal (para ambos os IDs)
        associated_details_1 = []
        
        for record_id in record_ids:
            # Buscar objetos associados do primeiro objeto associado
            associated_objects_1 = get_associated_objects(record_id, associated_object_1_id)
            if associated_objects_1 and 'results' in associated_objects_1:
                for associated in associated_objects_1['results']:
                    associated_details_1.append({
                        'customer_id': record_id,
                        'associated_object_1_id': associated['id'],
                    })
            
        # Criar DataFrames com detalhes dos objetos associados
        df_associated_1 = pd.DataFrame(associated_details_1)
    
        # Expandir os dados das propriedades do objeto principal
        customers = df_records.drop(columns=['properties']).join(customers)
        
        # Combinar os registros principais com os objetos associados
        customers = customers.merge(df_associated_1, left_on='id', right_on='customer_id', how='left')
    
        # objeto purchase
    object_id = "2-34344148"
    url = f"https://api.hubapi.com/crm/v3/objects/{object_id}/search"

    # Requisição: Obter registros e todas as propriedades
    query2 = {
        "properties": ["amount", "discount", "gross_value", "payment_end_date", "payment_frequency", 
        "payment_start_date", "service_currency", "service_due_date", "service_end_date", "service_name", 
        "service_start_date", "service_status", "service_type", "sprint_points_plan", "subscription_service_plan",
        "gross_value","discount","mrr","intermediation_fee",'payment_frequency'],  
        "limit": 100  
    }

    # Buscar os registros
    result = fetch_records(query2)

    # Processar os registros
    if result and 'results' in result:
        all_records = result['results']

        # Cria DataFrame para registros
        df_records = pd.DataFrame(all_records)
        df_purchase = pd.json_normalize(df_records['properties']) if 'properties' in df_records.columns else pd.DataFrame()

    df_final = customers.merge(df_purchase, left_on='associated_object_1_id', right_on='hs_object_id', how='left')

    #BUSCAR PURCHASE
    # IDs principais
    purchase_object_id = "2-34344148"  # Objeto Purchase
    squad_object_id = "2-32628364"  # Objeto Squad

    # URL para buscar registros do objeto Purchase
    url_purchase = f"https://api.hubapi.com/crm/v3/objects/{purchase_object_id}/search"

    # Função para buscar os registros do objeto Purchase
    def fetch_purchase_records(query):
        response = requests.post(url_purchase, headers=headers, json=query)
        return response.json() if response.status_code == 200 else None

    # Função para obter os Squads associados a cada registro do objeto Purchase
    def get_squads_for_purchase(purchase_id):
        assoc_url = f"https://api.hubapi.com/crm/v3/objects/{purchase_object_id}/{purchase_id}/associations/{squad_object_id}"
        response = requests.get(assoc_url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro ao obter Squads associados para Purchase {purchase_id}: {response.status_code}")
            return None

    # Query para buscar registros do objeto Purchase com todas as propriedades relevantes
    purchase_query = {
        "properties": ["amount", "discount", "gross_value", "payment_end_date", "payment_frequency", 
        "payment_start_date", "service_currency", "service_due_date", "service_end_date", "service_name", 
        "service_start_date", "service_status", "service_type", "sprint_points_plan", "subscription_service_plan",
        "gross_value", "discount", "mrr", "intermediation_fee", "payment_frequency","slack_channel"],  
        "limit": 100  
    }

    # Buscar registros do objeto Purchase
    purchase_result = fetch_purchase_records(purchase_query)

    # Processar registros do objeto Purchase
    if purchase_result and 'results' in purchase_result:
        purchase_records = purchase_result['results']

        # Criar DataFrame com os registros do objeto Purchase
        df_purchase = pd.DataFrame(purchase_records)
        purchase_details = pd.json_normalize(df_purchase['properties']) if 'properties' in df_purchase.columns else pd.DataFrame()

        # Obter IDs dos registros de Purchase
        purchase_ids = df_purchase['id'].tolist()

        # Buscar Squads associados para cada registro de Purchase
        squad_details = []

        for purchase_id in purchase_ids:
            squads2 = get_squads_for_purchase(purchase_id)
            if squads2 and 'results' in squads2:
                for squad in squads2['results']:
                    squad_details.append({
                        'purchase_id': purchase_id,
                        'team_id': squad['id'],
                    })

        # Criar DataFrame com detalhes dos Squads associados
        df_squad_details = pd.DataFrame(squad_details)

        # Expandir propriedades do objeto Purchase
        purchase_details = df_purchase.drop(columns=['properties']).join(purchase_details)

        # Combinar os registros do objeto Purchase com os Squads associados
        purchase_with_squads = purchase_details.merge(
            df_squad_details, left_on='id', right_on='purchase_id', how='left'
        )

        # Resultado final
        
    else:
        print("Nenhum registro de Purchase encontrado.")

    aux_pur = pd.merge(purchase_with_squads,teams, how='left', on="team_id")

    aux_pur = aux_pur[['id','slack_channel','squad_name']]

    purchase = aux_pur.rename(columns={'id': 'associated_object_1_id'})

    customers = pd.merge(df_final,purchase, how='left', on='associated_object_1_id')

    return customers 
