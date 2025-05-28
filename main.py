#!/usr/bin/python3

import time
import os
import subprocess
from google.cloud import bigquery
from google.oauth2 import service_account
from data_extraction.squads_extraction import extract_squads_data
from data_extraction.customers_extraction import extract_customers_data
from data_extraction.tasks_extraction import extract_tasks_data
from data_transformation.squads_transformation import transform_squads_data
from data_transformation.customers_transformation import transform_customers_data
from data_transformation.tasks_transformation import transform_tasks_data
from task_management.tasks_creation import create_tasks_in_history  

# Função para executar queries
def run_query(client, query):
    try:
        job = client.query(query)
        job.result()
        print("Query executada com sucesso")
    except Exception as e:
        print(f"Ocorreu um erro ao executar a query: {e}")

# Função para carregar queries de arquivo SQL
#def load_query_from_file(query_name, file_path="sql_queries/queries.sql"):
    #with open(file_path, "r") as file:
        #queries = file.read()

    # Dicionário para armazenar as queries
    #query_dict = {}

    # Divide as queries por comentário
    #for section in queries.split("-- Nome da query:"):
        #if section.strip():
            #lines = section.strip().split("\n")
            #query_key = lines[0].strip()  # Nome da query
            #query_sql = "\n".join(lines[1:]).strip()  # SQL da query
            #query_dict[query_key] = query_sql

    #return query_dict.get(query_name, None)

# CÓDIGO NUVEM
def load_query_from_file(query_name, file_path="/home/ricardo_semerene/etl_prod/sql_queries/queries.sql"):
    # Verifica se o arquivo existe antes de tentar abrir
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"O arquivo {file_path} não foi encontrado.")
    
    with open(file_path, "r") as file:
        queries = file.read()

    # Dicionário para armazenar as queries
    query_dict = {}

    # Divide as queries por comentário
    for section in queries.split("-- Nome da query:"):
        if section.strip():
            lines = section.strip().split("\n")
            query_key = lines[0].strip()  # Nome da query
            query_sql = "\n".join(lines[1:]).strip()  # SQL da query
            query_dict[query_key] = query_sql

    return query_dict.get(query_name, None)

# Função para carregar DataFrames para o BigQuery
def load_to_bigquery(df, table_id, client):
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"Dados carregados em {table_id}")

# Função para truncar tabelas no BigQuery
def truncate_table(table_id, client):
    query = f"TRUNCATE TABLE `{table_id}`"
    try:
        # Executar a query de truncar a tabela
        job = client.query(query)
        job.result()  # Aguarda a execução da query
        print(f"Tabela {table_id} foi truncada com sucesso.")
    except Exception as e:
        print(f"Ocorreu um erro ao truncar a tabela {table_id}: {e}")

def main():
    start_time = time.time()  # Inicia a contagem do tempo
    total_steps = 5  # Defina o número total de etapas
    current_step = 0  # Contador de etapas completadas

    # Carregar as credenciais do arquivo JSON
    #credentials = service_account.Credentials.from_service_account_file('bigquery_credentials.json')
    # CÓDIGO NUVEM
    credentials = service_account.Credentials.from_service_account_file('/home/ricardo_semerene/etl_prod/bigquery_credentials.json')

    # Criar o cliente BigQuery usando as credenciais
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Definir ambiente de produção
    project_id = 'starry-compiler-387319'
    dataset_stage = 'Stage'
    
    # IDs das tabelas de produção
    table_id_squad = f"{project_id}.{dataset_stage}.stg_squad"
    table_id_customer = f"{project_id}.{dataset_stage}.stg_customer"
    table_id_tasks = f"{project_id}.{dataset_stage}.stg_task"
   # table_id_service = f"{project_id}.{dataset_stage}.stg_service"
   # table_dim_service = f"{project_id}.Clickup_Source.dim_service"

    # 1. Extração de dados
    squads, teams = extract_squads_data()  
    customers = extract_customers_data(teams)  
    tasks_req = extract_tasks_data()  

    customer = customers.copy()
    tasks = tasks_req.copy()
    task_history = tasks_req.copy() 

    current_step += 1
    elapsed_time = time.time() - start_time
    print(f"Etapa {current_step}/{total_steps} concluída em {elapsed_time:.2f} segundos.")

    # 2. Transformação de dados
    exp_squad = transform_squads_data(squads) 
    print(exp_squad)
    exp_customer = transform_customers_data(customer) 
    print(exp_customer) 
    exp_task = transform_tasks_data(tasks, exp_squad)
    print(exp_task)

    current_step += 1
    elapsed_time = time.time() - start_time
    print(f"Etapa {current_step}/{total_steps} concluída em {elapsed_time:.2f} segundos.")

    # 3. Carga no BigQuery 
    truncate_table(table_id_squad, client)
    truncate_table(table_id_customer, client)
    truncate_table(table_id_tasks, client)
    # truncate_table(table_id_service, client)

    load_to_bigquery(exp_squad, table_id_squad, client)
    load_to_bigquery(exp_customer, table_id_customer, client)
   # load_to_bigquery(service, table_id_service, client)
    load_to_bigquery(exp_task, table_id_tasks, client)
    
    current_step += 1
    elapsed_time = time.time() - start_time
    print(f"Etapa {current_step}/{total_steps} concluída em {elapsed_time:.2f} segundos.")

    # 4. Executar queries SQL
   # query_tl_dim_customer = load_query_from_file("tl_dim_customer")
   # query_tl_dim_service = load_query_from_file("tl_dim_service")
   # query_tl_dim_squad = load_query_from_file("tl_dim_squad")
    #query_tl_dim_task = load_query_from_file("tl_dim_task")
   # query_tl_fat_sprint_detail = load_query_from_file("tl_fat_sprint_detail")

    #CÓDIGO NUVEM
    query_tl_dim_customer = load_query_from_file("tl_dim_customer", file_path="/home/ricardo_semerene/etl_prod/sql_queries/queries.sql")
   # query_tl_dim_service = load_query_from_file("tl_dim_service", file_path="/home/ricardo_semerene/etl_prod/sql_queries/queries.sql")
    query_tl_dim_squad = load_query_from_file("tl_dim_squad", file_path="/home/ricardo_semerene/etl_prod/sql_queries/queries.sql")
    query_tl_dim_task = load_query_from_file("tl_dim_task", file_path="/home/ricardo_semerene/etl_prod/sql_queries/queries.sql")
    query_tl_fat_sprint_detail = load_query_from_file("tl_fat_sprint_detail", file_path="/home/ricardo_semerene/etl_prod/sql_queries/queries.sql")

   # truncate_table(table_dim_service, client)

    #run_query(client, query_tl_dim_service)
    run_query(client, query_tl_dim_customer)
    run_query(client, query_tl_dim_squad)
    run_query(client, query_tl_dim_task)
    run_query(client, query_tl_fat_sprint_detail)

    current_step += 1
    elapsed_time = time.time() - start_time
    print(f"Etapa {current_step}/{total_steps} concluída em {elapsed_time:.2f} segundos.")

    # 5. Criar tasks no histórico
    create_tasks_in_history(task_history, exp_squad)

    elapsed_time = time.time() - start_time
    print(f"Todas as etapas concluídas em {elapsed_time:.2f} segundos.")

    # Após a execução do código
    print("Execução finalizada, desligando a instância...")
   # subprocess.run(["sudo", "shutdown", "-h", "now"])

if __name__ == "__main__":
     main()
