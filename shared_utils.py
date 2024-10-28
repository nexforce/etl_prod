from datetime import datetime
import pandas as pd

def convert_to_brazilian_date(value):
        if pd.isna(value):  # Verifica se o valor é NaN ou None
            return None
        try:
            # Se o valor for uma string numérica ou um número, converte
            if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
                # Converte de milissegundos para segundos
                timestamp = float(value) / 1000
                # Converte para datetime
                dt = datetime.fromtimestamp(timestamp)
                # Formata para dd/mm/aaaa
                return dt.strftime('%d/%m/%Y')
        except Exception as e:
            print(f"Erro ao converter o valor '{value}': {e}")
            return None

# Lista das colunas que você quer transformar
date_columns = ['start_date', 'due_date', 'date_closed', 'date_done', 'date_created', 'date_updated','go_live_date','last_renewal_date']