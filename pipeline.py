import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pandas import DataFrame
from duckdb import DuckDBPyRelation

load_dotenv()


def baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok= True)
    gdown.download_folder(url_pasta, output= diretorio_local, quiet=False, use_cookies= False)

# Listar arquivos CSV no diretório especificado

def listar_arquivos_csv (diretorio):
    arquivos_csv = []
    todos_os_arquivos = os.listdir(diretorio)
    for arquivo in todos_os_arquivos:
        if arquivo.endswith('.csv'):
            caminho_completo_arquivo = os.path.join(diretorio, arquivo)
            arquivos_csv.append(caminho_completo_arquivo)
            
        return arquivos_csv
    
#Ler um arquivo csv e retornar em um dataframe duckdb

def ler_csv (caminho_do_arquivo):
    dataframe_duckdb =  duckdb.read_csv(caminho_do_arquivo)
    
    return dataframe_duckdb

def transformar(df: DuckDBPyRelation) -> DataFrame: 
    dataframe_transformado = duckdb.sql("Select *, quantidade * valor as total_vendas from df").df()
    print(dataframe_transformado)
    return dataframe_transformado

def salvar_no_postgres(df_duckdb,tabela):
    DATABASE_URL = os.getenv('DATABASE_URL') 
    engine = create_engine(DATABASE_URL) # type: ignore
    df_duckdb.to_sql(tabela, con=engine, if_exists='append', index= False)




if __name__ == "__main__":

    url_pasta = "https://drive.google.com/drive/folders/1lVNzpYavpcUcEyyaIGwKPsqoFzoN-ZWH?usp"
    diretorio_local = "./pasta_gdown"
    baixar_os_arquivos_do_google_drive(url_pasta,diretorio_local)
      
    lista_de_arquivos = listar_arquivos_csv(diretorio_local)
    for caminho_do_arquivo in lista_de_arquivos:# type: ignore
        duck_db_df = ler_csv(caminho_do_arquivo)
        pandas_df_transformado =transformar(duck_db_df)
        salvar_no_postgres(pandas_df_transformado,"vendas_calculado")
