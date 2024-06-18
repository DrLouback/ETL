import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pandas import DataFrame
from duckdb import DuckDBPyRelation



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





if __name__ == "__main__":

    url_pasta = "https://drive.google.com/drive/folders/1lVNzpYavpcUcEyyaIGwKPsqoFzoN-ZWH?usp"
    diretorio_local = "./pasta_gdown"
    baixar_os_arquivos_do_google_drive(url_pasta,diretorio_local)
    arquivos = listar_arquivos_csv(diretorio_local)    
    ler_csv(arquivos)
    df_duckdb = ler_csv(arquivos)
    transformar(df_duckdb)