import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pandas import DataFrame
from duckdb import DuckDBPyRelation

#Aqui estou carregando as variáveis do arquivo .env
load_dotenv()

#Primeiro fazemos o extract dos arquivos que estão na pasta do google drive com gdown
def baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok= True)
    gdown.download_folder(url_pasta, output= diretorio_local, quiet=False, use_cookies= False)


# Fazer a listagem dos arquivos que foram baixados
def listar_arquivos_csv (diretorio):
    arquivos_csv = []
    todos_os_arquivos = os.listdir(diretorio)
    for arquivo in todos_os_arquivos:
        if arquivo.endswith('.csv'):
            caminho_completo_arquivo = os.path.join(diretorio, arquivo)
            arquivos_csv.append(caminho_completo_arquivo)
            
        return arquivos_csv
    

# Ler os arquivos .csv no Duckdb
def ler_csv (caminho_do_arquivo):
    dataframe_duckdb =  duckdb.read_csv(caminho_do_arquivo)
    
    return dataframe_duckdb

#Fazer o transform do .csv e passar para um dataframe Pandas:
def transformar(df: DuckDBPyRelation) -> DataFrame: 
    dataframe_transformado = duckdb.sql("Select *, quantidade * valor as total_vendas from df").df()
    
    return dataframe_transformado


#Fazer o load do dataframe transformado no banco de dados postgres:
def salvar_no_postgres(df_duckdb, tabela):
    DATABASE_URL = os.getenv("DATABASE_URL")  # Ex: 'postgresql://user:password@localhost:5432/database_name'
    if isinstance(DATABASE_URL,str):
        engine = create_engine(DATABASE_URL) #
    # Salvar o DataFrame no PostgreSQL
    df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)



if __name__ == "__main__":

    try:
        

        url_pasta = os.getenv('PASTA_GOOGLE')
        diretorio_local = os.getenv('DIRETORIO_LOCAL')

        if not url_pasta:
            raise ValueError ('''
                          A url da pasta do Google não está
                          definido corretamente no .env''')
        if not diretorio_local:
            raise ValueError ('''
                          O Diretrório local não está
                          definido corretamente no .env''')

        baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local)
        listar_arquivos = listar_arquivos_csv(diretorio_local)

        if not listar_arquivos:
            raise ValueError('A lista de arquivos está vazia')
        
        for caminho_arquivos in listar_arquivos:
            df = ler_csv(caminho_arquivos)
            df_transformado = transformar(df)
            salvar_no_postgres(df_transformado, 'vendas_calculadas')


    except Exception as e:
        print(f'Ocorreu um erro {e}')

    
       
