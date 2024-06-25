import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pandas import DataFrame
from duckdb import DuckDBPyRelation
from datetime import datetime

#Aqui estou carregando as variáveis do arquivo .env
load_dotenv()


def conectar_banco():
    return duckdb.connect(database='duckdb.db', read_only= False)


def inicializador_tabela(con):
    con.execute("""
                    CREATE TABLE IF NOT EXISTS historico_arquivos(
                    nome_arquivo VARCHAR,
                    horario_processamento TIMESTAMP)


                                        """)
#Função para registrar na database do duckdb os registros dos arquivos
def registrar_arquivos(con, nome_arquivo):
    con.execute(""" INSERT INTO historico_arquivos (nome_arquivo, horario_processamento)
                 VALUES (?,?)""", (nome_arquivo, datetime.now()))
    
def arquivos_processados(con):
    return set(row[0] for row in con.execute("SELECT nome_arquivo FROM historico_arquivos").fetchall() )


#Extract dos arquivos que estão na pasta do google drive com gdown
def baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok= True)
    gdown.download_folder(url_pasta, output= diretorio_local, quiet=False, use_cookies= False)


#Fazer a listagem dos arquivos que foram baixados
def listar_arquivo_e_tipos (diretorio):
    lista_arquivo = []
    todos_os_arquivos = os.listdir(diretorio)
    for arquivo in todos_os_arquivos:
        if arquivo.endswith('.csv') or arquivo.endswith('.json') or arquivo.endswith('.parquet'):
            caminho_completo_arquivo = os.path.join(diretorio, arquivo)
            tipo = arquivo.split(".")[-1]
            lista_arquivo.append((caminho_completo_arquivo, tipo)) #Append uma tupla

    return lista_arquivo
    

#Ler os arquivos no Duckdb
def ler_arquivo (caminho_do_arquivo, tipo):
    if tipo == 'csv':
        return duckdb.read_csv(caminho_do_arquivo)
    elif tipo == 'json':
        return duckdb.read_json(caminho_do_arquivo)
    elif tipo == 'parquet':
        return duckdb.read_parquet(caminho_do_arquivo)
    else:
        raise ValueError (f"Tipo de arquivo não suportado: {tipo}")
    

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





def pipeline():
        
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
        con = conectar_banco()
        inicializador_tabela(con)
        processados = arquivos_processados(con)
        lista_arquivo_tipo = listar_arquivo_e_tipos(diretorio_local)

        if not lista_arquivo_tipo:
            raise ValueError('A lista de arquivos está vazia')
        
        for caminho_arquivos, tipo in lista_arquivo_tipo:
            nome_arquivo = os.path.basename(caminho_arquivos)

            if nome_arquivo not in processados:
                df = ler_arquivo(caminho_arquivos, tipo)
                if df:
                    df_transformado = transformar(df)
                    salvar_no_postgres(df_transformado, 'vendas_calculadas')
                    registrar_arquivos(con,nome_arquivo)
                    print(processados)
                    print(f'Arquivo {nome_arquivo} foi processado e salvo.')
            else:
                print(f'Arquivo {nome_arquivo} já foi processado anteriormente.')



if __name__ == "__main__":  
    try:
        pipeline()
    except Exception as e:
        print(f'Ocorreu um erro {e}')
    
       
