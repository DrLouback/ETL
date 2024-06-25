import os
from dotenv import load_dotenv

load_dotenv()
database_url = os.getenv('DATABASE_URL')
pasta_google = os.getenv('PASTA_GOOGLE')
diretorio = os.getenv('DIRETORIO_LOCAL')
print(database_url)
print(pasta_google)
print(diretorio)