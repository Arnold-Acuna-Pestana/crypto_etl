#Import libraries
import requests
import json
import pprint
import pandas as pd
import psycopg2
import awswrangler as wr
import redshift_connector
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
load_dotenv()


#API Config
url = 'https://pro-api.coinmarketcap.com'
API = os.getenv('MY_KEY')
headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': API,
}

# URL de la API, endpoints, parámetros y headers
endpoint='/v1/cryptocurrency/listings/latest'
parameters = {
  'limit': '50'
}

# Creo función que recibe un endpoint y parámetros para obtener data de la API
def get_data(endpoint, params=None):
  endpoint_url=url+endpoint
  if params==None:
    params={}
  try:
    # Envio get request con parámetros y headers, luego imprime q
    response = requests.get(endpoint_url, params=parameters, headers=headers)
    print(f"Solicitando datos de: {response.url}")
    # Si la solicitud devuelve el código de estatus 200 es porque fue correcta
    if response.status_code==200:
      print(f"Código de estatus: {response.status_code}, la solicitud está OK.")
      # Se parsea la información en formato json
      cm_data = response.json()
    else:
      # Imprime mensaje por solitud fallida y el código asociado
      print(f"Falla en la solicitud con código de estatus: {response.status_code}.")
  except requests.exceptions.RequestException as e:
    # Manejo y notificación en caso de errores
    print(f"Ocurrió un error: {e}")

  return cm_data

# Llamo a la función
cm_json_data = get_data(endpoint, params=parameters)

# Normalizar datos
df = pd.json_normalize(cm_json_data['data'],
                    record_path=None,
                    meta=['name', 'symbol', 'cmc_rank',
                          'date_added',
                          'circulating_supply',
                          'max_supply',
                          'last_updated',
                          'tags',
                          ['quote', 'USD', 'price'],
                          ['quote', 'USD', 'volume_24h'],
                          ['quote', 'USD', 'percent_change_1h'],
                          ['quote', 'USD', 'percent_change_24h'],
                          ['quote', 'USD', 'percent_change_7d'],
                          ['quote', 'USD', 'percent_change_30d'],
                          ['quote', 'USD', 'percent_change_60d'],
                          ['quote', 'USD', 'percent_change_90d'],
                          ['quote', 'USD', 'market_cap'],
                          ['quote', 'USD', 'market_cap_dominance']],
                       meta_prefix=None,
                    sep='_')

# Añadir la columna de timestamp desde la clave 'status' del JSON
df['timestamp'] = cm_json_data['status']['timestamp']

# Se extrae dos features de los criptomonedas
df['feauture_1'] = df['tags'].apply(lambda x: x[0] if len(x) > 0 else None)
df['feauture_2'] = df['tags'].apply(lambda x: x[1] if len(x) > 1 else None)

# Cambio de nombre a las columnas
df.rename(columns={'quote_USD_price':'usd_price',
                   'quote_USD_volume_24h':'usd_volume_24h',
                   'quote_USD_percent_change_1h':'perc_change_1h',
                   'quote_USD_percent_change_24h':'perc_change_24h',
                   'quote_USD_percent_change_7d':'perc_change_7d',
                   'quote_USD_percent_change_30d':'perc_change_30d',
                   'quote_USD_percent_change_60d':'perc_change_60d',
                   'quote_USD_percent_change_90d':'perc_change_90d',
                   'quote_USD_market_cap':'market_cap',
                   'quote_USD_market_cap_dominance':'market_cap_dominance'}, inplace=True)

# Seleccionar columnas deseadas
df = df[['name', 'symbol', 'cmc_rank', 'usd_price', 'usd_volume_24h', 'perc_change_1h', 'perc_change_24h',
         'perc_change_7d', 'perc_change_30d', 'perc_change_60d', 'perc_change_90d', 'circulating_supply',
         'max_supply', 'market_cap', 'market_cap_dominance', 'feauture_1', 'feauture_2',
         'date_added', 'last_updated','timestamp']]
print(df)

# Crear engine a la base postgresql
# engine = create_engine('postgresql+psycopg2://arni:pass@localhost:5432/postgres')

# Crear engine a la base REDSHIFT
# redshift_conn_string = f"postgresql+psycopg2://2024_arnold_acuna:G9d&!Z1r3@wP@redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com:{os.getenv('PORT')}/{os.getenv('DB')}"
# engine = create_engine(redshift_conn_string)
#conn_string = f"postgresql+psycopg2://{USER}:{PASS}@{HOST}:{PORT}/{DB}"

# Carga de datos a la base postgresql
# df.to_sql(name='crypto_table', con=engine, schema=os.getenv('REDSHIFT_SCHEMA'), if_exists='append', index=False)


conn_params = {
  'host': os.getenv('HOST'),
  'database': os.getenv('DB'),
  'user': os.getenv('USER_REDSHIFT'),
  'password': os.getenv('PASS_REDSHIFT'),
  'port': os.getenv('PORT'),
}

conn = redshift_connector.connect(**conn_params)

wr.redshift.to_sql(
  df=df
, con=conn
, table="crypto_table"
, schema=os.getenv('REDSHIFT_SCHEMA')
, mode="overwrite"
, use_column_names=True
, lock=True
, index=False
)