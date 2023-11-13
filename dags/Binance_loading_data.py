#Library
import psycopg2
from dotenv import dotenv_values
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import json
import pandas as pd
import numpy as np


#Config db 
config = dotenv_values(".env") 
pg_host = config['PGHOST'] 
pg_user = config['PGUSER']
pg_password = config['PGPASSWORD']
pg_database = config['PGDATABASE']
conn = psycopg2.connect(
    host=pg_host,
    user=pg_user,
    password=pg_password,
    database=pg_database
)
# Create a cursor object
cur = conn.cursor()
# Execute SQL queries
cur.execute('SELECT table_name FROM information_schema.tables order by table_name;')
version = cur.fetchall()
print("Max_date", version)

#Get the latest data of table:


#with DAG('Binance_loading_data',start_date = datetime(2023,11,3),
#         schedule = '@daily',catchup=False) as dag:




