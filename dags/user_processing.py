from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import json
from pandas import json_normalize

def _process_user(ti):
    user = ti.xcom_pull(task_id = "exact_user")
    user = user['result'][0]
    processed_user = kson_normalize({
        'firstname':user['name']['first'],
        'lastname':user['name']['last']
        'country':user['location']['country']
    })

with DAG('user_processing',start_date = datetime(2022,1,1),
         schedule = '@daily',catchup=False) as dag:
    pass