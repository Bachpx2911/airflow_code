from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG('Test_dag',start_date = datetime(2022,1,1),
         schedule = '@daily',catchup=False) as dag:
    
# Specify the path to your Python script on the host machine
    script_path = 'C:/Users/ADMIN/Desktop/Data_Crypto/Get_recent_data.py'    
    # Create the table 'Binance_data' with the specified schema
    run_script = BashOperator(
    task_id='run_my_script',
    bash_command='python /opt/airflow/dags/Get_recent_data.py',  # Adjust the path accordingly
    dag=dag,
   )



# Set task dependencies
    run_script
