from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import get_data
import load_data

df = get_data.pull_from_api(39)
df2 = get_data.pull_from_api(140)

default_args = {
    'owner': 'eseosa',
    'start_date': datetime(2024, 9, 18),
}

# Using `with` to define the DAG
with DAG('daily_load', default_args=default_args, schedule_interval='@daily') as dag:
    load_prem = PythonOperator(
        task_id='task1',
        python_callable= load_data(df, 0)
    )
    
    load_liga = PythonOperator(
        task_id='task2',
        python_callable= load_data(df2, 1)
    )

    load_prem >> load_liga 