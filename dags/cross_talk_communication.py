import time

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
   'owner' : 'prajapat21'
}

def increment_by_1(counter):
    print(f"Count {counter}!")

    return counter + 1

def multiply_by_100(ti):
    value = ti.xcom_pull(task_ids='increment_by_1')
    print(f"Count {value}!")

    return value * 100


with DAG(
    dag_id='cross_talk_communication',
    description='Cross-talk communication with XCom',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    tags=['xcom', 'python']
) as dag:
    taskA = PythonOperator(
        task_id='increment_by_1',
        python_callable=increment_by_1,
        op_kwargs={'counter': 100}
    )

    taskB = PythonOperator(
        task_id='multiply_by_100',
        python_callable=multiply_by_100,
        op_kwargs={'counter': 9}
    )

taskA >> taskB