from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow.decorators import dag, task
import time
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'prajapat21',
}

@dag(dag_id = "hello_world",
     description = 'my first "Hello world" DAG!',
     default_args = default_args,
     start_date = days_ago(1),
     schedule_interval = '@daily')
def dag_with_taskflow_api():

    @task
    def task_a():
        print('TASK A executed!')

    @task
    def task_b():
        time.sleep(5)
        print('TASK B executed!')

    task_a() >> task_b()

dag_with_taskflow_api()