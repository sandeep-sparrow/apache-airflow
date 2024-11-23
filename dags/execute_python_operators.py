from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_agrs =  {
    'owner': 'prajapat21'
}

def print_function():
    print("The Simplest possible Python Operator!")

def task_a():
    print("Task A executed!")

def task_b():
    print("Task V executed!")

def greet_hello(name):
    print(f"Hello, {name}!")

def greet_hello_with_city(name, city):
    print(f"Hello, {name} from {city}")

with DAG(
    dag_id = 'execute_python_operators',
    description = 'Python operators in DAGs',
    default_args = default_agrs,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['simple', 'python']
) as dag:
    print_task = PythonOperator(
        task_id = 'python_task1',
        python_callable = print_function
    )

    task_a = PythonOperator(
        task_id = 'python_task2',
        python_callable = task_a
    )

    task_b = PythonOperator(
        task_id = 'python_task3',
        python_callable = task_b,
    )

    greet_hello = PythonOperator(
        task_id = "greet_hello",
        python_callable = greet_hello,
        op_kwargs={'name': 'Sandeep'}
    )

    greet_hello_with_city = PythonOperator(
        task_id = "greet_hello_with_city",
        python_callable = greet_hello_with_city,
        op_kwargs={'name': 'Sandeep', 'city': 'Mumbai'}
    )

print_task >> task_a >> task_b >> [greet_hello_with_city, greet_hello]