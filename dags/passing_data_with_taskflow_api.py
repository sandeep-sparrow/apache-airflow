import time

import json

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

default_args = {
   'owner' : 'prajapat21'
}

@dag(
    dag_id='cross_task_communication_taskflow_api',
    description='taskflow API data comm',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['taskflow', 'operators', 'python']
)
def passing_data_with_taskflow_api():

    @task
    def get_order_prices():

        order_price_data = {
            'o1': 234.45,
            'o2': 10.00,
            'o3': 34.77,
            'o4': 45.66,
            'o5': 399
        }

        return order_price_data

    @task
    def compute_sum(order_price_data: dict):

        total = 0
        for order in order_price_data:
            total += order_price_data[order]

        return total

    @task
    def compute_average(order_price_data: dict):
        
        total = 0
        count = 0
        for order in order_price_data:
            total += order_price_data[order]
            count += 1

        average = total / count
        return average

    
    @task
    def display_result(total, average):
        print(f"Total Price of goods {total}")
        print(f"Average Price of goods {average}")

    order_price_data = get_order_prices()
    total = compute_sum(order_price_data)
    average = compute_average(order_price_data)

    display_result(total, average)
    
passing_data_with_taskflow_api()