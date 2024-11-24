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
    dag_id='cross_task_communication_taskflow_api_adv',
    description='taskflow API data comm',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['taskflow', 'operators', 'python', 'multiple']
)
def passing_data_with_taskflow_api_adv():

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

    @task(multiple_outputs=True)
    def compute_total_and_average(order_price_data: dict):

        total = 0
        count = 0
        for order in order_price_data:
            total += order_price_data[order]
            count += 1

        average = total / count
        return {'total_price': total, 'average_price': average}


    @task
    def display_result(price_summary_dict: dict):

        total = price_summary_dict['total_price']
        average = price_summary_dict['average_price']

        print(f"Total Price of goods {total}")
        print(f"Average Price of goods {average}")

    order_price_data = get_order_prices()
    price_summary_dict = compute_total_and_average(order_price_data)
    
    display_result(price_summary_dict)
    
passing_data_with_taskflow_api_adv()