from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd
from airflow import DAG

import json, csv
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.decorators import dag, task

default_args = {
   'owner' : 'prajapat21'
}

@dag(
    dag_id='executing_sqlite_pipeline_taskflow_api',
    description='Pipeline using SQL operators using TaskFlow API',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['pipeline', 'sqlite']
)
def data_transformation_storage_pipeline():

    @task
    def read_dataset():
        df = pd.read_csv('/Users/prajapat21/airflow/datasets/car_data.csv')

        return df.to_json()

    @task
    def read_car_categories():
        df = pd.read_csv('/Users/prajapat21/airflow/datasets/car_categories.csv')

        return df.to_json()


    @task
    def create_table_car_data():
        sqlite_operator = SqliteOperator(
            task_id='create_table',
            sql = r"""
                CREATE TABLE IF NOT EXISTS car_data (
                    id INTEGER PRIMARY KEY,
                    brand TEXT NOT NULL,
                    model TEXT NOT NULL,  
                    body_style TEXT NOT NULL,
                    seat INTEGER NOT NULL,
                    price INTEGER NOT NULL
                );
            """,
            sqlite_conn_id='my_sqlite_conn',
        )
        sqlite_operator.execute(context=None)

    @task
    def insert_selected_data(**kwargs):
        ti = kwargs['ti']
        json_data = ti.xcom_pull(task_ids='read_dataset')

        df = pd.read_json(json_data)

        df = df[['Brand', 'Model', 'BodyStyle', 'Seats', 'PriceEuro']]

        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        insert_query = """
            INSERT INTO car_data (brand, model, body_style, seat, price)
             VALUES (?, ?, ?, ?, ?)
        """

        parameters = df.to_dict(orient='records')

        for record in parameters:
            sqlite_operator = SqliteOperator(
                task_id='insert_data',
                sqlite_conn_id='my_sqlite_conn',
                sql=insert_query,
                parameters=tuple(record.values()),
            )

            sqlite_operator.execute(context=None)

    read_dataset() >> create_table_car_data() >> insert_selected_data()

data_transformation_storage_pipeline()