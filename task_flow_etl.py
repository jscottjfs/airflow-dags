from datetime import datetime

from airflow.decorators import dag, task


@task
def extract_data():
    data = [1, 2, 3]
    return data


@task
def transform_data(data):
    transformed = [x * 2 for x in data]
    return transformed


@task
def load_data(data):
    print("Loaded data:", data)


default_args = {
    "start_date": datetime(2025, 1, 1),
}


@dag(
    schedule="@daily",
    default_args=default_args,
    catchup=False,
)
def task_flow_etl():
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)


dag_instance = task_flow_etl()
