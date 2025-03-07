import os

import pendulum
import yfinance as yf
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup


def read_stock_symbols():
    with open("/tmp/stocks.txt", "r") as file:
        return [line.strip() for line in file.readlines()]


def lookup_stock_prices(symbols):
    prices = {}
    for symbol in symbols:
        stock = yf.Ticker(symbol)
        prices[symbol] = stock.history(period="1d")["Close"][0]
    with open("/tmp/stock_prices.txt", "w") as file:
        for symbol, price in prices.items():
            file.write(f"{symbol}: {price}\n")


def lookup_stock_volumes(symbols):
    volumes = {}
    for symbol in symbols:
        stock = yf.Ticker(symbol)
        volumes[symbol] = stock.history(period="1d")["Volume"][0]
    with open("/tmp/stock_volumes.txt", "w") as file:
        for symbol, volume in volumes.items():
            file.write(f"{symbol}: {volume}\n")


def delete_stocks_file():
    os.remove("/tmp/stocks.txt")


with DAG(
    dag_id="lookup_stocks",
    schedule=None,
    start_date=pendulum.datetime(2023, 3, 3, tz="UTC"),
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    wait_for_file = FileSensor(
        task_id="wait_for_stocks_file",
        filepath="/tmp/stocks.txt",
        poke_interval=10,
        timeout=600,
    )

    read_symbols = PythonOperator(
        task_id="read_stock_symbols", python_callable=read_stock_symbols
    )

    with TaskGroup(group_id="lookup_tasks") as lookup_tasks:
        lookup_prices = PythonOperator(
            task_id="lookup_stock_prices",
            python_callable=lookup_stock_prices,
            op_args=[read_symbols.output],
        )

        lookup_volumes = PythonOperator(
            task_id="lookup_stock_volumes",
            python_callable=lookup_stock_volumes,
            op_args=[read_symbols.output],
        )

    end = PythonOperator(task_id="end", python_callable=delete_stocks_file)

    start >> wait_for_file >> read_symbols >> lookup_tasks >> end
