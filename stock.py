import json
from datetime import datetime

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import requests


def get_stock_report_today():
    url = 'https://financialmodelingprep.com/api/v3/quote-short/AAPL?apikey=de7a030f9a446755127cf507abd8de1f'
    response = requests.get(url)
    dataset = response.json()
    #with open('data2.json', 'w') as f:
        #json.dump(data2, f)
    return dataset


def save_data_into_db():
    #mysql_hook = MySqlHook(mysql_conn_id='app_db')
    dataset = get_stock_report_today()
    for data in dataset:
        import mysql.connector
        db = mysql.connector.connect(host='54.91.39.146',user='root',passwd='Noom@dti',db='airflow')

        cursor = db.cursor()
        price = data['price']
        symbol = data['symbol']
        volume = data['volume']

        cursor.execute('INSERT INTO tls_tb (price,symbol,volume)'
                  'VALUES("%s","%s","%s")',
                   (price,symbol,volume))

        db.commit()
        print("Record inserted successfully into  table")
        cursor.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),

}
def get_stock2_report_today():
    url = 'https://financialmodelingprep.com/api/v3/quote-short/TLS?apikey=de7a030f9a446755127cf507abd8de1f'
    response = requests.get(url)
    dataset = response.json()
    #with open('data2.json', 'w') as f:
        #json.dump(data2, f)
    return dataset


def save_data2_into_db():
    #mysql_hook = MySqlHook(mysql_conn_id='app_db')
    dataset = get_stock2_report_today()
    for data in dataset:
        import mysql.connector
        db = mysql.connector.connect(host='54.91.39.146',user='root',passwd='Noom@dti',db='airflow')

        cursor = db.cursor()
        price = data['price']
        symbol = data['symbol']
        volume = data['volume']

        cursor.execute('INSERT INTO tls_tb (price,symbol,volume)'
                  'VALUES("%s","%s","%s")',
                   (price,symbol,volume))

        db.commit()
        print("Record inserted successfully into  table")
        cursor.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),

}
with DAG('stock_data_pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline for stock report',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='get_stock_report_today',
        python_callable= get_stock_report_today
    )

    t2 = PythonOperator(
        task_id='save_data_into_db',
        python_callable=save_data_into_db
    )
    t3 = PythonOperator(
        task_id='get_stock2_report_today',
        python_callable= get_stock2_report_today
    )

    t4 = PythonOperator(
        task_id='save_data2_into_db',
        python_callable=save_data2_into_db
    )

    t1 >> t2 >> t3 >> t4
