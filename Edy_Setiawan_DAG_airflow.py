'''
=================================================

Name: Edy Setiawan

This program is designed to automate the transformation and loading of data from a PostgreSQL database to Elasticsearch.
The dataset used for this operation is focused on Company Bankruptcy Prediction. It comprises bankruptcy data sourced from the Taiwan Economic Journal, spanning the years 1999 to 2009.
=================================================
'''

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Function to get data from PostgreSQL
def get_data_from_postgresql():
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_m3", conn)  # Updated table name to 'table_m3'
    df.to_csv('/opt/airflow/data/P2M3_Edy_Setiawan_data_raw.csv',index=False)

# Function to clean the DataFrame
def clean_dataframe():
    df = pd.read_csv('/opt/airflow/data/P2M3_Edy_Setiawan_data_raw.csv')
    df_clean = df.drop_duplicates()
    df_clean.columns = (
        df_clean.columns.str.lower()
        .str.replace(' ', '_', regex=True)
        .str.replace('?', '', regex=True)
        .str.replace('/', '_or_', regex=True)
    )
    df_clean.to_csv('/opt/airflow/data/P2M3_Edy_Setiawan_data_clean.csv', index=False)

# Function to post the data to Kibana
def post_to_kibana():
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/data/P2M3_Edy_Setiawan_data_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(res)

# DAG setup
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
}

with DAG('P2M3_Data_Pipeline',
         description='End-to-end Data Pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         start_date=datetime(2023, 10, 1),
         catchup=False) as dag:
    
    # Task to fetch data from PostgreSQL
    fetch_task = PythonOperator(
        task_id='get_data_from_postgresql',
        python_callable=get_data_from_postgresql
    )
    
    # Task to clean the data
    clean_task = PythonOperator(
        task_id='clean_dataframe',
        python_callable=clean_dataframe
    )
    
    # Task to post to Kibana
    post_to_kibana_task = PythonOperator(
        task_id='post_to_kibana',
        python_callable=post_to_kibana
    )
    
    # Set task dependencies
    fetch_task >> clean_task >> post_to_kibana_task
