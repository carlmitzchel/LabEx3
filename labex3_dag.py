from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
import numpy as np
from datetime import date


def loading_branch_service():
    df_branch_service = pd.read_json("branch_service_transaction_info.json")
    df_branch_service.to_parquet("df_branch_service.parquet")

def loading_customer_transaction():
    df_customer_transaction = pd.read_json("customer_transaction_info.json")
    df_customer_transaction.to_parquet("df_customer_transaction.parquet")

def drop_dups_branch_service():
     df_branch_service = pd.read_parquet('df_branch_service.parquet', engine='pyarrow')
     df_branch_service = df_branch_service.drop_duplicates(subset=['txn_id'])
     df_branch_service.to_parquet("df_branch_service_drop_dups.parquet")

def drop_dups_customer_transaction():
     df_customer_transaction = pd.read_parquet('df_customer_transaction.parquet', engine='pyarrow')
     df_customer_transaction = df_customer_transaction.drop_duplicates(subset=['txn_id'])
     df_customer_transaction.to_parquet("df_customer_transaction_drop_dups.parquet")

def merge_dataframe():
     df_branch_service = pd.read_parquet('df_branch_service_drop_dups.parquet', engine='pyarrow')
     df_customer_transaction = pd.read_parquet('df_customer_transaction_drop_dups.parquet', engine='pyarrow')
     df_merged = pd.merge(df_customer_transaction, df_branch_service)
     df_merged.to_parquet('df_no_dups_merged.parquet')


args = {
    'owner': 'Zrey',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='labex3_dag',
    default_args=args,
     schedule_interval='@daily'
    
)

with dag:

        loading_branch_service = PythonOperator(
            task_id="loading_branch_service",
            python_callable=loading_branch_service
        )

        loading_customer_transaction = PythonOperator(
            task_id="loading_customer_transaction",
            python_callable=loading_customer_transaction
        )

        drop_dups_branch_service = PythonOperator(
            task_id="drop_dups_branch_service",
            python_callable=drop_dups_branch_service
        )

        drop_dups_customer_transaction = PythonOperator(
            task_id="drop_dups_customer_transaction",
            python_callable=drop_dups_customer_transaction
        )

        merge_dataframe = PythonOperator(
            task_id="merge_dataframe",
            python_callable=merge_dataframe
        )



loading_branch_service >> drop_dups_branch_service
loading_customer_transaction >> drop_dups_customer_transaction
[drop_dups_branch_service, drop_dups_customer_transaction] >> merge_dataframe