from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator


def process():
    return

with DAG("labex3_dag", start_date=datetime(2021, 1, 1),
    schedule_interval="@hourly", catchup=False) as dag:

        process_a = PythonOperator(
            task_id="process",
            python_callable=process
        )



