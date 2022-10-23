from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess, functools

###############################################
# DAG Definition
###############################################

def download_data(exec_date):
    try:
        for taxi_type in ["green","yellow"]:
            #print(exec_date[:4]) YYYY
            #print(exec_date[5:7]) mm
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{exec_date[:4]}-{int(exec_date[5:7]):02d}.parquet"
            subprocess.run(["wget", url, "-O", f"/tmp/{taxi_type}_tripdata_{exec_date}.parquet"])

    except Exception as e:
        print("Error while downloading", e)
        raise

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1), 
    "start_date": datetime(2020, 1, 1),
    "end_date": datetime(2021, 12, 10),
    "catchup": True
}

with DAG(dag_id = 'download_data', schedule_interval = "@monthly", default_args = default_args,
    description = "Downloads NYC parquet file monthly for yellow and green taxi types from the official https://www1.nyc.gov/site/") as dag:
    
    EXEC_DATE = '{{ ds }}'

    start_task = DummyOperator(task_id='dummy_task')

    download_task = PythonOperator(task_id='download_task',
                                    python_callable=download_data,
                                    op_kwargs={'exec_date':EXEC_DATE})
                
    start_task >> download_task
