from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"

###############################################
# DAG Definition
###############################################

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1), 
    "start_date": datetime(2021, 11, 10),
    "end_date": datetime(2021, 12, 31),
    "catchup": True
}

with DAG(dag_id = 'bronze_table', schedule_interval = "@monthly", default_args = default_args,
    description = "Creates bronze layer table - union green and yellow datasets for every month") as dag:

    EXEC_DATE = '{{ ds }}'

    spark_job = SparkSubmitOperator(
        task_id="spark_job",
        application="/usr/local/spark/app/taxis_bronze.py",
        name="taxis_bronze",
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master}
    )

    # first we check if both parquet files for the current month were successfully downloaded
    for taxi_type in ["green","yellow"]:

        year= datetime.now().year
        month= datetime.now().month

        sensor = FileSensor(task_id="check_files_exist",
            filepath = f'/tmp/{taxi_type}_tripdata_{EXEC_DATE}.parquet',
            poke_interval = 30,
            timeout = 60 * 20,
            retries = 0)

        sensor >> spark_job