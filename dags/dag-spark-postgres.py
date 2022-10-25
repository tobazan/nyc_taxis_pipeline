from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"

postgres_db = "jdbc:postgresql://postgres/test"
postgres_user = "airflow"
postgres_pwd = "airflow"

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

with DAG(dag_id = 'spark-postgres', schedule_interval = "@monthly", default_args = default_args,
    description = "Creates bronze view and gold layer table - union green and yellow datasets for every month") as dag:

    EXEC_DATE = '{{ ds }}'

    pg_create_schema = PostgresOperator(
        task_id='pg_create_schema',
        postgres_conn_id='postgres_default',
        sql="CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION airflow;"
    )

    spark_job = SparkSubmitOperator(
        task_id="spark_job",
        application="/usr/local/spark/app/spark-postgres.py",
        name="spark-postgres",
        conn_id="spark_default",
        verbose=1,
        application_args = [postgres_db, postgres_user, postgres_pwd],
        jars = postgres_driver_jar,
        driver_class_path = postgres_driver_jar
    )

    # first we check if both parquet files for the current month were successfully downloaded
    for taxi_type in ["green","yellow"]:

        sensor = FileSensor(task_id="check_files_exist",
            filepath = f'/tmp/{taxi_type}_tripdata_{EXEC_DATE}.parquet',
            poke_interval = 30,
            timeout = 60 * 20,
            retries = 0)

        sensor >> pg_create_schema >> spark_job