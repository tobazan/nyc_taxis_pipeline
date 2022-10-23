from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"

postgres_db = "jdbc:postgresql://postgres/test"
postgres_user = "test"
postgres_pwd = "postgres"

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

with DAG(dag_id = 'gold_tables', schedule_interval = "@monthly", default_args = default_args,
    description = "Creates tables in PG data base") as dag:

    start_sensor = ExternalTaskSensor(task_id = "start_sensor",     
                                    poke_interval = 60,
                                    timeout = 60 * 10,
                                    retries = 0,
                                    external_dag_id='bronze_table')

    spark_job_load_postgres = SparkSubmitOperator(
        task_id="spark_job_load_postgres",
        application="/usr/local/spark/app/gold-postgres.py",
        name="gold-postgres",
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        application_args = [postgres_db, postgres_user, postgres_pwd],
        jars = postgres_driver_jar,
        driver_class_path = postgres_driver_jar)

    start_sensor >> spark_job_load_postgres