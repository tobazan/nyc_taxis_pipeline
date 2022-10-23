# Airflow Spark

The idea behind this project was to integrate my knowledge in Airflow, Postgres, pySpark, and Docker into a single end to end simple project

The architecture behind this I cloned Cordon Thiago's Github repo, but modified towards my goal

## Credits to his repo

    $ git clone https://github.com/cordon-thiago/airflow-spark

## Architecture components

![](./doc/architecture.png "Architecture")

## Steps to run this project locally

### Build airflow-spark driver image
___

Inside the taxis-project/docker/docker-airflow

    $ docker build --rm --force-rm -t docker-airflow-spark:1.10.7_3.1.2 .

### Start containers
___

Navigate to airflow-spark/docker and:

    $ docker-compose up

If you want to run in background:

    $ docker-compose up -d

Note: when running the docker-compose for the first time, the images `postgres:9.6` and `bitnami/spark:3.1.2` will be downloaded before the containers started.

### Check if you can access
___

Airflow: http://localhost:8282

Spark Master: http://localhost:8181

### Set spark-default conn
___

Before starting the DAGs the *spark-default* must be edited

![](./doc/airflow_spark_connection.png "spark-defualt")


