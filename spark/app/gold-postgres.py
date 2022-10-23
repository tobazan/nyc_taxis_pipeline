from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, lit, year, quarter
from pyspark.sql.types import *
from os.path import abspath

import logging, sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)

########### Parameters #############
postgres_db = sys.argv[3]
postgres_user = sys.argv[4]
postgres_pwd = sys.argv[5]
####################################

if __name__ == "__main__":

    warehouse_location = abspath('/usr/local/airflow/spark-warehouse')

    spark = SparkSession.builder \
                .appName("gold_layer") \
                .master("local") \
                .config("spark.sql.warehouse.dir", warehouse_location) \
                .enableHiveSupport() \
                .getOrCreate()
    
    try:
        df_probando = spark.sql("SELECT * FROM taxis_bronze LIMIT 25")
        df_probando.show()

        df_probando \
            .write \
            .format("jdbc") \
            .option("url", postgres_db) \
            .option("dbtable", "gold.probando") \
            .option("user", postgres_user) \
            .option("password", postgres_pwd) \
            .mode("overwrite") \
            .save()
    
    except Exception as e:
        logger.INFO("Error while writing probando df to PG database", e)