from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, lit, year, quarter, month
from pyspark.sql.types import *

import logging, sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)

postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]

yellow_schema = StructType([
                    StructField('VendorID', IntegerType(), True),
                    StructField('tpep_pickup_datetime', TimestampType(), True),
                    StructField('tpep_dropoff_datetime', TimestampType(), True),
                    StructField('passenger_count', IntegerType(), True),
                    StructField('trip_distance', DoubleType(), True),
                    StructField('RatecodeID', IntegerType(), True),
                    StructField('store_and_fwd_flag', StringType(), True),
                    StructField('PULocationID', IntegerType(), True),
                    StructField('DOLocationID', IntegerType(), True),
                    StructField('payment_type', IntegerType(), True),
                    StructField('fare_amount', DoubleType(), True),
                    StructField('extra', DoubleType(), True),
                    StructField('mta_tax', DoubleType(), True),
                    StructField('tip_amount', DoubleType(), True),
                    StructField('tolls_amount', DoubleType(), True),
                    StructField('improvement_surcharge', DoubleType(), True),
                    StructField('total_amount', DoubleType(), True),
                    StructField('congestion_surcharge', DoubleType(), True),
                    StructField('airport_fee', DoubleType(), True),
                ])

green_schema = StructType([
                    StructField('VendorID', IntegerType(), True),
                    StructField('lpep_pickup_datetime', TimestampType(), True),
                    StructField('lpep_dropoff_datetime', TimestampType(), True),
                    StructField('store_and_fwd_flag', StringType(), True),
                    StructField('RatecodeID', IntegerType(), True),
                    StructField('PULocationID', IntegerType(), True),
                    StructField('DOLocationID', IntegerType(), True),
                    StructField('passenger_count', IntegerType(), True),
                    StructField('trip_distance', DoubleType(), True),
                    StructField('fare_amount', DoubleType(), True),
                    StructField('extra', DoubleType(), True),
                    StructField('mta_tax', DoubleType(), True),
                    StructField('tip_amount', DoubleType(), True),
                    StructField('tolls_amount', DoubleType(), True),
                    StructField('ehail_fee', DoubleType(), True),
                    StructField('improvement_surcharge', DoubleType(), True),
                    StructField('total_amount', DoubleType(), True),
                    StructField('payment_type', IntegerType(), True),
                    StructField('trip_type', IntegerType(), True),
                    StructField('congestion_surcharge', DoubleType(), True)
                ])

if __name__ == "__main__":
    
    spark_conf = SparkConf().setMaster("local").setAppName("bronze_layer")
    spark = SparkSession.builder \
                .config(conf = spark_conf) \
                .getOrCreate()
    
    try:
        green_df = spark.read.parquet('/tmp/green*.parquet', schema=green_schema)
    except Exception as e:
        logger.INFO("Error while reading green taxis parquet", e)
    
    try:
        yellow_df = spark.read.parquet('/tmp/yellow*.parquet', schema=yellow_schema)
    except Exception as e:
        logger.INFO("Error while reading yellow taxis parquet", e)
    
    try:
        taxis_df = (yellow_df.select(
                        col("VendorID").alias("vendor_id"),
                        col("PULocationID").alias("pickup_location_id"),
                        col("tpep_pickup_datetime").alias("pickup_ts"),
                        col("DOLocationID").alias("dropoff_location_id"),
                        col("tpep_dropoff_datetime").alias("dropoff_ts"),
                        col("RatecodeID").alias("rate_code_id"),
                        "passenger_count",
                        "store_and_fwd_flag",
                        "trip_distance",
                        "total_amount",
                        "fare_amount",
                        "extra",
                        "mta_tax",
                        "tip_amount",
                        "tolls_amount",
                        "improvement_surcharge",
                        "congestion_surcharge",
                    ).withColumn("taxi_type", lit("yellow"))
            ).union(green_df.select(
                        col("VendorID").alias("vendor_id"),
                        col("PULocationID").alias("pickup_location_id"),
                        col("lpep_pickup_datetime").alias("pickup_ts"),
                        col("DOLocationID").alias("dropoff_location_id"),
                        col("lpep_dropoff_datetime").alias("dropoff_ts"),
                        col("RatecodeID").alias("rate_code_id"),
                        "passenger_count",
                        "store_and_fwd_flag",
                        "trip_distance",
                        "total_amount",
                        "fare_amount",
                        "extra",
                        "mta_tax",
                        "tip_amount",
                        "tolls_amount",
                        "improvement_surcharge",
                        "congestion_surcharge",
                    ).withColumn("taxi_type", lit("green"))
            )

        taxis_df \
            .withColumn("pickup_year", year(col("pickup_ts"))) \
            .withColumn("pickup_month", month(col("pickup_ts"))) \
            .withColumn("pickup_quarter", quarter(col("pickup_ts"))) \
            .filter("pickup_year >= 2020") \
            .filter("pickup_year <= 2021") \
            .createOrReplaceTempView("taxis_bronze")

    except Exception as e:
        logger.INFO("Error while trying to write unified bronze table", e)

    #################### golda layer #####################################
    try:
        table01 = spark.sql("""
            SELECT pickup_year, pickup_month, taxi_type,
                    COUNT(*) AS trips_count
            FROM taxis_bronze
            GROUP BY 1,2,3
        """)

        table01.write \
            .format("jdbc") \
            .option("url", postgres_db) \
            .option("dbtable", "gold.trips_per_month") \
            .option("user", postgres_user) \
            .option("password", postgres_pwd) \
            .mode("overwrite") \
            .save()

        table02 = spark.sql("""
            SELECT pickup_year, pickup_month, taxi_type,
                    MAX(trip_distance) AS longest_trip
            FROM taxis_bronze
            GROUP BY 1,2,3
        """)

        table02.write \
            .format("jdbc") \
            .option("url", postgres_db) \
            .option("dbtable", "gold.monthly_longest_trip") \
            .option("user", postgres_user) \
            .option("password", postgres_pwd) \
            .mode("overwrite") \
            .save()

        table03 = spark.sql("""
            SELECT pickup_location_id, pickup_year, pickup_month, 
                    AVG(total_amount) AS avg_trip_cost
            FROM taxis_bronze
            GROUP BY 1,2,3
            ORDER BY pickup_year, pickup_month, avg_trip_cost DESC
        """)

        table03.write \
            .format("jdbc") \
            .option("url", postgres_db) \
            .option("dbtable", "gold.avg_cost_by_pickupLoc") \
            .option("user", postgres_user) \
            .option("password", postgres_pwd) \
            .mode("overwrite") \
            .save()

    except Exception as e:
        logger.INFO("Error while trying to write gold tables", e)