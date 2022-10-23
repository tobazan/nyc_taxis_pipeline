from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, lit, year, quarter
from pyspark.sql.types import *

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
    
    conf = SparkConf()
    conf.setMaster("local").setAppName("bronze_layer")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
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
            .withColumn("pickup_yr", year(col("pickup_ts"))) \
            .withColumn("pickup_qt", quarter(col("pickup_ts"))) \
            .filter("pickup_yr >= 2020") \
            .filter("pickup_yr <= 2021") \
            .write.partitionBy("pickup_qt") \
            .saveAsTable("taxis_bronze", mode="overwrite")

    except Exception as e:
        logger.INFO("Error while trying to write unified bronze table", e)