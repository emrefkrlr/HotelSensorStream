from pyspark.sql import SparkSession, functions as F
import os

# SparkSession
spark = SparkSession.builder \
    .appName('WriteToKafka') \
    .config('spark.driver.memory', '5g') \
    .config('spark.executor.memory', '5g') \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

input_path = "dataops/output/sensor_data.parquet"
kafka_bootstrap_servers = "kafka:9092"
topic = "sensor_data_topic"


df = spark.read.parquet(input_path)
df.limit(5).show(truncate=False)
df_to_kafka = df.selectExpr("to_json(struct(*)) AS value")

df_to_kafka.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", topic) \
    .save()

spark.stop()