from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# SparkSession
spark = SparkSession.builder \
    .appName("ReadFromKafkaToPostgres") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.3") \
    .config("spark.driver.memory", "5g") \
    .config("spark.executor.memory", "5g") \
    .getOrCreate()

# Kafka’dan veri oku
kafka_bootstrap_servers = "kafka:9092"
topic = "sensor_data_topic"

try:
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
    print(f"Kafka’dan veri okundu")
except Exception as e:
    print(f"Kafka’dan veri okuma hatası: {e}")
    spark.stop()
    exit(1)

# JSON şeması
schema = StructType([
    StructField("sensor_datetime_hm", TimestampType(), True),
    StructField("room_id", StringType(), True),
    StructField("avg_co2", DoubleType(), True),
    StructField("avg_humidity", DoubleType(), True),
    StructField("avg_light", DoubleType(), True),
    StructField("avg_pir", DoubleType(), True),
    StructField("avg_temperature", DoubleType(), True)
])

df_parsed = df.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data")
).select("data.*")

#172.19.0.3 
#localhost
# PostgreSQL’e yaz
jdbc_url = "jdbc:postgresql://postgres:5432/sensor_data"
properties = {
    "user": "postgres",
    "password": "password123",
    "driver": "org.postgresql.Driver"
}

def write_to_postgres(batch_df, batch_id):
    try:
        batch_df.write \
            .jdbc(url=jdbc_url, table="public.sensor_readings", mode="append", properties=properties)
        print("PostgreSQL’e yazdım")
    except Exception as e:
        print(f"PostgreSQL’e yazma hatası (batch {batch_id}): {e}")

try:
    query = df_parsed \
        .writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .start()
    query.awaitTermination()
except Exception as e:
    print(f"Streaming hatası: {e}")
    spark.stop()
    exit(1)