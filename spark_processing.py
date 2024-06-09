from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from kafka import KafkaProducer
import json


schema = StructType([
    StructField("location", ArrayType(DoubleType()), True),
    StructField("MQ-7", StructType([StructField("pollution_level", DoubleType(), True)]), True),
    StructField("MQ-135", StructType([StructField("pollution_level", DoubleType(), True)]), True),
    StructField("nearest_city", StringType(), True),
    StructField("distance_to_city_km", DoubleType(), True)
])

spark = SparkSession.builder \
    .appName("AirQualityProcessing") \
    .getOrCreate()

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("-----------------------------------------------------------------------------------------------------------------------")
print("-------------------------------------Procesando Mensajes de IOT AirMaster----------------------------------------------")
print("-----------------------------------------------------------------------------------------------------------------------")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "air_quality") \
    .option("startingOffsets", "earliest") \
    .load()

def count_rows_in_batch(batch_df, batch_id):
    row_count = batch_df.count()
    print("-----------------------------------------------------------------------------------------------------------------------")
    print(f"Batch ID: {batch_id}, Row count: {row_count}")
    print("-----------------------------------------------------------------------------------------------------------------------")
    

query = df.writeStream \
    .foreachBatch(count_rows_in_batch) \
    .start()
    
df = df.selectExpr("CAST(value AS STRING)")


json_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")


json_df_with_count = json_df.withColumn("count", lit(1))


count_df = json_df_with_count.groupBy().agg(count("count").alias("message_count"))


filtered_df = df.filter((col("MQ-07") > 5) & (col("MQ-135") > 5))

def send_to_kafka(df, epoch_id):

    for row in df.collect():
        producer.send('Air_Danger', {'message': 'Se detectaron emisiomes peligrosas en ..'})
    producer.flush()


def process_batch(batch_df, batch_id):

    cities = batch_df.select("location").distinct().collect()
    for city in cities:
        print(f"City: {city['location']}")


    batch_df.selectExpr("CAST(location AS STRING) AS key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "filtered_air_quality") \
        .save()

query = filtered_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .start()
    
print("-----------------------------------------------------------------------------------------------------------------------")
print("-------------------------------------Fin Batch de IOT AirMaster----------------------------------------------")
print("-----------------------------------------------------------------------------------------------------------------------")

query.awaitTermination()
