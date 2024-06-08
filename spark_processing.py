from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("co_level", DoubleType(), True),
    StructField("no2_level", DoubleType(), True),
    StructField("pm2_5_level", DoubleType(), True),
    StructField("distance", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

spark = SparkSession.builder \
    .appName("AirQualityThresholdsProcessing") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "air_quality") \
    .option("startingOffsets", "earliest") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

json_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

processed_df = json_df.filter(col("co_level") > 5.0)


logger.info("---------------------processing -------------------------")


log_messages_df = processed_df.withColumn(
    "value", concat(lit("mensaje del 'sensor id': "), col("sensor_id"))
).selectExpr("CAST(value AS STRING)")

# Escribir los mensajes de log al tópico de Kafka "logs"
#log_query = log_messages_df.writeStream \
#    .format("kafka") \
#    .option("kafka.bootstrap.servers", "kafka:9092") \
#    .option("topic", "logs") \
#    .option("checkpointLocation", "/tmp/checkpoints") \
#    .start()

query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


#log_query.awaitTermination()
query.awaitTermination()
