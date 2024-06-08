from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Definir el esquema para los datos entrantes
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("co_level", DoubleType(), True),
    StructField("no2_level", DoubleType(), True),
    StructField("pm2_5_level", DoubleType(), True),
    StructField("distance", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("AirQualityThresholdsProcessing") \
    .getOrCreate()

# Leer datos desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "air_quality") \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir la columna binaria 'value' a string
df = df.selectExpr("CAST(value AS STRING)")

# Analizar los datos JSON y aplicar el esquema
json_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Procesar los datos (por ejemplo, filtrar altos niveles de CO)
processed_df = json_df.filter(col("co_level") > 5.0)

# Log de un mensaje personalizado
logger.info("---------------------processing -------------------------")

# Crear mensajes de log
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

# Escribir los datos procesados a la consola (para pruebas)
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Esperar la terminación de las consultas
#log_query.awaitTermination()
query.awaitTermination()
