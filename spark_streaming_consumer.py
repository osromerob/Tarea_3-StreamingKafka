from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql.functions import current_timestamp
import logging


# Configura el nivel de log a WARN para reducir los mensajes INFO
spark = SparkSession.builder \
	.appName("KafkaSparkStreaming") \
	.getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir el esquema de los datos de entrada
schema = StructType([
	StructField("id_ICAO24", StringType()),
	StructField("callsign", StringType()),
	StructField("country", StringType()),
	StructField("longitude", FloatType()),
	StructField("altitude", FloatType()),
	StructField("timestamp", TimestampType())
	])

# Crear una sesión de Spark
spark = SparkSession.builder \
	.appName("OpenSkyDataAnalysis") \
	.getOrCreate()

# Configurar el lector de streaming para leer desde Kafka
df = spark \
	.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", "localhost:9092") \
	.option("subscribe", "OpenSky_data") \
	.load()

# Parsear los datos JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*") \
	.withColumn("timestamp", current_timestamp())

# Calcular el número de vuelos por país
windowed_stats = parsed_df \
	.groupBy(window(col("timestamp"), "1 minute"), "country") \
	.agg(count("callsign").alias("flight_count"))

# Escribir los resultados en la consola
query = windowed_stats \
	.writeStream \
	.outputMode("complete") \
	.format("console") \
	.start()

query.awaitTermination()
