"""Job de Spark Structured Streaming para el muestreo urbano.

Lee las lecturas de los sensores desde Kafka y calcula, por ventana de tiempo
de 1 minuto y por sensor_id, el promedio de temperatura y de calidad del aire.
El resultado se imprime en consola (se ve con `kubectl logs`).

Configuración por variables de entorno (ConfigMaps en Kubernetes):
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    SPARK_APP_NAME, SPARK_WINDOW_DURATION, SPARK_WATERMARK_DELAY,
    SPARK_TRIGGER_INTERVAL, SPARK_STARTING_OFFSETS, SPARK_CHECKPOINT_DIR,
    SPARK_SHUFFLE_PARTITIONS, SPARK_LOG_LEVEL
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# Estructura del JSON que envía el productor.
SENSOR_SCHEMA = StructType(
    [
        StructField("sensor_id", StringType(), nullable=False),
        StructField("temperature", DoubleType(), nullable=True),
        StructField("humidity", DoubleType(), nullable=True),
        StructField("air_quality_index", IntegerType(), nullable=True),
        StructField("timestamp", StringType(), nullable=False),
    ]
)


def env(name, default=None):
    value = os.environ.get(name, default)
    if value is None:
        print(f"[ERROR] Falta la variable de entorno {name}", file=sys.stderr)
        sys.exit(2)
    return value


def main():
    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS")
    topic = env("KAFKA_TOPIC")
    window_duration = env("SPARK_WINDOW_DURATION", "1 minute")
    watermark_delay = env("SPARK_WATERMARK_DELAY", "2 minutes")
    trigger_interval = env("SPARK_TRIGGER_INTERVAL", "10 seconds")
    starting_offsets = env("SPARK_STARTING_OFFSETS", "earliest")
    checkpoint_dir = env("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints/urban-sensors")

    spark = (
        SparkSession.builder.appName(env("SPARK_APP_NAME", "urban-sensors-streaming"))
        .config("spark.sql.shuffle.partitions", env("SPARK_SHUFFLE_PARTITIONS", "3"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(env("SPARK_LOG_LEVEL", "WARN"))

    # 1) Lectura del tópico de Kafka (value llega como bytes).
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2) JSON -> columnas. event_time es el timestamp que generó el sensor.
    readings = (
        raw.select(F.from_json(F.col("value").cast("string"), SENSOR_SCHEMA).alias("r"))
        .select("r.*")
        .withColumn("event_time", F.col("timestamp").cast("timestamp"))
        .dropna(subset=["sensor_id", "event_time"])
    )

    # 3) Ventana de 1 minuto por sensor: promedio de temperatura y calidad del aire.
    averages = (
        readings.withWatermark("event_time", watermark_delay)
        .groupBy(F.window("event_time", window_duration), F.col("sensor_id"))
        .agg(
            F.round(F.avg("temperature"), 2).alias("avg_temperature"),
            F.round(F.avg("air_quality_index"), 2).alias("avg_air_quality_index"),
            F.count("*").alias("readings"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "sensor_id",
            "avg_temperature",
            "avg_air_quality_index",
            "readings",
        )
    )

    # 4) Salida a consola. En modo "update" cada micro-lote muestra las ventanas
    #    que cambiaron, sin esperar a que cierre el minuto.
    query = (
        averages.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 50)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=trigger_interval)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
