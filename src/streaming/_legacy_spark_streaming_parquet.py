"""
DEPRECATED:
This script is a simplified example (Kafka -> Parquet) and is NOT the official pipeline.
Use: src/streaming/stream_kafka_to_bronze_delta.py instead.

Recommended action:
- Rename to: src/streaming/_legacy_spark_streaming_parquet.py
  OR delete it to avoid confusion.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType


schema = (
    StructType()
    .add("sensor_id", StringType())
    .add("site", StringType())
    .add("temperature", DoubleType())
    .add("vibration", DoubleType())
    .add("pressure", DoubleType())
    .add("event_time", StringType())
)


spark = SparkSession.builder.appName("EIDP_Legacy_Streaming_Parquet").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "sensor-data")
    .option("startingOffsets", "latest")
    .load()
)

df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select(
    "data.*"
)

df_final = df_parsed.withColumn("event_ts", to_timestamp(col("event_time")))

query = (
    df_final.writeStream.format("parquet")
    .option("path", "/opt/eidp/data/legacy/bronze_parquet")
    .option("checkpointLocation", "/opt/eidp/data/legacy/_checkpoints")
    .outputMode("append")
    .start()
)

query.awaitTermination()
