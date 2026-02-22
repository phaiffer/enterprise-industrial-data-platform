import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    sha2,
    concat_ws,
    to_timestamp,
    current_timestamp,
    expr,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def build_spark(app_name: str) -> SparkSession:
    """
    Create a Spark session. Delta + S3A settings are expected to be provided
    via spark-defaults.conf (mounted by Docker Compose).
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def sensor_schema() -> StructType:
    """
    Expected Kafka JSON payload schema (official contract).
    """
    return StructType(
        [
            StructField("sensor_id", StringType(), False),
            StructField("site", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("vibration", DoubleType(), True),
            StructField("pressure", DoubleType(), True),
            StructField("event_time", StringType(), True),  # ISO8601 string
        ]
    )


if __name__ == "__main__":
    # ---- Runtime configuration ----
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "sensor-data")

    bronze_path = os.getenv("BRONZE_PATH", "s3a://eidp/lake/bronze/sensors")
    quarantine_path = os.getenv(
        "QUARANTINE_PATH", "s3a://eidp/lake/quarantine/sensors_invalid"
    )

    checkpoint_bronze = os.getenv(
        "CHECKPOINT_BRONZE", "s3a://eidp/lake/_checkpoints/bronze/sensors"
    )
    checkpoint_quarantine = os.getenv(
        "CHECKPOINT_QUARANTINE", "s3a://eidp/lake/_checkpoints/quarantine/sensors_invalid"
    )

    watermark_delay = os.getenv("WATERMARK_DELAY", "10 minutes")
    spark_log_level = os.getenv("SPARK_LOG_LEVEL", "WARN")
    starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", "latest")

    spark = build_spark("EIDP_Streaming_Bronze")
    spark.sparkContext.setLogLevel(spark_log_level)

    # ---- Read from Kafka ----
    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    df_raw = df_kafka.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_ingest_time"),
    )

    schema = sensor_schema()

    # ---- Parse JSON ----
    df_parsed = df_raw.withColumn("data", from_json(col("raw_value"), schema))

    # Valid events (parsed)
    df_valid = (
        df_parsed.filter(col("data").isNotNull())
        .select(
            col("kafka_key"),
            col("raw_value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_ingest_time"),
            col("data.sensor_id").alias("sensor_id"),
            col("data.site").alias("site"),
            col("data.temperature").alias("temperature"),
            col("data.vibration").alias("vibration"),
            col("data.pressure").alias("pressure"),
            col("data.event_time").alias("event_time_raw"),
        )
        .withColumn("event_time", to_timestamp(col("event_time_raw")))
        .withColumn("processed_at", current_timestamp())
    )

    # Generate a deterministic event_id to deduplicate resent data
    df_valid = df_valid.withColumn(
        "event_id",
        sha2(
            concat_ws(
                "||",
                col("sensor_id"),
                col("site"),
                col("event_time_raw"),
                col("temperature").cast("string"),
                col("vibration").cast("string"),
                col("pressure").cast("string"),
            ),
            256,
        ),
    )

    # Partition helpers
    df_valid = (
        df_valid.withColumn("event_date", expr("to_date(event_time)"))
        .withColumn("event_hour", expr("date_format(event_time, 'yyyy-MM-dd-HH')"))
    )

    # Watermark + dedup
    df_dedup = df_valid.withWatermark("event_time", watermark_delay).dropDuplicates(
        ["event_id"]
    )

    # Invalid events (failed parsing) -> quarantine
    df_invalid = (
        df_parsed.filter(col("data").isNull())
        .select(
            col("kafka_key"),
            col("raw_value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_ingest_time"),
        )
        .withColumn("processed_at", current_timestamp())
    )

    # ---- Write Bronze Delta ----
    bronze_query = (
        df_dedup.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_bronze)
        .partitionBy("event_date", "site")
        .start(bronze_path)
    )

    # ---- Write Quarantine Delta ----
    quarantine_query = (
        df_invalid.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_quarantine)
        .start(quarantine_path)
    )

    spark.streams.awaitAnyTermination()
