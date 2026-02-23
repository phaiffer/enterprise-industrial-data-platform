from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


def build_spark(app_name: str) -> SparkSession:
    """
    Create Spark session. Delta + S3A should be configured via spark-defaults.conf.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


if __name__ == "__main__":
    spark = build_spark("EIDP_Batch_Silver")
    spark.sparkContext.setLogLevel("WARN")

    raw_path = "/opt/eidp/data/raw/sensors_batch.csv"

    df_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(raw_path)
    )

    # Enforce event_time as timestamp and basic type safety
    df_silver = df_raw.withColumn("event_time", to_timestamp(col("event_time"))).filter(
        col("sensor_id").isNotNull()
    )

    silver_path = "s3a://eidp/lake/silver/sensors"

    (
        df_silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )

    print(f"Silver Delta written to: {silver_path}")
