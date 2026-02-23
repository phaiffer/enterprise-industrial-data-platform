import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count
from pyspark.sql.functions import max as smax
from pyspark.sql.functions import min as smin


def build_spark(app_name: str) -> SparkSession:
    """
    Create Spark session. Delta + S3A should be configured via spark-defaults.conf.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


if __name__ == "__main__":
    spark = build_spark("EIDP_Batch_Gold")
    spark.sparkContext.setLogLevel("WARN")

    silver_path = "s3a://eidp/lake/silver/sensors"
    df_silver = spark.read.format("delta").load(silver_path)

    # Compute KPIs per site and sensor
    df_gold = df_silver.groupBy("site", "sensor_id").agg(
        count("*").alias("events"),
        avg("temperature").alias("avg_temperature"),
        smin("temperature").alias("min_temperature"),
        smax("temperature").alias("max_temperature"),
        avg("vibration").alias("avg_vibration"),
        avg("pressure").alias("avg_pressure"),
    )

    gold_path = "s3a://eidp/lake/gold/sensor_kpis"
    (
        df_gold.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path)
    )
    print(f"Gold Delta written to: {gold_path}")

    # Optional: write to Postgres for dashboard/BI consumption
    enable_pg = os.getenv("ENABLE_POSTGRES_SINK", "false").lower() == "true"
    if enable_pg:
        pg_url = os.getenv("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/eidp")
        pg_user = os.getenv("PG_USER", "eidp")
        pg_pass = os.getenv("PG_PASSWORD", "eidp")
        pg_table = os.getenv("PG_TABLE", "public.sensor_kpis")

        (
            df_gold.write.format("jdbc")
            .option("url", pg_url)
            .option("dbtable", pg_table)
            .option("user", pg_user)
            .option("password", pg_pass)
            .mode("overwrite")
            .save()
        )

        print(f"Gold KPIs written to Postgres table: {pg_table}")
        print("NOTE: spark-submit must include the PostgreSQL JDBC driver package.")
