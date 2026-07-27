from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, to_timestamp, struct
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


def main() -> None:
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
    checkpoint_root = os.getenv("SPARK_CHECKPOINT_ROOT", "/opt/urbanpulse/checkpoints")
    zone_profile_path = os.getenv(
        "ZONE_PROFILE_PATH", "/opt/urbanpulse/reference-data/zone_profile.csv"
    )

    spark = (
        SparkSession.builder.appName("UrbanPulseHealthAdvisories")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField("sensor_id", StringType(), False),
            StructField("zone", StringType(), False),
            StructField("pm25", DoubleType(), False),
            StructField("pm10", DoubleType(), False),
            StructField("no2", DoubleType(), False),
            StructField("aqi", DoubleType(), True),
            StructField("timestamp", StringType(), False),
        ]
    )
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", "urbanpulse.air_quality")
        .option("startingOffsets", "earliest")
        .load()
    )
    aqi = (
        raw.select(from_json(col("value").cast("string"), schema).alias("event"))
        .select("event.*")
        .withColumn("event_time", to_timestamp("timestamp"))
        .dropna(subset=["zone", "aqi", "event_time"])
        .withWatermark("event_time", "20 minutes")
    )
    zone_profile = spark.read.option("header", True).option("inferSchema", True).csv(zone_profile_path)
    aqi.createOrReplaceTempView("aqi_stream")
    zone_profile.createOrReplaceTempView("zone_profile")

    # A 10-minute window sliding every minute is a true rolling average rather
    # than a 10-minute tumbling average. The join is streaming-to-static.
    advisories = spark.sql(
        """
        WITH rolling_aqi AS (
          SELECT
            zone,
            window(event_time, '10 minutes', '1 minute') AS advisory_window,
            AVG(aqi) AS rolling_avg_aqi,
            COUNT(*) AS readings
          FROM aqi_stream
          GROUP BY zone, window(event_time, '10 minutes', '1 minute')
        )
        SELECT
          r.zone,
          z.zone_name,
          z.population,
          z.number_of_schools,
          r.advisory_window.start AS window_start,
          r.advisory_window.end AS window_end,
          r.rolling_avg_aqi,
          r.readings,
          'UNHEALTHY' AS advisory_level
        FROM rolling_aqi r
        INNER JOIN zone_profile z ON r.zone = z.zone
        WHERE r.rolling_avg_aqi > 150
        """
    )
    kafka_rows = advisories.select(
        col("zone").cast("string").alias("key"),
        to_json(struct(*[col(name) for name in advisories.columns])).alias("value"),
    )
    query = (
        kafka_rows.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("topic", "urbanpulse.health_advisories")
        .option("checkpointLocation", f"{checkpoint_root}/aqi-health-advisories")
        .outputMode("update")
        .queryName("aqi_health_advisories")
        .start()
    )
    try:
        query.awaitTermination()
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

