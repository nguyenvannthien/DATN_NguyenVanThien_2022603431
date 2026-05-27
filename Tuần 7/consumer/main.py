import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


def write_to_postgres(batch_df, batch_id):
    postgres_url = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/weather_db")
    postgres_user = os.getenv("POSTGRES_USER", "weather_user")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "weather_password")

    output_df = batch_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temperature"),
        col("avg_feels_like"),
        col("avg_humidity"),
        col("avg_precipitation"),
        col("avg_wind_speed"),
        col("avg_pressure")
    )

    output_df.write \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", "weather_stats") \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"Batch {batch_id} written to PostgreSQL")


def main():
    # 1. Initialize Spark Session
    spark = SparkSession.builder \
        .appName("WeatherStreamingConsumer") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.7.3"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Schema for JSON data from Kafka
    schema = StructType([
        StructField("time", StringType()),
        StructField("temperature", DoubleType()),
        StructField("apparent_temperature", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("precipitation", DoubleType()),
        StructField("wind_speed", DoubleType()),
        StructField("pressure", DoubleType()),
        StructField("timestamp", DoubleType())
    ])

    # 3. Read from Kafka
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("WEATHER_TOPIC", "weather_data")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .option("group.id", "weather-consumer-group") \
        .option("kafka.group.id", "weather-consumer-group") \
        .option("startingOffsets", "earliest") \
        .load()

    # 4. Parse JSON data from Kafka value
    weather_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", col("timestamp").cast(TimestampType()))

    # 5. Window aggregation
    weather_stats_df = weather_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(window(col("event_time"), "5 minutes")) \
        .agg(
            avg("temperature").alias("avg_temperature"),
            avg("apparent_temperature").alias("avg_feels_like"),
            avg("humidity").alias("avg_humidity"),
            avg("precipitation").alias("avg_precipitation"),
            avg("wind_speed").alias("avg_wind_speed"),
            avg("pressure").alias("avg_pressure")
        )

    # 6. Output to Console 
    console_query = weather_stats_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", "/tmp/checkpoints/weather_stats_console") \
        .trigger(processingTime="10 seconds") \
        .start()

    # 7. Output to PostgreSQL
    postgres_query = weather_stats_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "/tmp/checkpoints/weather_stats_postgres") \
        .trigger(processingTime="10 seconds") \
        .start()

    console_query.awaitTermination()
    postgres_query.awaitTermination()


if __name__ == "__main__":
    main()