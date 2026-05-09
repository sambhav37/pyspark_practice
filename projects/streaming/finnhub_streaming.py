#!/usr/bin/env python3
"""
Finnhub Stock Data Streaming Processor

Consumes real-time stock trades from Kafka and processes them with PySpark Structured Streaming.
Performs windowed aggregations and writes to Minio S3 in Parquet format.
"""

import os
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, sum, count, avg,
    min, max, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, LongType, TimestampType, ArrayType
)

def get_spark_session(app_name: str) -> SparkSession:
    """Create Spark session with configuration loaded from properties file"""
    config = configparser.ConfigParser()
    config.read('properties/spark.properties')

    spark = SparkSession.builder.master('local[2]').appName(app_name)
    spark = spark.config('spark.sql.shuffle.partitions', '2')
    spark = spark.config('spark.default.parallelism', '2') \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")

    # Set all properties from the file
    for section in config.sections():
        for key, value in config.items(section):
            spark = spark.config(key, value)

    return spark.getOrCreate()

def main():
    """Main streaming application"""
    # Load configuration
    config = configparser.ConfigParser()
    config.read('properties/spark.properties')

    bootstrap_servers = config.get('DEFAULT', 'spark.app.kafka_bootstrap_servers', fallback='kafka:9092')
    topic = config.get('DEFAULT', 'spark.app.finnhub_kafka_topic', fallback='finnhub-stocks')
    checkpoint_dir = config.get('DEFAULT', 'spark.app.finnhub_checkpoint_dir', fallback='checkpoints/finnhub_stocks_checkpoint')
    output_path = config.get('DEFAULT', 'spark.app.finnhub_output_path', fallback='s3a://sampra/output/streaming/finnhub_stock_aggregates')

    spark = get_spark_session("FinnhubStockStream")

    # Define schema for Finnhub trade events
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("volume", IntegerType(), True),
        StructField("timestamp", LongType(), True),
        StructField("event_time", StringType(), True),  # ISO format
        StructField("conditions", ArrayType(StringType()), True),
        StructField("trade_id", LongType(), True),
        StructField("exchange", StringType(), True),
        StructField("trade_type", StringType(), True),
    ])

    # Read from Kafka
    raw_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON value
    parsed_df = raw_kafka.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Convert event_time string to timestamp
    processed_df = parsed_df.withColumn(
        "event_time",
        to_timestamp(col("event_time"))
    )

    # Add watermark for late data handling
    watermarked_df = processed_df.withWatermark("event_time", "3 minutes")

    # Windowed aggregation: aggregate trades per symbol per 5-minute window
    windowed_aggregates = (
        watermarked_df
        .groupBy(
            window(col("event_time"), "2 minutes"),
            col("symbol")
        )
        .agg(
            count("*").alias("trade_count"),
            sum(col("volume")).alias("total_volume"),
            avg(col("price")).alias("avg_price"),
            min(col("price")).alias("min_price"),
            max(col("price")).alias("max_price"),
            sum(expr("price * volume")).alias("total_value")  # Approximate dollar volume
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("trade_count"),
            col("total_volume"),
            col("avg_price"),
            col("min_price"),
            col("max_price"),
            col("total_value")
        )
    )

    # Write to Parquet on S3
    parquet_query = (
        windowed_aggregates
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_dir)
        .partitionBy("window_start")  # Partition by window start time
        .trigger(processingTime="1 minute")
        .start()
    )

    print("=== Finnhub Stock Streaming Started ===")
    print(f"Kafka servers: {bootstrap_servers}")
    print(f"Topic: {topic}")
    print(f"Output path: {output_path}")
    print(f"Checkpoint: {checkpoint_dir}")
    print("\nPress Ctrl+C to stop...")

    try:
        parquet_query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming query...")
        parquet_query.stop()
        print("Streaming stopped.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()