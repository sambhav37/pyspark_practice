#!/usr/bin/env python3
"""
Wikipedia Recent Changes Streaming Processor

Consumes real Wikimedia edit events from Kafka and processes them with PySpark Structured Streaming.
Performs windowed aggregations and writes to Minio S3 in Parquet format.
"""

import os
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, abs,
    when, lit, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, LongType, TimestampType
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
    topic = config.get('DEFAULT', 'spark.app.wikimedia_kafka_topic', fallback='wikimedia-edits')
    checkpoint_dir = config.get('DEFAULT', 'spark.app.wikimedia_checkpoint_dir', fallback='checkpoints/wikimedia_edits_checkpoint')
    output_path = config.get('DEFAULT', 'spark.app.wikimedia_output_path', fallback='s3a://sampra/output/streaming/wikimedia_page_counts')

    spark = get_spark_session("WikimediaEditStream")

    # Define schema for Wikimedia events
    # Based on https://stream.wikimedia.org/v2/stream/recentchange
    schema = StructType([
        StructField("event_id", LongType(), True),
        StructField("event_type", StringType(), True),
        StructField("page_title", StringType(), True),
        StructField("user", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("event_time", StringType(), True),  # ISO format
        StructField("namespace", IntegerType(), True),
        StructField("comment", StringType(), True),
        StructField("bot", BooleanType(), True),
        StructField("minor", BooleanType(), True),
        StructField("wiki", StringType(), True),
        StructField("server_name", StringType(), True),
        StructField("length_old", IntegerType(), True),
        StructField("length_new", IntegerType(), True),
        StructField("edit_size", IntegerType(), True),
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

    # Filter for main namespace (articles) and non-bot edits
    filtered_df = processed_df.filter(
        (col("namespace") == 0) &  # Main namespace
        (col("bot") == False) &    # Exclude bots
        (col("event_type") == "edit")  # Only edits
    )

    # Add watermark for late data handling
    watermarked_df = filtered_df.withWatermark("event_time", "10 minutes")

    # Windowed aggregation: count edits per page per 5-minute window
    windowed_counts = (
        watermarked_df
        .groupBy(
            window(col("event_time"), "2 minutes"),
            col("page_title")
        )
        .agg(
            count("*").alias("edit_count"),
            count(when(col("minor") == True, 1)).alias("minor_edits"),
            count(when(col("minor") == False, 1)).alias("major_edits")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("page_title"),
            col("edit_count"),
            col("minor_edits"),
            col("major_edits")
        )
    )

    # Write to Parquet on S3
    parquet_query = (
        windowed_counts
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_dir)
        .partitionBy("window_start")  # Partition by window start time
        .trigger(processingTime="1 minute")
        .start()
    )

    print("=== Wikimedia Edit Streaming Started ===")
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