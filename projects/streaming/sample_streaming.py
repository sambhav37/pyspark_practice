import os
import configparser

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType


def get_spark_session(app_name: str) -> SparkSession:
    # Load properties from file
    config = configparser.ConfigParser()
    config.read('properties/spark.properties')
    
    spark = SparkSession.builder.appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    
    # Set all properties from the file
    for section in config.sections():
        for key, value in config.items(section):
            spark = spark.config(key, value)
    
    return spark.getOrCreate()


def main():
    # Load config
    config = configparser.ConfigParser()
    config.read('properties/spark.properties')
    
    bootstrap_servers = config.get('DEFAULT', 'spark.app.kafka_bootstrap_servers', fallback='kafka:9092')
    topic = config.get('DEFAULT', 'spark.app.kafka_topic', fallback='wikipedia-edits')
    checkpoint_dir = config.get('DEFAULT', 'spark.app.checkpoint_dir', fallback='checkpoints/wikipedia_edits_checkpoint')
    output_path = config.get('DEFAULT', 'spark.app.output_path', fallback='s3a://sampra/output/streaming/page_counts')

    spark = get_spark_session("WikipediaEditStream")

    schema = StructType([
        StructField("page_title", StringType(), True),
        StructField("user", StringType(), True),
        StructField("edit_size", IntegerType(), True),
        StructField("event_time", StringType(), True),
        StructField("edit_id", StringType(), True),
    ])

    raw_kafka = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .load()
    )

    parsed = (
        raw_kafka
            .selectExpr("CAST(value AS STRING) AS json_str")
            .select(from_json(col("json_str"), schema).alias("data"))
            .select(
                col("data.page_title").alias("page_title"),
                col("data.user").alias("user"),
                col("data.edit_size").alias("edit_size"),
                to_timestamp(col("data.event_time")).alias("event_time"),
            )
            .where(col("event_time").isNotNull())
    )

    page_counts = (
        parsed
            .withWatermark("event_time", "2 minutes")
            .groupBy(
                window("event_time", "1 minute"),
                "page_title"
            )
            .count()
    )

    # Write to S3 (Minio) in Parquet format with append mode
    query = (
        page_counts
            .writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_dir)
            .start()
    )

    print("Streaming query started.")
    print(f"Listening to Kafka topic '{topic}' on {bootstrap_servers}")
    print(f"Writing to: {output_path}")
    print(f"Checkpoint directory: {checkpoint_dir}")
    print("Press Ctrl+C to stop.")

    query.awaitTermination()


def write_to_postgres(df, epoch_id, url, user, password, table):
    df.write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .save()


if __name__ == "__main__":
    main()
