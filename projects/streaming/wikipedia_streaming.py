import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType


def get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name) \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()


def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "wikipedia-edits")
    checkpoint_dir = os.getenv("CHECKPOINT_DIR", "checkpoints/wikipedia_edits_checkpoint")

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
            .groupBy("page_title")
            .count()
    )

    # For complete mode, we can sort and limit
    top_pages = page_counts.orderBy(col("count").desc()).limit(5)

    query = (
        top_pages
            .writeStream
            .outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .option("checkpointLocation", checkpoint_dir)
            .start()
    )

    print("Streaming query started.")
    print(f"Listening to Kafka topic '{topic}' on {bootstrap_servers}")
    print(f"Checkpoint directory: {checkpoint_dir}")
    print("Press Ctrl+C to stop.")

    query.awaitTermination()


if __name__ == "__main__":
    main()
