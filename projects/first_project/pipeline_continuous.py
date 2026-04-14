from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
import math

math.pow
# --- INITIALIZATION ---
# Create the SparkSession (This is the entry point for Spark)
spark = SparkSession.builder \
    .appName("SocketWordCount") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# OPTIONAL: Reduce shuffle partitions from 200 to 2 for smoother local testing
spark.conf.set("spark.sql.shuffle.partitions", "2")

# --- 1. INPUT ---
# Connect to the socket on localhost:9999
# 'readStream' creates a streaming DataFrame
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# --- 2. TRANSFORMATION ---
# Split lines into words using 'explode' (turns arrays into rows)
# words = lines.select(
#    explode(
#        split(lines.value, " ")
#    ).alias("word")
# )



# --- 3. OUTPUT ---
# Start the stream and output results to the console
# query = words.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .trigger(processingTime="1 second") \
#     .start()


simple_lines = lines.select("value")
query = simple_lines.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(continuous="1 second") \
    .start()


# Keep the application running to listen for incoming data
query.awaitTermination()