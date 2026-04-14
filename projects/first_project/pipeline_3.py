# Files to use: * u.data: The ratings (UserID, ItemID, Rating, Timestamp).
#u.item: The movie metadata (MovieID, Title, Release Date, etc.).


# Task 1: Data Ingestion (Schema Building)
# Step A: Read u.data (tab-separated) using the StructType schema we created earlier.

# Step B: Read u.item. Note: This file uses a different separator (|) and contains many columns. You only need movie_id and movie_title.


# Task 2: Data Transformation
# Aggregation: Calculate the average rating and the count of ratings for every movie_id.

# Filtering: Keep only movies that have more than 100 total ratings (to avoid "one-hit wonders" with a single 5-star rating).

# Task 3: The Join
# Operation: Perform an inner join between your Aggregated Ratings and the Movie Metadata.

# Goal: Replace the numeric movie_id with the human-readable movie_title.

# Task 4: Output
# Sort the final list by Average Rating in descending order.

# Show the top 10 results.


# spark-submit --properties-file properties/spark.properties pipeline_3.py

from utils.session_creator import get_spark_session
from utils.logger import get_logger
from utils.reader import read_csv
from utils.writer import write_to_csv, write_to_parquet

from pyspark.sql.functions import avg, count, col, desc
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, StringType

logger = get_logger('Pipeline 3')

spark = get_spark_session()

input_path_1 = spark.conf.get("spark.app.input_path_1")
input_path_2 = spark.conf.get("spark.app.input_path_2")

# Schema for u.data(UserID, ItemID, Rating, Timestamp)
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

rating_df = spark.read \
        .schema(schema) \
        .option("delimiter", '\t') \
        .csv(input_path_1)


# Schema for u.item 
movie_schema = StructType([
    StructField("movie_id", IntegerType(), True),
    StructField("movie_title", StringType(), True)
])

movies_df = spark.read \
    .schema(movie_schema) \
    .option("delimiter", "|") \
    .csv(input_path_2)



ratings_movie_df = rating_df.groupBy("item_id").agg(count(col("rating")).alias("cnt"), avg(col("rating")).alias("avg")).filter(col("cnt")>100)

rating_df.printSchema()
rating_df.show(10,False)


movies_df.printSchema()
movies_df.show(20,False)



# avg_rating_by_id = df.groupBy("item_id").avg("rating")
#avg_rating_by_id.show(avg_rating_by_id.count())
rating_df.select(avg("rating")).show()

rating_df.groupBy("item_id").count().show()

ratings_movie_df.show()

join_df = ratings_movie_df.join(movies_df, ratings_movie_df['item_id']==movies_df['movie_id'], how="inner").sort(desc("avg"))

join_df.show()
