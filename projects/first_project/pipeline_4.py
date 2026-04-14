# spark-submit --properties-file properties/spark.properties pipeline_4.py

from pyspark.sql import Window

from utils.session_creator import get_spark_session
from utils.logger import get_logger

from pyspark.sql.functions import avg, count, col, desc
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, StringType, DateType

logger = get_logger('Pipeline 4')

spark = get_spark_session()

input_path_2 = spark.conf.get("spark.app.input_path_2")

movie_schema = StructType([
    StructField("movie_id", IntegerType(), True),
    StructField("movie_title", StringType(), True),
    StructField("date", DateType(), True)
])

movies_df = spark.read \
    .schema(movie_schema) \
    .option("delimiter", "|") \
    .option("dateFormat", "d-MMM-yyyy") \
    .csv(input_path_2)

movies_df.printSchema()
movies_df.show(20,False)

movies_df.write.partitionBy("date").mode("overwrite").parquet("s3a://sampra/output/partitioned_data")

w = Window.PartitionBy("").orderBy(" ")

#df.over.(rank(w))

logger.info("Partitioning started")

# windowSpec = Window.partitionBy("date")

logger.info("Partitioning ended")

