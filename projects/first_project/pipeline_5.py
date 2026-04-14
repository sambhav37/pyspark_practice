# download some data from internet and do the setup - bring that data inside docker so that 
# spark can read
# print the schema of the data
# find me the avg of any numberic field
# write the results in s3 in parquet format

# spark-submit --properties-file properties/spark.properties pipeline_5.py

from utils.session_creator import get_spark_session
from utils.logger import get_logger
from utils.reader import read_csv
from utils.writer import write_to_csv, write_to_parquet

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

logger = get_logger('Pipeline 5')

spark = get_spark_session()

input_path_3 = spark.conf.get("spark.app.input_path_3")
input_path_4 = spark.conf.get("spark.app.input_path_4")
input_path_5 = spark.conf.get("spark.app.input_path_5")
input_path_6 = spark.conf.get("spark.app.input_path_6")
input_path_7 = spark.conf.get("spark.app.input_path_7")

# schema = StructType([
#     StructField("user_id", IntegerType(), True),
#     StructField("item_id", IntegerType(), True),
#     StructField("rating", DoubleType(), True),
#     StructField("timestamp", LongType(), True)
# ])

df = spark.read.json(input_path_3)



df.printSchema()
df.show(10,False)

