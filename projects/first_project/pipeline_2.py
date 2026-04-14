# download some data from internet and do the setup - bring that data inside docker so that 
# spark can read
# print the schema of the data
# find me the avg of any numberic field
# write the results in s3 in parquet format

# spark-submit --properties-file properties/spark.properties pipeline_2.py

from pyspark.sql.functions import avg

from utils.session_creator import get_spark_session
from utils.logger import get_logger
from utils.reader import read_csv
from utils.writer import write_to_csv, write_to_parquet

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

logger = get_logger('Pipeline 2')

spark = get_spark_session()

input_path = spark.conf.get("spark.app.input_path")

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

df = spark.read \
        .schema(schema) \
        .option("delimiter", "::") \
        .csv(input_path)

df.printSchema()
df.show(10,False)

df.select(avg("rating")).show()

 # 3. Write Data using the utility
#output_path = "s3a://sampra/output/first/test_data.csv"
output_path = spark.conf.get("spark.app.output_path")

#write_to_csv(df, output_path)
write_to_parquet(df, output_path)

df.select()