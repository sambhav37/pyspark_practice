from utils.session_creator import get_spark_session
from utils.writer import write_to_csv
from utils.logger import get_logger

# spark-submit --properties-file properties/spark.properties pipeline_1.py

# Initialize logger for main
logger = get_logger("MainPipeline")

def run_pipeline():

    logger.info("Pipeline Execution Started")

    # 1. Initialize Session
    spark = get_spark_session("MyProductionApp")

    # 2. Create or Read Data
    data = [(1, "foo"), (2, "bar")]
    columns = ["id", "val"]
    df = spark.createDataFrame(data, columns)

    output_path = spark.conf.get("spark.app.output_path")
    batch_id = spark.conf.get("spark.app.batch_id", "N/A")

    logger.info(f"Processing Batch ID: {batch_id}")

    # 3. Write Data using the utility
    # output_path = "s3a://sampra/output/first/test_data.csv"
    write_to_csv(df, output_path)
    
    logger.info("Pipeline Execution Finished Successfully")

if __name__ == "__main__":
    run_pipeline()