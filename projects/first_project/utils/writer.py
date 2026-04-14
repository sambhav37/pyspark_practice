from utils.logger import get_logger

logger = get_logger(__name__)
    
def write_to_csv(df, path, mode="overwrite", num_files=1):
    try:
        logger.info(f"Starting write operation to: {path}")
        df.coalesce(num_files) \
          .write \
          .mode(mode) \
          .option("header", "true") \
          .csv(path)
          
        logger.info("Successfully completed the write operation.")
    except Exception as e:
        logger.error(f"Failed to write data to {path}. Error: {str(e)}")
        raise
    

def write_to_parquet(df, path, mode="overwrite"):
    """Writes a dataframe to S3A/MinIO as CSV."""
    df.write \
      .mode(mode) \
      .parquet(path)