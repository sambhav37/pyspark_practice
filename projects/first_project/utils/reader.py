def read_csv(spark, path, header=True, infer_schema=True, delimiter=','):
    return spark.read \
        .option("header", header) \
        .option("inferSchema", infer_schema) \
        .option("delimiter", delimiter) \
        .csv(path)