if __name__ == "__main__":
    """
    PySpark job to process data in a parallelized way.
    
    This job is designed to run only in the cloud with the AWS EMR Serverless service, not on your local computer.

    Command line arguments:
        filename (str): _
        input_path (str): S3 URI to folder with input JSON files, e.g. "s3://my-input-bucket/sample-dataset"
        output_path (str): S3 URI to folder where analytics outputs will be written, e.g. "s3://my-output-bucket/output"
    """

    import sys

    # noinspection PyUnresolvedReferences
    import pyspark

    from processing import process_data

    _, input_path, output_path = sys.argv

    print(f"Start Spark session (Spark version: {pyspark.__version__})")
    spark = pyspark.sql.SparkSession.builder.appName("DemoPySpark").getOrCreate()

    print('Read and process data')
    df_in = spark.read.json(f"{input_path}/*.json")
    df_out = process_data(df=df_in)

    print('Write output files to S3')
    cols = ['year', 'date']
    df_out.repartition(*cols).write.partitionBy(*cols).mode('overwrite').parquet(f"{output_path}/measurements_data")

    print("Spark job completed successfully. Refer output at S3 path: " + output_path)

    spark.stop()
