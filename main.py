"""
This script launches the analysis of the MyCompany dataset using PySpark with AWS EMR Serverless service.
Please look at README.md for usage instructions.
"""

import logging

import yaml

from src.aws_emr import EMRServerless

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":

    logging.info('Loading configuration file')
    with open("conf.yaml", "r") as file:
        conf = yaml.load(file, Loader=yaml.FullLoader)['emr']

    logging.info("Creating and starting EMR Serverless Spark App")
    emr = EMRServerless()
    emr.create_application(**conf['app'])
    emr.start_application()
    logging.info(emr)

    logging.info("Submitting new Spark job")
    job_run_id = emr.run_spark_job(**conf['spark_job'])
    job_status = emr.get_job_run(job_run_id=job_run_id)
    logging.info(f"Job is finished: {job_run_id}, status is: {job_status.get('state')}")

    logging.info("Stopping and deleting EMR Serverless Spark App")
    emr.stop_application()
    emr.delete_application()

    logging.info("Fetching and printing the Spark logs")
    print(emr.fetch_driver_log(conf['spark_job']['s3_bucket_name'], job_run_id))

    logging.info("Done!")
