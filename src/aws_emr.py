"""
This class is used to interact with AWS EMR Serverless service.
Source: https://github.com/aws-samples/emr-serverless-samples/blob/main/examples/python-api/emr_serverless.py
"""

import gzip
from pathlib import Path
from typing import List

import boto3


class EMRServerless:
    """
    An example implementation of running a PySpark job on EMR Serverless.
    This class provides support for creating an EMR Serverless Spark application, running a job,
    fetching driver logs, and shutting the application back down.
    By default, all calls are synchronous in that they wait for the Application to reach the desired state.
    - `create_application` waits for the application to reach the `CREATED` state.
    - `start_application` waits for the `STARTED` state.
    - `stop_application` waits for the `STOPPED state.
    - `run_spark_job` waits until the job is in a terminal state.
    """

    def __init__(self, application_id: str = None) -> None:
        self.application_id = application_id
        self.s3_log_prefix = "logs"
        self.s3_src_prefix = "src"
        self.s3_spark_entrypoint = f"{self.s3_src_prefix}/entrypoint.py"
        self.app_type = "SPARK"
        self.client = boto3.client("emr-serverless")

    def __str__(self):
        return f"EMR Serverless {self.app_type} Application: {self.application_id}"

    def create_application(self, name: str, release_label: str, wait: bool = True):
        """
        Create a new application with the provided name and release_label - the application needs to be started after.
        """
        if self.application_id is not None:
            raise Exception(f"Application already created (application_id: `{self.application_id}`)")

        response = self.client.create_application(name=name, releaseLabel=release_label, type=self.app_type)
        self.application_id = response.get("applicationId")

        app_ready = False
        while wait and not app_ready:
            response = self.client.get_application(applicationId=self.application_id)
            app_ready = response.get("application").get("state") == "CREATED"

    def start_application(self, wait: bool = True) -> None:
        """
        Start the application - by default, wait until the application is started.
        """
        if self.application_id is None:
            raise Exception("No application_id - please use creation_application first.")

        self.client.start_application(applicationId=self.application_id)

        app_started = False
        while wait and not app_started:
            response = self.client.get_application(applicationId=self.application_id)
            app_started = response.get("application").get("state") == "STARTED"

    def stop_application(self, wait: bool = True) -> None:
        """
        Stop the application - by default, wait until the application is stopped.
        """
        self.client.stop_application(applicationId=self.application_id)

        app_stopped = False
        while wait and not app_stopped:
            response = self.client.get_application(applicationId=self.application_id)
            app_stopped = response.get("application").get("state") == "STOPPED"

    def delete_application(self) -> None:
        """
        Delete the application - it must be stopped first.
        """
        self.client.delete_application(applicationId=self.application_id)

    def upload_python_files(self, entrypoint: str, s3_bucket_name: str, py_files: List[str] = None) -> List[str]:
        """
        Uploads PySpark entrypoint (Python script) and optional extra Python files to S3 bucket.
        """
        s3_client = boto3.client("s3")
        s3_client.upload_file(entrypoint, s3_bucket_name, self.s3_spark_entrypoint)
        if py_files:
            s3_py_file_keys = [f"{self.s3_src_prefix}/{Path(py_file).name}" for py_file in py_files]
            for py_file, s3_py_file_key in zip(py_files, s3_py_file_keys):
                s3_client.upload_file(py_file, s3_bucket_name, s3_py_file_key)
            s3_py_files = [f"s3://{s3_bucket_name}/{key}" for key in s3_py_file_keys]
        else:
            s3_py_files = []
        s3_client.close()
        return s3_py_files

    def run_spark_job(
            self,
            entrypoint: str,
            job_role_arn: str,
            arguments: list,
            s3_bucket_name: str,
            spark_submit_parameters: dict,
            py_files: List[str] = None,
            wait: bool = True,

    ) -> str:
        """
        Runs the Spark job identified by `script_location`. Arguments can also be provided via the `arguments` parameter
        By default, spark-submit parameters are hard-coded and logs are sent to the provided s3_bucket_name.
        This method is blocking by default until the job is complete.
        """

        # Upload Python files to S3 storage
        s3_py_files = self.upload_python_files(entrypoint=entrypoint, py_files=py_files, s3_bucket_name=s3_bucket_name)
        spark_submit_parameters['spark.submit.pyFiles'] = ",".join(s3_py_files)

        # Submit Spark job
        response = self.client.start_job_run(
            applicationId=self.application_id,
            executionRoleArn=job_role_arn,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": f"s3://{s3_bucket_name}/{self.s3_spark_entrypoint}",
                    "entryPointArguments": arguments,
                    "sparkSubmitParameters": " ".join([f"--conf {k}={v}" for k, v in spark_submit_parameters.items()])
                }
            },
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{s3_bucket_name}/{self.s3_log_prefix}/"
                    }
                }
            },
        )
        job_run_id = response.get("jobRunId")

        job_done = False
        while wait and not job_done:
            jr_response = self.get_job_run(job_run_id)
            job_done = jr_response.get("state") in [
                "SUCCESS",
                "FAILED",
                "CANCELLING",
                "CANCELLED",
            ]

        return job_run_id

    def get_job_run(self, job_run_id: str) -> dict:
        response = self.client.get_job_run(
            applicationId=self.application_id, jobRunId=job_run_id
        )
        return response.get("jobRun")

    def fetch_driver_log(
            self, s3_bucket_name: str, job_run_id: str, log_type: str = "stdout"
    ) -> str:
        """
        Access the specified `log_type` Driver log on S3 and return the full log string.
        """
        s3_client = boto3.client("s3")
        file_location = f"{self.s3_log_prefix}/" \
                        f"applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/{log_type}.gz"
        try:
            response = s3_client.get_object(Bucket=s3_bucket_name, Key=file_location)
            file_content = gzip.decompress(response["Body"].read()).decode("utf-8")
        except s3_client.exceptions.NoSuchKey:
            file_content = ""
        return str(file_content)
