# Pyspark-EMR-Serverless

This project is a basic demo of how to run big data analytic using **PySpark**
running on the **AWS ElasticMapReduce (EMR) Serverless** service.

## Prerequisites
  
### Setup a Python virtual environment

**Prerequisites** : 
- `python==3.9` and `pip` are installed on your computer.
- `git` is installed on your computer.

**Steps**

* clone the content of this Git repository:

```
> git clone git@gitlab.com:data-terrae/projects/captchacracker.git
```

- open a terminal, and **move to the project's root folder**:

```sh
> cd my\path\to\pyspark-emr-serverless
```

- create and activate a virtual environment for the project:

```sh
my\path\to\pyspark-emr-serverless> python -m venv myenv
my\path\to\pyspark-emr-serverless> myenv\Scripts\activate
(myenv) my\path\to\pyspark-emr-serverless>
```

- install Python libraries that are required to run the project:

```sh
(myenv) my\path\to\pyspark-emr-serverless> pip install -r requirements.txt
```

### Setup an AWS account

This projects uses **AWS cloud services** to store data and run analytics. 
In order to run the project, you need to have an active **AWS account**.
See [here](https://aws.amazon.com/fr/premiumsupport/knowledge-center/create-and-activate-aws-account/) to create one if
needed.

### Grant permissions to use EMR Serverless

To use EMR Serverless, you need an AWS IAM user with an attached policy that grants permissions for EMR Serverless. 
To create a user and attach the appropriate policy to that user, follow the instructions in [Create a user and grant permissions](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/setting-up.html#setting-up-iam).

### Prepare storage on AWS S3

The project uses **AWS S3** to store input data as well as analytics outputs and logs.
Before running the project, you must:

- Upload **input data** to AWS S3
    - create an AWS S3 bucket with custom name (e.g., `my-input-bucket`)
    - manually upload the input dataset to this S3 bucket


- Create a **second AWS S3 bucket** (e.g., `my-output-bucket`), that will later be used to:
    - upload Python files from your computer
    - store PySpark logs
    - store analytics output data

To create a bucket, follow the instructions in [*Creating a bucket*](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/create-bucket.html) in the Amazon Simple Storage Service Console User Guide.
Once you have created the two AWS S3 buckets, you can edit the project **configuration file** `conf.yaml` with the appropriate paths:

    - `spark_job.arguments`: full S3 URIs to analytics input/output data folders
    - `spark_job.s3_bucket_name`: name of the second S3 bucket (for logging and outputs)


### AWS credentials

The project uses the **AWS ElasticMapReduce (EMR) Serverless** service to parallelize data processing with **PySpark**.
In order to run the project, you need to have:

- an **AWS IAM user** with appropriate permissions:
    - full access the AWS EMR service API
    - read/write access to the AWS S3 buckets used in the project
  
- an **AWS credentials file** stored on your computer (`~/.aws/credentials`), that contains the user **access keys**,
  for example:

> *~/.aws/credentials*
> ```
> [default]
> aws_access_key_id=AKIAIOSFODNN7EXAMPLE
> aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
> ```

The `boto3` Python library will read these AWS credentials to access your AWS resources.
See [`boto3 documentation`](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials)
for further guidance on how to manage AWS credentials.

### Create a job runtime role

Job runs in AWS EMR Serverless use a runtime role that provides granular permissions to specific AWS services and resources at runtime. 
In this tutorial, a first S3 bucket `my-input-bucket` hosts the input data.
The bucket `my-output-bucket` stores the job Python files, logs and output data.

To set up a job runtime role, first create a runtime role with a trust policy so that EMR Serverless can use the new role. 
Next, attach the required S3 access policy to that role.
The following steps guide you through the process:

>
> 1. Navigate to the IAM console at https://console.aws.amazon.com/iam/
> 2. In the left navigation pane, choose Roles.
> 3. Choose Create role.
> 4. For role type, choose Custom trust policy and paste the following trust policy. This allows jobs submitted to your Amazon EMR Serverless applications to access other AWS services on your behalf.
>
> ```
> {
>   "Version": "2012-10-17",
>   "Statement": [
>     {
>       "Effect": "Allow",
>       "Principal": {
>         "Service": "emr-serverless.amazonaws.com"
>       },
>       "Action": "sts:AssumeRole"
>     }
>   ]
> } 
> ```
> 
> 5. Choose Next to navigate to the Add permissions page, then choose Create policy.
> 6. The Create policy page opens on a new tab. Paste the policy JSON below.
> 
> 
> ```
>     {
>         "Version": "2012-10-17",
>         "Statement": [
>             {
>                 "Sid": "ReadAccessForEMRSamples",
>                 "Effect": "Allow",
>                 "Action": [
>                     "s3:GetObject",
>                     "s3:ListBucket"
>                 ],
>                 "Resource": [
>                     "arn:aws:s3:::*.elasticmapreduce",
>                     "arn:aws:s3:::*.elasticmapreduce/*"
>                 ]
>             },
>             {
>                 "Sid": "FullAccessToOutputBucket",
>                 "Effect": "Allow",
>                 "Action": [
>                     "s3:PutObject",
>                     "s3:GetObject",
>                     "s3:ListBucket",
>                     "s3:DeleteObject"
>                 ],
>                 "Resource": [
>                     "arn:aws:s3:::my-input-bucket",
>                     "arn:aws:s3:::my-input-bucket/*",
>                     "arn:aws:s3:::my-output-bucket",
>                     "arn:aws:s3:::my-output-bucket/*",
>                 ]
>             },
>             {
>                 "Sid": "GlueCreateAndReadDataCatalog",
>                 "Effect": "Allow",
>                 "Action": [
>                     "glue:GetDatabase",
>                     "glue:CreateDatabase",
>                     "glue:GetDataBases",
>                     "glue:CreateTable",
>                     "glue:GetTable",
>                     "glue:UpdateTable",
>                     "glue:DeleteTable",
>                     "glue:GetTables",
>                     "glue:GetPartition",
>                     "glue:GetPartitions",
>                     "glue:CreatePartition",
>                     "glue:BatchCreatePartition",
>                     "glue:GetUserDefinedFunctions"
>                ],
>                "Resource": ["*"]
>            }
>        ]
>    }
> ```
>
> 7. On the Review policy page, enter a name for your policy, such as EMRServerlessS3AndGlueAccessPolicy.
> 8. Refresh the Attach permissions policy page, and choose EMRServerlessS3AndGlueAccessPolicy.
> 9. In the Name, review, and create page, for Role name, enter a name for your role, for example, EMRServerlessS3RuntimeRole. To create this IAM role, choose Create role.



## Usage

To run analytics, just run:

```bash
my\path\to\pyspark-emr-serverless> python main.py
```

The `main.py` script will perform the following tasks:
- create an AWS EMR Serverless Application (EMR app')
- upload the Spark entrypoint `spark_job.py` from your computer to AWS S3 bucket (`my-output-bucket`)
- upload required Python files (listed in configuration file) from your computer to AWS S3 bucket
- submit PySpark job on the EMR app'
  - PySpark job reads input data from AWS S3 bucket `my-input-bucket`
  - PySpark job writes logs to AWS S3 bucket `my-output-bucket`
  - PySpark job writes output data to AWS S3 bucket `my-output-bucket`
- wait for PySpark job to finish  
- fetch PySpark logs from AWS S3 and display them on the console
- close and delete the EMR app'

When the job is completed, the analytics output data is available in the output S3 bucket `my-output-bucket`.

You can control Spark job configuration by editing the `conf.yaml` file. 