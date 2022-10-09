"""
Useful data processing functions to be used by the PySpark processing job.

Note:
    These functions are designed to run only in the cloud with the AWS EMR Serverless service, not on your computer.
    Hence, there is no need to install pyspark on your local Python environment.
"""

# noinspection PyUnresolvedReferences
import pandas as pd
# noinspection PyUnresolvedReferences
import pyspark
# noinspection PyUnresolvedReferences
import pyspark.sql.functions as F
# noinspection PyUnresolvedReferences
from pyspark.sql.dataframe import DataFrame


def process_data(df: DataFrame) -> DataFrame:
    """
    Main processing function.

    Args:
        df (DataFrame): input PySpark dataframe

    Returns: (DataFrame) output PySpark dataframe
    """
    return df \
        .withColumnRenamed("startTime", "start_time") \
        .withColumn("exercise_id", F.monotonically_increasing_id()) \
        .withColumn("date", F.to_date(F.substring(F.col('start_time'), 1, 10), "yyyy-MM-dd")) \
        .withColumn("year", F.year(F.col('date'))) \
        .withColumn('exercise_type', F.regexp_replace('exercise_type', 'METER_ENDURANCE', 'METER')) \
        .filter(df.year >= 2019) \
        .to_pandas_on_spark(index_col='measurement_id') \
        .apply(some_pandas_function, axis=1) \
        .to_spark()


def some_pandas_function(row: pd.Series):
    """
    Computes some stuff with pandas on a single dataframe row.

    Args:
        row (pd.Series): a row from input dataframe

    Returns: (pd.Series) a series with the same attributes as input series + two extra attributes:
        attr0 (float): _
        attr1 (float): _

    """
    return pd.Series({**row.to_dict(), 'attr0': 0, 'attr1': True})
