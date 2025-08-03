from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, BooleanType, StringType
from datetime import datetime

def write_logs_raw(log_list : List[str]) -> None:
    """
    Writes a list of raw ingestion log entries to the Hive table `dwh.ops_raw_load`.

    Each entry in the log list is expected to be a tuple containing:
        - timestamp string in the format "%Y%m%d_%H%M%S"
        - success flag (True or False)
        - source file path (string)
        - destination file path (string)

    Args:
        log_list (List[str]): A list of tuples representing log records.

    Raises:
        ValueError: If any timestamp string in the log list cannot be parsed.

    Returns:
        None
    """
    spark = SparkSession.builder \
    .appName("write_logs_raw") \
    .enableHiveSupport() \
    .getOrCreate()

    converted_rows = []
    for ts_str, is_success, file_source, file_destination in log_list:
        try:
            ts = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
        except ValueError as e:
            raise ValueError(f"Invalid timestamp format for '{ts_str}': {e}")

        converted_rows.append((ts, bool(is_success), file_source, file_destination))


    schema = StructType([
    StructField("ts", TimestampType(), True),
    StructField("is_success", BooleanType(), True),
    StructField("file_source", StringType(), True),
    StructField("file_destination", StringType(), True)
    ])

    df = spark.createDataFrame(converted_rows, schema=schema)

    df.write.mode("append").saveAsTable("dwh.ops_raw_load")


def write_logs_stage(is_success : bool, param_str : str, error_message: str) -> None:
    """
    Logs the outcome of a staging process to the Hive table `dwh.ops_stage_load`.

    Used to capture metadata around transformation stage runs, including:
        - timestamp of the run
        - success flag (True or False)
        - parameter string identifying the job context
        - error message, if any

    Args:
        is_success (bool): Indicates whether the job succeeded.
        param_str (str): String describing input parameters or job context.
        error_message (str): Error message encountered, or an empty string if successful.

    Returns:
        None
    """
    spark = SparkSession.builder \
    .appName("write_logs_stage") \
    .enableHiveSupport() \
    .getOrCreate()

    row_data = [(datetime.now(), is_success, param_str, error_message)]
    df = spark.createDataFrame(row_data, ["ts", "is_success", "param_str", "error_message"])

    df.write.mode("append").saveAsTable("dwh.ops_stage_load")
















