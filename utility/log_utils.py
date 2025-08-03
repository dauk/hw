from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, BooleanType, StringType
from datetime import datetime

def write_logs_raw(log_list : List[str]) -> None:
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
    spark = SparkSession.builder \
    .appName("write_logs_stage") \
    .enableHiveSupport() \
    .getOrCreate()

    row_data = [(datetime.now(), is_success, param_str, error_message)]
    df = spark.createDataFrame(row_data, ["ts", "is_success", "param_str", "error_message"])

    df.write.mode("append").saveAsTable("dwh.ops_stage_load")
















