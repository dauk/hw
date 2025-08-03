
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, substring_index, to_date, regexp_replace, trim
from delta.tables import DeltaTable
import traceback

import sys
sys.path.append("/Workspace/Users/dauuuk@gmail.com/hw/utility")
from utils import get_access_secret_keys
from date_utils import get_date_glob
from log_utils import write_logs_stage


ACCESS_KEY, SECRET_KEY = get_access_secret_keys()

spark = SparkSession.builder \
    .appName("prcess_stage") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.logStore.crossCloud.fatal", "false") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", ACCESS_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", SECRET_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")


def get_raw_data_df(bucket,source_path,file_type,days_to_load):
    """
    Reads raw data files from S3 using Spark based on the file type and date glob pattern.

    Args:
        bucket (str): S3 bucket name.
        source_path (str): Path within the bucket to read from.
        file_type (str): Type of files to read (e.g., csv, json, text).
        days_to_load (int): Number of past days to include in the glob pattern.

    Returns:
        DataFrame: Raw Spark DataFrame loaded from S3.
    """
    date_glob = get_date_glob(days_to_load)
    
    supported_readers = {"csv", "json", "parquet", "orc", "text"}
    if file_type not in supported_readers:
        raise ValueError(f"Unsupported file type: {file_type}")

    full_path = f"s3a://{bucket}/{source_path}/{date_glob}/*.{"txt" if file_type == "text" else file_type}"
    #TODO: currently only tested with text, working with other formats will require additional development
    read_func = getattr(spark.read, file_type)
    raw_df = read_func(full_path)
    return raw_df



def get_clean_data_df(raw_df, **kwargs):
    """
    Cleans the raw data based on optional filtering rules.

    Args:
        raw_df (DataFrame): Raw Spark DataFrame.
        **kwargs: Optional filters such as 'starts_with' or 'regex_clear' as expressions.

    Returns:
        DataFrame: Cleaned DataFrame with a single 'value' column.
    """
    filtered_df = raw_df
    if "starts_with" in kwargs:
        filtered_df = filtered_df.filter(col("value").startswith(kwargs["starts_with"]))
    if "regex_clear" in kwargs:
        filtered_df = filtered_df.withColumn(
        "clean_value",
        eval(kwargs["regex_clear"])
        )
    return filtered_df.select("clean_value").withColumnRenamed("clean_value", "value")

def add_date(df):
    """
    Adds a 'date' column by extracting it from the input file path.

    Args:
        df (DataFrame): DataFrame with at least a 'value' column.

    Returns:
        DataFrame: DataFrame with 'value' and 'date' columns.
    """
    df_with_date = df.withColumn("file_path", input_file_name())
    last_two_segments = substring_index(col("file_path"), "/", -2)
    ymd_string = substring_index(last_two_segments, "/", 1)
    df_with_date = df_with_date.withColumn("date", to_date(ymd_string, "yyyy-MM-dd"))
    return df_with_date.select("value", "date")

def write_to_silver(df, bucket, sink_path):
    """
    Writes a DataFrame to Delta format in the specified S3 location, partitioned by date.

    Args:
        df (DataFrame): Spark DataFrame to write.
        bucket (str): S3 bucket name.
        sink_path (str): Path inside the bucket where data should be written.

    Returns:
        None
    """
    table_path = f"s3a://{bucket}/{sink_path}/"

    df.write.format("delta") \
        .partitionBy("date") \
        .mode("overwrite") \
        .save(table_path)

    print(f"wrote to {bucket}/{sink_path}")

def process_data(source_bucket_name,source_prefix,sink_bucket_name,sink_prefix,data_type,days_to_load,**kwargs):
    """
    Main orchestration function for the ETL pipeline. Reads raw data, cleans it, adds date, and writes to silver layer.

    Args:
        source_bucket_name (str): S3 bucket name for the source data.
        source_prefix (str): Prefix (path) in the source bucket.
        sink_bucket_name (str): S3 bucket name for the output (silver layer).
        sink_prefix (str): Prefix in the sink bucket.
        data_type (str): File type of the source data.
        days_to_load (int): Number of days worth of data to load.
        **kwargs: Optional cleaning arguments passed to `get_clean_data_df`.

    Returns:
        None
    """
    print("=== process_data parameters ===")
    print(f"source_bucket_name: {source_bucket_name}")
    print(f"source_prefix: {source_prefix}")
    print(f"sink_bucket_name: {sink_bucket_name}")
    print(f"sink_prefix: {sink_prefix}")
    print(f"data_type: {data_type}")
    print(f"days_to_load: {days_to_load}")
    print(f"kwargs: {kwargs}")
    print("================================")

    parma_str = f"source_bucket_name: {source_bucket_name} source_prefix: {source_prefix} sink_bucket_name: {sink_bucket_name} sink_prefix: {sink_prefix} data_type: {data_type} days_to_load: {days_to_load} kwargs: {kwargs}" 
    
    try:
        raw_df = get_raw_data_df(source_bucket_name,source_prefix,data_type,days_to_load)
        clean_df = get_clean_data_df(raw_df, **kwargs)
        clean_df_with_date = add_date(clean_df)
        write_to_silver(clean_df_with_date,sink_bucket_name,sink_prefix)
        write_logs_stage(True,parma_str,'')
    except Exception as e:
        tb = traceback.extract_tb(e.__traceback__)
        failed_func = tb[-1].name  
        error_message = f"Error in function '{failed_func}': {e}"
        write_logs_stage(False,parma_str,error_message)




