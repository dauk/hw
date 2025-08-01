
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, substring_index, to_date, regexp_replace, trim
from delta.tables import DeltaTable

import sys
sys.path.append("/Workspace/Users/dauuuk@gmail.com/hw/utility")
from utils import get_access_secret_keys
from date_utils import get_date_glob


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
    date_glob = get_date_glob(days_to_load)
    
    supported_readers = {"csv", "json", "parquet", "orc", "text"}
    if file_type not in supported_readers:
        raise ValueError(f"Unsupported file type: {file_type}")

    full_path = f"s3a://{bucket}/{source_path}/{date_glob}/*.{"txt" if file_type == "text" else file_type}"
    #TODO: currently only tested with text, working with other formats will require additional development
    read_func = getattr(spark.read, file_type)
    raw_df = read_func(full_path)
    print("raw")#todelete
    raw_df.show() #todelete
    raw_df.count() #todelete
    return raw_df



def get_clean_data_df(raw_df, **kwargs):
    filtered_df = raw_df
    if "starts_with" in kwargs:
        filtered_df = filtered_df.filter(col("value").startswith(kwargs["starts_with"]))
    if "regex_clear" in kwargs:
        x = 'regexp_replace(regexp_replace(col("value"), r"^\\|\\|", ""), r"\\^$", "")' #todelete
        filtered_df = filtered_df.withColumn(
        "clean_value",
        eval(kwargs["regex_clear"])
        #eval(x) #todelete
        )
        print(x == kwargs["regex_clear"])
    print(f'good: {x}')
    print(f'bad: {kwargs["regex_clear"]}')
    print("filtered") #todelete
    filtered_df.show() #todelete
    return filtered_df.select("clean_value").withColumnRenamed("clean_value", "value")

def add_date(df):
    df_with_date = df.withColumn("file_path", input_file_name())
    last_two_segments = substring_index(col("file_path"), "/", -2)
    ymd_string = substring_index(last_two_segments, "/", 1)
    df_with_date = df_with_date.withColumn("date", to_date(ymd_string, "yyyy-MM-dd"))
    print("with date")
    df_with_date.show() #todelete
    return df_with_date.select("value", "date")

def write_to_silver(df, bucket, sink_path):
    table_path = f"s3a://{bucket}/{sink_path}/"

    df.write.format("delta") \
        .partitionBy("date") \
        .mode("overwrite") \
        .save(table_path)

    print(f"wrote to {bucket}/{sink_path}")

def process_data(source_bucket_name,source_prefix,sink_bucket_name,sink_prefix,data_type,days_to_load,**kwargs):
    print("=== process_data parameters ===")
    print(f"source_bucket_name: {source_bucket_name}")
    print(f"source_prefix: {source_prefix}")
    print(f"sink_bucket_name: {sink_bucket_name}")
    print(f"sink_prefix: {sink_prefix}")
    print(f"data_type: {data_type}")
    print(f"days_to_load: {days_to_load}")
    print(f"kwargs: {kwargs}")
    print("================================")

    raw_df = get_raw_data_df(source_bucket_name,source_prefix,data_type,days_to_load)
    clean_df = get_clean_data_df(raw_df, **kwargs)
    clean_df_with_date = add_date(clean_df)
    write_to_silver(clean_df_with_date,sink_bucket_name,sink_prefix)




