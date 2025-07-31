
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, substring_index, to_date, regexp_replace

import sys
sys.path.append("/Workspace/Users/dauuuk@gmail.com/hw/utility")
from utils import get_access_secret_keys
from date_utils import get_date_glob

ACCESS_KEY, SECRET_KEY = get_access_secret_keys()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", ACCESS_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", SECRET_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")


# # Start Spark session
# spark = SparkSession.builder.appName("FilterBadHeader").getOrCreate()
# spark.databricks.delta.logStore.crossCloud.fatal = False
# #spark.conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")






spark = SparkSession.builder \
    .appName("DeltaCrossCloud") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print(spark.conf.get("spark.databricks.delta.logStore.crossCloud.fatal"))
spark.conf.set("spark.databricks.delta.logStore.crossCloud.fatal", "false")

print(spark.conf.get("spark.databricks.delta.logStore.crossCloud.fatal"))

# Read file as plain text (not as CSV)
#raw_df = spark.read.text("s3a://hwbronze/ads_and_trackers/vendorA/20250731/adblock_20250731_124530.txt")
raw_df = spark.read.text("s3a://hwbronze/ads_and_trackers/vendorA/{2025-07-01,2025-07-31}/")

# Filter lines that start with '||'
filtered_df = raw_df.filter(col("value").startswith("||"))

filtered_df = filtered_df.withColumn("file_path", input_file_name())



# # Extract last two segments: '20250731/filename.txt'
last_two_segments = substring_index(col("file_path"), "/", -2)

# # Then get the 2nd-to-last segment (the date string)
ymd_string = substring_index(last_two_segments, "/", 1)

# # Convert to DateType
filtered_df = filtered_df.withColumn("ymd_string", ymd_string)\
                           .withColumn("date", to_date(col("ymd_string"), "yyyy-MM-dd"))


                          

filtered_df = filtered_df.withColumn(
    "clean_value",
    regexp_replace(regexp_replace(col("value"), r"^\|\|", ""), r"\^$", "")
)



filtered_df = filtered_df.select("date", "clean_value")

filtered_df.show(truncate=False)
# filtered_df.show()

filtered_df.write.format("delta").partitionBy("date").mode("overwrite").save("s3a://hwsilver/ads_and_trackers/vendorA/")






