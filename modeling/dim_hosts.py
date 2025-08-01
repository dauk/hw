from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import sys
sys.path.append("/Workspace/Users/dauuuk@gmail.com/hw/utility")
from utils import get_access_secret_keys
from functools import reduce

ACCESS_KEY, SECRET_KEY = get_access_secret_keys()

spark = SparkSession.builder \
    .appName("process_model") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.logStore.crossCloud.fatal", "false") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", ACCESS_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", SECRET_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")



# Load the DataFrames
df_list = [
    spark.read.format("delta").load("s3a://hwsilver/ads_and_trackers/vendorA/").withColumn('vendor',lit('vendorA')).withColumn('category',lit('ads_and_trackers')),
    spark.read.format("delta").load("s3a://hwsilver/ads_and_trackers/vendorB/").withColumn('vendor',lit('vendorB')).withColumn('category',lit('ads_and_trackers')),
    spark.read.format("delta").load("s3a://hwsilver/ads_and_trackers/vendorC/").withColumn('vendor',lit('vendorC')).withColumn('category',lit('ads_and_trackers')),
    spark.read.format("delta").load("s3a://hwsilver/malware/vendorA/").withColumn('vendor',lit('vendorA')).withColumn('category',lit('malware'))
]

df_all = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), df_list)

df_all.createOrReplaceTempView('tmp_hosts')

spark.sql("select vendor, count(*) from tmp_hosts group by vendor").show()

