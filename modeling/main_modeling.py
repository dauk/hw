from pyspark.sql import SparkSession
from dim_category import load_dim_category
from dim_vendor import load_dim_vendor
from dim_host import load_dim_host
from fact_hosts import load_fact_hosts
from obt_hosts import load_obt_hosts
from stg_hosts import load_stg_hosts


def load_model():
    spark = SparkSession.builder \
    .appName("prcess_model") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.logStore.crossCloud.fatal", "false") \
    .getOrCreate()


    load_stg_hosts()#must load first
    load_dim_category(spark)
    load_dim_vendor(spark)
    load_dim_host(spark)
    load_fact_hosts(spark)
    load_obt_hosts(spark)
    

if __name__ == "__main__":
    load_model()