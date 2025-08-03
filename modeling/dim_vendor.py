from pyspark.sql import SparkSession

def load_dim_vendor(spark) -> None:
    print("====STARTING LOAD DIMENSION TABLE dim_vendor====")
    spark.sql("""
        INSERT INTO dwh.dim_vendor (vendor_name)
        SELECT DISTINCT vendor 
        FROM dwh.stg_hosts
        WHERE vendor NOT IN (
            SELECT vendor_name FROM dwh.dim_vendor
        )
    """)
    print("====FINISHED LOAD DIMENSION TABLE dim_vendor====")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("load_dim_vendor") \
        .enableHiveSupport() \
        .getOrCreate()
    
    load_dim_vendor(spark)