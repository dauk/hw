from pyspark.sql import SparkSession

def load_dim_category(spark) -> None:
    print("====STARTING LOAD DIMENSION TABLE dim_category====")
    spark.sql("""
        INSERT INTO dwh.dim_category (category)
        SELECT DISTINCT category 
        FROM dwh.stg_hosts
        WHERE category NOT IN (
            SELECT category FROM dwh.dim_category
        )
    """)
    print("====FINISHED LOAD DIMENSION TABLE dim_category====")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("load_dim_category") \
        .enableHiveSupport() \
        .getOrCreate()
    
    load_dim_category(spark)