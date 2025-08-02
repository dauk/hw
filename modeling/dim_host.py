def load_dim_host():
    spark.sql("""
        INSERT INTO dwh.dim_host (host)
        SELECT DISTINCT value 
        FROM dwh.stg_hosts
        WHERE value NOT IN (
            SELECT host FROM dwh.dim_host
        )
    """)
