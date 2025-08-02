def load_obt_hosts(spark):
    print("====STARTING LOAD OBT TABLE obt_hosts====")
    spark.sql("""
        delete from dwh.obt_hosts where
        date in (
        SELECT date
        FROM dwh.stg_hosts
        )
    """)

    spark.sql("""
        INSERT INTO dwh.obt_hosts (date, host, category, vendor)
        SELECT date, value, category, vendor
        FROM dwh.stg_hosts
    """)
    print("====FINISHED LOAD OBT TABLE obt_hosts====")
