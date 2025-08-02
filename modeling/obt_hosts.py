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
