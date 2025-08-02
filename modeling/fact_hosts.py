def load_fact_hosts():
    spark.sql("""
        delete from dwh.fact_hosts where
        date in (
        SELECT date
        FROM dwh.stg_hosts
        )
    """)

    spark.sql("""
        INSERT INTO dwh.fact_hosts (date, host_key, category_key, vendor_key)
        SELECT h.date, d.host_key, c.category_key, v.vendor_key
        FROM dwh.stg_hosts h
        left join dwh.dim_host d
        on h.value = d.host
        left join dwh.dim_category c
        on h.category = c.category
        left join dwh.dim_vendor v
        on h.vendor = v.vendor_name
    """)
