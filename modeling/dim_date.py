from pyspark.sql.functions import (
    col, sequence, explode, to_date, date_format, year, month, dayofmonth,
    dayofweek, weekofyear, quarter, lit, dayofyear, when
)
import datetime

start_date = datetime.date(2015, 1, 1)
end_date = datetime.date(2030, 12, 31)

date_df = (
    spark.range(1)
    .select(sequence(lit(start_date), lit(end_date)).alias("date_seq"))
    .select(explode(col("date_seq")).alias("date"))
)

calendar_df = (
    date_df
    .withColumn("date", to_date("date"))
    .withColumn("year", year("date"))
    .withColumn("month", month("date"))
    .withColumn("day", dayofmonth("date"))
    .withColumn("day_of_week", dayofweek("date"))  # 1 = Sunday, 7 = Saturday
    .withColumn("week_of_year", weekofyear("date"))
    .withColumn("quarter", quarter("date"))
    .withColumn("day_of_year", dayofyear("date"))
    .withColumn("day_name", date_format("date", "EEEE"))
    .withColumn("month_name", date_format("date", "MMMM"))
    .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), lit(True)).otherwise(lit(False)))
    .withColumn("is_leap_year", (col("year") % 4 == 0)
                & ((col("year") % 100 != 0) | (col("year") % 400 == 0)))
)

calendar_df.write.format("delta").mode("overwrite").saveAsTable("dwh.dim_date")
