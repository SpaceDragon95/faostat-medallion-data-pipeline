# Databricks notebook source
# DBTITLE 1,read gold_master to df
df = spark.table("workspace.byte_crew.gold_master")

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Aggregate unique years into an array and sort them
country_years = (
    df.groupBy("country")
    .agg(
        F.sort_array(F.collect_set("year")).alias("years_present"),
        F.countDistinct("year").alias("year_count"),
    )
)

# 2. Countries with all 10 years
complete_countries = country_years.filter(F.col("year_count") == 10).select(
    "country", "years_present"
)

# 3. Countries missing at least one year
incomplete_countries = country_years.filter(F.col("year_count") < 10).select(
    "country", "years_present"
)

print(f"Complete: {complete_countries.count()} countries")
display(complete_countries)

print(f"Missing years: {incomplete_countries.count()} countries")
display(incomplete_countries)

# COMMAND ----------

display(df.head(10))

# COMMAND ----------

# DBTITLE 1,calculate gap in daily supply
from pyspark.sql import functions as F

df = df.withColumn(
    "gap_from_400",
    # F.round(F.lit(400) - F.col("fruit_veg_supply_g_day"), 1) # changed the order of the operands
     F.round(F.col("fruit_veg_supply_g_day") - F.lit(400),1)
)

display(df)

# COMMAND ----------

# DBTITLE 1,Add the trend_column
from pyspark.sql import Window
from pyspark.sql import functions as F

# 1. Define window specifications per country ordered by year
w_asc = Window.partitionBy("country").orderBy("year")
w_desc = Window.partitionBy("country").orderBy(F.col("year").desc())

# 2. Get the first and last supply values for each country
df_trend = df.withColumn(
    "_start_g_day", F.first("fruit_veg_supply_g_day").over(w_asc)
).withColumn(
    "_end_g_day", F.first("fruit_veg_supply_g_day").over(w_desc)
)

# 3. Add the 'trend' classification column using CASE WHEN logic
df_final = df_trend.withColumn(
    "trend",
    F.when(
        (F.col("_start_g_day") >= 400) & (F.col("_end_g_day") >= 400),
        "Above 400g",
    )
    .when(
        (F.col("_start_g_day") < 400) & (F.col("_end_g_day") >= 400),
        "Reached 400g",
    )
    .when(
        F.col("_end_g_day") > F.col("_start_g_day"),
        "Closing the gap",
    )
    .when(
        F.col("_end_g_day") < F.col("_start_g_day"),
        "Moving away",
    )
    .otherwise("Stable"),
).drop("_start_g_day", "_end_g_day")  # Remove intermediate calculation columns

display(df_final)

# COMMAND ----------

df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("byte_crew.gold_master")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELE

# COMMAND ----------


gold_master = spark.table("workspace.byte_crew.gold_master")
display(gold_master)