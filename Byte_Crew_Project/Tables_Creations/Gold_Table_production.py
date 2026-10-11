# Databricks notebook source
# MAGIC %md
# MAGIC # Production Gold Table Creation

# COMMAND ----------

gold_production = spark.table("workspace.byte_crew.silver_production_quantity")
display(gold_production.head(5))

# COMMAND ----------

gold_production.select("item").distinct().show()

# COMMAND ----------

from pyspark.sql import functions as F
gold_production= gold_production.withColumn(
    "item",
    F.when(F.col("item").startswith("Fruits -"), "Fruits").otherwise(F.col("item"))
)

# COMMAND ----------

gold_production.select("item").distinct().show()

# COMMAND ----------

from pyspark.sql import functions as F
df_pivoted = (
    gold_production
    .filter(F.col("item").isin(["Fruits", "Vegetables"]))
    .groupBy("country_code", "country", "year")
    .pivot("item",["Fruits", "Vegetables"])
    .agg(F.first("value"))
    .withColumnRenamed("Fruits", "fruit_prod_1000t")
    .withColumnRenamed("Vegetables","veg_prod_1000t")
    .fillna({"fruit_prod_1000t":0, "veg_prod_1000t":0 })
)
display(df_pivoted.head(5))

# COMMAND ----------

# DBTITLE 1,new column:total fruit_veg
df_pivoted = df_pivoted.withColumn(
    "fruit_veg_prod_1000t",
    F.round(
        F.coalesce(F.col("fruit_prod_1000t"), F.lit(0)) +
        F.coalesce(F.col("veg_prod_1000t"), F.lit(0)), 2
    )
)
display(df_pivoted.head(5))

# COMMAND ----------

gold_production = df_pivoted
display(gold_production.head(5))

# COMMAND ----------

# DBTITLE 1,create gold_production table
gold_production.write.mode("overwrite").saveAsTable("workspace.byte_crew.gold_production")

# COMMAND ----------

