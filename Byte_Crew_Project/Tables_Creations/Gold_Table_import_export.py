# Databricks notebook source
# MAGIC %md
# MAGIC # Import Export Gold Table Creation

# COMMAND ----------

gold_import_export = spark.table("workspace.byte_crew.silver_import_export")
display(gold_import_export.head(50))

# COMMAND ----------

gold_import_export.select("item", "element").distinct().show()
#item: Fruits - Excluding Wine , Vegetables 
#element: Import quantity, Export quantity

# COMMAND ----------

# DBTITLE 1,Change Fruits - Excluding Wine to "Fruits"
from pyspark.sql import functions as F
gold_import_export = gold_import_export.withColumn(
    "item",
    F.when(F.col("item").startswith("Fruits -"), "Fruits").otherwise(F.col("item"))
)
gold_import_export.select("item").distinct().show()



# COMMAND ----------

display(gold_import_export.head(50))

# COMMAND ----------

# DBTITLE 1,Pivot table to create fruit&veg import&export columns
df_pivoted = (
    gold_import_export
    .filter(F.col("item").isin(["Fruits", "Vegetables"]))
    .filter(F.col("element").isin(["Import quantity", "Export quantity"]))
    .groupBy("country_code", "country", "year", "item", "element")
    .agg(F.sum("value").alias("value"))
    .groupBy("country_code", "country", "year", "item")
    .pivot("element")
    .agg(F.first("value"))
)
display(df_pivoted.head(5))


# COMMAND ----------

from pyspark.sql import functions as F
item_pivoted = (
    df_pivoted
    .groupBy("country_code", "country", "year")
    .pivot("item", ["Fruits", "Vegetables"])
    .agg(F.first("Import quantity").alias("import"), 
         F.first("Export quantity").alias("export")
    )
)
#rename the columns and as well fill missing values with 0 (if any)
item_pivoted = (
    item_pivoted
    .withColumnRenamed("Fruits_import", "fruit_import_1000t")
    .withColumnRenamed("Fruits_export", "fruit_export_1000t")
    .withColumnRenamed("Vegetables_import", "veg_import_1000t")
    .withColumnRenamed("Vegetables_export", "veg_export_1000t")
    .fillna({
        "fruit_import_1000t": 0,
        "fruit_export_1000t": 0,
        "veg_import_1000t": 0,
        "veg_export_1000t": 0
    }
    )
)
#add combined total and calculate net trade
item_pivoted = (
    item_pivoted
    .withColumn("fruit_veg_import_1000t",
                F.round(F.col("fruit_import_1000t") + F.col("veg_import_1000t"),2))
    .withColumn("fruit_veg_export_1000t",
                F.round(F.col("fruit_export_1000t") + F.col("veg_export_1000t"),2))
    .withColumn("net_trade_1000t",
                F.round(F.col("fruit_veg_import_1000t") - F.col("fruit_veg_export_1000t"),2))
)

                

display(item_pivoted.head(5))


# COMMAND ----------

gold_import_export = item_pivoted
display(gold_import_export.head(5))

# COMMAND ----------

# DBTITLE 1,Create table gold import export table
gold_import_export.write.mode("overwrite").saveAsTable("workspace.byte_crew.gold_import_export")

# COMMAND ----------

# MAGIC %md
# MAGIC