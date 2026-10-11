# Databricks notebook source
# MAGIC %md
# MAGIC # Food Supply Gold Table Creation

# COMMAND ----------

gold_food_supply = spark.table("workspace.byte_crew.silver_food_supply")
display(gold_food_supply.head(5))

# COMMAND ----------

display(gold_food_supply.head(50))

# COMMAND ----------

gold_food_supply.select("item").distinct().show()
#Fruits - Excluding Wine
#Vegetables

# COMMAND ----------

# DBTITLE 1,Change Fruit - exlcuding wine to Fruits
from pyspark.sql import functions as F
gold_food_supply = gold_food_supply.withColumn(
    "item",
    F.when(F.col("item").startswith("Fruits -"), "Fruits").otherwise(F.col("item"))
)
gold_food_supply.select("item").distinct().show()

# COMMAND ----------

# DBTITLE 1,Pivot Item to create fruit and veg supply value columns
pivoted_df = (
    gold_food_supply
    .filter(F.col("item").isin(["Fruits", "Vegetables"]))
    .groupBy("country_code", "country", "year")
    .pivot("item", ["Fruits", "Vegetables"])
    .agg(F.first("value"))
    .withColumnRenamed("Fruits", "fruit_supplykg_cap")
    .withColumnRenamed("Vegetables", "veg_supplykg_cap")
)
display(pivoted_df.head(10))

              


# COMMAND ----------

# DBTITLE 1,Add fruit , veg & calculate per day in g
pivoted_df = pivoted_df.withColumn(
    "fruit_veg_supplykg_cap",
    F.round(F.coalesce(F.col("fruit_supplykg_cap"), F.lit(0)) + 
    F.coalesce(F.col("veg_supplykg_cap"), F.lit(0)), 2)
)
pivoted_df = pivoted_df.withColumn(
    "fruit_veg_supply_g_day",
    F.round((F.col("fruit_veg_supplykg_cap") * 1000)/365.0,2)
)
display(pivoted_df.head(10))


# COMMAND ----------

# DBTITLE 1,Create column to check if it meets 400g criteria or not
pivoted_df = pivoted_df.withColumn(
    "target_400g",
    F.when(F.col("fruit_veg_supply_g_day").isNull(), F.lit("Uknown"))
    .when(F.col("fruit_veg_supply_g_day") >= 400, F.lit("Meets 400g"))
    .otherwise(F.lit("Below 400g"))
)
display(pivoted_df.head(5))


# COMMAND ----------

# DBTITLE 1,Create bin to categorize supply adequacy target
pivoted_df = pivoted_df.withColumn(
    "supply_target",
    F.when(F.col("fruit_veg_supply_g_day").isNull(), F.lit("Unknown"))
     .when(F.col("fruit_veg_supply_g_day") < 100, F.lit("Critical deficiency"))
     .when(F.col("fruit_veg_supply_g_day") < 200, F.lit("Severe deficiency"))
     .when(F.col("fruit_veg_supply_g_day") < 300, F.lit("Moderate deficiency"))
     .when(F.col("fruit_veg_supply_g_day") < 400, F.lit("Mild deficiency"))
     .otherwise(F.lit("Meets WHO 400g"))
)

display(pivoted_df.limit(10))

# COMMAND ----------

gold_food_supply = pivoted_df

display(gold_food_supply.head(5))

# COMMAND ----------

# DBTITLE 1,Create gold_food_supply
gold_food_supply.write.mode("overwrite").saveAsTable("workspace.byte_crew.gold_food_supply")
