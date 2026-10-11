# Databricks notebook source
# MAGIC %md
# MAGIC # Supply Loss Gold Table Creation

# COMMAND ----------

gold_supply_loss = spark.table("workspace.byte_crew.silver_supply_loss")
display(gold_supply_loss.head(5))

# COMMAND ----------

gold_supply_loss.select("item", "element").distinct().show()
#item: Fruits - Excluding Wine , Vegetables 
#element: Domestic supply quantity, Losses

# COMMAND ----------

from pyspark.sql import functions as F
gold_supply_loss= gold_supply_loss.withColumn(
    "item",
    F.when(F.col("item").startswith("Fruits -"), "Fruits").otherwise(F.col("item"))
)
gold_supply_loss.select("item").distinct().show()


# COMMAND ----------

display(gold_supply_loss.head(5))

# COMMAND ----------

# DBTITLE 1,Pivot table to categorize by element and items
#pivot and rename the columns:
from pyspark.sql import functions as F
df_pivoted = (
    gold_supply_loss
    .filter(F.col("item").isin(["Fruits", "Vegetables"]))
    .filter(F.col("element").isin(["Losses", "Domestic supply quantity"]))
    .groupBy("country_code", "country", "year")
    .pivot("item", ["Fruits", "Vegetables"])
    .agg(
        F.first(F.when(F.col("element") == "Losses", F.col("value")), ignorenulls=True).alias("loss"),
        F.first(F.when(F.col("element") == "Domestic supply quantity", F.col("value")), ignorenulls=True).alias("supply")
    )
    .withColumnRenamed("Fruits_loss", "fruit_losses_1000t")
    .withColumnRenamed("Vegetables_loss", "veg_losses_1000t")
    .withColumnRenamed("Fruits_supply", "fruit_domestic_supply_1000t")
    .withColumnRenamed("Vegetables_supply", "veg_domestic_supply_1000t")
    .fillna({
        "fruit_losses_1000t": 0,
        "veg_losses_1000t": 0,
        "fruit_domestic_supply_1000t": 0,
        "veg_domestic_supply_1000t": 0
    })

    )
 

display(df_pivoted.head(5))


# COMMAND ----------

# DBTITLE 1,calculate the combined totals for domestic supply (fruit+veg) and losses(fruit+veg)
df_pivoted = (
    df_pivoted
    .withColumn(
        "fruit_veg_losses_1000t",
        F.round(
            F.col("fruit_losses_1000t") + F.col("veg_losses_1000t"),2))
    .withColumn(
        "fruit_veg_domestic_supply_1000t",
        F.round(
            F.col("fruit_domestic_supply_1000t") + F.col("veg_domestic_supply_1000t"),2))
    )

display(df_pivoted.head(5))



# COMMAND ----------

# DBTITLE 1,Calculate loss_rate_percentage
df_pivoted = df_pivoted.withColumn(
    "loss_rate_pct",
    F.when(F.col("fruit_veg_domestic_supply_1000t") > 0,
        F.round(
            (F.col("fruit_veg_losses_1000t") / F.col("fruit_veg_domestic_supply_1000t")) * 100.0,
            2
        )
    ).otherwise(F.lit(0.0))
)

display(df_pivoted.limit(10))

# COMMAND ----------

gold_supply_loss = df_pivoted
display(gold_supply_loss.head(5))

# COMMAND ----------

# DBTITLE 1,gold_supply_loss table creation
gold_supply_loss.write.mode("overwrite").saveAsTable("workspace.byte_crew.gold_supply_loss")