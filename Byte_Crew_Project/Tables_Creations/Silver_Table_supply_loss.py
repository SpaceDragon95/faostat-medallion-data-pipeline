# Databricks notebook source
# MAGIC %md
# MAGIC # Supply Loss Silver Table Creation

# COMMAND ----------

# DBTITLE 1,Create starting df
bronze_supply_loss_df = spark.table("workspace.byte_crew.bronze_supply_loss")

display(bronze_supply_loss_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT Area,Element, Item, Year, Value FROM byte_crew.bronze_supply_loss WHERE `Area Code` = 210 AND Year = 2020

# COMMAND ----------

# DBTITLE 1,Select Silver Columns and rename
from pyspark.sql import functions as f

fs_silver_columns =[
    "Area Code",
    "Area",
    "Year",
    "Unit",
    "Item",
    "Item Code",
    "Element",
    "Value"

]

fs_column_rename_map = {
    "Area Code": "country_code",
    "Area": "country",
    "Year": "year",
    "Item Code": "item_code",
    "Item": "item",
    "Element": "element",
    "Value": "value" ,
    "Unit": "unit"
}

silver_supply_loss_df = bronze_supply_loss_df.select(*fs_silver_columns)

for old_name, new_name in fs_column_rename_map.items():
    silver_supply_loss_df = silver_supply_loss_df.withColumnRenamed(old_name, new_name)

display(silver_supply_loss_df.head(5))

# COMMAND ----------

# DBTITLE 1,Trimming all columns & Validating no nulls
for column_name in silver_supply_loss_df.columns:
    silver_supply_loss_df = silver_supply_loss_df.withColumn(
        column_name, f.trim(f.col(column_name))
    )

display(silver_supply_loss_df.select([
    f.sum(f.col(column).isNull().cast("int")).alias(column)
    for column in silver_supply_loss_df.columns
]))

display(silver_supply_loss_df.select([
    f.sum((f.col(column) == "").cast("int")).alias(column)
    for column in silver_supply_loss_df.columns
]))

# COMMAND ----------

# DBTITLE 1,Applying data type
silver_fs_data_map = {
    "country_code": "int",
    "country": "string",
    "item": "string",
    "item_code": "int",
    "year": "int",
    "element":"string",
    "value": "double",
    "unit": "string"
}

for column_name, data_type in silver_fs_data_map.items():
    silver_supply_loss_df = silver_supply_loss_df.withColumn(
        column_name,
        f.col(column_name).cast(data_type)
    )

silver_supply_loss_df.printSchema()


# COMMAND ----------

# DBTITLE 1,Validating ranges 2

silver_supply_loss_df.select("item", "item_code", "element").distinct().show()

# COMMAND ----------

# DBTITLE 1,Checking for duplicates
duplicate_fs_rows = (
    silver_supply_loss_df
    .groupBy("country_code", "year", "element")
    .count()
    .filter(f.col("count") > 1)
)

display(duplicate_fs_rows.head(5))

# COMMAND ----------

# DBTITLE 1,Create Silver Table
silver_supply_loss_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.byte_crew.silver_supply_loss")
