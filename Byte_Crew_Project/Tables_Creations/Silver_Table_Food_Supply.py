# Databricks notebook source
# MAGIC %md
# MAGIC # Food Supply Silver Table Creation

# COMMAND ----------

# DBTITLE 1,Create starting df
bronze_food_supply_df = spark.table("workspace.byte_crew.bronze_food_supply")

display(bronze_food_supply_df)

# COMMAND ----------

# DBTITLE 1,Select Silver Columns and rename
from pyspark.sql import functions as f

fs_silver_columns =[
    "Area Code",
    "Area",
    "Year",
    "Value",
    "Unit",
    "Item",
    "Item Code",
]

fs_column_rename_map = {
    "Area Code": "country_code",
    "Area": "country",
    "Year": "year",
    "Unit": "unit",
    "Value": "value",
    "Item Code": "item_code",
    "Item": "item"
}

silver_food_supply_df = bronze_food_supply_df.select(*fs_silver_columns)

for old_name, new_name in fs_column_rename_map.items():
    silver_food_supply_df = silver_food_supply_df.withColumnRenamed(old_name, new_name)

display(silver_food_supply_df)

# COMMAND ----------

# DBTITLE 1,Trimming all columns & Validating no nulls
for column_name in silver_food_supply_df.columns:
    silver_food_supply_df = silver_food_supply_df.withColumn(
        column_name, f.trim(f.col(column_name))
    )

display(silver_food_supply_df.select([
    f.sum(f.col(column).isNull().cast("int")).alias(column)
    for column in silver_food_supply_df.columns
]))

display(silver_food_supply_df.select([
    f.sum((f.col(column) == "").cast("int")).alias(column)
    for column in silver_food_supply_df.columns
]))

# COMMAND ----------

# DBTITLE 1,Applying data type
silver_fs_data_map = {
    "country_code": "int",
    "country": "string",
    "item": "string",
    "item_code": "int",
    "year": "int",
    "value": "double",
    "unit": "string"
}

for column_name, data_type in silver_fs_data_map.items():
    silver_food_supply_df = silver_food_supply_df.withColumn(
        column_name,
        f.col(column_name).cast(data_type)
    )

silver_food_supply_df.printSchema()


# COMMAND ----------

# DBTITLE 1,Validate ranges 1
display(
    silver_food_supply_df.select(
        f.min("country_code").alias("min_country_code"),
        f.max("country_code").alias("max_country_code"),
        f.min("year").alias("min_year"),
        f.max("year").alias("max_year"),
        f.min("value").alias("min_value"),
        f.max("value").alias("max_value"),
        f.min("item_code").alias("mim_item_code"),
        f.max("item_code").alias("max_item_code")
    )
)

# COMMAND ----------

# DBTITLE 1,Validating ranges 2
silver_food_supply_df.select("unit").distinct().show()
silver_food_supply_df.select("item", "item_code").distinct().show()

# COMMAND ----------

# DBTITLE 1,Checking for duplicates
duplicate_fs_rows = (
    silver_food_supply_df
    .groupBy("country_code", "year", "item_code")
    .count()
    .filter(f.col("count") > 1)
)

display(duplicate_fs_rows)

# COMMAND ----------

# DBTITLE 1,Create Silver Table
# silver_food_supply_df.write.mode("overwrite").saveAsTable("workspace.byte_crew.silver_food_supply")

silver_food_supply_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.byte_crew.silver_food_supply")

# COMMAND ----------

display(silver_food_supply_df.head(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from byte_crew.silver_food_supply WHERE country_code = 210 AND Year = 2017;