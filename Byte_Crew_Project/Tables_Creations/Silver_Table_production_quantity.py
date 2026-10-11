# Databricks notebook source
# MAGIC %md
# MAGIC # Production Quantity Silver Table Creation

# COMMAND ----------

# DBTITLE 1,Create starting df
bronze_production_quantity_df = spark.table("workspace.byte_crew.bronze_production_quantity")

display(bronze_production_quantity_df.head(5))

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

silver_production_quantity_df = bronze_production_quantity_df.select(*fs_silver_columns)

for old_name, new_name in fs_column_rename_map.items():
    silver_production_quantity_df = silver_production_quantity_df.withColumnRenamed(old_name, new_name)

display(silver_production_quantity_df)

# COMMAND ----------

display(silver_production_quantity_df.select([
    f.sum(
        f.when(f.col(c).isNull() | (f.col(c).cast("string") == ""), 1)
         .otherwise(0)
    ).alias(c)
    for c in silver_production_quantity_df.columns
]))

# COMMAND ----------

# DBTITLE 1,Trimming all columns & Validating no nulls
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

#Identify string columns only
string_cols = [
    field.name for field in silver_production_quantity_df.schema.fields 
    if isinstance(field.dataType, StringType)
]

#Trim string columns cleanly
for col_name in string_cols:
    silver_production_quantity_df = silver_production_quantity_df.withColumn(
        col_name, f.trim(f.col(col_name))
    )

#Check for NULL values across all columns
display(silver_production_quantity_df.select([
    f.sum(f.col(col_name).isNull().cast("int")).alias(col_name)
    for col_name in silver_production_quantity_df.columns
]))

# Check for Empty Strings (string columns only)
if string_cols:
    display(silver_production_quantity_df.select([
        f.sum((f.col(col_name) == "").cast("int")).alias(col_name)
        for col_name in string_cols
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
    silver_production_quantity_df = silver_production_quantity_df.withColumn(
        column_name,
        f.col(column_name).cast(data_type)
    )

silver_production_quantity_df.printSchema()


# COMMAND ----------

# DBTITLE 1,Validate ranges 1
display(
    silver_food_supply_df.select(
        f.min("country_code").alias("min_country_code"),
        f.max("country_code").alias("max_country_code"),
        f.min("year").alias("min_year"),
        f.max("year").alias("max_year"),
        f.min("value").alias("min_production"),
        f.max("value").alias("max_production"),
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
silver_production_quantity_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.byte_crew.silver_production_quantity")

