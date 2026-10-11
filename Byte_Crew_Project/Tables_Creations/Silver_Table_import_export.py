# Databricks notebook source
# MAGIC %md
# MAGIC # Import Export Silver Table Creation

# COMMAND ----------

# DBTITLE 1,Create starting df
bronze_import_export_df = spark.table("workspace.byte_crew.bronze_import_export")

display(bronze_import_export_df.head(5))

# COMMAND ----------

bronze_import_export_df.select("Element").distinct().show()

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
    "Unit": "unit",
    "Item Code": "item_code",
    "Element": "element",
    "Item": "item",
    "Value": "value"
}

silver_import_export_df = bronze_import_export_df.select(*fs_silver_columns)

for old_name, new_name in fs_column_rename_map.items():
    silver_import_export_df = silver_import_export_df.withColumnRenamed(old_name, new_name)

display(silver_import_export_df.head(5))

# COMMAND ----------

# DBTITLE 1,Trimming all columns & Validating no nulls
for column_name in silver_import_export_df.columns:
    silver_import_export_df = silver_import_export_df.withColumn(
        column_name, f.trim(f.col(column_name))
    )

display(silver_import_export_df.select([
    f.sum(f.col(column).isNull().cast("int")).alias(column)
    for column in silver_import_export_df.columns
]))

display(silver_import_export_df.select([
    f.sum((f.col(column) == "").cast("int")).alias(column)
    for column in silver_import_export_df.columns
]))

# COMMAND ----------



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
    silver_import_export_df = silver_import_export_df.withColumn(
        column_name,
        f.col(column_name).cast(data_type)
    )

silver_import_export_df.printSchema()


# COMMAND ----------

# DBTITLE 1,Validate ranges 1
display(
    silver_import_export_df.select(
        f.min("country_code").alias("min_country_code"),
        f.max("country_code").alias("max_country_code"),
        f.min("year").alias("min_year"),
        f.max("year").alias("max_year"),
        f.min("value").alias("min_importexport"),
        f.max("value").alias("max_importexport"),
        f.min("item_code").alias("mim_item_code"),
        f.max("item_code").alias("max_item_code")
    )
)

# COMMAND ----------

# DBTITLE 1,Validating ranges 2
silver_import_export_df.select("unit").distinct().show()
silver_import_export_df.select("element").distinct().show()
silver_import_export_df.select("item", "item_code").distinct().show()

# COMMAND ----------

# DBTITLE 1,Checking for duplicates
duplicate_fs_rows = (
    silver_import_export_df
    .groupBy("country_code", "year", "item_code")
    .count()
    .filter(f.col("count") > 1)
)

display(duplicate_fs_rows)

# COMMAND ----------

# DBTITLE 1,Create Silver Table
silver_import_export_df.write.mode("overwrite").saveAsTable("workspace.byte_crew.silver_import_export")


# COMMAND ----------

silver_import_export_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.byte_crew.silver_import_export")