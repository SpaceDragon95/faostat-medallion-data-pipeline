# Databricks notebook source
# MAGIC %md
# MAGIC # Total Population Silver Table Creation

# COMMAND ----------

# DBTITLE 1,Create starting df
bronze_population_df = spark.table("workspace.byte_crew.bronze_total_population")

display(bronze_population_df)

# COMMAND ----------

bronze_population_df.select("Item").distinct().show()

# COMMAND ----------

# DBTITLE 1,Select Silver Columns and rename
from pyspark.sql import functions as f

fs_silver_columns =[
    "Area Code",
    "Area",
    "Year",
    "Value",
    "Unit"
]

fs_column_rename_map = {
    "Area Code": "country_code",
    "Area": "country",
    "Year": "year",
    "Value": "value",
    "Unit": "unit",

}

silver_population_df = bronze_population_df.select(*fs_silver_columns)

for old_name, new_name in fs_column_rename_map.items():
    silver_population_df = silver_population_df.withColumnRenamed(old_name, new_name)

display(silver_population_df)

# COMMAND ----------

# DBTITLE 1,Trimming all columns & Validating no nulls
for column_name in silver_population_df.columns:
    silver_food_supply_df = silver_population_df.withColumn(
        column_name, f.trim(f.col(column_name))
    )

display(silver_population_df.select([
    f.sum(f.col(column).isNull().cast("int")).alias(column)
    for column in silver_population_df.columns
]))

display(silver_population_df.select([
    f.sum((f.col(column) == "").cast("int")).alias(column)
    for column in silver_population_df.columns
]))

# COMMAND ----------

# DBTITLE 1,Applying data type
from pyspark.sql import functions as f
silver_fs_data_map = {
    "country_code": "int",
    "country": "string",
    "year": "int",
    "value": "long",
    "unit": "string"
}

for column_name, data_type in silver_fs_data_map.items():
  if column_name == "value":
        # Handle string decimal -> double -> round -> long
    silver_population_df = silver_population_df.withColumn(
            column_name,
            f.round(f.col(column_name).cast("double"), 0).cast(data_type)
        )
  else:
    silver_population_df = silver_population_df.withColumn(
            column_name,
            f.col(column_name).cast(data_type)
        )

silver_population_df.printSchema()


# COMMAND ----------

display(silver_population_df.head(6))

# COMMAND ----------

# DBTITLE 1,Validate ranges 1
display(
    silver_population_df.select(
        # f.min("country_code").alias("min_country_code"),
        # f.max("country_code").alias("max_country_code"),
        # f.min("year").alias("min_year"),
        # f.max("year").alias("max_year"),
        f.min("value").alias("min_population"),
        f.max("value").alias("max_population"),
      
    )
)

# COMMAND ----------

# DBTITLE 1,Validating ranges 2
silver_population_df.select("unit").distinct().show()


# COMMAND ----------

# DBTITLE 1,Checking for duplicates
duplicate_fs_rows = (
    silver_population_df
    .groupBy("country_code", "year" )
    .count()
    .filter(f.col("count") > 1)
)

display(duplicate_fs_rows)

# COMMAND ----------

# DBTITLE 1,Create Silver Table
silver_population_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.byte_crew.silver_population")
