# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Table Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Food Supply Data

# COMMAND ----------

# DBTITLE 1,Create food supply df
import json

food_supply_path = "/Volumes/workspace/byte_crew/raw/fbs_supply.json"

with open(food_supply_path) as f:
    food_supply_json = json.load(f)

food_supply_df = spark.createDataFrame(food_supply_json["data"])

# COMMAND ----------

# DBTITLE 1,Inspect food supply data
food_supply_df.printSchema()
display(food_supply_df.limit(10))

print(f"Row count: {food_supply_df.count()}")
print(f"Column count: {len(food_supply_df.columns)}")


# COMMAND ----------

# DBTITLE 1,Create bronze food supply table
(
    food_supply_df.write
        .format("delta")
        .option("delta.columnMapping.mode", "name")
        .mode("overwrite")
        .saveAsTable("workspace.byte_crew.bronze_food_supply")
)

# COMMAND ----------

# DBTITLE 1,Test bronze food supply table 1
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.byte_crew.bronze_food_supply
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Test bronze food supply table 2
# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM workspace.byte_crew.bronze_food_supply;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Total Population Data

# COMMAND ----------

# DBTITLE 1,Create population df
import json

population_path = "/Volumes/workspace/byte_crew/raw/pe_tpop.json"

with open(population_path) as f:
    population_json = json.load(f)

population_df = spark.createDataFrame(population_json["data"])

# COMMAND ----------

# DBTITLE 1,Inspect population data
population_df.printSchema()
display(population_df.limit(10))

print(f"Row count: {population_df.count()}")
print(f"Column count: {len(population_df.columns)}")

# COMMAND ----------

# DBTITLE 1,Population Area & Year count
print(
    "Distinct areas:",
    population_df.select("Area Code").distinct().count()
)

print(
    "Distinct years:",
    population_df.select("Year").distinct().count()
)

population_df.groupBy("Year") \
    .count() \
    .orderBy("Year") \
    .show()

# COMMAND ----------

# DBTITLE 1,Verifying row counts population data
area_count = population_df.select("Area Code").distinct().count()
year_count = population_df.select("Year").distinct().count()

print(f"Expected area-year rows: {area_count * year_count}")
print(f"Actual rows: {population_df.count()}")


# COMMAND ----------

# DBTITLE 1,Create bronze population data
(
    population_df.write
        .format("delta")
        .option("delta.columnMapping.mode", "name")
        .mode("overwrite")
        .saveAsTable("workspace.byte_crew.bronze_total_population")
)

# COMMAND ----------

# DBTITLE 1,Bronze Population table count
# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Import & Export data

# COMMAND ----------

# DBTITLE 1,Create import export df
import json

import_export_path = "/Volumes/workspace/byte_crew/raw/fbs_import_export.json"

with open(import_export_path, "r") as f:
    IMPORT_EXPORT = json.load(f)

import_export_df = spark.createDataFrame(IMPORT_EXPORT["data"])

# COMMAND ----------

# DBTITLE 1,Inspect import export data
import_export_df.printSchema()
display(import_export_df.limit(10))

print(f"Row count: {import_export_df.count()}")
print(f"Column count: {len(import_export_df.columns)}")


# COMMAND ----------

# DBTITLE 1,Create bronze import export table
(
    import_export_df.write
        .format("delta")
        .option("delta.columnMapping.mode", "name")
        .mode("overwrite")
        .saveAsTable("workspace.byte_crew.bronze_import_export")
)

# COMMAND ----------

# DBTITLE 1,Test bronze import export table
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.byte_crew.bronze_import_export
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Test bronze import export table count
# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM workspace.byte_crew.bronze_import_export;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Production Quantity Data

# COMMAND ----------

# DBTITLE 1,Create production quantity df
import json

production_quantity_path = "/Volumes/workspace/byte_crew/raw/production_quantity.json"

with open(production_quantity_path) as f:
    PRODUCTION_QUANTITY = json.load(f)

production_quantity_df = spark.createDataFrame(PRODUCTION_QUANTITY["data"])

# COMMAND ----------

# DBTITLE 1,Inspect production quantity df
production_quantity_df.printSchema()
display(production_quantity_df.limit(10))

print(f"Row count: {production_quantity_df.count()}")
print(f"Column count: {len(production_quantity_df.columns)}")

# COMMAND ----------

# DBTITLE 1,Create bronze production table
(
    production_quantity_df.write
        .format("delta")
        .option("delta.columnMapping.mode", "name")
        .mode("overwrite")
        .saveAsTable("workspace.byte_crew.bronze_production_quantity")
)

# COMMAND ----------

# DBTITLE 1,Test bronze production table
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.byte_crew.bronze_production_quantity
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Bronze Production Quantity Table Row Count
# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM workspace.byte_crew.bronze_production_quantity;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Domestic Supply and loss data

# COMMAND ----------

# DBTITLE 1,Create supply loss df
import json

supply_loss_path = "/Volumes/workspace/byte_crew/raw/fbs_supply_loss.json"

with open(supply_loss_path, "r") as f:
    SUPPLY_LOSS_JSON = json.load(f)

supply_loss_df = spark.createDataFrame(SUPPLY_LOSS_JSON["data"])

# COMMAND ----------

# DBTITLE 1,Inspect supply loss data
supply_loss_df.printSchema()
display(supply_loss_df.limit(10))

print(f"Row count: {supply_loss_df.count()}")
print(f"Column count: {len(supply_loss_df.columns)}")

# COMMAND ----------

# DBTITLE 1,Create bronze supply loss table
(
    supply_loss_df.write
        .format("delta")
        .option("delta.columnMapping.mode", "name")
        .saveAsTable("workspace.byte_crew.bronze_supply_loss")
)

# COMMAND ----------

# DBTITLE 1,Test bronze supply loss table
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.byte_crew.bronze_supply_loss
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Count bronze supply loss table
# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM workspace.byte_crew.bronze_supply_loss;