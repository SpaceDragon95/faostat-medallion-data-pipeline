# Databricks notebook source
# MAGIC %md
# MAGIC # Profiling each column for data mapping

# COMMAND ----------

import json
import pandas as pd

food_supply_path = "/Volumes/workspace/byte_crew/raw/fbs_supply.json"

with open(food_supply_path, "r") as f:
    FOOD_SUPPLY = json.load(f)

food_supply_df = pd.DataFrame(FOOD_SUPPLY["data"])

# COMMAND ----------

food_supply_df[["Domain Code", "Domain"]].drop_duplicates()

# COMMAND ----------

food_supply_df[["Area Code", "Area"]].drop_duplicates()

# COMMAND ----------

food_supply_df["Area Code"].astype(str).str.startswith("0").any()

# COMMAND ----------

food_supply_df["Area Code"].unique()

# COMMAND ----------

food_supply_df["Area Code"].isna().sum()

# COMMAND ----------

food_supply_df[["Element Code", "Element"]].drop_duplicates()

# COMMAND ----------

food_supply_df[["Item Code", "Item"]].drop_duplicates()

# COMMAND ----------

food_supply_df[["Year Code", "Year"]].drop_duplicates()

# COMMAND ----------

food_supply_df["Unit"].isna().sum()

# COMMAND ----------

food_supply_df["Unit"].unique()

# COMMAND ----------

food_supply_df["Value"].isna().sum()

# COMMAND ----------

pd.to_numeric(food_supply_df["Value"], errors="coerce").describe()

# COMMAND ----------

pd.to_numeric(food_supply_df["Value"], errors="coerce").isna().sum()

# COMMAND ----------

food_supply_df["Value"].str.split(".").str[1].str.len().value_counts()

# COMMAND ----------

food_supply_df["Flag"].value_counts(dropna=False)

# COMMAND ----------

food_supply_df["Flag Description"].value_counts(dropna=False)

# COMMAND ----------

food_supply_df["Note"].isna().sum()

# COMMAND ----------

food_supply_df["Note"].dropna().unique()

# COMMAND ----------

import json
import pandas as pd

population_path = "/Volumes/workspace/byte_crew/raw/pe_tpop.json"

with open(population_path, "r") as f:
    TOTAL_POPULATION = json.load(f)["data"]

population_df = pd.DataFrame(TOTAL_POPULATION)


# COMMAND ----------

population_df[["Domain Code", "Domain"]].drop_duplicates()

# COMMAND ----------

population_df["Area Code"].unique()

# COMMAND ----------

population_df[["Element Code", "Element"]].drop_duplicates()

# COMMAND ----------

population_df[["Item Code", "Item"]].drop_duplicates()

# COMMAND ----------

population_df[["Year Code", "Year"]].drop_duplicates

# COMMAND ----------

population_df["Unit"].isna().sum()

# COMMAND ----------

population_df["Unit"].unique()

# COMMAND ----------

population_df["Value"].isna().sum()

# COMMAND ----------

pd.to_numeric(population_df["Value"], errors="coerce").describe()

# COMMAND ----------

population_df["Flag"].value_counts(dropna=False)

# COMMAND ----------

population_df["Flag Description"].value_counts(dropna=False)

# COMMAND ----------

population_df

# COMMAND ----------

population_df[["Item Code", "Item"]].drop_duplicates()

# COMMAND ----------

import json
import pandas as pd

import_export_path = "/Volumes/workspace/byte_crew/raw/fbs_import_export.json"

with open(import_export_path, "r") as f:
    import_export = json.load(f)
import_export_df = pd.DataFrame(import_export["data"])


# COMMAND ----------

import_export_df[["Domain Code", "Domain"]].drop_duplicates()

# COMMAND ----------

import_export_df[["Element Code", "Element"]].drop_duplicates()

# COMMAND ----------

import_export_df[["Item Code", "Item"]].drop_duplicates()

# COMMAND ----------

import_export_df["Item Code"].isna().sum()

# COMMAND ----------

import_export_df["Unit"].unique()

# COMMAND ----------

pd.to_numeric(import_export_df["Value"], errors="coerce").describe()

# COMMAND ----------

import_export_df[["Flag", "Flag Description"]].drop_duplicates

# COMMAND ----------

import_export_df[["Note"]].drop_duplicates()

# COMMAND ----------

import json
import pandas as pd

production_path = "/Volumes/workspace/byte_crew/raw/production_quantity.json"

with open(production_path, "r") as f:
    PRODUCTION = json.load(f)

production_df = pd.DataFrame(PRODUCTION["data"])

# COMMAND ----------

production_df.isna().sum()

# COMMAND ----------

pd.to_numeric(production_df["Value"], errors="coerce").describe()

# COMMAND ----------

production_df["Unit"].drop_duplicates()

# COMMAND ----------

supply_loss_path = "/Volumes/workspace/byte_crew/raw/fbs_supply_loss.json"

with open(supply_loss_path, "r") as f:
    SUPPLY_LOSS = json.load(f)

supply_loss_df = pd.DataFrame(SUPPLY_LOSS["data"])

# COMMAND ----------

supply_loss_df.isna().sum()

# COMMAND ----------

pd.to_numeric(supply_loss_df["Value"], errors="coerce").describe()

# COMMAND ----------

supply_loss_df["Unit"].drop_duplicates()

# COMMAND ----------

import json

food_supply_path = "/Volumes/workspace/byte_crew/raw/fbs_supply.json"

with open(food_supply_path, "r") as f:
    FOOD_SUPPLY = json.load(f)

food_supply_df = spark.createDataFrame(FOOD_SUPPLY["data"])

food_supply_df.printSchema()
display(food_supply_df.limit(10))

print(f"Row count: {food_supply_df.count()}")
print(f"Column count: {len(food_supply_df.columns)}")

# COMMAND ----------

print(type(FOOD_SUPPLY))
print(FOOD_SUPPLY.keys())
print(type(FOOD_SUPPLY["data"]))
print(type(FOOD_SUPPLY["data"][0]))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS
# MAGIC workspace.byte_crew.total_population;