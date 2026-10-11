# Databricks notebook source
# MAGIC %md
# MAGIC # Import & Export Data Extraction

# COMMAND ----------

# MAGIC %run "/Workspace/Shared/Testing _Setup_Folder/FAOSTAT_Authentication"

# COMMAND ----------

import requests
import json

FBS_IM_EX_API = "https://faostatservices.fao.org/api/v1/en/data/FBS?area=5000%3E&element=2610%2C2910&item=2918%2C2919&year=2023%2C2022%2C2021%2C2020%2C2019%2C2018%2C2017%2C2016%2C2015%2C2014"

headers = {"Authorization": f"Bearer { access_token }"}

# COMMAND ----------

# DBTITLE 1,Request Import & Export Data
response = requests.get(
    FBS_IM_EX_API,
    headers=headers,
    timeout=30
)

response.raise_for_status()

print("Status:", response.status_code)
print("Response:", response.text[:1000])

# COMMAND ----------

# DBTITLE 1,Save Raw Import & Export Data
FBS_IM_EX = response.json()

file_path = "/Volumes/workspace/byte_crew/raw/fbs_import_export.json"

with open(file_path, "w") as f:
    json.dump(FBS_IM_EX, f)

# COMMAND ----------

# DBTITLE 1,Load & Preview Import & Export Data
import pandas as pd

file_path = "/Volumes/workspace/byte_crew/raw/fbs_import_export.json"

with open(file_path, "r") as f:
    FBS_IM_EX = json.load(f)

import_export_df = pd.DataFrame(FBS_IM_EX["data"])

display(import_export_df)

# COMMAND ----------

# DBTITLE 1,Inspect Import & Export Schema
import_export_schema = pd.DataFrame({
    "column_name": import_export_df.columns,
    "data_type": import_export_df.dtypes.astype(str).values
})

display(import_export_schema)

# COMMAND ----------

# DBTITLE 1,Save Import & Export Schema
schema_path = "/Volumes/workspace/byte_crew/raw/fbs_import_export_schema.csv"

import_export_schema.to_csv(schema_path, index=False)