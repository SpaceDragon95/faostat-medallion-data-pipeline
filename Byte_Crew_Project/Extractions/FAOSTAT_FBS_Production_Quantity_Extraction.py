# Databricks notebook source
# MAGIC %md
# MAGIC # Production Quantity Extraction

# COMMAND ----------

# MAGIC %run "/Workspace/Shared/Testing _Setup_Folder/FAOSTAT_Authentication"

# COMMAND ----------

import requests
import json
import pandas

FBS_PQ_API = "https://faostatservices.fao.org/api/v1/en/data/FBS?area=5000%3E&element=2510&item=2918%2C2919&year=2023%2C2022%2C2021%2C2020%2C2019%2C2018%2C2017%2C2016%2C2015%2C2014"

headers = {"Authorization": f"Bearer { access_token }"}

# COMMAND ----------

response = requests.get(
    FBS_PQ_API,
    headers=headers,
    timeout=30
)

response.raise_for_status()

print("Status:", response.status_code)
print("Response:", response.text[:1000])

# COMMAND ----------

FBS_PQ = response.json()

file_path = "/Volumes/workspace/byte_crew/raw/production_quantity.json"

with open(file_path, "w") as f:
    json.dump(FBS_PQ, f)

# COMMAND ----------

import pandas as pd

file_path = "/Volumes/workspace/byte_crew/raw/production_quantity.json"

with open(file_path, "r") as f:
    FBS_PQ = json.load(f)

fbs_pq_df = pd.DataFrame(FBS_PQ["data"])

display(fbs_pq_df)

# COMMAND ----------

fbs_pq_schema = pd.DataFrame({
    "column_name": fbs_pq_df.columns,
    "data_type": fbs_pq_df.dtypes.astype(str).values
})

display(fbs_pq_schema)

# COMMAND ----------

schema_path = "/Volumes/workspace/byte_crew/raw/fbs_pq_schema.csv"

fbs_pq_schema.to_csv(schema_path, index=False)