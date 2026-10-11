# Databricks notebook source
# MAGIC %md
# MAGIC # Food Supply Data Extraction

# COMMAND ----------

# DBTITLE 1,Run FAOSTAT Authentication
# MAGIC %run "/Workspace/Shared/Testing _Setup_Folder/FAOSTAT_Authentication"

# COMMAND ----------

# DBTITLE 1,Configure Food Supply API Request
import requests
import json

FBS_API = "https://faostatservices.fao.org/api/v1/en/data/FBS?area=5000%3E&element=645&item=2918%2C2919&year=2023%2C2022%2C2021%2C2020%2C2019%2C2018%2C2017%2C2016%2C2015%2C2014"

headers = { "Authorization": f"Bearer { access_token }"}

# COMMAND ----------

# DBTITLE 1,Request Population Data
response = requests.get(
    FBS_API,
    headers=headers,
    timeout=30
)

response.raise_for_status()

print("Status:", response.status_code)
print("Response:", response.text[:1000])

# COMMAND ----------

# DBTITLE 1,Save Raw Food Supply Data
FBS_SUPPLY = response.json()

file_path = "/Volumes/workspace/byte_crew/raw/fbs_supply.json"

with open(file_path, "w") as f:
  json.dump(FBS_SUPPLY, f)


# COMMAND ----------

# DBTITLE 1,Load & Preview Data/ Inspect Schema
import json
import pandas as pd

file_path = "/Volumes/workspace/byte_crew/raw/fbs_supply.json"

with open(file_path, "r") as f:
    FBS_SUPPLY = json.load(f)

supply_df = pd.DataFrame(FBS_SUPPLY["data"])

supply_schema = pd.DataFrame({
    "column_name": supply_df.columns,
    "data_type": supply_df.dtypes.astype(str).values
})

display(supply_schema)

# COMMAND ----------

# DBTITLE 1,Save Food Supply Schema
schema_path = "/Volumes/workspace/byte_crew/raw/fbs_supply_schema.csv"

supply_schema.to_csv(schema_path, index=False)