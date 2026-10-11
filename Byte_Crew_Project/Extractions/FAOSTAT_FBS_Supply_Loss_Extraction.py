# Databricks notebook source
# MAGIC %md
# MAGIC # Supply & Loss Extraction

# COMMAND ----------

# DBTITLE 1,Run FAOSTAT Authentication
# MAGIC %run "/Workspace/Shared/Testing _Setup_Folder/FAOSTAT_Authentication"

# COMMAND ----------

# DBTITLE 1,Configure Supply & Loss API Request
import requests
import json

FBS_SL_API = "https://faostatservices.fao.org/api/v1/en/data/FBS?area=5000%3E&element=2300%2C2120&item=2918%2C2919&year=2023%2C2022%2C2021%2C2020%2C2019%2C2018%2C2017%2C2016%2C2015%2C2014"

headers = {"Authorization": f"Bearer { access_token }"}

# COMMAND ----------

# DBTITLE 1,Request Supply & Loss Data
response = requests.get(
    FBS_SL_API,
    headers=headers,
    timeout=30
)

response.raise_for_status()

print("Status:", response.status_code)
print("Response:", response.text[:1000])

# COMMAND ----------

# DBTITLE 1,Save Raw Supply & Loss Data
FBS_SL = response.json()

file_path = "/Volumes/workspace/byte_crew/raw/fbs_supply_loss.json"

with open(file_path, "w") as f:
    json.dump(FBS_SL, f)
    

# COMMAND ----------

# DBTITLE 1,Load & Preview Supply & Loss Data
import pandas as pd

file_path = "/Volumes/workspace/byte_crew/raw/fbs_supply_loss.json"

with open(file_path, "r") as f:
    FBS_SL = json.load(f)

fbs_sl_df = pd.DataFrame(FBS_SL["data"])

display(fbs_sl_df)

# COMMAND ----------

# DBTITLE 1,Inspect Supply & Loss Schema
fbs_sl_schema = pd.DataFrame({
    "column_name": fbs_sl_df.columns,
    "data_type": fbs_sl_df.dtypes.astype(str).values
})

display(fbs_sl_schema)

# COMMAND ----------

schema_path = "/Volumes/workspace/byte_crew/raw/fbs_supply_loss.csv"

fbs_sl_schema.to_csv(schema_path, index=False)