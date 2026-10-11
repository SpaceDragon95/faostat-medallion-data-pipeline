# Databricks notebook source
# MAGIC %md
# MAGIC # Population Data Extraction

# COMMAND ----------

# DBTITLE 1,Run FAOSTAT Authentication
# MAGIC %run "/Workspace/Shared/Testing _Setup_Folder/FAOSTAT_Authentication"

# COMMAND ----------

# DBTITLE 1,Configure Population API Request
import requests
import json

PE_API = "https://faostatservices.fao.org/api/v1/en/data/OA?area=5000%3E&element=511&item=3010&year=2023%2C2022%2C2021%2C2020%2C2019%2C2018%2C2017%2C2016%2C2015%2C2014"

headers = {"Authorization": f"Bearer { access_token }"}

# COMMAND ----------

# DBTITLE 1,Request Population Data
response = requests.get(
    PE_API,
    headers=headers,
    timeout=30
)

response.raise_for_status()

print("Status:", response.status_code)
print("Response:", response.text[:1000])

# COMMAND ----------

# DBTITLE 1,Save Raw Population Data
PE_TPOP = response.json()

file_path = "/Volumes/workspace/byte_crew/raw/pe_tpop.json"

with open(file_path, "w") as f:
    json.dump(PE_TPOP, f)


# COMMAND ----------

# DBTITLE 1,Load & Preview Population Data
import json
import pandas as pd

file_path = "/Volumes/workspace/byte_crew/raw/pe_tpop.json"

with open(file_path, "r") as f:
    PE_TPOP = json.load(f)

tpop_df = pd.DataFrame(PE_TPOP["data"])

display(tpop_df)

# COMMAND ----------

# DBTITLE 1,Inspect Population Schema
tpop_schema = pd.DataFrame({
    "column_name": tpop_df.columns,
    "data_type": tpop_df.dtypes.astype(str).values
})

display(tpop_schema)

# COMMAND ----------

# DBTITLE 1,Save Population Schema
schema_path = "/Volumes/workspace/byte_crew/raw/total_pop_schema.csv"

tpop_schema.to_csv(schema_path, index=False)