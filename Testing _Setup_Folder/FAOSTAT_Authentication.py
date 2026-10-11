# Databricks notebook source
# MAGIC %md
# MAGIC # Login and token retrivial automation

# COMMAND ----------

import requests

LOGIN_URL = "https://faostatservices.fao.org/api/v1/auth/login"

faostat_password = dbutils.secrets.get(
    scope = "Byte_Crew_FAOSTAT",
    key = "faostat_password"
    )

faostat_username = dbutils.secrets.get(
    scope="Byte_Crew_FAOSTAT",
    key="faostat_username"
)

login_payload = {
    "username": faostat_username,
    "password": faostat_password
}

response = requests.post(
    LOGIN_URL, 
    data=login_payload,
    timeout=30
    )

if response.status_code == 200:
    login_data = response.json()
    access_token = login_data["AuthenticationResult"]["AccessToken"]
    print("Successfully logged in and received token!")
else:
    print(f"Login failed. Status code: {response.status_code}")
    print(response.text)
