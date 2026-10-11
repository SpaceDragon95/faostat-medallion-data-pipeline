# Databricks notebook source
import getpass

faostat_password = getpass.getpass("Enter your FAOSTAT password: ")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

w.secrets.put_secret(
    scope="Byte_Crew_FAOSTAT",
    key="faostat_password",
    string_value=faostat_password,
)

# COMMAND ----------

dbutils.secrets.list("Byte_Crew_FAOSTAT")

# COMMAND ----------

