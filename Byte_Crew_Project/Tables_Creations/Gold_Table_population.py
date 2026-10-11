# Databricks notebook source
# MAGIC %md
# MAGIC Population Gold Table creation

# COMMAND ----------

gold_population = spark.table("workspace.byte_crew.silver_population ")
display(gold_population .head(5))

# COMMAND ----------

gold_population = (
    gold_population 
    .withColumnRenamed("value", "population_1000")
    .drop("unit")
)
display(gold_population .head(5))

# COMMAND ----------

# DBTITLE 1,Create gold_population table
gold_population.write.mode("overwrite").saveAsTable("workspace.byte_crew.gold_population")