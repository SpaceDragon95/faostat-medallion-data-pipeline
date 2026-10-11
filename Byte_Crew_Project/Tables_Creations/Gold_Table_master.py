# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Fact table 
# MAGIC - quick analysis of all tables
# MAGIC - added region column to gold_master
# MAGIC - gold_master table creation

# COMMAND ----------

# MAGIC %md
# MAGIC JOIN tables:
# MAGIC - gold_food_supply
# MAGIC - gold_production
# MAGIC - gold_import_export
# MAGIC - gold_supply_loss
# MAGIC - gold population

# COMMAND ----------

tables = [
    "gold_food_supply",
    "gold_production",
    "gold_import_export",
    "gold_population",
    "gold_supply_loss",
]

spark.sql("USE byte_crew")

for table in tables:
    count = spark.table(f"byte_crew.{table}").count()
    print(f"{table}: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC gold_production: unique country: 177
# MAGIC gold_food_supply: unique country: 178
# MAGIC gold_food_supply: unique country: 178

# COMMAND ----------

tables = [
    "gold_food_supply",
    "gold_import_export",
      "gold_supply_loss",
    "gold_population",
    "gold_production",
  
]

spark.sql("USE byte_crew")
for table in tables:
    # Use country_code (or country) to find unique occurrences
    unique_countries = (
        spark.table(f"byte_crew.{table}")
        .select("country_code")
        .dropna()
        .distinct()
        .count()
    )
    print(f"{table}: {unique_countries:,} unique countries")

# COMMAND ----------

tables = [
    "gold_food_supply",
    "gold_import_export",
      "gold_supply_loss",
    "gold_population",
    "gold_production",
  
]

spark.sql("USE byte_crew")
for table in tables:
    # Use country_code (or country) to find unique occurrences
    unique_countries = (
        spark.table(f"byte_crew.{table}")
        .select( "country_code")
        .dropna()
        .distinct()
        .count()
    )
    print(f"{table}: {unique_countries:,} unique ")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE byte_crew;
# MAGIC -- SELECT * FROM gold_production limit 5;
# MAGIC -- SELECT * FROM gold_food_supply WHERE country = 'Japan'
# MAGIC SELECT * FROM gold_production WHERE country_code = 128

# COMMAND ----------

gold_production = spark.table("byte_crew.gold_production")
gold_food_supply = spark.table("byte_crew.gold_food_supply")

extra_rows = gold_food_supply.join(
    gold_production, on="country_code", how="left_anti"
)

print(f"Extra rows count: {extra_rows.count():,}")
display(extra_rows)

# COMMAND ----------

# DBTITLE 1,food_supply comparison with population
gold_population = spark.table("byte_crew.gold_population")
gold_food_supply = spark.table("byte_crew.gold_food_supply")

extra_rows = gold_population.join(
    gold_food_supply, on="country_code", how="left_anti"
)

print(f"Extra rows count present in population table: {extra_rows.count():,}")
display(extra_rows)


# COMMAND ----------


display(extra_rows.select("country").distinct())


# COMMAND ----------

# DBTITLE 1,gold_master = Join all tables
from pyspark.sql import functions as F

join_keys = ["country_code","country", "year"]

# Start with food supply as the base driver table
gold_master = (
    spark.table("byte_crew.gold_food_supply")
    .join(spark.table("byte_crew.gold_import_export"), on=join_keys, how="left")
    .join(spark.table("byte_crew.gold_supply_loss"), on=join_keys, how="left")
    .join(spark.table("byte_crew.gold_production"), on=join_keys, how="left")
    .join(spark.table("byte_crew.gold_population"), on=join_keys, how="left")
)

# Sanity check row count: it should remain exactly 1,731
print(f"Master table rows: {gold_master.count():,}")

# COMMAND ----------

display(gold_master.head(10))

# COMMAND ----------

# DBTITLE 1,Count nulls
from pyspark.sql import functions as F

float_types = ("double", "float")

null_exprs = [
    F.sum(
        F.when(
            F.col(c).isNull() | (F.isnan(c) if t.lower() in float_types else F.lit(False)),
            1
        ).otherwise(0)
    ).alias(c)
    for c, t in gold_master.dtypes
]

null_counts = gold_master.select(null_exprs)
display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC -- there are 6 nulls in production columns because prod table had empty column for marshal 

# COMMAND ----------

# DBTITLE 1,list the null items
from functools import reduce
from pyspark.sql import functions as F

float_types = ("double", "float")

# Build a condition checking if each column is null (or NaN if numeric float)
conditions = [
    F.col(c).isNull() | (F.isnan(c) if t.lower() in float_types else F.lit(False))
    for c, t in gold_master.dtypes
]

# Combine all conditions with OR: cond1 | cond2 | cond3 ...
any_null_condition = reduce(lambda a, b: a | b, conditions)

# Filter and display rows containing any null
rows_with_nulls = gold_master.filter(any_null_condition)

display(rows_with_nulls)

# COMMAND ----------

# DBTITLE 1,Convert the null into 0
fill_values = {
    "fruit_prod_1000t": 0.0,
    "veg_prod_1000t": 0.0,
    "fruit_veg_prod_1000t": 0.0
}

gold_master = gold_master.fillna(fill_values)

# COMMAND ----------

display(gold_master)

# COMMAND ----------

# DBTITLE 1,cross-check for nulls
total_nulls = gold_master.filter(
    " OR ".join([f"`{c}` IS NULL" for c in gold_master.columns])
).count()

assert total_nulls == 0, f"Cross-check failed: {total_nulls} row(s) still contain nulls."
print("Cross-check passed: No null rows present.")

# COMMAND ----------

# DBTITLE 1,country list
df = gold_master
                 
country_list = (
    df.select("country_code", "country")
    .dropna()
    .distinct()
    .orderBy("country_code")
)
display(country_list)

# COMMAND ----------

# MAGIC %pip install country_converter

# COMMAND ----------

# DBTITLE 1,add region column
# import country_converter as coco
# from pyspark.sql import functions as F

# # Function to convert country name → UN region
# def get_region(country):
#     return coco.convert(names=country, to="UNregion")

# # Create PySpark UDF
# region_udf = F.udf(get_region, "string")

# # Add region column
# country_list = country_list.withColumn(
#     "region",
#     region_udf(F.col("country"))
# )

# display(country_list)

import country_converter as coco
from pyspark.sql import functions as F

region_fixes = {
    "Naoero": "Micronesia",
    "China, Taiwan Province of": "Eastern Asia"
}

def get_region(country):
    if not country:
        return None
    # Check manual overrides first
    if country in region_fixes:
        return region_fixes[country]
    
    # Fallback to country_converter
    res = coco.convert(names=country, to="UNregion", not_found=None)
    return res

# Register PySpark UDF
region_udf = F.udf(get_region, "string")

# Add region column
country_list = country_list.withColumn(
    "region",
    region_udf(F.col("country"))
)

display(country_list)

# COMMAND ----------

# DBTITLE 1,join gold_master with country list to add regions
regions = country_list.select("country_code", "region").distinct()
gold_master = gold_master.join(regions, on ="country_code", how="left")
display(gold_master)


# COMMAND ----------

print(f"total columns: {len(gold_master.columns)}")

# COMMAND ----------

gold_master.printSchema()

# COMMAND ----------

gold_master = gold_master.withColumnRenamed("supply_target", "adequacy_level")

# COMMAND ----------

display(gold_master.head(10))

# COMMAND ----------

target_cols = ["target_400g", "adequacy_level"]
remaining_cols = [c for c in gold_master.columns if c not in target_cols]
new_order = remaining_cols + target_cols
gold_master = gold_master.select(*new_order)
display(gold_master.head(5))

# COMMAND ----------

# DBTITLE 1,Save to table gold_master
gold_master.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_master")


# COMMAND ----------

# MAGIC %sql
# MAGIC USE byte_crew;
# MAGIC select * from gold_master limit 5