# Databricks notebook source
import pandas as pd
import json

# File paths
path_production = "/Volumes/workspace/byte_crew/raw/production_quantity.json"
path_FBS = "/Volumes/workspace/byte_crew/raw/fbs_supply.json"
path_import_export = "/Volumes/workspace/byte_crew/raw/fbs_import_export.json"
path_supply_loss = "/Volumes/workspace/byte_crew/raw/fbs_supply_loss.json"
path_population = "/Volumes/workspace/byte_crew/raw/pe_tpop.json"


# Read JSON files
with open(path_production, "r") as file:
    production_data = json.load(file)

with open(path_FBS, "r") as file:
    fbs_data = json.load(file)

with open(path_import_export, "r") as file:
    import_export_data = json.load(file)

with open(path_supply_loss, "r") as file:
    supply_loss_data = json.load(file)

with open(path_population, "r") as file:
    population_data = json.load(file)


# Convert to Pandas DataFrames
df_production = pd.DataFrame(production_data["data"])
df_FBS = pd.DataFrame(fbs_data["data"])
df_import_export = pd.DataFrame(import_export_data["data"])
df_supply_loss = pd.DataFrame(supply_loss_data["data"])
df_population = pd.DataFrame(population_data["data"])

# COMMAND ----------

df_FBS.head()

# COMMAND ----------

# Check rows and columns
df_FBS.shape

# COMMAND ----------

df_FBS.columns

# COMMAND ----------

df_FBS.describe()

# COMMAND ----------

# Show unique count for each column
for column in df_FBS.columns:
    print(column, ":", df_FBS[column].nunique())

# COMMAND ----------

# Show unique values for columns with 10 or fewer unique values
for column in df_FBS.columns:
    if df_FBS[column].nunique():
        print(f"\n{column}:")
        print(df_FBS[column].unique())

# COMMAND ----------

# Check for missing country names
print("Missing:", df_FBS["Area"].isna().sum())

# Check for duplicate country names caused by extra spaces
df_FBS["Area"] = df_FBS["Area"].str.strip()

# Check number of countries
print("Countries:", df_FBS["Area"].nunique())

# Display countries alphabetically
print(sorted(df_FBS["Area"].unique()))

# COMMAND ----------

# Find all Area names containing "China"
df_FBS[
    df_FBS["Area"].str.contains("China", case=False, na=False)
]["Area"].unique()

# COMMAND ----------


# China appears as four separate areas in the dataset:
# Hong Kong SAR, Macao SAR, mainland China, and Taiwan Province of.
# Keep these areas separate to preserve the original FAOSTAT geographic categories.
# Keep China's geographic areas separate.
# Per-capita food supply values should not be directly summed across areas.

# COMMAND ----------

# Keep columns needed for the food supply analysis
df_FBS_clean = df_FBS[
    ["Area", "Year", "Item", "Element", "Unit", "Value"]
].copy()

df_FBS_clean.head()

# COMMAND ----------

# Convert Value to numeric
df_FBS_clean["Value"] = pd.to_numeric(
    df_FBS_clean["Value"],
    errors="coerce"
)

# Convert kg/person/year to grams/person/day
df_FBS_clean["g/person/day"] = (
    df_FBS_clean["Value"] * 1000 / 365
)

# Round to 2 decimal places
df_FBS_clean["g/person/day"] = df_FBS_clean["g/person/day"].round(2)

df_FBS_clean.head()

# COMMAND ----------

# Rename Value to make its meaning clearer
df_FBS_clean = df_FBS_clean.rename(
    columns={"Value": "Food Supply kg/person/year"}
)

# COMMAND ----------

df_FBS_clean

# COMMAND ----------

# Add a status column based on the 400 g/person/day target
df_FBS_clean["Status"] = df_FBS_clean["g/person/day"].apply(
    lambda x: "400g or more" if x >= 400 else "Below 400g"
)

df_FBS_clean

# COMMAND ----------

# Group by country and year and combine Fruits + Vegetables
df_FBS_grouped = (
    df_FBS_clean
    .groupby(["Area", "Year"], as_index=False)["g/person/day"]
    .sum()
)

# Rename the combined column
df_FBS_grouped = df_FBS_grouped.rename(
    columns={"g/person/day": "Fruit + Vegetable g/person/day"}
)

# Add 400g target status
df_FBS_grouped["Status"] = df_FBS_grouped[
    "Fruit + Vegetable g/person/day"
].apply(
    lambda x: "400g or more" if x >= 400 else "Below 400g"
)

df_FBS_grouped

# COMMAND ----------

# DBTITLE 1,comparing data
# MAGIC %sql
# MAGIC Use byte_crew;
# MAGIC SELECT * from gold_food_supply WHERE country = "Afghanistan" AND year = 2014;

# COMMAND ----------

import plotly.express as px

# Create an animated world map
fig = px.choropleth(
    df_FBS_grouped,
    locations="Area",
    locationmode="country names",
    color="Status",
    hover_name="Area",
    hover_data={
        "Year": True,
        "Fruit + Vegetable g/person/day": ":.2f",
        "Status": True
    },
    animation_frame="Year",
    title="Fruit and Vegetable Supply Compared with the 400g/Day Target"
)

fig.show()

# COMMAND ----------

df_production.head()

# COMMAND ----------

df_production.shape

# COMMAND ----------

# Get common Area-Year-Item combinations
intersection = pd.merge(
    df_FBS_grouped[["Area", "Year"]],
    df_production[["Area", "Year"]],
    on=["Area", "Year"],
    how="inner"
).drop_duplicates()

display(intersection)

print("Intersection shape:", intersection.shape)

# COMMAND ----------

df_fbs_production = pd.merge(
    df_FBS_grouped,
    df_production,
    on=["Area", "Year"],
    how="inner",
    suffixes=("_supply", "_production")
)

print(df_fbs_production.shape)

# COMMAND ----------

df_fbs_production.head()

# COMMAND ----------

df_fbs_production.isnull().sum()

# COMMAND ----------

df_production_fbs = pd.merge(
    df_production,        # LEFT table
    df_FBS_grouped,       # RIGHT table
    on=["Area", "Year"],
    how="inner",
    suffixes=("_production", "_supply")
)

# COMMAND ----------

df_production_fbs.head()

# COMMAND ----------

df_production_fbs = df_production_fbs[
    [
        "Area",
        "Year",
        "Item",
        "Value",
        "Unit",
        "Fruit + Vegetable g/person/day",
        "Status"
    ]
].copy()

# COMMAND ----------

df_production_fbs = df_production_fbs.rename(
    columns={
        "Value": "Production Value"
    }
)

# COMMAND ----------

df_production_fbs.head()

# COMMAND ----------



# COMMAND ----------

df_production_fbs["Production Value"] = pd.to_numeric(
    df_production_fbs["Production Value"],
    errors="coerce"
)

# COMMAND ----------

production_comparison = (
    df_production_fbs
    .groupby("Status")["Production Value"]
    .median()
    .reset_index()
)

display(production_comparison)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC _you can copy and paste each question above the cell you work to find answer_

# COMMAND ----------

# MAGIC %md
# MAGIC Main Question: Which countries have the largest gap between fruit-and-vegetable availability and the 400 g/day reference, and what food-system factors characterize those gaps?
# MAGIC
# MAGIC **RQ 1**: Where?: Which countries and regions appear below the 400 g/day reference? ✅
# MAGIC
# MAGIC **RQ 2**: Trend: Which countries are closing the gap, stable, falling behind, or consistently above the reference over time?
# MAGIC
# MAGIC **RQ 3**: Patterns: What patterns characterize countries with the largest fruit-and-vegetable availability deficits?  What patterns in production, population, trade or food utilization appear alongside large or small gaps?
# MAGIC
# MAGIC **RQ 4**: Learn: Which comparable countries achieve better fruit-and-vegetable availability and could serve as useful benchmarks for further investigation?
# MAGIC **Main KPI**
# MAGIC
# MAGIC 400G Availability Gap = Apparent Fruit & Vegetable Availability − 400 g/person/day
# MAGIC
# MAGIC **Supporting KPI** Supply Adequacy % = Apparent F&V Availability ÷ 400 × 100
# MAGIC
# MAGIC **Other Possible Measures**
# MAGIC Annual supply gap; production growth; supply growth; imports; exports; population trends; regional averages; utilization/loss indicators where appropriate.
# MAGIC
# MAGIC **_Possible Country Categories_**
# MAGIC Above Reference / Near Reference / Moderate Gap / Large Gap. Final thresholds should be decided after examining the dataset.
# MAGIC
# MAGIC _**Possible Trend Categories**_
# MAGIC Closing the Gap / Stable / Falling Behind / Consistently Adequate
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC