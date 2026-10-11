# Databricks notebook source
# MAGIC %md
# MAGIC - Which countries and regions appear below the 400 g/day reference?
# MAGIC - Which countries are closing the gap, stable, falling behind, or consistently above the reference over time?
# MAGIC

# COMMAND ----------

gold_food_supply1 = spark.table(
    "workspace.byte_crew.gold_food_supply"
).toPandas()

display(gold_food_supply1)

# COMMAND ----------



# COMMAND ----------


rq1 = (
    gold_food_supply1
    .groupby("country", as_index=False)
    .agg(
        avg_g_day=("fruit_veg_supply_g_day", "mean")
    )
)

rq1["avg_g_day"] = rq1["avg_g_day"].round(1)

rq1["target_400g"] = rq1["avg_g_day"].apply(
    lambda x: "Below 400g" if x < 400 else "Meets 400g"
)

rq1 = rq1.sort_values("avg_g_day")

display(rq1)

# COMMAND ----------

import country_converter as coco

def get_region(country):

    # Manual fixes for FAOSTAT country names
    region_fixes = {
        "Naoero": "Micronesia",
        "China, Taiwan Province of": "Eastern Asia"
    }

    # If country needs a manual fix, return it
    if country in region_fixes:
        return region_fixes[country]

    # Otherwise use country_converter
    return coco.convert(
        names=country,
        to="UNregion"
    )

gold_food_supply1["region"] = gold_food_supply1["country"].apply(get_region)

display(gold_food_supply1)

# COMMAND ----------

import plotly.express as px

map_df = (
    gold_food_supply1[
        [
            "country",
            "year",
            "fruit_veg_supply_g_day",
            "target_400g",
            "supply_target",
            "region"
        ]
    ]
    .sort_values("year")
    .copy()
)

fig = px.choropleth(
    map_df,
    locations="country",
    locationmode="country names",
    color="fruit_veg_supply_g_day",

    # Creates the year selector
    animation_frame="year",

    hover_name="country",
    hover_data={
        "region": True,
        "fruit_veg_supply_g_day": True,
        "target_400g": True,
        "supply_target": True
    },

    # Keep the scale consistent across years
    range_color=(0, 800),

    title="Fruit and Vegetable Supply by Country Over Time",

    labels={
        "fruit_veg_supply_g_day": "Supply (g/day)",
        "year": "Year"
    }
)

fig.update_geos(
    showcoastlines=True,
    showframe=False
)

fig.show()

# COMMAND ----------

gold_food_supply1["gap_from_400"] = (
    gold_food_supply1["fruit_veg_supply_g_day"] - 400
).round(1)

display(gold_food_supply1)

# COMMAND ----------

# MAGIC %md
# MAGIC closing the gap: 
# MAGIC * Above 400g: Supply started and ended at or above 400 g/person/day.
# MAGIC * Reached 400g: Supply started below 400g and ended at or above 400g.
# MAGIC * Closing the gap: Supply remained below 400g but increased toward the reference.
# MAGIC * Moving away: Supply decreased and moved farther from the 400g reference.
# MAGIC * Stable: Supply did not change between the start and end.

# COMMAND ----------

# Sort by country and year
df = gold_food_supply1.sort_values(["country", "year"])

# Get first and last value for each country
rq2 = df.groupby("country")["fruit_veg_supply_g_day"].agg(
    start_g_day="first",
    end_g_day="last"
).reset_index()


# Classify each country
def trend(row):

    if row["start_g_day"] >= 400 and row["end_g_day"] >= 400:
        return "Above 400g"

    elif row["start_g_day"] < 400 and row["end_g_day"] >= 400:
        return "Reached 400g"

    elif row["end_g_day"] > row["start_g_day"]:
        return "Closing the gap"

    elif row["end_g_day"] < row["start_g_day"]:
        return "Moving away"

    else:
        return "Stable"


rq2["trend"] = rq2.apply(trend, axis=1)

display(rq2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Country Trends in Fruit & Vegetable Supply Relative to the 400g/day Reference**

# COMMAND ----------

import plotly.express as px

trend_counts = (
    rq2["trend"]
    .value_counts()
    .reset_index()
)

trend_counts.columns = ["trend", "number_of_countries"]

fig = px.bar(
    trend_counts,
    x="trend",
    y="number_of_countries",
    text="number_of_countries",
    title="Countries by Fruit and Vegetable Supply Trend",
    labels={
        "trend": "Trend",
        "number_of_countries": "Number of Countries"
    }
)

fig.show()

# COMMAND ----------

moving_away_deficit = rq2[
    (rq2["trend"] == "Moving away") &
    (rq2["end_g_day"] < 400)
]

moving_away_deficit = moving_away_deficit.sort_values(
    "end_g_day"
)

display(moving_away_deficit)

# COMMAND ----------

import plotly.express as px

moving = rq2[rq2["trend"] == "Moving away"].copy()

moving["decline"] = moving["start_g_day"] - moving["end_g_day"]

fig = px.bar(
    moving.sort_values("decline"),
    x="decline",
    y="country",
    orientation="h",
    title="Countries Moving Away from the 400g/day Reference"
)

fig.show()