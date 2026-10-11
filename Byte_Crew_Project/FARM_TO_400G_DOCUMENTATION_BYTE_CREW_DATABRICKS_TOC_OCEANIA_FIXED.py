# Databricks notebook source
# MAGIC %md
# MAGIC # 🌱 FROM FARM TO 400G 🍎
# MAGIC
# MAGIC ### Can our food systems supply enough fruits and vegetables for a healthy plate?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table of Contents
# MAGIC
# MAGIC Databricks automatically creates a **clickable Table of Contents** from the Markdown headings in this notebook.  
# MAGIC Use the **Table of Contents icon on the left side of the notebook** to jump directly to any section.
# MAGIC
# MAGIC 1. **Project Overview**
# MAGIC 2. **Data Preparation & Quality**
# MAGIC 3. **Research-Question Analysis**
# MAGIC    - 3.1 **RQ1 - Where?**
# MAGIC    - 3.2 **RQ2 - Progress?**
# MAGIC    - 3.3 **RQ3 - Patterns?**
# MAGIC 4. **Robustness & Interpretation**
# MAGIC 5. **Implications & Conclusion**
# MAGIC 6. **Appendix A - Reusable Analytical Views**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Project Overview
# MAGIC
# MAGIC ### 1.1 Problem, Objective & Research Questions
# MAGIC
# MAGIC [WHO healthy-diet guidance](https://www.who.int/news-room/fact-sheets/detail/healthy-diet) recommends at least **400 g/day of fruits and vegetables for individuals older than 10 years**. As part of a healthy, balanced diet, adequate fruit and vegetable intake is associated with a lower risk of cardiovascular disease and certain cancers.
# MAGIC
# MAGIC Though the 400 g/day recommendation sets a clear dietary target, achieving it also depends on whether our food systems can support it. That led us to look beyond dietary guidance and examine the supply side of the equation:
# MAGIC
# MAGIC > **Can our food systems supply enough fruits and vegetables for a healthy plate?**
# MAGIC
# MAGIC **Objective:** Measure the gap between national fruit-and-vegetable availability and the **400 g/person/day healthy-diet reference**, then investigate the food-system patterns associated with countries that fall short.
# MAGIC
# MAGIC
# MAGIC Using FAOSTAT Food Balance data for **2014–2023**, Byte Crew examines three main connected research questions:
# MAGIC
# MAGIC 1. **RQ1 - WHERE?** Where was F&V availability typically above or below the 400 g/day reference across the study period?
# MAGIC 2. **RQ2 - PROGRESS?** How did countries move relative to the reference from their first to last available observation?
# MAGIC 3. **RQ3 - PATTERNS?** What production, import, and recorded food-loss patterns distinguish countries whose period-average availability remained below 400 g/day?
# MAGIC
# MAGIC > **Availability ≠ consumption**  
# MAGIC > **Correlation ≠ causation**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Key Results at a Glance
# MAGIC
# MAGIC **57 of 178 countries (32.0%)** had period-average F&V availability below the **400 g/person/day reference**, while **121 countries** met or exceeded it on average. This shows that the food-supply picture remained uneven across countries during 2014–2023.
# MAGIC
# MAGIC **Countries followed different endpoint trajectories:** **111** started and ended at or above 400 g/day, **12** reached the reference, **30** were closing the gap, and **25** were moving away. This shows that countries were not all progressing in the same direction.
# MAGIC
# MAGIC **Below-400 countries showed the clearest differences in production and imports.** Median F&V production was **97.2 vs 215.3 kg/person/year** and imports were **11.8 vs 99.7 kg/person/year**, while recorded loss share differed much less (**8.6% vs 7.1%**). Production also showed the clearest positive association with availability (**r = 0.46**).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Analytical Methodology at a Glance
# MAGIC
# MAGIC We used a **country-first, exploratory food-systems approach** with FAOSTAT Food Balance data covering **2014–2023**.
# MAGIC
# MAGIC During data preparation, fruit and vegetable supply was combined and converted to **g/person/day**. The analysis then used these prepared measures to examine each country’s **typical position relative to the 400 g/day reference**, its **first-to-last progress**, and the **food-system patterns associated with countries remaining below 400 g/day**.
# MAGIC
# MAGIC #### 1.3.1 Core Calculations
# MAGIC
# MAGIC **Period-average F&V availability**  
# MAGIC `Mean annual F&V availability (g/person/day) across available years`
# MAGIC
# MAGIC **400G Gap**  
# MAGIC `F&V availability − 400`
# MAGIC
# MAGIC Countries were classified as **Below 400g** or **Meets 400g** using their period-average availability. Progress was assessed using each country’s **first and last available observations**, while production, imports, and recorded loss share were examined using **available cases for each indicator**.
# MAGIC
# MAGIC The analytical workflow is presented in three stages:
# MAGIC
# MAGIC - **Section I - Data Cleaning & Preparation:** documenting the Bronze–Silver–Gold pipeline, cleaning rules, data-quality findings, and final analytical dataset.
# MAGIC - **Section II - Python Analysis & Key Findings:** examining where countries stood relative to 400 g/day, how they progressed over time, and what food-system patterns distinguished below-400 countries.
# MAGIC - **Section III - Robustness, Interpretation & Implications:** reviewing analytical safeguards, limitations, and the broader food-system meaning of the findings.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Preparation & Quality
# MAGIC
# MAGIC ### 2.1 Bronze → Silver → Gold Pipeline
# MAGIC
# MAGIC The project follows a simple **Bronze → Silver → Gold** data pipeline. 
# MAGIC - **Bronze** represents the original FAOSTAT Food Balance source extracts as received/downloaded. 
# MAGIC - **Silver** contains the curated country and regional FAOSTAT records retained for the **2014–2023** project scope: Fruits - Excluding Wine, Vegetables, and the selected food-system elements. 
# MAGIC - **Gold** reorganizes the country-level Silver data to a country-year analytical structure, combines fruit and vegetable measures where the required components are available, derives the 400 g/day benchmark and progress measures, and adds the food-system indicators used in RQ3. 
# MAGIC
# MAGIC
# MAGIC ### 2.2 Data Cleaning Rules
# MAGIC
# MAGIC - Restricted the analysis period to **2014–2023**.
# MAGIC - Retained **Fruits - Excluding Wine** and **Vegetables** as the two F&V item groups.
# MAGIC - Used **Food supply quantity (kg/capita/yr)** as the primary availability measure.
# MAGIC - Retained the supporting elements needed to investigate food-system patterns, including **production, imports, exports, domestic supply, food, losses, processing, and residuals** at the Silver stage.
# MAGIC - Kept **country records separate from official World and regional aggregates** to avoid geographic overlap and double counting.
# MAGIC - Excluded the aggregate **China** record from country-level analysis because it overlaps with separately reported China areas.
# MAGIC - Required both fruit and vegetable components before calculating a combined F&V total for an element.
# MAGIC - Preserved genuine **missing values as missing** rather than converting absent records to zero.
# MAGIC - Converted combined F&V availability from **kg/capita/year to g/person/day** using `(kg/cap/year × 1000) ÷ 365`.
# MAGIC - Derived the signed **Gap to 400**, annual benchmark status, period-average benchmark status, and first-to-last progress fields.
# MAGIC - Retained coverage and QA fields so incomplete time coverage or food-system indicators remain visible during analysis.
# MAGIC - Retained unusual observations unless the source evidence indicated an error; magnitude alone was not used as a reason for deletion.
# MAGIC
# MAGIC ### 2.3 Key Data Quality Findings
# MAGIC
# MAGIC | **Data quality finding** | **Evidence found** | **Treatment / decision** |
# MAGIC |---|---|---|
# MAGIC | **China geographic overlap** | The Silver country extract contained an aggregate `China` record alongside separately reported China areas. | Excluded the aggregate China record from country-level analysis, reducing the analytical geography from **179 to 178 countries/areas**. |
# MAGIC | **Uneven country-year coverage** | Of the 178 analytical countries/areas, **168 have all 10 years**, while **10 have partial coverage** within 2014–2023. | No imputation was applied. RQ1 averages each country's available years and RQ2 uses its first and last available observations; coverage status remains explicit. |
# MAGIC | **Missingness is mainly structural** | The Silver extract contains few ordinary blank cells; missingness often appears as an **absent country-year-item record**. | Combined indicators are calculated only when the required fruit and vegetable components exist; otherwise the result remains missing. |
# MAGIC | **Production has limited component gaps** | Production is not complete for every country-year and F&V component, including a small number of fully missing or one-component country-years. | Missing production is preserved as missing rather than interpreted as zero. |
# MAGIC | **Imports are comparatively complete** | Import quantity contains both fruit and vegetable components for all **1,731 analytical country-year pairs**. | Retained as a core RQ3 indicator and standardized to kg/person/year. |
# MAGIC | **Loss data contain some component gaps** | Loss records cover all analytical country-years, but a small number lack one of the two F&V components. | Combined loss measures remain missing when both components are not available; RQ3 uses indicator-specific available cases. |
# MAGIC | **Processing is substantially incomplete** | Processing is available for fewer countries and often contains only one F&V component; only **316 country-years** had both fruit and vegetable processing components in the supplied Silver data. | Processing was retained during exploration but not used as a primary RQ3 comparison indicator. |
# MAGIC | **FAOSTAT source flags** | The supplied Silver observations carry the FAOSTAT **`E = Estimated value`** flag. | Source flags were retained during preparation and the results are interpreted as descriptive estimates rather than exact observed quantities. |
# MAGIC | **Official regional aggregates differ from country classifications** | The regional Silver file contains official FAOSTAT aggregates for World, Africa, Americas, Asia, Europe, and Oceania, while the final Gold dataset is country-based. | Regional aggregates were kept separate from the country analytical dataset and are not mixed into RQ1–RQ3 country counts. |
# MAGIC
# MAGIC ### 2.4 Final Analytical Dataset
# MAGIC
# MAGIC After these checks, the consolidated analytical dataset contains **1,731 country-year observations across 178 countries/areas**. It includes annual F&V availability and gap measures, period-average benchmark fields for RQ1, first-to-last progress fields for RQ2, and period-average production, imports, and recorded loss-share measures for RQ3. Country-period fields are repeated across each country's annual rows so both annual exploration and one-row-per-country analysis can be produced from the same file. Missing food-system indicators remain missing, and RQ3 therefore uses **indicator-specific available cases** rather than a single universal complete-case sample.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Analysis Setup & Validation
# MAGIC
# MAGIC We imported the required libraries, defined the shared chart styling, loaded the consolidated analytical dataset, and performed a compact structural validation. 

# COMMAND ----------


from pyspark.sql import functions as F
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

GOLD_PATH = "/Volumes/workspace/byte_crew/raw/Faostat_Fbs_Gold_Dataset.csv"
BENCHMARK = 400.0

# Visual style for all charts.

FOCUS_COLOR = "#7A5AA6"
FOCUS_DARK = "#5D3E8A"
FOCUS_LIGHT = "#B9A2D4"
FOCUS_PALE = "#E8E0F1"
CONTEXT_COLOR = "#B8B8B0"
CONTEXT_DARK = "#777777"
TEXT_COLOR = "#333333"
BACKGROUND = "white"

plt.rcParams.update({
    "figure.facecolor": BACKGROUND,
    "axes.facecolor": BACKGROUND,
    "axes.edgecolor": "#8A8A84",
    "axes.labelcolor": TEXT_COLOR,
    "xtick.color": TEXT_COLOR,
    "ytick.color": TEXT_COLOR,
    "font.size": 11
})

gold = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(GOLD_PATH)
)

qa_basic = (
    gold.agg(
        F.count("*").alias("rows"),
        F.countDistinct("Country").alias("countries"),
        F.min("Year").alias("min_year"),
        F.max("Year").alias("max_year")
    )
    .first()
)

print(f"Gold rows: {qa_basic['rows']:,}")
print(f"Unique countries: {qa_basic['countries']:,}")
print(f"Year range: {qa_basic['min_year']}–{qa_basic['max_year']}")

assert qa_basic["rows"] == 1731, "Unexpected Gold row count."
assert qa_basic["countries"] == 178, "Unexpected country count."
assert qa_basic["min_year"] == 2014 and qa_basic["max_year"] == 2023, "Unexpected year range."

country_summary = (
    gold.select(
        "M49",
        "Country",
        "Years_Observed",
        "Coverage_Status",
        "Start_Year",
        "End_Year",
        "Period_Avg_FV_g_person_day",
        "Period_Gap_to_400_g_day",
        "Period_Shortfall_g_day",
        "Period_Benchmark_Status",
        "Start_FV_g_person_day",
        "End_FV_g_person_day",
        "Change_g_person_day",
        "Progress_Category",
        "Period_Avg_Production_kg_person_year",
        "Period_Avg_Imports_kg_person_year",
        "Period_Avg_Loss_Share_pct",
        "Pattern_Years_Complete",
        "Pattern_Period_Status",
        "RQ3_Eligible"
    )
    .dropDuplicates(["Country"])
)

assert country_summary.count() == 178, "Country summary should contain 178 unique countries."

display(gold.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Research-Question Analysis
# MAGIC
# MAGIC The analytical sequence is:
# MAGIC
# MAGIC **Where is the period-average gap?** → **How did countries move?** → **What food-system patterns distinguish countries still below 400?**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 RQ1 - WHERE?
# MAGIC #### Where was F&V availability typically above or below 400 g/day during 2014–2023?
# MAGIC
# MAGIC RQ1 uses each country's **period-average F&V availability**.
# MAGIC
# MAGIC This avoids treating one year as representative of the entire study period and keeps countries with partial coverage in the analysis using their **available observations within 2014–2023**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1.1 Period-Average Benchmark Classification
# MAGIC
# MAGIC We summarized the period-average benchmark status already calculated in the analytical dataset.
# MAGIC
# MAGIC **Analytical unit:** one country  
# MAGIC **Time basis:** each country's available observations within 2014–2023  
# MAGIC **Benchmark:** 400 g/person/day
# MAGIC
# MAGIC The code:
# MAGIC 1. groups the one-row-per-country table by `Period_Benchmark_Status`;
# MAGIC 2. counts countries in each benchmark group;
# MAGIC 3. calculates each group's share of the 178-country analytical base;
# MAGIC 4. independently recounts the Below-400 group;
# MAGIC 5. asserts that the validated Below-400 count remains **57**.
# MAGIC
# MAGIC The assertion is a reproducibility safeguard: if a future file produces a different count, the notebook stops rather than silently presenting outdated headline results.

# COMMAND ----------

# We summarized period-average benchmark status at one row per country.

rq1_counts = (
    country_summary
    .groupBy("Period_Benchmark_Status")
    .count()
    .withColumn(
        "Percent_of_Countries",
        F.round(F.col("count") / F.lit(178) * 100, 1)
    )
    .orderBy("Period_Benchmark_Status")
)

display(rq1_counts)

below_count = (
    country_summary
    .filter(F.col("Period_Benchmark_Status") == "Below 400g")
    .count()
)

assert below_count == 57, "RQ1 below-400 count no longer matches the validated Gold dataset."

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1.2 Geographic Distribution
# MAGIC
# MAGIC We mapped where period-average F&V availability was higher or lower across countries using the same one-country analytical view as the RQ1 summary.
# MAGIC
# MAGIC The map includes small **boxed regional guide labels** for orientation only. The analysis itself remains **country-level** and does not use regional aggregates.

# COMMAND ----------

# We mapped the same period-average country sample and added soft boxed region labels.

import plotly.express as px
import plotly.graph_objects as go

rq1_map_pd = (
    country_summary
    .select(
        "Country",
        "Period_Avg_FV_g_person_day",
        "Period_Gap_to_400_g_day",
        "Period_Benchmark_Status"
    )
    .toPandas()
)

fig = px.choropleth(
    rq1_map_pd,
    locations="Country",
    locationmode="country names",
    color="Period_Avg_FV_g_person_day",
    hover_name="Country",
    hover_data={
        "Period_Avg_FV_g_person_day": ":.1f",
        "Period_Gap_to_400_g_day": ":.1f",
        "Period_Benchmark_Status": True
    },
    color_continuous_scale=[
        [0.00, "#5D3E8A"],
        [0.20, "#8068A0"],
        [0.40, "#B7A8C5"],
        [0.49, "#D9D2DF"],
        [0.50, "#E6E6E1"],
        [0.65, "#D4D4CE"],
        [0.82, "#B8B8B1"],
        [1.00, "#96968F"]
    ],
    range_color=(0, 800)
)

fig.update_geos(
    showcoastlines=True,
    coastlinecolor="#D8D8D3",
    showframe=False,
    showcountries=True,
    countrycolor="#F5F5F1",
    showland=True,
    landcolor="#F1F1EE",
    showocean=True,
    oceancolor="white"
)

fig.update_layout(
    paper_bgcolor="white",
    plot_bgcolor="white",
    margin=dict(l=0, r=0, t=15, b=0),
    coloraxis_colorbar=dict(
        title="g/person/day",
        tickvals=[0, 200, 400, 600, 800],
        thickness=12,
        len=0.72,
        outlinewidth=0,
        tickfont=dict(color="#6D6D68")
    )
)

region_annotations = [
    dict(text="<b>Americas</b>", x=0.16, y=0.48),
    dict(text="<b>Europe</b>",   x=0.50, y=0.71),
    dict(text="<b>Africa</b>",   x=0.50, y=0.45),
    dict(text="<b>Asia</b>",     x=0.70, y=0.66),
    dict(text="<b>Oceania</b>",  x=0.82, y=0.34),
]

for label in region_annotations:
    fig.add_annotation(
        x=label["x"],
        y=label["y"],
        xref="paper",
        yref="paper",
        text=label["text"],
        showarrow=False,
        font=dict(size=11, color="#666661"),
        bgcolor="rgba(242,242,239,0.95)",
        bordercolor="#DDDDD7",
        borderwidth=0.7,
        borderpad=4
    )

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1.3 Annual Exploration
# MAGIC
# MAGIC The period-average map provides the main RQ1 classification. The interactive view below adds the annual dimension: use the **year slider** to move through 2014–2023 and observe how country-level F&V availability changes over time.
# MAGIC
# MAGIC The same boxed regional guide labels are retained here to make the animated geography easier to read.

# COMMAND ----------

# We added an annual map slider with the same soft boxed region labels.

annual_map_pd = (
    gold.select(
        "Country",
        "Year",
        "FV_g_person_day",
        "Gap_to_400_g_day",
        "Annual_Benchmark_Status",
        "Period_Benchmark_Status"
    )
    .orderBy("Year", "Country")
    .toPandas()
)

annual_fig = px.choropleth(
    annual_map_pd,
    locations="Country",
    locationmode="country names",
    color="FV_g_person_day",
    animation_frame="Year",
    hover_name="Country",
    hover_data={
        "FV_g_person_day": ":.1f",
        "Gap_to_400_g_day": ":.1f",
        "Annual_Benchmark_Status": True,
        "Period_Benchmark_Status": True
    },
    range_color=(0, 800),
    color_continuous_scale=[
        [0.00, "#5D3E8A"],
        [0.20, "#8068A0"],
        [0.40, "#B7A8C5"],
        [0.49, "#D9D2DF"],
        [0.50, "#E6E6E1"],
        [0.65, "#D4D4CE"],
        [0.82, "#B8B8B1"],
        [1.00, "#96968F"]
    ],
    labels={
        "FV_g_person_day": "F&V availability (g/person/day)",
        "Year": "Year"
    }
)

annual_fig.update_geos(
    showcoastlines=True,
    coastlinecolor="#D8D8D3",
    showframe=False,
    showcountries=True,
    countrycolor="#F5F5F1",
    showland=True,
    landcolor="#F1F1EE",
    showocean=True,
    oceancolor="white"
)

annual_fig.update_layout(
    paper_bgcolor="white",
    plot_bgcolor="white",
    margin=dict(l=0, r=0, t=10, b=0),
    transition_duration=0,
    coloraxis_colorbar=dict(
        title="g/person/day",
        tickvals=[0, 200, 400, 600, 800],
        thickness=12,
        len=0.72,
        outlinewidth=0,
        tickfont=dict(color="#6D6D68")
    )
)

region_annotations = [
    dict(text="<b>Americas</b>", x=0.16, y=0.48),
    dict(text="<b>Europe</b>",   x=0.50, y=0.71),
    dict(text="<b>Africa</b>",   x=0.50, y=0.45),
    dict(text="<b>Asia</b>",     x=0.70, y=0.66),
    dict(text="<b>Oceania</b>",  x=0.82, y=0.34),
]

for label in region_annotations:
    annual_fig.add_annotation(
        x=label["x"],
        y=label["y"],
        xref="paper",
        yref="paper",
        text=label["text"],
        showarrow=False,
        font=dict(size=11, color="#666661"),
        bgcolor="rgba(242,242,239,0.95)",
        bordercolor="#DDDDD7",
        borderwidth=0.7,
        borderpad=4
    )

annual_fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1.4 Key Finding
# MAGIC
# MAGIC **57 of 178 countries (32.0%)** had period-average apparent F&V availability below the 400 g/day reference.
# MAGIC
# MAGIC The remaining **121 countries (68.0%)** met or exceeded the reference on a period-average basis.
# MAGIC
# MAGIC This is a **country classification**, not a population-weighted estimate and not a measure of individual intake.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1.5 Interpretation
# MAGIC
# MAGIC The country-period view shows that the availability gap is **substantial but geographically uneven**. A large majority of countries met the reference on average, yet nearly one-third remained below it across their available study-period observations.
# MAGIC
# MAGIC Because this is an average over time, it describes a country's **typical position during the period**. It does not imply that every annual observation remained on the same side of the benchmark.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 RQ2 - PROGRESS?
# MAGIC #### How did countries move relative to the 400 g/day reference?
# MAGIC
# MAGIC RQ2 uses each country's **first and last available observations** within 2014–2023.
# MAGIC
# MAGIC The resulting categories are **endpoint trajectory categories**: they summarize how a country started and ended relative to the reference, rather than describing every annual fluctuation between those endpoints.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2.1 Endpoint Trajectory Definitions
# MAGIC
# MAGIC Classification is **hierarchical**: threshold-based categories are assigned first, then the remaining countries are classified by the direction of their first-to-last change.
# MAGIC
# MAGIC - **Above 400g:** started and ended at or above 400 g/day
# MAGIC - **Reached 400g:** started below 400 and ended at or above 400
# MAGIC - **Closing the gap:** after the two threshold categories above were assigned, ended higher than it started
# MAGIC - **Moving away:** ended lower than it started
# MAGIC - **Stable:** no first-to-last change
# MAGIC
# MAGIC These labels describe **endpoint movement**, not every year between the endpoints. In particular, **Above 400g does not mean the country was above 400 in every year**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2.2 Endpoint Trajectory Distribution
# MAGIC
# MAGIC We summarized each country's validated **first-to-last endpoint trajectory category**.
# MAGIC
# MAGIC The Gold dataset already contains `Progress_Category`. We counted countries in each category, calculated their share of the 178-country analytical base, and retained the expected-count assertions as a reproducibility check.
# MAGIC
# MAGIC A category label summarizes **endpoint status and direction** and should not be interpreted as a complete description of every year between those endpoints.

# COMMAND ----------

# We summarized first-to-last progress categories for each country.

rq2_counts = (
    country_summary
    .groupBy("Progress_Category")
    .count()
    .withColumn(
        "Percent_of_Countries",
        F.round(F.col("count") / F.lit(178) * 100, 1)
    )
)

progress_order = [
    "Above 400g",
    "Reached 400g",
    "Closing the gap",
    "Moving away",
    "Stable"
]

rq2_pd = rq2_counts.toPandas()
rq2_pd["Progress_Category"] = pd.Categorical(
    rq2_pd["Progress_Category"],
    categories=progress_order,
    ordered=True
)
rq2_pd = rq2_pd.sort_values("Progress_Category")

display(spark.createDataFrame(rq2_pd.astype(object).where(pd.notnull(rq2_pd), None)))

expected_counts = {
    "Above 400g": 111,
    "Reached 400g": 12,
    "Closing the gap": 30,
    "Moving away": 25
}

observed_counts = dict(zip(rq2_pd["Progress_Category"].astype(str), rq2_pd["count"]))
for category, expected in expected_counts.items():
    assert int(observed_counts.get(category, 0)) == expected, f"Unexpected RQ2 count for {category}."

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2.3 Distribution Visualization
# MAGIC
# MAGIC We visualized the validated RQ2 category counts.
# MAGIC
# MAGIC The plotting logic is intentionally simple:
# MAGIC - category counts determine bar height and are displayed from largest to smallest;
# MAGIC - transparent, hatched bars distinguish the categories without adding visual weight;
# MAGIC - purple highlights movement categories of particular analytical interest;
# MAGIC - direct value labels reduce the need for gridlines;
# MAGIC - decorative chart borders are removed.
# MAGIC
# MAGIC No statistical transformation occurs in this cell. It is plotted directly from the previously validated category table.

# COMMAND ----------

# We plotted the validated RQ2 category counts without recalculating them.

plot_rq2 = (
    rq2_pd
    .dropna(subset=["Progress_Category"])
    .sort_values("count", ascending=False)
    .copy()
)

fig, ax = plt.subplots(figsize=(9, 5.2))

bars = ax.bar(
    plot_rq2["Progress_Category"].astype(str),
    plot_rq2["count"],
    facecolor="none",
    edgecolor=[
        FOCUS_DARK if category == "Moving away"
        else FOCUS_COLOR if category in ["Reached 400g", "Closing the gap"]
        else CONTEXT_DARK
        for category in plot_rq2["Progress_Category"].astype(str)
    ],
    linewidth=1.8,
    hatch="////"
)

for bar, value in zip(bars, plot_rq2["count"]):
    ax.text(
        bar.get_x() + bar.get_width()/2,
        bar.get_height() + 2,
        f"{int(value)}",
        ha="center",
        va="bottom",
        color=TEXT_COLOR,
        fontsize=11
    )

ax.set_ylabel("Countries")
ax.set_xlabel("")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.grid(False)
plt.xticks(rotation=18, ha="right")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2.4 Countries Moving Away
# MAGIC
# MAGIC We focused on the **10 largest first-to-last declines** among the **25 countries** classified as **Moving away**.
# MAGIC
# MAGIC The chart highlights the **5 largest declines in purple** and keeps the remaining 5 countries in **grey** as context. This keeps the storytelling emphasis on the countries contributing the biggest negative movement away from the reference.

# COMMAND ----------

# We focused the chart on the 10 largest declines and greyed the remaining context.

moving_away = (
    country_summary
    .filter(F.col("Progress_Category") == "Moving away")
    .select(
        "Country",
        "Start_FV_g_person_day",
        "End_FV_g_person_day"
    )
    .withColumn(
        "Decline_g_person_day",
        F.round(
            F.col("Start_FV_g_person_day") - F.col("End_FV_g_person_day"),
            1
        )
    )
    .orderBy(F.col("Decline_g_person_day").desc())
)

moving_pd = moving_away.toPandas()

assert len(moving_pd) == 25, "Unexpected number of countries classified as Moving away."

top10_moving = moving_pd.head(10).copy()
top10_moving["Focus_Group"] = ["Focus" if i < 5 else "Context" for i in range(len(top10_moving))]

fig, ax = plt.subplots(figsize=(8.8, 5.6))

edge_colors = [
    FOCUS_DARK if group == "Focus" else CONTEXT_COLOR
    for group in top10_moving["Focus_Group"]
]

bars = ax.barh(
    top10_moving["Country"],
    top10_moving["Decline_g_person_day"],
    facecolor="none",
    edgecolor=edge_colors,
    linewidth=1.8,
    hatch="////"
)

ax.invert_yaxis()

for idx, (bar, value, group) in enumerate(zip(bars, top10_moving["Decline_g_person_day"], top10_moving["Focus_Group"])):
    if group == "Focus":
        ax.text(
            value + 2,
            bar.get_y() + bar.get_height()/2,
            f"{value:.1f}",
            va="center",
            color=TEXT_COLOR,
            fontsize=10
        )

ax.set_xlabel("Decline in F&V availability (g/person/day)")
ax.set_ylabel("")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.grid(False)

ax.text(
    0.0,
    1.05,
    "Top 10 declines among the 25 countries classified as Moving away",
    transform=ax.transAxes,
    ha="left",
    va="bottom",
    fontsize=10,
    color=TEXT_COLOR
)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2.5 Key Finding
# MAGIC
# MAGIC Across the 178 countries:
# MAGIC
# MAGIC - **111** were **Above 400g**
# MAGIC - **12** **Reached 400g**
# MAGIC - **30** were **Closing the gap**
# MAGIC - **25** were **Moving away**
# MAGIC - **0** were **Stable**
# MAGIC
# MAGIC The progress picture is therefore mixed: some countries moved toward or reached the reference, while others moved farther away.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2.6 Interpretation
# MAGIC
# MAGIC Most countries started and ended at or above the benchmark, but that should not be interpreted as uninterrupted adequacy.
# MAGIC
# MAGIC The categories that matter most for directional change are:
# MAGIC
# MAGIC - **Reached 400g:** countries that crossed from below to at or above the reference
# MAGIC - **Closing the gap:** countries that improved but did not meet the threshold classification
# MAGIC - **Moving away:** countries whose endpoint availability was lower than their starting value
# MAGIC
# MAGIC The focused **Moving away** chart shows that the largest declines were concentrated in a smaller subset of countries rather than being evenly distributed across all 25.
# MAGIC
# MAGIC These categories describe **endpoint movement** and do not capture every annual fluctuation between the first and last available observations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 RQ3 - PATTERNS?
# MAGIC #### What food-system patterns distinguish countries whose period-average F&V availability remained below 400 g/day?
# MAGIC
# MAGIC RQ3 uses **period-level country measures**:
# MAGIC
# MAGIC - RQ1 identifies each country's **period-average benchmark status**
# MAGIC - RQ2 describes **first-to-last movement**
# MAGIC - RQ3 compares **period-average food-system indicators**
# MAGIC
# MAGIC For RQ3, every indicator is analysed using the countries with a **valid value for that indicator**. We do not require a country to have complete production, import, and loss data simultaneously before it can contribute to a valid single-indicator analysis.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.3.1 Missing-Data & Available-Case Approach
# MAGIC
# MAGIC RQ3 uses an **available-case approach by indicator**. A country contributes to the production, import, or loss analysis whenever that specific indicator is available. Missing values are not replaced with zero, and a missing value for one indicator does not automatically remove that country from every RQ3 calculation. Each result therefore reports its own valid **n**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Available Cases by Indicator
# MAGIC
# MAGIC RQ3 starts from **all 178 countries** and then applies missing-data handling separately for each indicator.
# MAGIC
# MAGIC Valid country counts are:
# MAGIC
# MAGIC | Indicator | Below 400 | Meets 400 |
# MAGIC |---|---:|---:|
# MAGIC | Period-average production | **56** | **120** |
# MAGIC | Period-average imports | **57** | **121** |
# MAGIC | Period-average loss share | **57** | **120** |
# MAGIC
# MAGIC This approach uses the maximum legitimate evidence available for each calculation without imputing missing values.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Analytical Base & Sample Sizes
# MAGIC
# MAGIC RQ3 starts from **all 178 country-level records** rather than filtering first to one common “complete” subset.
# MAGIC
# MAGIC The code counts non-missing period-average values separately for:
# MAGIC - production;
# MAGIC - imports;
# MAGIC - loss share.
# MAGIC
# MAGIC This is the operational implementation of the **indicator-complete available-case approach**.
# MAGIC
# MAGIC The `Countries_Total` column remains visible so the reader can distinguish:
# MAGIC - the total benchmark-group population, and
# MAGIC - the number of valid observations available for each specific indicator.
# MAGIC
# MAGIC The assertion confirms that the RQ3 base still begins with the full 178-country analytical population.

# COMMAND ----------

# We started RQ3 with all countries and counted valid observations by indicator.

rq3 = country_summary

rq3_indicator_counts = (
    rq3
    .groupBy("Period_Benchmark_Status")
    .agg(
        F.count("Period_Avg_Production_kg_person_year").alias("Production_n"),
        F.count("Period_Avg_Imports_kg_person_year").alias("Imports_n"),
        F.count("Period_Avg_Loss_Share_pct").alias("Loss_Share_n"),
        F.count("*").alias("Countries_Total")
    )
    .orderBy("Period_Benchmark_Status")
)

display(rq3_indicator_counts)

assert rq3.count() == 178, "RQ3 country base should contain all 178 countries."

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.3.2 Food-System Profile Comparison
# MAGIC
# MAGIC We compare medians because country-level production and trade measures are highly heterogeneous and can contain very large values.
# MAGIC
# MAGIC The comparison uses:
# MAGIC
# MAGIC - **Period-average F&V production per person**
# MAGIC - **Period-average F&V imports per person**
# MAGIC - **Period-average recorded loss share**

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Exact Median Comparisons
# MAGIC
# MAGIC We compared countries **Below 400g** with countries **Meeting 400g** using exact country-level medians for production, imports, and recorded loss share.
# MAGIC
# MAGIC Medians are used because country-level production and trade measures are highly skewed and can contain very large values. Each indicator reports its own valid `n` because the available-case approach does not force all indicators to share the same sample size.
# MAGIC
# MAGIC With only 178 country-level records, the exact median is calculated directly in Pandas rather than using an approximate percentile function.

# COMMAND ----------

# We calculated exact country-level medians and valid sample sizes for each indicator.

rq3_country_pd = (
    rq3.select(
        "Period_Benchmark_Status",
        "Period_Avg_Production_kg_person_year",
        "Period_Avg_Imports_kg_person_year",
        "Period_Avg_Loss_Share_pct"
    )
    .toPandas()
)

rq3_summary_pd = (
    rq3_country_pd
    .groupby("Period_Benchmark_Status", as_index=False)
    .agg(
        Median_Production_kg_person_year=("Period_Avg_Production_kg_person_year", "median"),
        Production_n=("Period_Avg_Production_kg_person_year", "count"),
        Median_Imports_kg_person_year=("Period_Avg_Imports_kg_person_year", "median"),
        Imports_n=("Period_Avg_Imports_kg_person_year", "count"),
        Median_Loss_Share_pct=("Period_Avg_Loss_Share_pct", "median"),
        Loss_Share_n=("Period_Avg_Loss_Share_pct", "count")
    )
)

rq3_summary = spark.createDataFrame(rq3_summary_pd)

display(rq3_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Group Comparison Results
# MAGIC
# MAGIC | Indicator | Below 400 | Meets 400 |
# MAGIC |---|---:|---:|
# MAGIC | Median period-average F&V production | **97.2 kg/person/year (n=56)** | **215.3 kg/person/year (n=120)** |
# MAGIC | Median period-average F&V imports | **11.8 kg/person/year (n=57)** | **99.7 kg/person/year (n=121)** |
# MAGIC | Median period-average loss share | **8.6% (n=57)** | **7.1% (n=120)** |
# MAGIC
# MAGIC The largest descriptive differences appear in **production and imports**. Recorded loss share differs much less between the two groups.
# MAGIC
# MAGIC Because valid sample size differs slightly by indicator, each result reports its own **n** rather than implying one fixed RQ3 sample.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Individual Indicator Charts
# MAGIC
# MAGIC We visualized **production, imports, and recorded loss share as three separate figures** so each can be reviewed and copied independently.
# MAGIC
# MAGIC Within each figure, the two benchmark groups are ordered from the larger median to the smaller median, while the purple focus color highlights the **Below 400** group.

# COMMAND ----------

# We prepared the RQ3 summary once for the three indicator charts.

rq3_summary_pd = rq3_summary.toPandas().copy()
rq3_summary_pd["Display_Status"] = rq3_summary_pd["Period_Benchmark_Status"].replace({
    "Below 400g": "Below 400",
    "Meets 400g": "Meets 400"
})

status_order = ["Below 400", "Meets 400"]

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Production

# COMMAND ----------

# We compared median F&V production between the two benchmark groups.

chart_df = (
    rq3_summary_pd
    .set_index("Display_Status")
    .reindex(status_order)
    .reset_index()
    .sort_values("Median_Production_kg_person_year", ascending=False)
)

fig, ax = plt.subplots(figsize=(6.2, 4.6))

bars = ax.bar(
    chart_df["Display_Status"],
    chart_df["Median_Production_kg_person_year"],
    facecolor="none",
    edgecolor=[
        FOCUS_COLOR if status == "Below 400" else CONTEXT_DARK
        for status in chart_df["Display_Status"]
    ],
    linewidth=1.8,
    hatch="////"
)

for bar, value in zip(bars, chart_df["Median_Production_kg_person_year"]):
    ax.text(
        bar.get_x() + bar.get_width()/2,
        bar.get_height(),
        f"{value:.1f}",
        ha="center",
        va="bottom",
        color=TEXT_COLOR,
        fontsize=11
    )

ax.set_ylabel("kg/person/year")
ax.set_xlabel("")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.grid(False)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Imports

# COMMAND ----------

# We compared median F&V imports between the two benchmark groups.

chart_df = (
    rq3_summary_pd
    .set_index("Display_Status")
    .reindex(status_order)
    .reset_index()
    .sort_values("Median_Imports_kg_person_year", ascending=False)
)

fig, ax = plt.subplots(figsize=(6.2, 4.6))

bars = ax.bar(
    chart_df["Display_Status"],
    chart_df["Median_Imports_kg_person_year"],
    facecolor="none",
    edgecolor=[
        FOCUS_COLOR if status == "Below 400" else CONTEXT_DARK
        for status in chart_df["Display_Status"]
    ],
    linewidth=1.8,
    hatch="////"
)

for bar, value in zip(bars, chart_df["Median_Imports_kg_person_year"]):
    ax.text(
        bar.get_x() + bar.get_width()/2,
        bar.get_height(),
        f"{value:.1f}",
        ha="center",
        va="bottom",
        color=TEXT_COLOR,
        fontsize=11
    )

ax.set_ylabel("kg/person/year")
ax.set_xlabel("")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.grid(False)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Recorded Loss Share

# COMMAND ----------

# We compared median recorded loss share between the two benchmark groups.

chart_df = (
    rq3_summary_pd
    .set_index("Display_Status")
    .reindex(status_order)
    .reset_index()
    .sort_values("Median_Loss_Share_pct", ascending=False)
)

fig, ax = plt.subplots(figsize=(6.2, 4.6))

bars = ax.bar(
    chart_df["Display_Status"],
    chart_df["Median_Loss_Share_pct"],
    facecolor="none",
    edgecolor=[
        FOCUS_COLOR if status == "Below 400" else CONTEXT_DARK
        for status in chart_df["Display_Status"]
    ],
    linewidth=1.8,
    hatch="////"
)

for bar, value in zip(bars, chart_df["Median_Loss_Share_pct"]):
    ax.text(
        bar.get_x() + bar.get_width()/2,
        bar.get_height(),
        f"{value:.1f}",
        ha="center",
        va="bottom",
        color=TEXT_COLOR,
        fontsize=11
    )

ax.set_ylabel("% of domestic supply")
ax.set_xlabel("")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.grid(False)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.3.3 Relationships Within Below-400 Countries
# MAGIC
# MAGIC The correlation stage focuses only on countries whose **period-average availability remains below 400 g/day**.
# MAGIC
# MAGIC Three relationships are examined:
# MAGIC
# MAGIC 1. Production vs period-average F&V availability
# MAGIC 2. Imports vs period-average F&V availability
# MAGIC 3. Recorded loss share vs period-average shortfall
# MAGIC
# MAGIC Production and imports are components of the broader food-balance system, so some association with availability is expected. These correlations are therefore interpreted as **diagnostic associations**, not independent causal effects.
# MAGIC
# MAGIC Pearson correlations summarize **linear association only**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Correlation Sample
# MAGIC
# MAGIC The correlation stage asks a narrower question than the group comparison:
# MAGIC
# MAGIC > **Within countries whose period-average availability is below 400 g/day, how are food-system indicators associated with the size or level of the availability gap?**
# MAGIC
# MAGIC We therefore:
# MAGIC 1. filtered the full RQ3 country table to `Below 400g`;
# MAGIC 2. selected only the variables needed for the three correlation tests;
# MAGIC 3. converted the small country-level table to Pandas;
# MAGIC 4. reported how many below-400 countries have a valid value for each indicator.
# MAGIC
# MAGIC Importantly, the table is **not pre-filtered to complete cases across all three indicators**. Pairwise missing-value handling occurs separately for each correlation in the next cell.

# COMMAND ----------

# We restricted correlation analysis to countries below 400 g/day on period average.

rq3_below = (
    rq3
    .filter(F.col("Period_Benchmark_Status") == "Below 400g")
    .select(
        "Country",
        "Period_Avg_FV_g_person_day",
        "Period_Shortfall_g_day",
        "Period_Avg_Production_kg_person_year",
        "Period_Avg_Imports_kg_person_year",
        "Period_Avg_Loss_Share_pct"
    )
)

rq3_below_pd = rq3_below.toPandas()

print("Below-400 countries in RQ3 base:", len(rq3_below_pd))
print("Production available:", rq3_below_pd["Period_Avg_Production_kg_person_year"].notna().sum())
print("Imports available:", rq3_below_pd["Period_Avg_Imports_kg_person_year"].notna().sum())
print("Loss share available:", rq3_below_pd["Period_Avg_Loss_Share_pct"].notna().sum())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Pearson Correlations
# MAGIC
# MAGIC We calculated the three final RQ3 correlations using **pairwise complete observations**.
# MAGIC
# MAGIC The helper function:
# MAGIC - keeps only the two variables needed for one relationship;
# MAGIC - removes rows missing either of those two values;
# MAGIC - calculates the Pearson correlation coefficient;
# MAGIC - returns both `r` and the valid sample size `n`.
# MAGIC
# MAGIC This means a country with valid production and availability can contribute to the production correlation even if its loss-share value is missing.
# MAGIC
# MAGIC The three tested relationships are:
# MAGIC 1. production vs F&V availability;
# MAGIC 2. imports vs F&V availability;
# MAGIC 3. loss share vs 400G shortfall.
# MAGIC
# MAGIC Pearson `r` measures **linear association**, not causation.

# COMMAND ----------

# We calculated Pearson correlations using pairwise-complete observations.

def pearson_result(data, x_col, y_col):
    clean = data[[x_col, y_col]].dropna()
    r = clean[x_col].corr(clean[y_col])
    return float(r), int(len(clean))

correlation_specs = [
    (
        "Production vs F&V availability",
        "Period_Avg_Production_kg_person_year",
        "Period_Avg_FV_g_person_day"
    ),
    (
        "Imports vs F&V availability",
        "Period_Avg_Imports_kg_person_year",
        "Period_Avg_FV_g_person_day"
    ),
    (
        "Loss share vs 400G shortfall",
        "Period_Avg_Loss_Share_pct",
        "Period_Shortfall_g_day"
    )
]

corr_rows = []
for relationship, x_col, y_col in correlation_specs:
    r, n = pearson_result(rq3_below_pd, x_col, y_col)
    corr_rows.append({
        "Relationship": relationship,
        "Pearson_r": round(r, 3),
        "n": n
    })

corr_results_pd = pd.DataFrame(corr_rows)
display(spark.createDataFrame(corr_results_pd))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Correlation Results
# MAGIC
# MAGIC Using **pairwise complete observations** among period-average below-400 countries:
# MAGIC
# MAGIC - **Production vs availability:** **r = 0.46, n = 56**
# MAGIC - **Imports vs availability:** **r = 0.25, n = 57**
# MAGIC - **Loss share vs shortfall:** **r = -0.22, n = 57**
# MAGIC
# MAGIC Production again provides the clearest signal, while imports show a weaker positive relationship. Recorded loss share does not show a strong positive relationship with shortfall size.
# MAGIC
# MAGIC The different **n** values are intentional: each relationship uses all countries with valid data for the variables involved.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reusable Correlation Plot Function
# MAGIC
# MAGIC We used one function to standardize the appearance and calculation logic of the three RQ3 scatterplots.
# MAGIC
# MAGIC For each relationship it:
# MAGIC - applies the same pairwise-complete filtering used for the numerical correlation;
# MAGIC - calculates Pearson `r`;
# MAGIC - plots individual countries as neutral context points;
# MAGIC - fits a simple linear trend line when sufficient variation exists;
# MAGIC - optionally adds the black dotted **400g** benchmark;
# MAGIC - displays both `r` and `n` directly on the figure.
# MAGIC
# MAGIC Using one shared function prevents small styling or filtering differences from being introduced across the production, import, and loss charts.
# MAGIC
# MAGIC The fitted line is descriptive only and should not be interpreted as a causal model.

# COMMAND ----------

# We used one plotting function to keep filtering and styling consistent across correlations.

def plot_correlation(
    data,
    x_col,
    y_col,
    x_label,
    y_label,
    benchmark_y=None
):
    clean = data[[x_col, y_col]].dropna().copy()
    r = clean[x_col].corr(clean[y_col])

    fig, ax = plt.subplots(figsize=(7.2, 5.0))

    ax.scatter(
        clean[x_col],
        clean[y_col],
        s=36,
        alpha=0.62,
        color=CONTEXT_DARK,
        edgecolors="none"
    )

    if len(clean) >= 2 and clean[x_col].nunique() > 1:
        slope, intercept = np.polyfit(clean[x_col], clean[y_col], 1)
        x_vals = np.linspace(clean[x_col].min(), clean[x_col].max(), 200)
        ax.plot(
            x_vals,
            slope * x_vals + intercept,
            color=FOCUS_COLOR,
            linewidth=2.5
        )

    if benchmark_y is not None:
        ax.axhline(
            benchmark_y,
            color="black",
            linestyle=":",
            linewidth=1.5,
            zorder=5
        )
        ax.text(
            0.99,
            benchmark_y,
            "400g",
            transform=ax.get_yaxis_transform(),
            ha="right",
            va="bottom",
            color="black",
            fontsize=10
        )

    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(False)

    ax.text(
        0.02,
        0.98,
        f"r = {r:.2f}   n = {len(clean)}",
        transform=ax.transAxes,
        ha="left",
        va="top",
        color=TEXT_COLOR,
        fontsize=11
    )

    plt.tight_layout()
    plt.show()

    return r, len(clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Production vs Availability
# MAGIC
# MAGIC This cell applies the shared correlation function to **period-average F&V production per person** and **period-average F&V availability** among below-400 countries.
# MAGIC
# MAGIC The 400 g/day horizontal reference is retained so the viewer can see how the observations sit relative to the benchmark even though all countries in this subset have period-average values below it.
# MAGIC
# MAGIC The returned `production_r` and `production_n` preserve the exact statistic and pairwise-complete sample size used in the figure.

# COMMAND ----------

# We tested the relationship between production and period-average F&V availability.

production_r, production_n = plot_correlation(
    rq3_below_pd,
    x_col="Period_Avg_Production_kg_person_year",
    y_col="Period_Avg_FV_g_person_day",
    x_label="Period-average F&V production (kg/person/year)",
    y_label="Period-average F&V availability (g/person/day)",
    benchmark_y=400
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretation:** Production shows a **moderate positive association** with period-average F&V availability among countries below the reference. Higher production per person tends to coincide with higher apparent availability, but the relationship is not deterministic and does not establish causation.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Imports vs Availability
# MAGIC
# MAGIC This cell applies the same pairwise-complete correlation and visualization logic to **period-average F&V imports per person**.
# MAGIC
# MAGIC Keeping the same function, benchmark treatment, and visual grammar as the production plot makes the two relationships directly comparable in presentation while preserving their separate underlying sample sizes.

# COMMAND ----------

# We tested the relationship between imports and period-average F&V availability.

imports_r, imports_n = plot_correlation(
    rq3_below_pd,
    x_col="Period_Avg_Imports_kg_person_year",
    y_col="Period_Avg_FV_g_person_day",
    x_label="Period-average F&V imports (kg/person/year)",
    y_label="Period-average F&V availability (g/person/day)",
    benchmark_y=400
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretation:** Imports show a **weaker positive association** with availability. Imported supply may complement domestic production in some settings, but import dependence, purchasing power, distribution, affordability, and other factors can shape how trade translates into apparent availability.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Recorded Loss Share vs Shortfall
# MAGIC
# MAGIC This cell examines whether countries with a higher **recorded F&V loss share** also tend to have a larger shortfall from the 400 g/day reference.
# MAGIC
# MAGIC The y-variable here is **shortfall**, not availability:
# MAGIC - a larger value means the country is farther below 400 g/day;
# MAGIC - a value closer to zero means it is nearer the benchmark.
# MAGIC
# MAGIC The same available-case rule applies: only countries with both a valid loss-share value and a valid shortfall enter this correlation.

# COMMAND ----------

# We tested the relationship between recorded loss share and the shortfall from 400 g/day.

loss_r, loss_n = plot_correlation(
    rq3_below_pd,
    x_col="Period_Avg_Loss_Share_pct",
    y_col="Period_Shortfall_g_day",
    x_label="Period-average recorded F&V loss share (%)",
    y_label="Period-average shortfall from 400 g/day"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretation:** Recorded loss share shows only a **weak inverse linear relationship** with shortfall size in this sample. The available loss measure therefore does not emerge as a strong standalone explanation of why some countries remain farther below 400 g/day.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Robustness & Interpretation
# MAGIC
# MAGIC The methodology prioritizes **internal consistency, transparent coverage, and restrained interpretation**, with a clearly defined time basis for each research question.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Analytical Safeguards
# MAGIC
# MAGIC **1. Consistent analytical source:**
# MAGIC All results are generated from the same consolidated country-year dataset.
# MAGIC
# MAGIC **2. Geography is kept at the country level:**
# MAGIC Official regional aggregates are not mixed with country observations in the primary analysis.
# MAGIC
# MAGIC **3. Partial country coverage is disclosed:**
# MAGIC Countries do not all have identical year coverage. RQ1 uses available observations; RQ2 uses first and last available observations. Coverage fields remain in the Gold data so this can be inspected.
# MAGIC
# MAGIC **4. Missing source components are not treated as zero:**
# MAGIC Production, imports, and loss share are analysed with an **available-case approach**. A missing value removes a country only from the calculation that requires that indicator; it does not automatically remove the country from every RQ3 analysis.
# MAGIC
# MAGIC **5. Sample size is reported separately by indicator:**
# MAGIC Group medians and correlations report their own **n** so differences in indicator availability remain transparent.
# MAGIC
# MAGIC **6. Medians are used for group comparisons:**
# MAGIC Country-level production and trade values are skewed and heterogeneous, so medians reduce sensitivity to extreme observations.
# MAGIC
# MAGIC **7. Correlation is not interpreted as causation:**
# MAGIC Production, imports, and losses are examined as diagnostic patterns only.
# MAGIC
# MAGIC **8. Rank-based sensitivity check:**
# MAGIC Pearson results are compared with Spearman correlations to test whether the broad interpretation changes when rank order is used instead of a strictly linear measure.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Pearson vs Spearman Sensitivity Check
# MAGIC
# MAGIC We compared the primary **Pearson correlations** with **Spearman rank correlations** using the same pairwise-complete Below-400 samples.
# MAGIC
# MAGIC Spearman correlation is less sensitive to extreme values and does not require a strictly linear relationship, so it provides a useful sensitivity check for the direction and relative strength of the observed associations.

# COMMAND ----------

# We compared Pearson and Spearman correlations on the same pairwise-complete samples.

robustness_specs = [
    (
        "Production",
        "Period_Avg_Production_kg_person_year",
        "Period_Avg_FV_g_person_day"
    ),
    (
        "Imports",
        "Period_Avg_Imports_kg_person_year",
        "Period_Avg_FV_g_person_day"
    ),
    (
        "Recorded loss share",
        "Period_Avg_Loss_Share_pct",
        "Period_Shortfall_g_day"
    )
]

robustness_rows = []

for indicator, x_col, y_col in robustness_specs:
    pair = rq3_below_pd[[x_col, y_col]].dropna()

    robustness_rows.append({
        "Indicator": indicator,
        "n": len(pair),
        "Pearson_r": pair[x_col].corr(pair[y_col], method="pearson"),
        "Spearman_rho": pair[x_col].corr(pair[y_col], method="spearman")
    })

robustness_results = pd.DataFrame(robustness_rows).round({
    "Pearson_r": 2,
    "Spearman_rho": 2
})

display(robustness_results)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Robustness Interpretation
# MAGIC
# MAGIC The rank-based check supports the same broad pattern:
# MAGIC
# MAGIC - **Production:** positive under both measures (**Pearson r = 0.46; Spearman ρ = 0.65; n = 56**)
# MAGIC - **Imports:** weak positive association (**r = 0.25; ρ = 0.18; n = 57**)
# MAGIC - **Recorded loss share:** weak negative association with shortfall size (**r = -0.22; ρ = -0.19; n = 57**)
# MAGIC
# MAGIC Production remains the clearest relationship, while imports and recorded loss share remain comparatively weak. The difference between Pearson and Spearman for production also suggests that the relationship is monotonic but not perfectly linear.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Interpretation Guardrails
# MAGIC
# MAGIC The project uses **400 g/day as a food-system reference benchmark**.
# MAGIC
# MAGIC FAOSTAT Food Balance data describe **apparent national availability**, not:
# MAGIC - individual dietary intake,
# MAGIC - within-country inequality,
# MAGIC - affordability,
# MAGIC - household access,
# MAGIC - food preference,
# MAGIC - dietary quality beyond F&V quantity.
# MAGIC
# MAGIC A country whose national apparent availability meets 400 g/day can still contain populations with inadequate access or consumption. Likewise, a country below 400 g/day may face multiple interacting supply, distribution, economic, and demand-side constraints.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Implications & Conclusion
# MAGIC
# MAGIC ### 5.1 Evidence-Aligned Implications
# MAGIC
# MAGIC The analysis supports a **diagnostic sequence**, not a universal policy prescription.
# MAGIC
# MAGIC **1. Use the period-average gap to identify persistent supply concerns.**  
# MAGIC Countries below 400 g/day on average warrant closer examination because the shortfall is not limited to a single-year snapshot.
# MAGIC
# MAGIC **2. Use progress categories to distinguish direction of travel.**  
# MAGIC A below-reference country that is closing the gap presents a different analytical problem from one moving away.
# MAGIC
# MAGIC **3. Investigate production constraints where domestic supply is persistently low.**  
# MAGIC Production showed the clearest relationship with availability in the below-400 sample.
# MAGIC
# MAGIC **4. Examine import capacity and market access alongside production.**  
# MAGIC Imports were substantially lower among below-400 countries and showed a weaker positive association with availability.
# MAGIC
# MAGIC **5. Treat recorded losses as one part of a broader food-system diagnosis.**  
# MAGIC The loss measure did not show a strong linear relationship with shortfall size, so loss reduction alone should not be inferred as the primary explanation from this dataset.
# MAGIC
# MAGIC The appropriate next question for any country is therefore not simply *"Which indicator is low?"* but *"Which combination of production, trade, distribution, affordability, losses, and other food-system conditions best explains its observed availability pattern?"*

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Final Synthesis
# MAGIC
# MAGIC **RQ1 - WHERE?** 
# MAGIC
# MAGIC **57 of 178 countries (32.0%)** had period-average F&V availability below 400 g/person/day during their available observations within 2014–2023.
# MAGIC
# MAGIC **RQ2 - PROGRESS?**
# MAGIC
# MAGIC Country trajectories were mixed:
# MAGIC - **111 Above 400g**
# MAGIC - **12 Reached 400g**
# MAGIC - **30 Closing the gap**
# MAGIC - **25 Moving away**
# MAGIC
# MAGIC **RQ3 - PATTERNS?**
# MAGIC
# MAGIC Using all valid observations available for each indicator, below-400 countries had substantially lower median production and imports than countries meeting the reference.
# MAGIC
# MAGIC Among below-400 countries with complete data for each specific relationship:
# MAGIC - production had the clearest positive association with availability (**r = 0.46, n = 56**),
# MAGIC - imports showed a weaker positive relationship (**r = 0.25, n = 57**),
# MAGIC - recorded loss share showed little relationship with the size of the shortfall (**r = -0.22, n = 57**).
# MAGIC
# MAGIC #### Overall Takeaway
# MAGIC
# MAGIC The 400G challenge is **not only about whether enough fruits and vegetables exist globally**. National availability remains uneven, progress differs across countries, and below-reference countries show different food-system profiles.
# MAGIC
# MAGIC The analysis therefore supports a **country-specific diagnostic approach**: identify the gap, understand its direction, and then investigate the food-system pathways most relevant to that context.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Limitations
# MAGIC
# MAGIC - FAOSTAT Food Balance Sheets measure **apparent national food availability**, not individual consumption.
# MAGIC - The analysis covers **2014–2023**.
# MAGIC - RQ1 averages each country's **available observations**, so countries with partial coverage contribute fewer years.
# MAGIC - RQ2 uses **first and last available observations**, which are not identical calendar endpoints for every country.
# MAGIC - Progress categories summarize endpoints and do not capture every fluctuation between them.
# MAGIC - Country-period averages can smooth meaningful year-to-year variation.
# MAGIC - RQ3 uses period-average production, imports, and recorded loss share; these indicators do not capture every food-system constraint.
# MAGIC - The implied-population conversion used for per-person production/import metrics is an analytical derivation from the available Food and supply fields.
# MAGIC - RQ3 uses **available cases by indicator**, so sample size can differ slightly across production, import, and loss analyses.
# MAGIC - Available-case analysis assumes that excluding missing observations does not introduce important systematic bias; results should therefore be interpreted descriptively.
# MAGIC - Pearson correlation measures linear association only and is sensitive to data structure and outliers; Spearman rank correlations are included as a sensitivity check.
# MAGIC - No causal claims are made.
# MAGIC - The 400 g/day value is used as a **reference benchmark**, not as proof of adequate individual dietary intake.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Sources & References
# MAGIC
# MAGIC 1. **World Health Organization (WHO).** *Healthy diet.*  
# MAGIC    https://www.who.int/news-room/fact-sheets/detail/healthy-diet  
# MAGIC    Used for the **400 g/day fruit-and-vegetable reference**.
# MAGIC
# MAGIC 2. **Food and Agriculture Organization of the United Nations (FAO).** *FAOSTAT Food Balances (Global, National — Annual).*  
# MAGIC    https://data.fao.org/catalog/iso/2f264bb6-1238-459a-bf8b-0e2d0a16804a  
# MAGIC    Primary source for the **Food Balance** data used in the analysis.
# MAGIC
# MAGIC 3. **FAOSTAT Food Balances metadata.**  
# MAGIC    https://www.fao.org/faostat/en/#data/FBS/metadata  
# MAGIC    Used for dataset definitions, units, source flags, and interpretation of Food Balance elements.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix A - Reusable Analytical Views
# MAGIC
# MAGIC Temporary Spark views are created for validation and optional SQL exploration.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A.1 Databricks Analytical Views
# MAGIC
# MAGIC This code cell registers temporary SQL views for validation, ad-hoc exploration, and optional dashboard work.
# MAGIC
# MAGIC The views expose:
# MAGIC - the complete country-year Gold dataset;
# MAGIC - the one-row-per-country summary;
# MAGIC - the full RQ3 country base;
# MAGIC - the below-400 RQ3 subset.
# MAGIC
# MAGIC These are **session-level temporary views**. They do not overwrite the source CSV or create a competing Gold dataset.
# MAGIC
# MAGIC The purpose is convenience: the same validated objects used in Python can also be queried through Spark SQL without rebuilding the analytical logic.

# COMMAND ----------

# We registered temporary SQL views for validation and optional exploration.

gold.createOrReplaceTempView("vw_farm400_final_gold")
country_summary.createOrReplaceTempView("vw_farm400_country_summary")
rq3.createOrReplaceTempView("vw_farm400_rq3_all_countries")
rq3_below.createOrReplaceTempView("vw_farm400_rq3_below400")

print("Created views:")
print("- vw_farm400_final_gold")
print("- vw_farm400_country_summary")
print("- vw_farm400_rq3_all_countries")
print("- vw_farm400_rq3_below400")