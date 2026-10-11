# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##RQ3 — What food-system patterns characterize countries below the 400 g/day benchmark?
# MAGIC Research question
# MAGIC What patterns characterize countries with the largest fruit-and-vegetable availability deficits, and what changes in production, trade, food loss, and population are associated with countries closing or widening the gap from 2014 to 2023?
# MAGIC
# MAGIC This notebook uses two complementary analyses:
# MAGIC
# MAGIC * 2023 snapshot analysis — identifies countries below the 400 g/person/day benchmark and examines which countries have the largest availability deficits.
# MAGIC * 2014–2023 change analysis — keeps countries with data available in both 2014 and 2023, then compares each country's change in the availability gap with changes in production, imports, net trade, food loss, and population.
# MAGIC
# MAGIC gap_from_400 = fruit_veg_supply_g_day - 400
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

#load the gold master table
gold_master = spark.table("workspace.byte_crew.gold_master")
gold_master=gold_master.toPandas()
display(gold_master)

# COMMAND ----------




# COMMAND ----------

# Convert combined F&V production to g/person/day:
# divide production (1000 t) by population (1000 persons),
# multiply by 1000 to convert tonnes to kg,
# multiply by 1000 again to convert kg to grams,
# then divide by 365 to convert yearly production to daily production.
gold_master['fruit_veg_prod_g_percap_day']= (gold_master['fruit_veg_prod_1000t']/gold_master['population_1000'])*100000/365


# COMMAND ----------

gold_master['fruit_veg_prod_g_percap_day']

# COMMAND ----------

# Convert F&V net trade to g/person/day
# divide net trade (1000 t) by population (1000 persons),
# multiply by 1,000,000 to convert tonnes to grams,
# then divide by 365 to convert yearly net trade to daily net trade.
gold_master['net_trade_g_percap_day'] = (
    (gold_master['net_trade_1000t'] * 1000 * 1_000_000)
    / (gold_master['population_1000'] * 1000)
) / 365

# COMMAND ----------

gold_master['net_trade_g_percap_day']

# COMMAND ----------

#converting population
gold_master['population'] = gold_master['population_1000'] * 1000

# COMMAND ----------

gold_master['fruit_veg_import_g_percap_day'] = (
    gold_master['fruit_veg_import_1000t'] * 1_000_000
    / gold_master['population_1000']
    / 365
)

# COMMAND ----------


below = gold_master[
    gold_master["target_400g"] == "Below 400g"
]

display(below)
deficit_2023 = below.copy()

# COMMAND ----------



# COMMAND ----------

deficit_2023.groupby(['adequacy_level','trend']).size()

# COMMAND ----------

# MAGIC %md
# MAGIC #
# MAGIC #2023 snapshot — What characterizes the countries with the largest gaps?

# COMMAND ----------

deficit_2023=deficit_2023[deficit_2023["year"]==2023]
deficit_2023= deficit_2023.sort_values(by="gap_from_400",ascending=True)
display(deficit_2023)

# COMMAND ----------

import pandas as pd
pd.crosstab(
    deficit_2023['adequacy_level'],
    deficit_2023['trend'],
    normalize='index'
) * 100

# COMMAND ----------

# MAGIC %md
# MAGIC **Countries with more severe F&V availability deficits in 2023 were more likely to be moving away from the 400 g/person/day reference, while countries with mild or moderate deficits were more likely to be closing the gap**

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

trend_pct = deficit_2023.groupby('trend')['country'].count() / deficit_2023
severe_critical = deficit_2023[
    deficit_2023['adequacy_level'].isin(
        ['Critical deficiency', 'Severe deficiency']
    )
]

plt.figure(figsize=(12, 6))

sns.barplot(
    data=severe_critical,
    x='country',
    y='gap_from_400',
    hue='adequacy_level'
)

plt.xticks(rotation=90)
plt.ylabel('Gap from 400g/day')
plt.title('Critical and Severe F&V Availability Deficits — 2023')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %bmd
# MAGIC In 2023, 19 countries experienced severe or critical fruit and vegetable availability deficits. Zambia, Ethiopia, and Gambia had the largest gaps, each exceeding 300 g/person/day below the 400 g benchmark, and were classified as critically deficient. The remaining 16 countries were severely deficient, with gaps of roughly 200–280 g/person/day. Many of these countries are located in Africa, indicating a concentration of the most substantial F&V availability shortfalls in the region. These results identify where deficits are most pronounced but do not establish their causes; production, trade, and food losses are examined separately as potential associated factors.

# COMMAND ----------

variables = [
    'gap_from_400',
    'fruit_veg_prod_g_percap_day',
    'net_trade_g_percap_day',
    'loss_rate_pct',
    'population_1000'
]

corr = deficit_2023[variables].corr(method='spearman')

plt.figure(figsize=(9, 6))

sns.heatmap(
    corr,
    annot=True,
    fmt='.2f',
    cmap='coolwarm',
    center=0
)

plt.title(
    'Correlation of F&V Availability Gap with Production, '
    'Net Trade, Losses, and Population — 2023'
)

plt.tight_layout()
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(9, 6))

sns.scatterplot(
    data=deficit_2023,
    x='fruit_veg_prod_g_percap_day',
    y='gap_from_400'
)

plt.xlabel('F&V Production (g/person/day)')
plt.ylabel('Gap from 400 (g/person/day)')
plt.title('F&V Production vs Gap from 400 — 2023')

plt.grid(alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Among countries below the 400 g/person/day benchmark in 2023, higher per-capita fruit and vegetable production was associated with a smaller availability deficit (Spearman ρ ≈ 0.63). However, substantial variation remains, suggesting that production alone does not explain differences in F&V availability.

# COMMAND ----------

df_2014_2023 = below[
    below['year'].isin(range(2014, 2024))

]
df_2014_2023

# COMMAND ----------

df_2014_2023.pivot(
    index='country',
    columns='year',
    values=[
        'gap_from_400',
        'fruit_veg_prod_g_percap_day',
        'net_trade_g_percap_day',
        'loss_rate_pct',
        'population_1000'
    ]
)

df_2014_2023

# COMMAND ----------

df_2014_2023 = df_2014_2023[
    df_2014_2023['year'].isin([2014, 2023])
]

countries_both = df_2014_2023.groupby('country')['year'].nunique()

countries_both = countries_both[countries_both == 2].index

df_2014_2023 = df_2014_2023[
    df_2014_2023['country'].isin(countries_both)
]


df_2014_2023

# COMMAND ----------

df_2014_2023.groupby('country')['year'].nunique().unique()

# COMMAND ----------

gap_change = df_2014_2023.pivot(
    index='country',
    columns='year',
    values='gap_from_400'
)

gap_change['gap_change'] = (
    gap_change[2023] - gap_change[2014]
)

gap_change.sort_values(
    by='gap_change',
    ascending=False
)

# COMMAND ----------

gap_change['gap_change'].sort_values().plot(
    kind='bar',
    figsize=(14,6)
)

plt.axhline(0, linestyle='--')
plt.ylabel('Change in Gap (g/day)')
plt.title('Change in F&V Availability Gap: 2014–2023')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Burkina Faso had a positive gap change of +266.2 g/person/day, meaning it closed its F&V availability gap by 266.2 g/person/day between 2014 and 2023.
# MAGIC
# MAGIC Haiti had a negative gap change of approximately −94.7 g/person/day, meaning its F&V availability gap widened by 94.7 g/person/day between 2014 and 2023.
# MAGIC
# MAGIC

# COMMAND ----------

def get_change(df, column):
    temp = df[
        df['year'].isin([2014, 2023])
    ].pivot(
        index='country',
        columns='year',
        values=column
    ).dropna()

    temp[f'{column}_change'] = temp[2023] - temp[2014]

    return temp[[f'{column}_change']]

# COMMAND ----------

gap = get_change(df_2014_2023, 'gap_from_400')
production = get_change(df_2014_2023, 'fruit_veg_prod_g_percap_day')
imports = get_change(df_2014_2023, 'fruit_veg_import_g_percap_day')
net_trade = get_change(df_2014_2023, 'net_trade_g_percap_day')
loss = get_change(df_2014_2023, 'loss_rate_pct')
population = get_change(df_2014_2023, 'population')

# COMMAND ----------

production.sort_values(by='fruit_veg_prod_g_percap_day_change', ascending=False)

# COMMAND ----------

imports

# COMMAND ----------

net_trade

# COMMAND ----------

loss.sort_values(by='loss_rate_pct_change', ascending=False)

# COMMAND ----------

population

# COMMAND ----------

change_analysis = (
    gap
    .join(production)
    .join(imports)
    .join(net_trade)
    .join(loss)
    .join(population)
)


change_analysis

# COMMAND ----------

correlation = change_analysis.select_dtypes(
    include='number'
).corr(method='spearman')

correlation['gap_from_400_change'].sort_values(
    ascending=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC From 2014 to 2023, production had the strongest relationship with closing the fruit and vegetable availability gap.
# MAGIC
# MAGIC Production (0.75): Strong relationship. Countries that increased F&V production per person were more likely to close the gap.
# MAGIC Imports (0.33): Some relationship. Increasing imports may help countries improve F&V availability.
# MAGIC Net trade (0.25): Weak relationship. Changes in trade were only slightly related to closing the gap.
# MAGIC Losses (-0.34): Negative relationship. When food losses increased, countries tended to make less progress. Reducing losses may help close the gap.
# MAGIC Population (0.03): Almost no relationship. Population growth did not appear to be related to whether the gap improved.
# MAGIC
# MAGIC
# MAGIC Main observation
# MAGIC
# MAGIC Production stands out as the strongest factor linked to closing the F&V availability gap. Imports and lower food losses may also help, while population change shows almost no relationship.

# COMMAND ----------

change_with_details = change_analysis.join(
    df_2014_2023.set_index('country'),
    how='left'
)

# COMMAND ----------

change_with_details

# COMMAND ----------

change_with_details.groupby('country')['year'].apply(
    lambda x: set(x) == {2014, 2023}
).value_counts()

# COMMAND ----------

change_with_details.groupby('trend')[
    [
        'fruit_veg_prod_g_percap_day_change',
        'fruit_veg_import_g_percap_day_change',
        'net_trade_g_percap_day_change',
        'loss_rate_pct_change',
        'population_change'
    ]
].median()