# FAOSTAT Food Supply Medallion Pipeline

A Databricks medallion pipeline that uses token-based authentication to retrieve FAOSTAT source data, then cleans, validates, and aggregates the data through Bronze, Silver, and Gold layers for analysis.

Originally developed as part of Byte Crew’s submission to the 2026 Women in Data Datathon, this repository documents my data-engineering contributions, including FAOSTAT authentication and extraction, Databricks secret management, data-quality validation, and Bronze, Silver, and Gold table development.

## Project Overview

The project examined global fruit and vegetable food supply in relation to a daily target of 400 grams per person. Data from multiple FAOSTAT datasets was integrated to support analysis of food availability, production, imports and exports, supply losses, and population.

The pipeline was developed in Databricks using a medallion architecture. Source data was retained in its original form before being cleaned, standardized, validated, and transformed into analysis-ready tables.

## Pipeline Architecture

### 1. Authentication

The authentication workflow retrieves FAOSTAT credentials from Databricks Secrets and submits a login request to obtain an access token. The token is then passed to the extraction notebooks through the authorization header.

Credentials and access tokens are not stored in this repository.

### 2. Extraction

Python and the Requests library were used to retrieve JSON data from authenticated FAOSTAT API endpoints. Separate extraction notebooks were created for:

- Food supply
- Production quantities
- Imports and exports
- Supply losses
- Population

HTTP status validation and request timeouts were used to identify unsuccessful API responses. The retrieved JSON files were stored in a Databricks Volume for downstream processing.

### 3. Bronze Layer

Bronze tables preserve the source data with minimal changes. This layer provides a traceable copy of the ingested FAOSTAT records and serves as the starting point for transformation.

### 4. Silver Layer

Silver-layer transformations prepare the data for analysis by:

- Selecting and renaming relevant columns
- Trimming and standardizing text values
- Converting columns to appropriate data types
- Reshaping data where necessary
- Checking for nulls, duplicates, and unexpected values
- Standardizing related fields across datasets

### 5. Gold Layer

Gold tables aggregate and combine the cleaned datasets into analysis-ready outputs. Calculations include estimated grams available per person per day and categories representing the severity of shortfalls from the 400-gram target.

A consolidated Gold table supports downstream SQL analysis and dashboard visualizations.

### 6. Analysis

The analysis layer was used to compare food-supply patterns across countries and years. It supports questions involving changes over time, differences between countries, and the relationship between food availability and the 400-gram daily target.

## Technologies Used

- Databricks
- Python
- PySpark
- Spark SQL
- Delta Lake
- Requests
- Databricks Secrets
- Unity Catalog and Volumes

## Repository Structure

```text
Byte_Crew_Project/
├── Extractions/            # FAOSTAT API extraction notebooks
├── Tables_Creations/       # Bronze, Silver, and Gold table notebooks
├── Data_Analysis_1/        # Analytical queries and project analysis
└── documentation notebook

Testing _Setup_Folder/
├── FAOSTAT_Authentication.py
├── FAOSTAT_Credential_Setup.py
└── Profiling_Data_for_Mapping.py
```

The Python files are Databricks source exports and may contain Databricks-specific notebook commands.

## Running the Project

This project was developed in a shared Databricks workspace. To run it in another environment:

1. Configure the required FAOSTAT credentials in a Databricks secret scope.
2. Update workspace, notebook, catalog, schema, and Volume paths as needed.
3. Run the authentication notebook.
4. Run the extraction notebooks.
5. Create the Bronze tables.
6. Run the Silver transformation and validation notebooks.
7. Build the Gold analytical tables.
8. Run the analysis notebooks.

Because the exported notebooks retain workspace-specific references, some paths must be updated before execution in another Databricks workspace.

## Team Project

This pipeline was developed as part of a collaborative Women in Data Datathon project by Byte Crew. This repository focuses on the data-engineering workflow and my individual contributions.

The original team submission is available in the [From Farm to 400g repository](https://github.com/lindaxeva/from-farm-to-400g).

## Data Source

Data was retrieved from the [FAOSTAT platform](https://www.fao.org/faostat/), maintained by the Food and Agriculture Organization of the United Nations.
