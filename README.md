# Food Inspection ETL

An end-to-end data engineering pipeline that consolidates food inspection records from the cities of Chicago and Dallas into a unified dimensional star schema. Built with PySpark on Databricks, the pipeline follows a medallion architecture (Bronze, Silver, Gold) and leverages Delta Live Tables for orchestration, data quality enforcement, and slowly changing dimension management.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Sources and Profiling](#data-sources-and-profiling)
- [Data Quality Findings](#data-quality-findings)
- [Pipeline Breakdown](#pipeline-breakdown)
- [Dimensional Model](#dimensional-model)
- [Power BI Dashboard](#power-bi-dashboard)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Setup and Execution](#setup-and-execution)
- [Documentation](#documentation)

---

## Overview

Food inspection data across U.S. cities varies significantly in format, schema, and scoring methodology. Chicago uses categorical outcomes (Pass, Fail, Pass with Conditions) and embeds violations in a single free-text field across 17 columns. Dallas uses numeric inspection scores (0–100) and spreads violations across 24 separate columns within a 114-column schema. This pipeline reconciles those differences by ingesting both datasets, applying data quality rules, normalizing them into a common schema, and loading them into a star schema suitable for analytical querying and dashboarding.

All initial data profiling and standardization was performed in Alteryx Designer prior to building the PySpark pipeline, using the Field Summary Report macro to generate field-level statistics for both cities.

---

## Architecture

The pipeline is organized into three layers following the Databricks medallion architecture:

```
BRONZE                        SILVER                           GOLD
─────────────────────────     ─────────────────────────────    ──────────────────────────────
Raw tab-delimited CSVs        Validated, cleaned, and          Star schema:
from Chicago and Dallas       unified into a single schema     - fact_inspection_result
ingested as-is into           with standardized columns,       - dim_facility (SCD Type 2)
Delta tables.                 data quality expectations        - dim_location
                              enforced, and violations         - dim_violation
                              extracted into arrays.           - dim_date
                                                               - inspection_violation_bridge
```

Data flows through the following stages:

```
pl1_bronze_chicago ──┐                                     ┌── dim_facility (SCD2)
                     ├─▶ pl2_silver ─▶ pl3_silver ─▶ pl4 ──┼── dim_location
pl1_bronze_dallas ───┘    (quality     (schema        (key  ├── dim_violation
                          gates)       unification)   gen)  ├── fact_inspection_result
                                                            └── inspection_violation_bridge
```

Each stage is implemented as a Databricks Delta Live Tables (DLT) pipeline definition, enabling automatic dependency resolution, incremental processing via Structured Streaming, and built-in data quality monitoring.

---

## Data Sources and Profiling

### Chicago Food Inspections

Source format: Tab-delimited CSV with 17 columns per file. The raw data consists of five yearly slices:

| File | Rows |
|---|---|
| Chicago_2021-2022.tsv | 32,804 |
| Chicago_2022-2023.tsv | 35,197 |
| Chicago_2023-2024.tsv | 37,486 |
| Chicago_2024-2025.tsv | 22,100 |
| Chicago_2025-partyear.tsv | 2,875 |
| **Total** | **130,462** |

Inspection date range: 2021-01-04 to 2025-03-05.

Key fields include `DBA Name`, `AKA Name`, `Facility Type`, `Address`, `Zip`, `Latitude`, `Longitude`, `Inspection Date`, `Inspection Type`, `Results` (categorical: Pass / Fail / Pass w/ Conditions / No Entry), and `Violations` (a single free-text field containing numbered violation descriptions separated by comment markers).

A date format inconsistency was identified in the 2021–2022 file, where `Inspection Date` used a non-standard day-first format (`%d-%m-%Y`). This was normalized to `%Y-%m-%d` during the Alteryx profiling phase before ingestion into the pipeline.

### Dallas Restaurant Inspections

Source format: Tab-delimited CSV with 114 columns per file. Five yearly slices:

| File | Rows |
|---|---|
| Dallas_2021-2022.tsv | 17,586 |
| Dallas_2022-2023.tsv | 17,391 |
| Dallas_2023-2024.tsv | 10,676 |
| Dallas_2024-2025.tsv | 1,566 |
| Dallas_2025-partyear.tsv | 0 |
| **Total** | **47,219** |

Inspection date range: 2021-01-02 to 2024-02-29 (2025 has no data).

Key fields include `Restaurant Name`, `Street Address`, `Zip Code`, `Lat Long Location` (combined coordinate string), `Inspection Date`, `Inspection Type`, `Inspection Score` (numeric, 0–100), and 24 individual `Violation Description` columns (`Violation Description - 1` through `Violation Description - 24`). Unlike Chicago, Dallas files did not include a reliable City column, so a literal `"Dallas"` value was assigned during standardization.

### Cross-City Schema Alignment

The profiling phase identified and aligned the following fields across both cities to establish a shared inspection grain:

| Unified Field | Chicago Source | Dallas Source |
|---|---|---|
| `facility_name` | `DBA Name` | `Restaurant Name` |
| `aka_name` | `AKA Name` | `Restaurant Name` |
| `facility_type` | `Facility Type` | Literal `"Restaurant"` |
| `address_line` | `Address` | `Street Address` |
| `zip_code` | `Zip` | `Zip Code` |
| `latitude` / `longitude` | `Latitude` / `Longitude` | Regex-extracted from `Lat Long Location` |
| `full_date` | `Inspection Date` | `Inspection Date` |
| `inspection_type` | `Inspection Type` | `Inspection Type` |
| `violation_score` | Derived from `Results` | `Inspection Score` |
| `clean_violations` | Regex-extracted from `Violations` | Array of 24 description columns |
| `city_name` | From source data | Formula-assigned `"Dallas"` |

Chicago-only fields retained for enrichment: `Inspection_ID`, `Alternate_Name`, `License_Number`, `Facility_Type`, `Risk_Category`, `State`, `Inspection_Result`, `Violation_Description`. Dallas-only fields in the profiling subset: `Violation_Score`, `Month`, `Year`.

---

## Data Quality Findings

The Alteryx Field Summary Report macro was used to generate field-level profiling statistics (data types, record counts, null percentages, distinct value counts, and basic numeric statistics) for both cities. The key findings that informed the pipeline's data quality rules:

**Chicago:**
- Core inspection attributes (`Facility_Name`, `Inspection_Date`, `Inspection_Type`, `Inspection_Result`) are nearly fully populated with zero nulls.
- `Zip_Code` has 9 missing values (0.01%), which are dropped in the silver zone.
- Approximately 31% of records lack a `Violation_Description`, indicating either no recorded violations or missing detail. This impacts violation-level analyses and the requirement that every inspection should have at least one distinct violation.

**Dallas:**
- All selected core fields are fully populated (0% null for key attributes).
- `Violation_Score` ranges from -26 to 100. While the upper bound of 100 matches the business rule, negative scores suggest data entry errors. The silver zone enforces a 0–100 range constraint, dropping records with out-of-range scores.
- The 114-column schema is significantly wider than Chicago's 17 columns, but only the inspection-level fields are selected for the unified model.

**Cross-City:**
- Both cities expose a common set of inspection-level attributes after alignment.
- City identification is explicit and consistent: Chicago via source data, Dallas via formula assignment.
- Naming conventions and core data types were aligned during profiling to support downstream silver and gold transformations.

---

## Pipeline Breakdown

### Stage 1 — Bronze to Silver (`1_bronze_to_silver.py`)

This is the most substantial script in the pipeline. It handles everything from raw ingestion to producing a clean, unified dataset ready for dimensional modeling.

**Ingestion:** Both city datasets are read from Databricks Volumes as tab-delimited CSVs with multiLine support enabled to handle embedded newlines in violation text.

**Data Quality Enforcement:** DLT expectations (`expect_or_drop`) are applied at the silver layer to drop rows that violate any of the following constraints:

- Chicago: non-null facility name, non-null inspection date, non-null inspection type, valid zip code (0–99999), non-null results
- Dallas: non-null restaurant name, non-null inspection date, non-null inspection type, valid zip code (0–99999), inspection score between 0 and 100

**Score Normalization:** Chicago's categorical results are converted to numeric scores for cross-city comparability: Pass = 90, Pass w/ Conditions = 80, Fail = 70, No Entry = 0.

**Violation Extraction:** Chicago's free-text violations field is parsed using the regex pattern `[0-9]+\.\s*(.*?)\s*- Comments:` to extract individual violation descriptions into an array. Dallas's 24 separate violation columns are consolidated into a single array using `array_compact` to strip nulls.

**Union and Key Generation:** The two city datasets are unioned by name, a `date_key` column is generated in `yyyyMMdd` format, a `last_updated` timestamp is added, and each row receives a UUID-based `inspection_key`.

### Stage 2a — Facility Dimension (`2a_silver_to_facility.py`)

Builds `dim_rpl_facility` as a **Slowly Changing Dimension Type 2**. Uses Databricks `create_auto_cdc_flow` to track changes in `facility_name`, `aka_name`, and `facility_type` over time. Changes are sequenced by `last_updated` and the framework automatically manages `__START_AT` and `__END_AT` timestamps to maintain a full history of facility attribute changes.

### Stage 2b — Location Dimension (`2b_silver_to_location.py`)

Builds `dim_rpl_location` using CDC flow keyed on the composite natural key of `address_line` + `zip_code`. Each location record captures the street address, zip code, latitude, and longitude.

### Stage 2c — Violation Dimension (`2c_silver_to_violation.py`)

Explodes the `clean_violations` array from the silver layer to produce one row per unique violation description. Uses CDC flow keyed on `violation_description` to maintain a deduplicated dimension of all observed violations across both cities.

### Stage 3 — Fact Table (`3_load_fact.py`)

Produces `fact_rpl_inspection_result` by joining the unified silver dataset to the facility and location dimensions. Each fact row contains the `inspection_key` (surrogate), `date_key`, `facility_key`, `location_key`, `inspection_type`, `violation_score`, `city_name`, and a `loaded_timestamp`.

### Stage 4 — Violation Bridge Table (`4_violation_fact_bridge.py`)

Resolves the many-to-many relationship between inspections and violations. Explodes the violations array from the silver layer, joins to `dim_rpl_violation` on the description text, and produces a bridge table mapping each `inspection_key` to its associated `violation_id` values.

### Supporting: Date Dimension (`date_dim.py`)

Generates `dim_date` using a SQL-based approach that produces 50,000 rows starting from 2010-01-01. Each row includes `date_str` (yyyyMMdd format, serves as the join key), `full_date`, `day_of_month`, `month_number`, `month_name`, `quarter`, `year`, `week_of_year`, and `is_weekend_flag`.

### Supporting: Table Export (`write_tables.py`)

Iterates over all six final tables (fact, four dimensions, and the bridge) and writes each to CSV with headers in a Databricks Volume path for downstream consumption by tools like Power BI.

---

## Dimensional Model

```
                         ┌─────────────────────┐
                         │     dim_date         │
                         │─────────────────────│
                         │ date_str        PK   │
                         │ full_date            │
                         │ day_of_month         │
                         │ month_number         │
                         │ month_name           │
                         │ quarter              │
                         │ year                 │
                         │ week_of_year         │
                         │ is_weekend_flag      │
                         └─────────┬───────────┘
                                   │
┌──────────────────────┐  ┌────────┴──────────────────────┐  ┌──────────────────────┐
│   dim_facility       │  │   fact_inspection_result      │  │    dim_location       │
│──────────────────────│  │───────────────────────────────│  │──────────────────────│
│ facility_id     PK   │◄─│ inspection_key           PK   │─▶│ location_id      PK  │
│ facility_name        │  │ date_key                 FK   │  │ address_line         │
│ aka_name             │  │ facility_key             FK   │  │ zip_code             │
│ facility_type        │  │ location_key             FK   │  │ latitude             │
│ __START_AT           │  │ inspection_type               │  │ longitude            │
│ __END_AT             │  │ violation_score               │  └──────────────────────┘
│ (SCD Type 2)         │  │ city_name                     │
└──────────────────────┘  │ loaded_timestamp              │
                          └───────────┬───────────────────┘
                                      │
                          ┌───────────┴───────────────────┐
                          │ inspection_violation_bridge    │
                          │───────────────────────────────│
                          │ inspection_key           FK   │
                          │ violation_id             FK   │
                          └───────────┬───────────────────┘
                                      │
                          ┌───────────┴───────────────────┐
                          │      dim_violation             │
                          │───────────────────────────────│
                          │ violation_id             PK   │
                          │ violation_description         │
                          └───────────────────────────────┘
```

The bridge table exists because a single inspection can have multiple violations and a single violation type can appear across many inspections. This many-to-many relationship is resolved through the bridge rather than embedding arrays in the fact table.

---

## Power BI Dashboard

The Power BI dashboard (`docs/GroupProject_team11.pbix`) connects to the star schema output and provides interactive analysis across five report pages. The data model in Power BI references all dimension and fact tables (`dim_date`, `dim_facility`, `dim_location`, `dim_violation`, `fact_inspection`) with the bridge table linking inspections to violations.

### Page 1 — Inspection Overview

Top-level KPI cards displaying aggregate metrics: **177K total inspections**, **81.46 average violation score**, **130K Chicago inspections**, and **47K Dallas inspections**. A city-level slicer (Blank / Chicago / Dallas) filters all visuals on the page. Two primary charts appear on this page: a line chart showing total inspections by day broken down by city, and a bar chart showing total inspections by inspection type and city. Chicago dominates in volume across most inspection types, with "Canvass" being the most frequent type for Chicago and "Routine" for Dallas.

### Page 2 — Geographic and Facility Analysis

A stacked bar chart of total inspections by `facility_type` and city, showing the distribution across restaurants, grocery stores, schools, bakeries, daycare facilities, long-term care facilities, and other types. Chicago contributes data across a wider range of facility types, while Dallas data is predominantly categorized as "Restaurant." Below this chart, a geographic map visualization plots inspection locations across the United States using latitude and longitude from `dim_location`, with bubble size representing inspection volume by zip code. The map shows two distinct clusters corresponding to Chicago and Dallas.

### Page 3 — Facility Scorecard

A tabular report listing individual facilities with their `facility_type`, `city_name`, and `Average Violation Score`. This view allows filtering by `facility_type` (with a slicer) to compare average scores across specific establishment categories and cities. The overall average across all facilities is 81.48.

### Page 4 — Inspection Detail Table

A row-level detail table displaying individual inspection records with `inspection_key`, `inspection_type`, sum of `violation_score`, `city_name`, and `facility_name`. This table is filterable by city and supports drill-through from the summary pages, providing full traceability from aggregate metrics down to individual inspection events. The table shows a total violation score sum of 13,442,961 across all records.

### Page 5 — Granular Inspection Records

The most detailed view, showing individual inspection records with `facility_name`, `facility_type`, `address_line`, `zip_code`, `inspection_type`, `violation_score`, `city_name`, `year`, and `quarter`. This page supports drill-down from the inspection type filter (with "Canvass" selected in the report screenshot) and includes an `inspection_key` filter for isolating specific records. This view is designed for operational-level analysis where individual record-level detail is required.

### DAX Measures

The dashboard includes custom DAX measures for computed KPIs, including `Dallas Inspections` (calculated using `CALCULATE` with a filter on `city_name = "Dallas"`), `Chicago Inspections` (equivalent filter for Chicago), `Total Inspections`, `Average Violation Score`, and `Total Violations`. These measures power the KPI cards and are reused across multiple report pages.

---

## Project Structure

```
Food_Inspection_ETL/
├── etl/
│   ├── 1_bronze_to_silver.py            # Raw ingestion, quality gates, schema unification
│   ├── 2a_silver_to_facility.py         # Facility dimension with SCD Type 2
│   ├── 2b_silver_to_location.py         # Location dimension
│   ├── 2c_silver_to_violation.py        # Violation dimension
│   ├── 3_load_fact.py                   # Central fact table
│   ├── 4_violation_fact_bridge.py       # Many-to-many bridge table
│   ├── date_dim.py                      # Date dimension generator
│   └── write_tables.py                  # CSV export for all final tables
├── docs/
│   ├── GroupProjectReport.pdf           # Project report with methodology and analysis
│   ├── GroupProject.DM1                 # ER/Studio data model file
│   ├── GroupProject.yxmd                # Alteryx workflow
│   └── GroupProject_team11.pbix         # Power BI dashboard
├── .gitignore
└── README.md
```

---

## Tech Stack

| Component | Technology | Role |
|---|---|---|
| Data Profiling | Alteryx Designer | Initial data profiling, field summary reports, schema standardization, date normalization |
| Processing | PySpark | All data transformations, joins, schema mapping, and regex extraction |
| Orchestration | Databricks Delta Live Tables | Pipeline dependency management, incremental processing, data quality expectations |
| Storage | Delta Lake | ACID-compliant columnar storage with schema enforcement and time travel |
| CDC / SCD | DLT `create_auto_cdc_flow` | Automatic change data capture for SCD Type 2 dimension tracking |
| Streaming | Structured Streaming | Used in silver layer for incremental reads via `readStream` |
| Data Modeling | ER/Studio | Entity-relationship diagram design |
| Visualization | Power BI Desktop | Interactive dashboards with DAX measures, geographic mapping, and drill-through reporting |

---

## Setup and Execution

**Prerequisites:** A Databricks workspace with Unity Catalog enabled and a configured Volume for data storage.

**Step 1 — Upload raw data.** Place the Chicago and Dallas TSV files into Databricks Volumes:

```
/Volumes/workspace/damg7370/restaurant_data/chicago/
/Volumes/workspace/damg7370/restaurant_data/dallas/
```

The raw data consists of five yearly slices per city (2021–2022 through 2025-partyear), totaling 130,462 Chicago records and 47,219 Dallas records.

**Step 2 — Create a DLT pipeline.** Import the ETL scripts from the `etl/` directory into a Databricks Delta Live Tables pipeline configuration. The DLT framework will automatically resolve dependencies between tables based on the `@dp.table()` decorators.

**Step 3 — Run the pipeline.** Execute the DLT pipeline. The framework processes tables in dependency order:

```
Bronze (ingestion) → Silver (quality + schema) → Gold (dimensions + fact + bridge)
```

**Step 4 — Generate the date dimension.** Run `date_dim.py` in a separate Databricks notebook, as it uses raw SQL rather than the DLT decorator pattern.

**Step 5 — Export tables.** Run `write_tables.py` to write all final tables to CSV in the output Volume.

**Step 6 — Connect Power BI.** Open `docs/GroupProject_team11.pbix` in Power BI Desktop. The dashboard expects the six tables (fact_rpl_inspection_result, dim_rpl_facility, dim_rpl_location, dim_rpl_violation, dim_date, dim_rpl_inspection_violation_bridge) either as CSV imports or via a direct connection to the Delta tables on Databricks.

---

## Documentation

The `docs/` directory contains supporting materials for this project:

- **GroupProjectReport.pdf** — Full project report covering the problem statement, data profiling methodology (Alteryx workflow details, field summary reports for both cities), data quality findings, cross-city standardization status, dimensional modeling rationale, Databricks pipeline execution screenshots, and Power BI dashboard analysis.
- **GroupProject.DM1** — The entity-relationship data model designed in ER/Studio, defining all tables, columns, keys, and relationships in the star schema.
- **GroupProject.yxmd** — The Alteryx workflow that performs initial data profiling and standardization: reads all yearly TSV files per city, normalizes date formats, assigns city identifiers, drops unnecessary fields, renames columns to a common standard, enforces data types, unions yearly slices, and generates field summary reports.
- **GroupProject_team11.pbix** — A Power BI dashboard with five report pages providing KPI summaries, inspection trends by day and type, geographic mapping, facility-level scorecards, and drill-through detail tables.

---

*Developed as part of DAMG 7370 — Designing Advanced Data Architectures for Business Intelligence.*
