# 🍔 Food Inspection ETL Pipeline

A PySpark-based ETL pipeline built on **Databricks** that ingests, cleans, and models food inspection data from **Chicago** and **Dallas** into a dimensional star schema. The pipeline follows the **Bronze → Silver → Gold** medallion architecture using Delta Live Tables (DLT).

---

## 📐 Architecture

```
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────────────┐
│     BRONZE        │      │     SILVER        │      │         GOLD              │
│                  │      │                  │      │                          │
│  Raw CSV Ingestion│─────▶│  Cleaned, Validated│─────▶│  Star Schema             │
│  • Chicago TSV   │      │  & Unified Data   │      │  • Fact Table            │
│  • Dallas TSV    │      │  • Quality Gates  │      │  • Dimension Tables      │
│                  │      │  • Schema Mapping │      │  • Bridge Table          │
└──────────────────┘      └──────────────────┘      └──────────────────────────┘
```

---

## 🗂️ Project Structure

```
Food_Inspection_ETL/
│
├── etl/
│   ├── 1_bronze_to_silver.py            # Ingest raw CSVs → clean & unify schema
│   ├── 2a_silver_to_facility.py         # Build facility dimension (SCD Type 2)
│   ├── 2b_silver_to_location.py         # Build location dimension
│   ├── 2c_silver_to_violation.py        # Build violation dimension
│   ├── 3_load_fact.py                   # Load the inspection fact table
│   ├── 4_violation_fact_bridge.py       # Build inspection-violation bridge table
│   ├── date_dim.py                      # Generate date dimension
│   └── write_tables.py                  # Export final tables to CSV
│
├── docs/
│   ├── GroupProjectReport.pdf           # Detailed project report
│   ├── GroupProject.DM1                 # ER/Studio data model
│   ├── GroupProject.yxmd                # Alteryx workflow
│   └── GroupProject_team11.pbix         # Power BI dashboard
│
├── .gitignore
└── README.md
```

---

## ⭐ Star Schema Design

```
                    ┌───────────────┐
                    │   dim_date    │
                    │───────────────│
                    │ date_str (PK) │
                    │ full_date     │
                    │ day_of_month  │
                    │ month_number  │
                    │ month_name    │
                    │ quarter       │
                    │ year          │
                    │ week_of_year  │
                    │ is_weekend    │
                    └───────┬───────┘
                            │
┌───────────────┐   ┌──────┴────────────────────┐   ┌───────────────────┐
│ dim_facility  │   │  fact_inspection_result    │   │   dim_location    │
│───────────────│   │───────────────────────────│   │───────────────────│
│ facility_id PK│◄──│ inspection_key (PK)       │──▶│ location_id (PK)  │
│ facility_name │   │ date_key (FK)             │   │ address_line      │
│ aka_name      │   │ facility_key (FK)         │   │ zip_code          │
│ facility_type │   │ location_key (FK)         │   │ latitude          │
│ __START_AT    │   │ inspection_type           │   │ longitude         │
│ __END_AT      │   │ violation_score           │   └───────────────────┘
│ (SCD Type 2)  │   │ city_name                 │
└───────────────┘   │ loaded_timestamp          │
                    └──────────┬────────────────┘
                               │
                    ┌──────────┴────────────────┐
                    │ inspection_violation_bridge│
                    │───────────────────────────│
                    │ inspection_key (FK)       │
                    │ violation_id (FK)         │
                    └──────────┬────────────────┘
                               │
                    ┌──────────┴───────┐
                    │  dim_violation    │
                    │──────────────────│
                    │ violation_id (PK)│
                    │ violation_desc   │
                    └──────────────────┘
```

---

## 🔄 Pipeline Steps

### Step 1 — Bronze to Silver (`1_bronze_to_silver.py`)

Ingests raw tab-delimited CSV files from Chicago and Dallas, then applies a series of transformations:

- **Data Quality Expectations** — Drops rows with null names, null inspection dates, null inspection types, invalid zip codes, null results/scores
- **Schema Unification** — Maps city-specific column names to a common schema (e.g., `DBA Name` → `facility_name`, `Restaurant Name` → `facility_name`)
- **Score Normalization** — Converts Chicago's categorical results to numerical scores:
  - Pass → 90 | Pass w/ Conditions → 80 | Fail → 70 | No Entry → 0
- **Violation Extraction** — Parses Chicago's free-text violations using regex; combines Dallas's 24 separate violation columns into a single array
- **City Tagging** — Adds a `city_name` column to identify the source city

### Step 2 — Silver to Gold Dimensions

- **2a — Facility Dimension** (`2a_silver_to_facility.py`)
  Builds `dim_rpl_facility` using **SCD Type 2** via Databricks `create_auto_cdc_flow`, tracking changes to facility name, AKA name, and facility type over time with `__START_AT` and `__END_AT` timestamps.

- **2b — Location Dimension** (`2b_silver_to_location.py`)
  Builds `dim_rpl_location` keyed on `address_line` + `zip_code`, capturing latitude and longitude coordinates.

- **2c — Violation Dimension** (`2c_silver_to_violation.py`)
  Explodes the violations array and deduplicates to create `dim_rpl_violation` with unique violation descriptions.

### Step 3 — Fact Table (`3_load_fact.py`)

Joins the unified silver data with all dimension tables to produce `fact_rpl_inspection_result` containing surrogate keys to facility, location, and date dimensions along with inspection type, violation score, and city name.

### Step 4 — Bridge Table (`4_violation_fact_bridge.py`)

Resolves the many-to-many relationship between inspections and violations by creating `dim_rpl_inspection_violation_bridge`, linking each `inspection_key` to its associated `violation_id` values.

### Supporting Scripts

- **`date_dim.py`** — Generates a date dimension table spanning ~136 years (50,000 days from 2010-01-01) with day, month, quarter, year, week-of-year, and weekend flag attributes.
- **`write_tables.py`** — Exports all final fact and dimension tables to CSV format for downstream consumption.

---

## 🛠️ Tech Stack

| Technology | Purpose |
|---|---|
| **PySpark** | Data processing, transformations, and joins |
| **Databricks Pipelines (DLT)** | Pipeline orchestration, data quality expectations, SCD2 tracking |
| **Delta Lake** | ACID-compliant storage layer |
| **Power BI** | Interactive dashboards and visualizations |
| **Alteryx** | Supplementary data profiling and workflow |
| **ER/Studio** | Data model design |

---

## 📊 Data Sources

| City | Format | Records Include |
|---|---|---|
| **Chicago** | Tab-delimited CSV | DBA Name, AKA Name, Facility Type, Address, Zip, Lat/Long, Inspection Date, Inspection Type, Results (categorical), Violations (free text) |
| **Dallas** | Tab-delimited CSV | Restaurant Name, Street Address, Zip Code, Lat/Long, Inspection Date, Inspection Type, Inspection Score (numeric), 24 individual Violation Description columns |

---

## 🚀 Setup & Usage

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Access to a Databricks Volume for raw data storage

### Steps

1. **Upload raw data** to your Databricks Volume:
   ```
   /Volumes/workspace/damg7370/restaurant_data/chicago/
   /Volumes/workspace/damg7370/restaurant_data/dallas/
   ```

2. **Import ETL scripts** into a Databricks Delta Live Tables pipeline

3. **Run the pipeline** — scripts execute in order:
   ```
   1_bronze_to_silver → 2a/2b/2c (dimensions) → 3_load_fact → 4_violation_fact_bridge
   ```

4. **Run `date_dim.py`** separately in a notebook to create the date dimension

5. **Run `write_tables.py`** to export final tables to CSV

6. **Open `GroupProject_team11.pbix`** in Power BI Desktop to explore the dashboard

---

## 📄 Documentation

- **`docs/GroupProjectReport.pdf`** — Full project report with methodology, design decisions, and analysis
- **`docs/GroupProject.DM1`** — Entity-relationship data model
- **`docs/GroupProject.yxmd`** — Alteryx workflow for supplementary data processing
- **`docs/GroupProject_team11.pbix`** — Power BI dashboard connected to the star schema

---

## 📜 License

This project was developed for educational purposes as part of the DAMG 7370 course.
"# Food_Inspection_ETL" 
