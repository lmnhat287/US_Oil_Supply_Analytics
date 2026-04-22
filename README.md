# 🛢️ CrudeFlow Pipeline (Data Engineering Pipeline)

This repository contains the final project for the **Data Engineering Zoomcamp**. It is an End-to-End Data Pipeline designed to collect, process, and visualize the United States oil market data, helping stakeholders analyze supply-chain balances, price trends, and import/export ratios.

## 📌 1. Problem Description
Understanding the dynamics of the oil market requires tracking multiple indicators simultaneously. This project builds an automated pipeline to monitor key metrics such as **WTI Crude Oil Prices, Commercial Inventories, Refinery Inputs, and Import/Export volumes**. 

By transforming raw API data and static CSVs into a centralized Data Lakehouse, the system empowers data-driven business decisions through an interactive Power BI dashboard.

## 🏗️ 2. Architecture & Technologies
The project adopts a **Local Modern Data Stack** approach, completely containerized for reproducibility.

* **Language:** Python
* **Orchestration:** Apache Airflow (Dockerized)
* **Data Ingestion:** Python Requests / Pandas
* **Data Lake:** Local File System (Parquet format)
* **Data Warehouse:** DuckDB (In-process analytical database)
* **Data Transformation:** dbt Core (Data Build Tool)
* **Visualization:** Power BI (via Python Script / ODBC)

![Architecture Diagram](https://img.shields.io/badge/Architecture-Medallion_Data_Lakehouse-blue)
*(Note: You can later add a screenshot of your architecture diagram here)*

## 📥 3. Data Ingestion (Extraction & Loading)
Data is gathered from multiple sources:
1. **U.S. Energy Information Administration (EIA) API:** Fetches daily market fluctuations (oil prices, exports, stocks).
2. **U.S. ONRR Datasets:** Heavy static CSV files containing federal production and crude oil import data.

**Airflow DAGs:**
* `01_init_oil_history`: A one-time DAG to load historical CSV data and past API records into the Data Lake.
* `02_daily_price_update`: A scheduled DAG (`0 18 * * *` UTC) that fetches incremental daily API data.
* `03_data_lake_backup`: A weekly maintenance DAG to compress and archive the Data Lake.

## 🗄️ 4. Data Lakehouse & Transformations (dbt)
Instead of a traditional RDBMS like MySQL, this pipeline leverages **DuckDB** reading directly from **Parquet** files (Data Lake), offering blazing-fast analytical performance.

**dbt is used to enforce the Medallion Architecture:**
* **Silver Layer (Staging):** Standardizes column names, casts data types (Dates, Doubles), and cleans raw Parquet data (e.g., `stg_prices`, `stg_imports`).
* **Gold Layer (Marts/Facts):** Handles data frequency mismatches (daily prices vs. monthly production) and applies complex joins to create the final `mart_supply_chain` table.
* **Data Quality:** Configured generic tests (`not_null`, `accepted_values`) in `schema.yml` to ensure pipeline integrity.

## 📊 5. Dashboard
The final data is served to **Power BI** for visualization. The dashboard includes:
* **Market Overview:** A combined Line/Bar chart showing the inverse correlation between Oil Stocks and WTI Prices.
* **Supply Analysis:** Stacked bar charts and donuts comparing domestic production vs. imported oil volumes.
* **Seasonality:** Historical price tracking across different months and years.

## 🚀 6. Reproducibility (How to run this project)

### Prerequisites
* Docker & Docker Compose
* Python 3.10+
* dbt-duckdb

### Step-by-step Setup
**1. Clone the repository**
bash
git clone [https://github.com/your-username/US_Oil_Supply_Analytics.git](https://github.com/your-username/US_Oil_Supply_Analytics.git)
cd US_Oil_Supply_Analytics

**2. Start the Airflow Infrastructure**

Bash
docker-compose up -d
Access the Airflow UI at http://localhost:8080 (Default login: airflow/airflow). Trigger the 01_init_oil_history DAG to populate the Data Lake.

**3. Setup dbt Environment**

Bash
python -m venv venv
source venv/Scripts/activate  # (or venv\Scripts\activate on Windows)
pip install dbt-duckdb pandas requests
**4. Run dbt Transformations**

Bash
cd oil_transformation
dbt debug
dbt build
**5. Connect to Power BI**
Open Power BI Desktop, use the Python Script connector, and run:

Python
import duckdb
con = duckdb.connect(r'path/to/your/data_lake/processed/zoomcamp_dw.duckdb', read_only=True)
mart_supply_chain = con.execute("SELECT * FROM mart_supply_chain").df()
con.close()
