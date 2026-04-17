# Steam Analytics Platform

## Overview
The Steam Analytics Platform is an end-to-end data engineering solution designed to track and analyze the lifecycle, historical pricing, and concurrent player counts of the Steam gaming catalog. Because the native Steam API only exposes point-in-time pricing, this platform introduces a Slowly Changing Dimension (SCD Type 2) architecture to build a historical pricing ledger.

The project is built entirely on the Databricks Data Intelligence Platform, utilizing asynchronous data ingestion, the Medallion Architecture, and Databricks Asset Bundles (DABs) for infrastructure as code. The final dimensional model is optimized for direct consumption via Databricks Lakeview dashboards.

## Architecture


## Tech Stack
* **Compute & Processing:** Databricks Runtime (DBR) 15.3+ LTS, Apache Spark (PySpark), Spark SQL.
* **Storage:** Delta Lake, Databricks Unity Catalog (Volumes).
* **Ingestion:** Python `aiohttp` & `asyncio` for high-concurrency API requests, Databricks Auto Loader (`cloudFiles`).
* **Orchestration:** Databricks Workflows managed via Databricks Asset Bundles (DABs).
* **BI & Analytics:** Databricks Lakeview.

## Data Architecture (Medallion Pattern)

### 1. Bronze Layer (Raw Data)
* **Asynchronous Ingestion:** Utilizes `aiohttp` to manage concurrent requests to the Steam API, bypassing synchronous bottlenecks and handling rate limits (currently optimized for 200 requests / 5 min).
* **Landing Zone:** Raw JSON payloads are stored in Unity Catalog Volumes.
* **Auto Loader:** Continuous, stateful ingestion from Volumes into Bronze Delta tables using `readStream` with `trigger(availableNow=True)` for cost-effective batch processing.

### 2. Silver Layer (Conformed Data)
* **SCD Type 2 Implementation:** The `app_details` pipeline tracks price fluctuations and metadata changes over time using `MERGE` statements, maintaining `is_current` and `end_date` flags for historical accuracy.
* **Data Cleansing:** Normalization of nested JSON structures, UNIX timestamp conversions, and filtering of invalid records.

### 3. Gold Layer (Business Value)
* **Dimensional Modeling:** Implementation of a Star Schema. 
* **Bridge Tables:** Resolution of multi-valued attributes (e.g., Developers, Publishers, Genres, Categories) using `LATERAL VIEW explode` to ensure highly performant, one-to-many filtering in dashboards without relying on expensive `LIKE` queries.
* **Time Intelligence:** A comprehensive `dim_date` table and hourly-truncated fact tables enable granular trend analysis of peak player counts across different temporal axes.

## Getting Started (Local Setup & Deployment)

This project uses Databricks Asset Bundles (DABs) for seamless deployment and infrastructure as code.

### Prerequisites
To deploy and run this pipeline in your own environment, you will need:
1.  A **Databricks Workspace** (Community Edition or Enterprise).
2.  The **Databricks CLI** installed and configured (version `v0.212.0` or higher).
3.  A **Steam API Key** (You can request one [here](https://steamcommunity.com/dev/apikey)).

### Deployment Steps

**1. Clone the repository:**
```bash
git clone https://github.com/gatotroller/steam-analytics-databricks.git
cd steam-analytics-databricks
```

**2. Authenticate the Databricks CLI:**
Link your local terminal to your Databricks workspace.
```bash
databricks configure --profile default
```

**3. Configure the Steam API Secret:**
The ingestion scripts (`bronze` layer) rely on a Databricks secret scope to securely authenticate with Steam. Create the scope and store your key:
```bash
databricks secrets create-scope steam
databricks secrets put-secret steam api-key
# (Paste your Steam API key into the prompt that appears)
```

**4. Deploy the Asset Bundle:**
Deploy the workflows, environments, and tasks to your Databricks workspace using the configuration defined in `databricks.yml`.
```bash
databricks bundle deploy -t dev
```

**5. Run the Pipelines:**
Once deployed, you can trigger the jobs directly from your local CLI or via the Databricks Workflows UI.
```bash
# Run the daily catalog extraction and dimensional modeling
databricks bundle run daily_pipeline -t dev

# Run the hourly player count ingestion
databricks bundle run hourly_ingest -t dev
```

## Roadmap & Future Enhancements
This platform is actively evolving. Upcoming implementations include:
* **Spark Declarative Pipelines (SDP):** Migration of the transformation logic to Data Live Tables (DLT) for automated dependency management and native data lineage.
* **Data Quality (Expectations):** Implementation of row-level data quality rules to enforce constraints and quarantine anomalous records.
* **CI/CD Integration:** Automated deployment pipelines using GitHub Actions.
* **Code Refactoring:** Transition to a stricter DRY (Don't Repeat Yourself) paradigm and implementation of the Python `logging` module to replace standard output.
* **FinOps:** Implementation of cost tracking and estimation logic.
* **Alerting Engine:** Event-driven alerts for significant price drops, review bombing incidents, or drastic player churn.
* **Public Web Interface:** Development of a consumer-facing web application to democratize access to the aggregated analytics.