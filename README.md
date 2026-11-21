# Smart Retail Intelligence Platform

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.1%2B-FFF000.svg?logo=duckdb&logoColor=black)](https://duckdb.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.10-017CEE.svg?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5%2B-E25A1C.svg?logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.35%2B-FF4B4B.svg?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Power BI](https://img.shields.io/badge/Power_BI-Desktop-F2C811.svg?logo=power-bi&logoColor=black)](https://powerbi.microsoft.com/)
[![Groq](https://img.shields.io/badge/Groq-Llama_3.3_70B-F05A28.svg)](https://groq.com/)
[![uv](https://img.shields.io/badge/Package_Manager-uv-DE5FE9.svg)](https://github.com/astral-sh/uv)

A local-first, production-structured **Retail Data Engineering, Business Intelligence, and Evidence-Grounded GenAI Platform** built with **Apache Airflow**, **PySpark**, **DuckDB**, **PostgreSQL**, **Streamlit**, **Power BI Desktop**, and **Groq**.

The platform processes real-world retail transactions (UCI Online Retail dataset) alongside synthetic supply chain feeds, enforces Medallion Lakehouse principles (`Bronze → Silver → Gold`), automates data quality checks and quarantine rules, serves analytical views via DuckDB and PostgreSQL, and provides interactive executive dashboards and an evidence-grounded AI assistant.

---

## Table of Contents

- [1. Architecture & End-to-End Flow](#1-architecture--end-to-end-flow)
- [2. Key Architectural Features](#2-key-architectural-features)
- [3. Repository Structure](#3-repository-structure)
- [4. Environment Configuration](#4-environment-configuration)
- [5. Quickstart Guide (Docker)](#5-quickstart-guide-docker)
- [6. Local Development Guide (uv)](#6-local-development-guide-uv)
- [7. Interfaces & Capabilities](#7-interfaces--capabilities)
  - [Streamlit Dashboard (5 Tabs)](#streamlit-dashboard-5-tabs)
  - [Evidence-Grounded GenAI Assistant](#evidence-grounded-genai-assistant)
  - [Power BI Desktop Integration](#power-bi-desktop-integration)
  - [Apache Airflow Orchestration](#apache-airflow-orchestration)
- [8. Automated Testing & Verification](#8-automated-testing--verification)
- [9. Data Quality & Idempotency Design](#9-data-quality--idempotency-design)

---

## 1. Architecture & End-to-End Flow

```text
====================================================================================================
                  ORCHESTRATION: Apache Airflow (Daily DAG)  |  Streamlit UI (On-Demand)
====================================================================================================
                                                │
                                                ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 1: RAW SOURCE DATA                                                                         │
│  • UCI Online Retail transaction records + synthetic retail master & operational feeds           │
│  • Datasets: sales, products, stores, inventory, and supplier deliveries                         │
│  • Storage: data/source/<dataset>/                                                               │
└───────────────────────────────────────────────┬──────────────────────────────────────────────────┘
                                                │
                                                ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 2: BRONZE LAYER (IDEMPOTENT RAW INGESTION)                                                 │
│  • Schema validation & SHA-256 checksum verification (skips unchanged files on rerun)            │
│  • Date-partitioned raw staging: data/bronze/<dataset>/ingestion_date=YYYY-MM-DD/                │
│  • Audit metadata logged to PostgreSQL: ingestion_runs (status, row_count, checksum)             │
└───────────────────────────────────────────────┬──────────────────────────────────────────────────┘
                                                │
                                                ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 3: SILVER LAYER (CLEANED & VALIDATED PARQUET)                                              │
│  • Data type standardization, timestamp normalization, and record deduplication                  │
│  • Data quality enforcement: quarantines invalid negative stock (on_hand_qty < 0)                │
│  • Business rule enrichment: flags return transactions (is_return = True)                        │
│  • Storage: data/silver/*.parquet + data/silver/dq_report.json                                   │
└───────────────────────────────────────────────┬──────────────────────────────────────────────────┘
                                                │
                                                ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 4: GOLD LAYER (DIMENSIONAL STAR SCHEMA & BUSINESS KPIs)                                    │
│  • Dimensions: dim_product, dim_store, dim_supplier, dim_date                                    │
│  • Facts: fact_sales, fact_inventory_snapshot, fact_supplier_delivery                            │
│  • KPI Views: product sales, trailing 14d demand & days of cover, stockout risk tiering,         │
│               estimated revenue at risk, supplier on-time/fill rates, store WoW sales, anomalies │
│  • Outputs:                                                                                      │
│      1. Analytical Warehouse: data/gold/retail_analytics.duckdb                                  │
│      2. Serving Database:     PostgreSQL retail_db (gold_* tables & vw_pbi_* views)              │
└───────────────────────────────┬───────────────────────────────────┬──────────────────────────────┘
                                │                                   │
                ┌───────────────┘                                   └───────────────┐
                ▼                                                                   ▼
┌───────────────────────────────────────────────────┐   ┌──────────────────────────────────────────┐
│ INTERFACE A: STREAMLIT & GROUNDED GENAI           │   │ INTERFACE B: POWER BI DESKTOP            │
│  • 5-Tab interactive executive & ops dashboard    │   │  • Connects via PostgreSQL (port 5432)   │
│  • Parameterized read-only SQL + Groq LLM         │   │  • Star-schema relationships & DAX       │
│  • Offline deterministic fallback if no API key   │   │  • 3-Page interactive BI report          │
└───────────────────────────────────────────────────┘   └──────────────────────────────────────────┘
```

---

## 2. Key Architectural Features

* **Medallion Lakehouse Architecture (`Bronze → Silver → Gold`):** Separates immutable raw ingestion from cleaned, query-optimized Parquet datasets and dimensional star-schema analytics.
* **Idempotent Ingestion with SHA-256 Audit Tracking:** Every ingested file is cryptographically hashed with SHA-256 and recorded in PostgreSQL (`ingestion_runs`), ensuring duplicate drops are automatically skipped during pipeline reruns while capturing `SUCCESS`, `SKIPPED`, and `FAILED` states.
* **Automated Data Quality & Quarantine:** PySpark/Pandas validates timestamps, normalizes customer transactions, flags returns, isolates negative stock records (`on_hand_qty < 0`) into quarantine partitions, and outputs summary metrics to `dq_report.json`.
* **Dual Analytical Engine:**
  * **DuckDB:** Embedded analytical engine providing columnar SQL execution on Parquet and local Gold tables for low-latency dashboard queries and bounded GenAI routing.
  * **PostgreSQL 16:** Relational serving database hosting `gold_*` dimensional tables, metadata registries, and optimized `vw_pbi_*` views for Power BI Desktop and Airflow metadata.
* **Evidence-Grounded GenAI Retail Assistant:** Uses a bounded routing architecture: user questions are mapped to strict, parameterized read-only DuckDB analytical queries. Only verified analytical rows and deterministic context are passed to **Groq** (`llama-3.3-70b-versatile`), completely eliminating hallucinations. An offline deterministic fallback functions seamlessly when no API key is provided.

---

## 3. Repository Structure

```text
smart-retail-intelligence/
├── .env.example                 # Environment configuration template
├── .gitignore                   # Exclusions for lakehouse data, venvs, and secrets
├── Dockerfile                   # Multi-stage image build for App/ETL & Airflow (OpenJDK 21)
├── docker-compose.yml           # PostgreSQL 16, Airflow (LocalExecutor), & Streamlit stack
├── pyproject.toml               # uv project configuration, dependencies & pytest settings
├── README.md                    # Project documentation, architecture & quickstart
├── dags/
│   └── retail_etl_dag.py        # Airflow DAG: generate -> bronze -> silver -> gold
├── docker/
│   └── init-db.sql              # Database initialization script (retail_db & airflow_db)
├── sql/
│   ├── 01_build_gold.sql        # DuckDB dimensional star schema DDL/DML
│   ├── 02_business_kpis.sql     # DuckDB 7 core business KPI views
│   └── 03_powerbi_views.sql     # PostgreSQL serving views for Power BI Desktop
├── src/
│   ├── config.py                # Centralized project paths, database URIs, & thresholds
│   ├── analytics/
│   │   ├── __init__.py
│   │   └── gold_build.py        # DuckDB Gold builder & PostgreSQL synchronization
│   ├── dashboard/
│   │   ├── __init__.py
│   │   └── app.py               # 5-Tab Streamlit + Plotly executive dashboard
│   ├── genai/
│   │   ├── __init__.py
│   │   └── assistant.py         # Grounded SQL generator + Groq LLM / deterministic assistant
│   ├── generators/
│   │   ├── __init__.py
│   │   └── generate_data.py     # UCI Online Retail downloader + synthetic data generator
│   ├── ingestion/
│   │   ├── __init__.py
│   │   └── ingest.py            # Checksum-verified Bronze ingestion & audit logger
│   ├── powerbi/
│   │   └── POWERBI_SETUP.md     # Power BI Desktop connection setup & DAX measure guide
│   ├── tests/
│   │   ├── __init__.py
│   │   └── test_pipeline.py     # Automated idempotency, DQ, Gold math & GenAI test suite
│   └── transform/
│       ├── __init__.py
│       └── silver_etl.py        # PySpark + Pandas data cleaning, deduplication & DQ report
└── uv.lock                      # Deterministic dependency lockfile
```

---

## 4. Environment Configuration

Copy `.env.example` to create your active environment file:

```bash
cp .env.example .env
```

### Key Configuration Variables

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `POSTGRES_USER` | `retail_admin` | PostgreSQL administrator username |
| `POSTGRES_PASSWORD` | `retail_secret_pass` | PostgreSQL administrator password |
| `POSTGRES_HOST` | `postgres` (or `localhost`) | PostgreSQL hostname (`postgres` for Docker, `localhost` for local) |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `retail_db` | Main retail analytical serving database |
| `AIRFLOW_DB` | `airflow_db` | Dedicated metadata database for Apache Airflow |
| `AIRFLOW_UID` | `50000` | Host UID for Airflow container process |
| `AIRFLOW_ADMIN_USER` | `admin` | Airflow Web UI username |
| `AIRFLOW_ADMIN_PASSWORD` | `admin` | Airflow Web UI password |
| `GROQ_API_KEY` | *(Optional)* | Groq API Key for Llama 3.3. If blank, offline deterministic mode runs |
| `GROQ_MODEL` | `llama-3.3-70b-versatile` | Groq LLM model name |

---

## 5. Quickstart Guide (Docker)

The fastest and most reliable way to run the entire end-to-end platform is using Docker Compose:

### Step 1: Start the Infrastructure Stack
```bash
docker compose up --build -d
```

> **Note:** On first startup, wait approximately 20–30 seconds for `retail_airflow_init` to finish database migrations and create the default admin user.

Verify all containers are healthy:
```bash
docker compose ps
```

### Step 2: Access the Application Interfaces
* **Streamlit Dashboard & GenAI UI:** [`http://localhost:8501`](http://localhost:8501)
  * Click **Run Full ETL** in the left sidebar to generate source datasets, ingest into Bronze, clean into Silver Parquet, compile DuckDB Gold tables, and sync to PostgreSQL.
  * Click **+ New Batch** to drop an incremental file and verify SHA-256 idempotency in **Tab 5 (`Data Quality & Audit`)**.
* **Apache Airflow Orchestration UI:** [`http://localhost:8080`](http://localhost:8080)
  * Log in with credentials: `admin` / `admin`.
  * Locate `smart_retail_medallion_etl`, toggle it to unpaused, and click **Trigger DAG** to execute the pipeline.
* **PostgreSQL Serving Layer:** `localhost:5432` (`retail_db`)
  * Connect via DBeaver, pgAdmin, or Power BI Desktop using `retail_admin` / `retail_secret_pass`.

### Step 3: Tear Down
```bash
docker compose down
# To clear persistent database and lakehouse volumes:
docker compose down -v
```

---

## 6. Local Development Guide (uv)

For fast local iteration without containers, you can run the pipeline directly using [`uv`](https://github.com/astral-sh/uv):

### Step 1: Install uv & Synchronize Dependencies
```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Sync virtual environment and all dependencies
uv sync
```

### Step 2: Run Pipeline Stages Manually
```bash
# 1. Download UCI dataset & generate synthetic master feeds
uv run python -m src.generators.generate_data

# 2. Ingest into Bronze layer with SHA-256 verification
uv run python -m src.ingestion.ingest

# 3. Cleanse and validate into Silver Parquet
uv run python -m src.transform.silver_etl

# 4. Build DuckDB Gold star schema & KPI views
uv run python -m src.analytics.gold_build
```

### Step 3: Launch Streamlit Dashboard
```bash
uv run streamlit run src/dashboard/app.py
```

---

## 7. Interfaces & Capabilities

### Streamlit Dashboard (5 Tabs)

The Streamlit dashboard provides comprehensive real-time retail visibility:

1. **Tab 1: Executive Overview:** High-level executive KPI scorecards (Total Net Sales, Gross Sales, Return Rate %, Total Units Sold), weekly sales trends, and sales distribution by product category.
2. **Tab 2: Inventory Coverage & Stockout Risk:** Identifies high-risk SKUs using 14-day trailing demand, current on-hand stock, lead times, days of cover, and estimated revenue at risk.
3. **Tab 3: Supplier Reliability Scorecard:** Tracks vendor performance across fulfillment on-time delivery rates (`on_time_rate_pct`), fill rates, and average delay days.
4. **Tab 4: Grounded Retail GenAI Assistant:** Natural-language retail query assistant. Questions are routed through pre-approved parameterized SQL queries executed directly against DuckDB.
5. **Tab 5: Data Quality & Ingestion Audit:** Live inspection of the SHA-256 ingestion audit log (`ingestion_runs`) and data quality validation reports (`dq_report.json`).

### Evidence-Grounded GenAI Assistant

To prevent hallucinations, the GenAI assistant strictly uses a **two-layer grounding pattern**:
1. Natural language questions are categorized into specific analytical query intents (e.g., *highest stockout risk*, *supplier delivery performance*, *top categories*, *sales trends*).
2. The system executes a parameterized, read-only SQL query against DuckDB Gold views to extract ground-truth numbers.
3. If `GROQ_API_KEY` is present, the structured SQL result and strict metrics are passed to Groq (`llama-3.3-70b-versatile`) with instructions to only answer from the provided data.
4. If no API key is configured or network is unavailable, an **offline deterministic template engine** formulates the verified answer immediately.

### Power BI Desktop Integration

The platform provides a first-class Power BI Desktop integration via PostgreSQL:
* **Connection Target:** `localhost:5432` / Database: `retail_db`
* **Pre-built Views:** `vw_pbi_executive_summary`, `vw_pbi_inventory_action_list`, `vw_pbi_supplier_scorecard`
* **Complete Setup Guide:** Step-by-step instructions for data modeling, star-schema table relationships, and DAX measures are documented in [`src/powerbi/POWERBI_SETUP.md`](file:///Users/satendrapatel/Downloads/Data/smart-retail-intelligence/src/powerbi/POWERBI_SETUP.md).

### Apache Airflow Orchestration

The daily production orchestration pipeline is defined in `dags/retail_etl_dag.py`:
* **DAG ID:** `smart_retail_medallion_etl`
* **Task Sequence:** `generate_sources >> ingest_bronze >> transform_silver >> build_gold`
* **Execution Model:** Containerized LocalExecutor with automatic retries, task-level execution logs, and database connection pooling.

---

## 8. Automated Testing & Verification

The automated test suite in `src/tests/test_pipeline.py` validates end-to-end pipeline integrity:
* **SHA-256 Idempotency:** Confirms that re-ingesting identical files skips loading and logs audit metadata.
* **Data Quality Quarantine:** Tests that negative stock levels are quarantined and flagged returns are handled correctly.
* **Dimensional Math & KPIs:** Verifies Days of Cover calculations, stockout tiering logic, and revenue-at-risk computations.
* **GenAI Grounding:** Confirms that LLM prompts receive verified SQL context and produce bounded responses.

### Run Tests Inside Docker
```bash
docker compose exec streamlit uv run pytest -v
```

### Run Tests Locally
```bash
uv run pytest -v
```

---

## 9. Data Quality & Idempotency Design

| Layer | Input | Output | Verification & DQ Enforcement |
| :--- | :--- | :--- | :--- |
| **Source** | External feeds / UCI | Raw CSV / JSON | Schema structural checks, null validation |
| **Bronze** | Raw files | Partitioned raw staging | SHA-256 hashing; duplicate skipping; PostgreSQL run audit |
| **Silver** | Bronze partitions | Cleansed Parquet | Timestamp standardization, deduplication, return flagging, negative stock isolation (`dq_report.json`) |
| **Gold** | Silver Parquet | DuckDB & PostgreSQL | Dimensional star-schema integrity, foreign key checks, pre-computed KPI views |

---

## Summary

A complete, production-structured retail data lakehouse and intelligence platform designed for local evaluation, automated ETL orchestration, and interactive business intelligence.
