# Smart Retail Intelligence Platform

A production-structured **Retail Data Engineering, Lakehouse Analytics, Business Intelligence, and Evidence-Grounded GenAI Platform** built for **AWS environments** with **Amazon S3**, **Amazon RDS (PostgreSQL)**, **Apache Airflow**, **PySpark**, **DuckDB**, **Streamlit**, **Power BI Desktop**, and **Google Cloud Vertex AI (Gemini)**.

---

## 1. End-to-End Data Flowchart

The diagram below illustrates how retail data moves across the **Medallion Architecture (`Source → Bronze → Silver → Gold`)** in an AWS-compatible environment and into analytical and operational interfaces:

```text
====================================================================================================
                  ORCHESTRATION: Apache Airflow (AWS MWAA / Local) | Streamlit UI
====================================================================================================
                                                │
                                                ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 1: RAW SOURCE DATA                                                                         │
│  • UCI Online Retail transaction records + synthetic retail master & operational feeds           │
│  • Datasets: sales, products, stores, inventory, and supplier deliveries                         │
│  • Storage: data/source/ or s3://<bucket>/source/                                                │
└───────────────────────────────────────────────┬──────────────────────────────────────────────────┘
                                                │
                                                ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 2: BRONZE LAYER (IDEMPOTENT RAW INGESTION)                                                 │
│  • Schema validation & SHA-256 checksum verification (skips unchanged files on rerun)            │
│  • Date-partitioned raw staging: data/bronze/ or s3://<bucket>/bronze/ingestion_date=YYYY-MM-DD/  │
│  • Audit metadata logged to PostgreSQL / Amazon RDS: ingestion_runs                              │
└───────────────────────────────────────────────┬──────────────────────────────────────────────────┘
                                                │
                                                ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 3: SILVER LAYER (CLEANED & VALIDATED PARQUET)                                              │
│  • Data type standardization, timestamp normalization, and record deduplication                  │
│  • Data quality enforcement: quarantines invalid negative stock (on_hand_qty < 0)                │
│  • Business rule enrichment: flags customer returns (is_return = True)                           │
│  • Storage: data/silver/*.parquet (synced to s3://<bucket>/silver/) + dq_report.json              │
└───────────────────────────────────────────────┬──────────────────────────────────────────────────┘
                                                │
                                                ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 4: GOLD LAYER (DIMENSIONAL STAR SCHEMA & BUSINESS KPIs)                                    │
│  • Dimensions: dim_product, dim_store, dim_supplier, dim_date                                    │
│  • Facts: fact_sales, fact_inventory_snapshot, fact_supplier_delivery                            │
│  • KPI Views: trailing 14d demand, stockout risk tiering, revenue at risk, supplier scorecards   │
│  • Outputs:                                                                                      │
│      1. Analytical Warehouse: data/gold/retail_analytics.duckdb (synced to S3)                   │
│      2. Serving Database:     PostgreSQL / Amazon RDS (gold_* tables & vw_pbi_* views)           │
└───────────────────────────────┬───────────────────────────────────┬──────────────────────────────┘
                                │                                   │
                ┌───────────────┘                                   └───────────────┐
                ▼                                                                   ▼
┌───────────────────────────────────────────────────┐   ┌──────────────────────────────────────────┐
│ INTERFACE A: STREAMLIT & GROUNDED GENAI           │   │ INTERFACE B: POWER BI DESKTOP            │
│  • 5-Tab interactive executive & ops dashboard    │   │  • Connects via PostgreSQL / Amazon RDS  │
│  • Parameterized SQL + Vertex AI (Gemini)         │   │  • Star-schema relationships & DAX       │
│  • Offline deterministic fallback if no API key   │   │  • 3-Page interactive BI report          │
└───────────────────────────────────────────────────┘   └──────────────────────────────────────────┘
```

---

## 2. Key Architectural Features

* **AWS Lakehouse Compatible (`Amazon S3` & `Amazon RDS`):** Supports local lakehouse storage and automatic synchronization to Amazon S3 buckets, with metadata and BI serving backed by PostgreSQL / Amazon RDS.
* **Idempotent Ingestion & Auditability:** Every ingested file is hashed with SHA-256 and recorded in PostgreSQL (`ingestion_runs`), preventing duplicate loads during pipeline reruns while tracking `SUCCESS`, `SKIPPED`, and `FAILED` states.
* **Automated Data Quality Enforcement:** PySpark cleanses timestamps, deduplicates records, flags returns, quarantines negative stock values, and outputs pass/fail metrics to `dq_report.json`.
* **Dual Analytical Storage:** Uses **DuckDB** for fast local/in-memory columnar SQL analytics and **PostgreSQL / Amazon RDS** as the relational serving layer for Power BI Desktop and Airflow metadata.
* **Evidence-Grounded GenAI with Vertex AI:** Routes natural-language questions to pre-approved, parameterized read-only DuckDB queries and passes verified rows to **Google Cloud Vertex AI (Gemini 1.5 Flash)**, with a zero-dependency deterministic fallback when offline.

---

## 3. Repository Structure

```text
smart-retail-intelligence/
├── .env.example                 # Environment variables for AWS, PostgreSQL, and Vertex AI
├── .gitignore                   # Git exclusions for secrets, venvs, and lakehouse data
├── Dockerfile                   # Multi-stage build for App/ETL and Airflow (with OpenJDK)
├── docker-compose.yml           # PostgreSQL 16, Airflow (LocalExecutor), and Streamlit stack
├── pyproject.toml               # uv dependency and pytest configuration
├── README.md                    # Project documentation and architecture guide
├── dags/
│   └── retail_etl_dag.py        # Airflow DAG: generate -> bronze -> silver -> gold
├── docker/
│   └── init-db.sql              # Initializes retail_db and airflow_db in PostgreSQL
├── sql/
│   ├── 01_build_gold.sql        # DuckDB dimensional star schema DDL/DML
│   ├── 02_business_kpis.sql     # DuckDB 7 core business KPI views
│   └── 03_powerbi_views.sql     # PostgreSQL / RDS views for Power BI Desktop
├── src/
│   ├── config.py                # Centralized paths, AWS S3 helpers, DB URIs, and Vertex AI settings
│   ├── analytics/
│   │   └── gold_build.py        # DuckDB Gold builder, S3 sync, and PostgreSQL sync
│   ├── dashboard/
│   │   └── app.py               # Multi-tab Streamlit + Plotly executive & operational UI
│   ├── genai/
│   │   └── assistant.py         # Bounded SQL + Vertex AI (Gemini) / deterministic assistant
│   ├── generators/
│   │   └── generate_data.py     # UCI Online Retail loader + synthetic retail data generator
│   ├── ingestion/
│   │   └── ingest.py            # Checksum-verified Bronze ingestion, S3 sync & audit logging
│   ├── powerbi/
│   │   └── POWERBI_SETUP.md     # Power BI Desktop PostgreSQL / RDS connection & DAX guide
│   ├── tests/
│   │   └── test_pipeline.py     # Automated idempotency, DQ, Gold math & GenAI grounding tests
│   └── transform/
│       └── silver_etl.py        # PySpark + Pandas data cleaning, S3 sync, and DQ reporting
└── uv.lock                      # Dependency lockfile
```

---

## 4. Quickstart Guide

### Step 1: Configure Environment Variables
Copy `.env.example` to `.env` and configure your AWS, PostgreSQL, and Vertex AI credentials:
```bash
cp .env.example .env
```

Key environment options in `.env`:
* **AWS S3 (Optional):** `AWS_S3_BUCKET`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
* **PostgreSQL / Amazon RDS:** `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
* **Vertex AI:** `VERTEX_PROJECT_ID`, `VERTEX_LOCATION` (default: `us-central1`), `VERTEX_API_KEY`, `VERTEX_MODEL` (default: `gemini-1.5-flash`). Leaves deterministic mode active if left blank.

### Step 2: Build and Start the Stack
Start PostgreSQL, Apache Airflow, and the Streamlit dashboard:
```bash
docker compose up --build -d
```
Wait ~20 seconds on first startup for `retail_airflow_init` to complete database migrations.

### Step 3: Execute the Pipeline & Explore
* **Streamlit Dashboard & Grounded AI UI (`http://localhost:8501`):**
  * Click **Run Full ETL** in the left sidebar to generate datasets, ingest into Bronze, clean into Silver Parquet, build Gold tables, and sync to PostgreSQL / S3.
  * Use **Tab 4 (`Evidence-Grounded Retail Operations Assistant`)** to ask operational questions answered via Vertex AI (Gemini).
* **Apache Airflow UI (`http://localhost:8080`):**
  * Sign in with `admin` / `admin`, unpause `smart_retail_medallion_etl`, and trigger a run.
* **Power BI Desktop:**
  * Follow `src/powerbi/POWERBI_SETUP.md` to connect Power BI Desktop to PostgreSQL / Amazon RDS and build the 3-page report.

---

## 5. Running Automated Tests

Run the test suite using `uv`:
```bash
uv run pytest -v
```
Or inside the container:
```bash
docker compose exec streamlit uv run pytest -v
```
