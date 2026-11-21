import json
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

from src.analytics.gold_build import run_gold_pipeline
from src.config import DQ_REPORT_PATH, DUCKDB_PATH, POSTGRES_URI
from src.generators.generate_data import generate_all_datasets
from src.genai.assistant import ask_retail_assistant
from src.ingestion.ingest import run_ingestion
from src.transform.silver_etl import run_silver_etl

st.set_page_config(
    page_title="Smart Retail Intelligence Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _execute_full_pipeline(include_incremental: bool = False) -> None:
    with st.spinner("Running End-to-End Medallion Pipeline (Source → Bronze → Silver → Gold & Postgres)..."):
        generate_all_datasets(include_incremental=include_incremental)
        ingest_stats = run_ingestion()
        run_silver_etl()
        run_gold_pipeline(sync_postgres=True)
        st.cache_data.clear()
        st.sidebar.success(
            f"Pipeline complete! Ingested: {ingest_stats['SUCCESS']} | Skipped: {ingest_stats['SKIPPED']}"
        )


@st.cache_data(ttl=60)
def load_gold_table(query: str, params: tuple = ()) -> pd.DataFrame:
    if not DUCKDB_PATH.exists():
        return pd.DataFrame()
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        return con.execute(query, list(params)).fetchdf()
    finally:
        con.close()


@st.cache_data(ttl=30)
def load_ingestion_logs() -> pd.DataFrame:
    try:
        engine = create_engine(POSTGRES_URI, future=True)
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT run_id, dataset, status, row_count, file_size_bytes, "
                    "LEFT(checksum, 12) AS checksum_prefix, error_message, ingested_at "
                    "FROM ingestion_runs ORDER BY run_id DESC LIMIT 25"
                ),
                conn,
            )
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Sidebar: Pipeline Controls & Global Filters
# ---------------------------------------------------------------------------
st.sidebar.title("Smart Retail Intelligence")
st.sidebar.caption("AWS Lakehouse • S3 / RDS + DuckDB + Vertex AI")

col_btn1, col_btn2 = st.sidebar.columns(2)
if col_btn1.button("Run Full ETL", use_container_width=True, type="primary"):
    _execute_full_pipeline(include_incremental=False)
if col_btn2.button("+ New Batch", use_container_width=True):
    _execute_full_pipeline(include_incremental=True)

st.sidebar.divider()

if not DUCKDB_PATH.exists():
    st.warning(
        "Gold database not found yet. Click **Run Full ETL** in the sidebar to generate sample data "
        "and build the Bronze, Silver, and Gold layers."
    )
    st.stop()

stores_df = load_gold_table("SELECT DISTINCT store_name FROM dim_store ORDER BY store_name")
categories_df = load_gold_table("SELECT DISTINCT category FROM dim_product ORDER BY category")
suppliers_df = load_gold_table("SELECT DISTINCT supplier_name FROM dim_supplier ORDER BY supplier_name")
date_bounds = load_gold_table("SELECT MIN(sale_date) AS min_dt, MAX(sale_date) AS max_dt FROM fact_sales")

if not date_bounds.empty and "min_dt" in date_bounds.columns and pd.notna(date_bounds["min_dt"].iloc[0]):
    min_dt = pd.to_datetime(date_bounds["min_dt"].iloc[0]).date()
    max_dt = pd.to_datetime(date_bounds["max_dt"].iloc[0]).date()
else:
    min_dt = datetime.now().date()
    max_dt = datetime.now().date()

selected_dates = st.sidebar.date_input(
    "Sales Date Range",
    value=(min_dt, max_dt),
    min_value=min_dt,
    max_value=max_dt,
)
start_date = selected_dates[0] if isinstance(selected_dates, tuple) and len(selected_dates) > 0 else min_dt
end_date = selected_dates[1] if isinstance(selected_dates, tuple) and len(selected_dates) > 1 else max_dt

stores_list = ["All"] + (stores_df["store_name"].dropna().tolist() if "store_name" in stores_df.columns else [])
categories_list = ["All"] + (categories_df["category"].dropna().tolist() if "category" in categories_df.columns else [])
suppliers_list = ["All"] + (suppliers_df["supplier_name"].dropna().tolist() if "supplier_name" in suppliers_df.columns else [])

selected_store = st.sidebar.selectbox("Store", stores_list)
selected_category = st.sidebar.selectbox("Product Category", categories_list)
selected_supplier = st.sidebar.selectbox("Supplier", suppliers_list)

# ---------------------------------------------------------------------------
# Load Filtered Analytical Datasets
# ---------------------------------------------------------------------------
sales_where = ["sale_date BETWEEN ? AND ?"]
sales_params: list = [str(start_date), str(end_date)]
if selected_store != "All":
    sales_where.append("store_name = ?")
    sales_params.append(selected_store)
if selected_category != "All":
    sales_where.append("category = ?")
    sales_params.append(selected_category)

sales_df = load_gold_table(
    f"SELECT * FROM gold_kpi_product_sales WHERE {' AND '.join(sales_where)}",
    tuple(sales_params),
)

risk_where = ["1=1"]
risk_params: list = []
if selected_store != "All":
    risk_where.append("store_name = ?")
    risk_params.append(selected_store)
if selected_category != "All":
    risk_where.append("category = ?")
    risk_params.append(selected_category)
if selected_supplier != "All":
    risk_where.append("supplier_name = ?")
    risk_params.append(selected_supplier)

risk_df = load_gold_table(
    f"SELECT * FROM gold_kpi_inventory_risk WHERE {' AND '.join(risk_where)}",
    tuple(risk_params),
)

sup_where = ["1=1"]
sup_params: list = []
if selected_supplier != "All":
    sup_where.append("supplier_name = ?")
    sup_params.append(selected_supplier)

supplier_df = load_gold_table(
    f"SELECT * FROM gold_kpi_supplier_performance WHERE {' AND '.join(sup_where)}",
    tuple(sup_params),
)

# ---------------------------------------------------------------------------
# Top KPI Cards
# ---------------------------------------------------------------------------
st.title("Smart Retail Intelligence Platform")

total_net_sales = float(sales_df["net_sales"].sum()) if not sales_df.empty else 0.0
rev_at_risk = float(risk_df["estimated_revenue_at_risk"].sum()) if not risk_df.empty else 0.0
high_risk_count = int((risk_df["stockout_risk"] == "High").sum()) if not risk_df.empty else 0
avg_otd = float(supplier_df["on_time_rate_pct"].mean()) if not supplier_df.empty else 0.0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Net Sales (Filtered)", f"${total_net_sales:,.2f}")
k2.metric("Est. Revenue at Risk", f"${rev_at_risk:,.2f}", delta="Heuristic Exposure", delta_color="inverse")
k3.metric("High-Risk SKU-Stores", f"{high_risk_count:,}", delta="Cover < Lead Time", delta_color="inverse")
k4.metric("Avg Supplier On-Time %", f"{avg_otd:.1f}%")

st.divider()

# ---------------------------------------------------------------------------
# Main Analytical Tabs
# ---------------------------------------------------------------------------
tab_overview, tab_inventory, tab_suppliers, tab_ai, tab_governance = st.tabs(
    [
        "1. Sales & Category Trends",
        "2. Inventory Risk & Cover",
        "3. Supplier & Store Dynamics",
        "4. Grounded AI Assistant",
        "5. Data Quality & Audit",
    ]
)

with tab_overview:
    if sales_df.empty:
        st.info("No sales data matches the selected filters.")
    else:
        c1, c2 = st.columns((3, 2))
        daily_trend = (
            sales_df.groupby("sale_date", as_index=False)[["net_sales", "gross_sales"]]
            .sum()
            .sort_values("sale_date")
        )
        fig_trend = px.line(
            daily_trend,
            x="sale_date",
            y="net_sales",
            title="Daily Net Sales Trend",
            markers=True,
            labels={"sale_date": "Date", "net_sales": "Net Sales ($)"},
        )
        fig_trend.update_layout(height=380, margin=dict(l=20, r=20, t=45, b=20))
        c1.plotly_chart(fig_trend, use_container_width=True)

        cat_perf = (
            sales_df.groupby("category", as_index=False)["net_sales"]
            .sum()
            .sort_values("net_sales", ascending=True)
        )
        fig_cat = px.bar(
            cat_perf,
            x="net_sales",
            y="category",
            orientation="h",
            title="Net Sales by Category",
            labels={"net_sales": "Net Sales ($)", "category": "Category"},
        )
        fig_cat.update_layout(height=380, margin=dict(l=20, r=20, t=45, b=20))
        c2.plotly_chart(fig_cat, use_container_width=True)

        st.subheader("Top Performing Products")
        top_prods = (
            sales_df.groupby(["product_id", "product_name", "category"], as_index=False)
            .agg(
                net_units=("net_units_sold", "sum"),
                returned_units=("returned_units", "sum"),
                net_sales=("net_sales", "sum"),
            )
            .sort_values("net_sales", ascending=False)
            .head(15)
        )
        st.dataframe(top_prods, use_container_width=True, hide_index=True)

with tab_inventory:
    if risk_df.empty:
        st.info("No inventory snapshot records match the selected filters.")
    else:
        capped_df = risk_df.copy()
        capped_df["days_of_cover_capped"] = capped_df["days_of_cover"].clip(upper=45)

        fig_scatter = px.scatter(
            capped_df,
            x="lead_time_days",
            y="days_of_cover_capped",
            color="stockout_risk",
            size="estimated_revenue_at_risk",
            hover_data=["store_name", "product_name", "on_hand_qty", "avg_daily_units"],
            color_discrete_map={"High": "#e74c3c", "Medium": "#f39c12", "Low": "#2ecc71"},
            title="Days of Stock Cover (Capped at 45d) vs. Supplier Lead Time",
            labels={
                "lead_time_days": "Supplier Lead Time (Days)",
                "days_of_cover_capped": "Days of Stock Cover",
                "stockout_risk": "Risk Level",
            },
        )
        fig_scatter.update_layout(height=400)
        st.plotly_chart(fig_scatter, use_container_width=True)

        st.subheader("Prioritized Replenishment & Stockout Risk Action List")
        priority_df = risk_df.sort_values(
            by=["estimated_revenue_at_risk", "days_of_cover"],
            ascending=[False, True],
        )
        st.dataframe(
            priority_df[
                [
                    "stockout_risk",
                    "store_name",
                    "product_id",
                    "product_name",
                    "category",
                    "supplier_name",
                    "on_hand_qty",
                    "avg_daily_units",
                    "days_of_cover",
                    "lead_time_days",
                    "estimated_revenue_at_risk",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

with tab_suppliers:
    col_s1, col_s2 = st.columns(2)
    with col_s1:
        st.subheader("Supplier On-Time Delivery Rate (%)")
        if not supplier_df.empty:
            sup_sorted = supplier_df.sort_values("on_time_rate_pct", ascending=True)
            fig_sup = px.bar(
                sup_sorted,
                x="on_time_rate_pct",
                y="supplier_name",
                orientation="h",
                color="avg_delay_days",
                color_continuous_scale="RdYlGn_r",
                labels={
                    "on_time_rate_pct": "On-Time Rate (%)",
                    "supplier_name": "Supplier",
                    "avg_delay_days": "Avg Delay (d)",
                },
            )
            fig_sup.update_layout(height=420, margin=dict(l=10, r=10, t=20, b=20))
            st.plotly_chart(fig_sup, use_container_width=True)

    with col_s2:
        st.subheader("Latest Store Week-over-Week (WoW) Sales")
        wow_df = load_gold_table(
            """
            WITH latest_wk AS (SELECT MAX(week_start_date) AS wk FROM gold_kpi_store_wow_sales)
            SELECT w.store_name, w.region, w.week_start_date, w.weekly_net_sales, w.prev_week_sales, w.wow_pct_change
            FROM gold_kpi_store_wow_sales w
            JOIN latest_wk l ON w.week_start_date = l.wk
            ORDER BY w.weekly_net_sales DESC
            """
        )
        st.dataframe(wow_df, use_container_width=True, hide_index=True)

    st.subheader("Detected Sales & Return Anomalies (Rolling 7-Day Baseline)")
    anomalies_df = load_gold_table(
        """
        SELECT sale_date, category, daily_gross_sales, daily_return_value, return_rate_pct, baseline_7d_sales, anomaly_flag
        FROM gold_kpi_sales_anomalies
        WHERE anomaly_flag != 'Normal'
        ORDER BY sale_date DESC
        LIMIT 20
        """
    )
    if anomalies_df.empty:
        st.success("No abnormal daily sales spikes or return-rate anomalies detected.")
    else:
        st.dataframe(anomalies_df, use_container_width=True, hide_index=True)

with tab_ai:
    st.subheader("Evidence-Grounded Retail Operations Assistant")
    st.caption(
        "Queries are strictly routed to pre-approved parameterized DuckDB SQL views. "
        "Only verified result rows and metric definitions are sent to Vertex AI (Gemini) or deterministic offline fallback."
    )

    preset_prompts = [
        "What products need urgent replenishment attention first, and why?",
        "Which suppliers are performing poorly on delivery lead times?",
        "Which products and categories generate the highest net sales?",
        "Which stores saw the biggest week-over-week sales change?",
        "Are there any unusual sales or return anomalies recently?",
    ]
    selected_preset = st.selectbox("Quick Operational Questions", ["Custom question..."] + preset_prompts)
    default_q = "" if selected_preset == "Custom question..." else selected_preset
    user_question = st.text_input(
        "Ask an operational question grounded in Gold analytics:",
        value=default_q,
        placeholder="e.g., Which high-risk SKUs have the highest revenue at risk?",
    )

    if st.button("Analyze Verified Metrics", type="primary") and user_question.strip():
        with st.spinner("Executing approved SQL query and generating grounded explanation..."):
            resp = ask_retail_assistant(
                question=user_question.strip(),
                store_filter=selected_store,
                category_filter=selected_category,
            )
        st.info(
            f"**Engine Mode:** `{resp.mode}` | **Approved SQL Intent:** `{resp.intent}` | "
            f"**Data Freshness:** `{resp.freshness_timestamp}`"
        )
        st.markdown(resp.answer)
        with st.expander("Inspect Exact SQL Evidence Rows Supplied to Model", expanded=True):
            st.dataframe(resp.evidence_df, use_container_width=True, hide_index=True)

with tab_governance:
    col_g1, col_g2 = st.columns(2)
    with col_g1:
        st.subheader("Silver Layer Data Quality Report")
        if Path(DQ_REPORT_PATH).exists():
            dq_data = json.loads(Path(DQ_REPORT_PATH).read_text(encoding="utf-8"))
            st.caption(f"Last Evaluated: {dq_data.get('generated_at', 'N/A')}")
            dq_rows = [
                {"dataset": ds, **metrics}
                for ds, metrics in dq_data.get("datasets", {}).items()
            ]
            st.dataframe(pd.DataFrame(dq_rows), use_container_width=True, hide_index=True)
        else:
            st.info("No `dq_report.json` found yet.")

    with col_g2:
        st.subheader("PostgreSQL Ingestion Audit Log (`ingestion_runs`)")
        logs_df = load_ingestion_logs()
        if logs_df.empty:
            st.info("PostgreSQL ingestion audit log unavailable or empty.")
        else:
            st.dataframe(logs_df, use_container_width=True, hide_index=True)