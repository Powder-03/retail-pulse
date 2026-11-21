import json
from dataclasses import dataclass
from pathlib import Path
import duckdb
import pandas as pd
from groq import Groq

from src.config import DUCKDB_PATH, GROQ_API_KEY, GROQ_MODEL, SAFETY_BUFFER_DAYS

METRIC_DEFINITIONS = {
    "days_of_cover": "on_hand_qty / trailing 14-day average daily units sold.",
    "stockout_risk": f"Heuristic classification comparing days_of_cover against supplier lead_time_days + {SAFETY_BUFFER_DAYS}-day safety buffer (High if days_of_cover < lead_time_days; Medium if < lead_time_days + {SAFETY_BUFFER_DAYS}; else Low).",
    "estimated_revenue_at_risk": "trailing 14-day avg_daily_sales_value * MAX(0, lead_time_days - days_of_cover). Explicitly an estimate, not realized loss.",
    "on_time_rate_pct": "(on_time_deliveries / completed_deliveries) * 100.",
}


@dataclass
class AssistantResponse:
    answer: str
    intent: str
    mode: str  # "groq" | "deterministic_fallback"
    evidence_df: pd.DataFrame
    freshness_timestamp: str


def _classify_intent(question: str) -> str:
    q = question.lower()
    if any(k in q for k in ("supplier", "delivery", "delay", "late", "on-time", "on time", "vendor")):
        return "supplier_performance"
    if any(k in q for k in ("top", "best", "revenue", "selling", "category", "sales trend")):
        return "top_sales"
    if any(k in q for k in ("store", "week", "wow", "growth", "drop")):
        return "store_wow"
    if any(k in q for k in ("anomaly", "unusual", "return", "spike")):
        return "anomalies"
    return "inventory_risk"


def _fetch_bounded_evidence(
    intent: str,
    duckdb_path: Path = DUCKDB_PATH,
    store_filter: str | None = None,
    category_filter: str | None = None,
) -> tuple[pd.DataFrame, str]:
    """Executes only pre-approved parameterized SQL queries against Gold DuckDB."""
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        freshness_row = con.execute(
            "SELECT COALESCE(CAST(MAX(snapshot_date) AS VARCHAR), 'N/A') FROM fact_inventory_snapshot"
        ).fetchone()
        freshness = freshness_row[0] if freshness_row else "N/A"

        where_clauses = ["1=1"]
        params: list = []

        if intent == "inventory_risk":
            if store_filter and store_filter != "All":
                where_clauses.append("store_name = ?")
                params.append(store_filter)
            if category_filter and category_filter != "All":
                where_clauses.append("category = ?")
                params.append(category_filter)

            sql = f"""
                SELECT
                    store_name,
                    product_id,
                    product_name,
                    category,
                    supplier_name,
                    on_hand_qty,
                    avg_daily_units,
                    days_of_cover,
                    lead_time_days,
                    stockout_risk,
                    estimated_revenue_at_risk
                FROM gold_kpi_inventory_risk
                WHERE {' AND '.join(where_clauses)}
                ORDER BY
                    CASE stockout_risk WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END,
                    estimated_revenue_at_risk DESC
                LIMIT 10
            """
            df = con.execute(sql, params).fetchdf()

        elif intent == "supplier_performance":
            sql = """
                SELECT
                    supplier_id,
                    supplier_name,
                    contracted_lead_time_days,
                    completed_deliveries,
                    on_time_deliveries,
                    late_deliveries,
                    on_time_rate_pct,
                    avg_delay_days,
                    fill_rate_pct
                FROM gold_kpi_supplier_performance
                ORDER BY on_time_rate_pct ASC NULLS LAST, avg_delay_days DESC
                LIMIT 10
            """
            df = con.execute(sql).fetchdf()

        elif intent == "top_sales":
            if store_filter and store_filter != "All":
                where_clauses.append("store_name = ?")
                params.append(store_filter)
            if category_filter and category_filter != "All":
                where_clauses.append("category = ?")
                params.append(category_filter)

            sql = f"""
                SELECT
                    product_id,
                    product_name,
                    category,
                    SUM(net_units_sold) AS net_units_sold,
                    ROUND(SUM(net_sales), 2) AS total_net_sales,
                    SUM(returned_units) AS returned_units
                FROM gold_kpi_product_sales
                WHERE {' AND '.join(where_clauses)}
                GROUP BY 1, 2, 3
                ORDER BY total_net_sales DESC
                LIMIT 10
            """
            df = con.execute(sql, params).fetchdf()

        elif intent == "store_wow":
            sql = """
                WITH latest_week AS (
                    SELECT MAX(week_start_date) AS max_wk FROM gold_kpi_store_wow_sales
                )
                SELECT
                    w.store_id,
                    w.store_name,
                    w.region,
                    w.week_start_date,
                    w.weekly_net_sales,
                    w.prev_week_sales,
                    w.wow_abs_change,
                    w.wow_pct_change
                FROM gold_kpi_store_wow_sales w
                CROSS JOIN latest_week lw
                WHERE w.week_start_date = lw.max_wk
                ORDER BY ABS(COALESCE(w.wow_pct_change, 0)) DESC
                LIMIT 10
            """
            df = con.execute(sql).fetchdf()

        else:  # anomalies
            sql = """
                SELECT
                    sale_date,
                    category,
                    daily_gross_sales,
                    daily_return_value,
                    return_rate_pct,
                    baseline_7d_sales,
                    anomaly_flag
                FROM gold_kpi_sales_anomalies
                WHERE anomaly_flag != 'Normal'
                ORDER BY sale_date DESC
                LIMIT 10
            """
            df = con.execute(sql).fetchdf()

        return df, freshness
    finally:
        con.close()


def _deterministic_summary(intent: str, df: pd.DataFrame, freshness: str) -> str:
    """Offline rule-based fallback ensuring zero-hallucination responses without an API key."""
    if df.empty:
        return (
            f"**Insufficient Evidence (Data as of {freshness}):** No matching records were returned "
            "by the approved Gold analytics query for your current filter selection."
        )

    lines = [f"**Grounded Operational Summary (Snapshot Date: {freshness})** *(Deterministic Mode)*\n"]

    if intent == "inventory_risk":
        high_risk = df[df["stockout_risk"] == "High"]
        lines.append(
            f"- Found **{len(high_risk)} High-Risk** SKU-store combinations in the top {len(df)} priority items."
        )
        for _, r in df.head(5).iterrows():
            lines.append(
                f"- **{r['product_name']} ({r['product_id']})** at *{r['store_name']}*: "
                f"**{r['stockout_risk']} Risk** — {r['on_hand_qty']} units on hand "
                f"({r['days_of_cover']} days cover vs. {r['lead_time_days']}d supplier lead time); "
                f"est. revenue at risk: **${r['estimated_revenue_at_risk']:,.2f}**."
            )
        lines.append(
            "\n*Caveat: Stockout risk and revenue at risk are heuristic estimates based on trailing 14-day demand.*"
        )

    elif intent == "supplier_performance":
        for _, r in df.head(5).iterrows():
            lines.append(
                f"- **{r['supplier_name']} ({r['supplier_id']})**: **{r['on_time_rate_pct']}%** on-time rate "
                f"across {r['completed_deliveries']} completed deliveries ({r['late_deliveries']} late, "
                f"avg delay **{r['avg_delay_days']} days**, fill rate {r['fill_rate_pct']}%)."
            )
        lines.append("\n*Caveat: Small delivery sample counts may skew percentage rates.*")

    elif intent == "top_sales":
        for _, r in df.head(5).iterrows():
            lines.append(
                f"- **{r['product_name']} ({r['product_id']})** [{r['category']}]: "
                f"**${r['total_net_sales']:,.2f}** net sales ({r['net_units_sold']} net units, "
                f"{r['returned_units']} returned units)."
            )

    elif intent == "store_wow":
        for _, r in df.head(5).iterrows():
            pct = f"{r['wow_pct_change']:+.1f}%" if pd.notna(r["wow_pct_change"]) else "N/A"
            lines.append(
                f"- **{r['store_name']} ({r['region']})** [Week of {r['week_start_date']}]: "
                f"**${r['weekly_net_sales']:,.2f}** ({pct} vs. prior week)."
            )

    else:
        for _, r in df.head(5).iterrows():
            lines.append(
                f"- **{r['sale_date']} — {r['category']}** [{r['anomaly_flag']}]: "
                f"Gross sales **${r['daily_gross_sales']:,.2f}** vs. 7-day baseline "
                f"**${r['baseline_7d_sales']:,.2f}** (Return rate: {r['return_rate_pct']}%)."
            )
        lines.append("\n*Caveat: Flags indicate statistical deviation from a 7-day rolling baseline for review.*")

    return "\n".join(lines)


def _groq_summary(
    question: str,
    intent: str,
    df: pd.DataFrame,
    freshness: str,
    api_key: str,
    model: str,
) -> str:
    if df.empty:
        return (
            f"**Insufficient Evidence (Data as of {freshness}):** The approved SQL query returned zero rows "
            "for the selected filters. Try broadening your Store or Category filter."
        )

    evidence_records = json.loads(df.to_json(orient="records", date_format="iso"))
    system_prompt = (
        "You are a grounded retail operations analyst assistant. "
        "Answer the user's question using ONLY the verified SQL result rows and metric definitions provided. "
        "Rules:\n"
        "1. Every numeric claim MUST match the supplied JSON rows exactly.\n"
        "2. Do NOT invent products, stores, suppliers, or numbers not present in the JSON.\n"
        "3. Explicitly mention the data freshness snapshot date and state caveats (e.g., rule-based heuristic risk or small sample sizes).\n"
        "4. Keep the response concise, structured with bullet points, and actionable for store/inventory managers."
    )

    user_payload = {
        "user_question": question,
        "query_intent": intent,
        "data_freshness_snapshot_date": freshness,
        "metric_definitions": METRIC_DEFINITIONS,
        "verified_sql_rows": evidence_records,
    }

    client = Groq(api_key=api_key)
    completion = client.chat.completions.create(
        model=model,
        temperature=0.1,
        max_tokens=600,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload, indent=2)},
        ],
    )
    return completion.choices[0].message.content.strip()


def ask_retail_assistant(
    question: str,
    duckdb_path: Path = DUCKDB_PATH,
    store_filter: str | None = None,
    category_filter: str | None = None,
    api_key: str | None = None,
    model: str = GROQ_MODEL,
) -> AssistantResponse:
    """Runs bounded SQL query and generates a grounded explanation via Groq or deterministic fallback."""
    intent = _classify_intent(question)
    evidence_df, freshness = _fetch_bounded_evidence(
        intent=intent,
        duckdb_path=duckdb_path,
        store_filter=store_filter,
        category_filter=category_filter,
    )

    effective_key = (api_key if api_key is not None else GROQ_API_KEY).strip()
    use_groq = bool(effective_key) and effective_key != "your_groq_api_key_here"

    if use_groq:
        try:
            answer = _groq_summary(question, intent, evidence_df, freshness, effective_key, model)
            return AssistantResponse(
                answer=answer,
                intent=intent,
                mode="groq",
                evidence_df=evidence_df,
                freshness_timestamp=freshness,
            )
        except Exception as exc:
            fallback_text = _deterministic_summary(intent, evidence_df, freshness)
            answer = f"*(Groq API unavailable: {exc} — switched to deterministic fallback)*\n\n{fallback_text}"
            return AssistantResponse(
                answer=answer,
                intent=intent,
                mode="deterministic_fallback",
                evidence_df=evidence_df,
                freshness_timestamp=freshness,
            )

    answer = _deterministic_summary(intent, evidence_df, freshness)
    return AssistantResponse(
        answer=answer,
        intent=intent,
        mode="deterministic_fallback",
        evidence_df=evidence_df,
        freshness_timestamp=freshness,
    )