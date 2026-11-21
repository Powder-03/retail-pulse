from pathlib import Path
import duckdb
import pandas as pd
import pytest

from src.analytics.gold_build import build_duckdb_gold
from src.config import SQL_DIR
from src.genai.assistant import ask_retail_assistant
from src.ingestion.ingest import get_engine, run_ingestion
from src.transform.silver_etl import run_silver_etl


@pytest.fixture
def temp_lakehouse(tmp_path: Path) -> dict[str, Path]:
    source = tmp_path / "source"
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    for sub in ("sales", "products", "stores", "inventory", "suppliers"):
        (source / sub).mkdir(parents=True, exist_ok=True)
    bronze.mkdir(parents=True, exist_ok=True)
    silver.mkdir(parents=True, exist_ok=True)
    gold.mkdir(parents=True, exist_ok=True)

    # Seed controlled, hand-verifiable fixtures
    pd.DataFrame(
        [
            {"supplier_id": "SUP-1", "supplier_name": "FastSupply", "lead_time_days": 5, "region": "North"},
            {"supplier_id": "SUP-2", "supplier_name": "SlowSupply", "lead_time_days": 10, "region": "South"},
        ]
    ).to_csv(source / "suppliers" / "sup_dim_seed.csv", index=False)

    pd.DataFrame(
        [
            {"product_id": "SKU-A", "product_name": "Alpha Widget", "category": "Electronics", "unit_price": 50.0, "supplier_id": "SUP-1"},
            {"product_id": "SKU-B", "product_name": "Beta Gadget", "category": "Home", "unit_price": 20.0, "supplier_id": "SUP-2"},
        ]
    ).to_csv(source / "products" / "products.csv", index=False)

    pd.DataFrame(
        [
            {"store_id": "STR-1", "store_name": "Downtown Store", "region": "North"},
        ]
    ).to_csv(source / "stores" / "stores.csv", index=False)

    # Sales over 14-day trailing window ending 2026-10-14:
    # SKU-A: 28 units over 14 days => avg_daily_units = 2.0, avg_daily_sales_value = $100.00
    # SKU-B: 14 units over 14 days => avg_daily_units = 1.0, avg_daily_sales_value = $20.00
    # Plus 1 return line (-2 units) and 1 exact duplicate row to test Silver DQ
    sales_rows = [
        {"invoice_id": "INV-1", "invoice_date": "2026-10-01 10:00:00", "product_id": "SKU-A", "store_id": "STR-1", "quantity": 14, "unit_price": 50.0},
        {"invoice_id": "INV-2", "invoice_date": "2026-10-14 10:00:00", "product_id": "SKU-A", "store_id": "STR-1", "quantity": 14, "unit_price": 50.0},
        {"invoice_id": "INV-2", "invoice_date": "2026-10-14 10:00:00", "product_id": "SKU-A", "store_id": "STR-1", "quantity": 14, "unit_price": 50.0},  # duplicate
        {"invoice_id": "INV-3", "invoice_date": "2026-10-14 11:00:00", "product_id": "SKU-B", "store_id": "STR-1", "quantity": 14, "unit_price": 20.0},
        {"invoice_id": "C-INV-4", "invoice_date": "2026-10-14 12:00:00", "product_id": "SKU-B", "store_id": "STR-1", "quantity": -2, "unit_price": 20.0},  # return
    ]
    pd.DataFrame(sales_rows).to_csv(source / "sales" / "sales.csv", index=False)

    # Inventory snapshots on 2026-10-14:
    # SKU-A: on_hand = 4 => days_of_cover = 4 / 2.0 = 2.0 days (< lead_time 5d => High risk; exposure = 3.0d; rev_at_risk = $300.00)
    # SKU-B: on_hand = 20 => days_of_cover = 20 / 1.0 = 20.0 days (>= 10+3 => Low risk; rev_at_risk = $0.00)
    # Plus 1 invalid negative stock row to test Silver quarantine
    inv_rows = [
        {"snapshot_date": "2026-10-14", "store_id": "STR-1", "product_id": "SKU-A", "on_hand_qty": 4, "reorder_level": 10},
        {"snapshot_date": "2026-10-14", "store_id": "STR-1", "product_id": "SKU-B", "on_hand_qty": 20, "reorder_level": 10},
        {"snapshot_date": "2026-10-14", "store_id": "STR-1", "product_id": "SKU-BAD", "on_hand_qty": -9, "reorder_level": 10},
    ]
    pd.DataFrame(inv_rows).to_csv(source / "inventory" / "inventory.csv", index=False)

    # Replace sup_dim_seed with full supplier_deliveries schema
    (source / "suppliers" / "sup_dim_seed.csv").unlink()
    deliv_rows = [
        {
            "delivery_id": "DEL-1",
            "supplier_id": "SUP-1",
            "supplier_name": "FastSupply",
            "lead_time_days": 5,
            "region": "North",
            "product_id": "SKU-A",
            "order_date": "2026-10-01",
            "expected_date": "2026-10-06",
            "actual_date": "2026-10-05",  # on time
            "qty_ordered": 100,
            "qty_received": 100,
        },
        {
            "delivery_id": "DEL-2",
            "supplier_id": "SUP-2",
            "supplier_name": "SlowSupply",
            "lead_time_days": 10,
            "region": "South",
            "product_id": "SKU-B",
            "order_date": "2026-10-01",
            "expected_date": "2026-10-11",
            "actual_date": "2026-10-14",  # 3 days late
            "qty_ordered": 50,
            "qty_received": 45,
        },
    ]
    pd.DataFrame(deliv_rows).to_csv(source / "suppliers" / "deliveries.csv", index=False)

    return {
        "tmp_path": tmp_path,
        "source": source,
        "bronze": bronze,
        "silver": silver,
        "gold": gold,
        "duckdb_path": gold / "test_retail.duckdb",
        "dq_report": silver / "dq_report.json",
        "meta_db_uri": f"sqlite:///{tmp_path / 'meta.db'}",
    }


def test_ingestion_idempotency_and_schema_validation(temp_lakehouse: dict[str, Path]) -> None:
    db_uri = temp_lakehouse["meta_db_uri"]
    source = temp_lakehouse["source"]
    bronze = temp_lakehouse["bronze"]

    # First run ingests all 5 valid files
    first_run = run_ingestion(source_root=source, bronze_root=bronze, db_uri=db_uri)
    assert first_run == {"SUCCESS": 5, "SKIPPED": 0, "FAILED": 0}

    # Second run on unchanged files must skip all 5 (idempotency)
    second_run = run_ingestion(source_root=source, bronze_root=bronze, db_uri=db_uri)
    assert second_run == {"SUCCESS": 0, "SKIPPED": 5, "FAILED": 0}

    # Drop a malformed CSV missing required columns
    bad_file = source / "products" / "malformed.csv"
    bad_file.write_text("wrong_col_1,wrong_col_2\n1,2\n", encoding="utf-8")

    third_run = run_ingestion(source_root=source, bronze_root=bronze, db_uri=db_uri)
    assert third_run["FAILED"] == 1
    assert third_run["SKIPPED"] == 5


def test_end_to_end_silver_gold_and_genai_grounding(temp_lakehouse: dict[str, Path]) -> None:
    # 1. Ingest
    run_ingestion(
        source_root=temp_lakehouse["source"],
        bronze_root=temp_lakehouse["bronze"],
        db_uri=temp_lakehouse["meta_db_uri"],
    )

    # 2. Silver PySpark + Pandas ETL
    dq_report = run_silver_etl(
        bronze_root=temp_lakehouse["bronze"],
        silver_root=temp_lakehouse["silver"],
        report_path=temp_lakehouse["dq_report"],
    )
    # 5 raw sales rows -> 1 exact duplicate removed = 4 clean rows
    assert dq_report["datasets"]["sales"]["clean_rows"] == 4
    # 3 raw inventory rows -> 1 negative stock quarantined = 2 clean rows
    assert dq_report["datasets"]["inventory"]["clean_rows"] == 2

    # 3. Gold DuckDB Star Schema & Business KPIs
    counts = build_duckdb_gold(
        duckdb_path=temp_lakehouse["duckdb_path"],
        silver_dir=temp_lakehouse["silver"],
        sql_dir=SQL_DIR,
    )
    assert counts["fact_sales"] == 4
    assert counts["gold_kpi_inventory_risk"] == 2

    # Verify hand-calculated stock cover & revenue at risk for SKU-A and SKU-B
    con = duckdb.connect(str(temp_lakehouse["duckdb_path"]), read_only=True)
    try:
        risk_rows = (
            con.execute(
                "SELECT product_id, days_of_cover, stockout_risk, estimated_revenue_at_risk "
                "FROM gold_kpi_inventory_risk ORDER BY product_id"
            )
            .fetchdf()
            .set_index("product_id")
        )
        assert risk_rows.loc["SKU-A", "days_of_cover"] == pytest.approx(2.0)
        assert risk_rows.loc["SKU-A", "stockout_risk"] == "High"
        assert risk_rows.loc["SKU-A", "estimated_revenue_at_risk"] == pytest.approx(300.0)

        assert risk_rows.loc["SKU-B", "days_of_cover"] == pytest.approx(20.0)
        assert risk_rows.loc["SKU-B", "stockout_risk"] == "Low"
        assert risk_rows.loc["SKU-B", "estimated_revenue_at_risk"] == pytest.approx(0.0)
    finally:
        con.close()

    # 4. Grounded GenAI Assistant (Deterministic offline fallback check)
    ai_resp = ask_retail_assistant(
        question="Which products need urgent replenishment attention?",
        duckdb_path=temp_lakehouse["duckdb_path"],
        api_key="",  # force deterministic fallback
    )
    assert ai_resp.mode == "deterministic_fallback"
    assert ai_resp.intent == "inventory_risk"
    assert "Alpha Widget" in ai_resp.answer
    assert "300.00" in ai_resp.answer