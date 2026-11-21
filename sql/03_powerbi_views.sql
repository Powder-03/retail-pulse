-- ============================================================================
-- 03_powerbi_views.sql
-- PostgreSQL serving views optimized for Power BI Desktop Direct/Import mode
-- ============================================================================

CREATE OR REPLACE VIEW vw_pbi_executive_summary AS
SELECT
    (SELECT COALESCE(ROUND(SUM(net_sales)::numeric, 2), 0) FROM gold_fact_sales) AS total_net_sales,
    (SELECT COALESCE(ROUND(SUM(estimated_revenue_at_risk)::numeric, 2), 0) FROM gold_kpi_inventory_risk) AS total_revenue_at_risk,
    (SELECT COUNT(*) FROM gold_kpi_inventory_risk WHERE stockout_risk = 'High') AS high_risk_sku_store_count,
    (SELECT COALESCE(ROUND(AVG(on_time_rate_pct)::numeric, 1), 0) FROM gold_kpi_supplier_performance) AS avg_supplier_otd_pct;

CREATE OR REPLACE VIEW vw_pbi_inventory_action_list AS
SELECT
    snapshot_date,
    region,
    store_id,
    store_name,
    category,
    product_id,
    product_name,
    supplier_id,
    supplier_name,
    lead_time_days,
    on_hand_qty,
    reorder_level,
    avg_daily_units,
    days_of_cover,
    stockout_risk,
    risk_exposure_days,
    estimated_revenue_at_risk
FROM gold_kpi_inventory_risk
ORDER BY
    CASE stockout_risk WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END,
    estimated_revenue_at_risk DESC;

CREATE OR REPLACE VIEW vw_pbi_supplier_scorecard AS
SELECT
    supplier_id,
    supplier_name,
    contracted_lead_time_days,
    total_orders,
    completed_deliveries,
    on_time_deliveries,
    late_deliveries,
    on_time_rate_pct,
    avg_delay_days,
    fill_rate_pct,
    CASE
        WHEN on_time_rate_pct >= 90 THEN 'Tier 1 - Reliable'
        WHEN on_time_rate_pct >= 75 THEN 'Tier 2 - Watchlist'
        ELSE 'Tier 3 - Critical Delay Risk'
    END AS supplier_tier
FROM gold_kpi_supplier_performance;