-- ============================================================================
-- 02_business_kpis.sql
-- Analytical Gold Views answering the 7 core business questions in DuckDB
-- ============================================================================

-- KPI 1: Product & Category Net Sales Ranking
CREATE OR REPLACE VIEW gold_kpi_product_sales AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    s.store_id,
    s.store_name,
    s.region,
    d.full_date AS sale_date,
    d.month_name,
    d.year,
    SUM(CASE WHEN NOT f.is_return THEN f.quantity ELSE 0 END) AS gross_units_sold,
    SUM(CASE WHEN f.is_return THEN ABS(f.quantity) ELSE 0 END) AS returned_units,
    SUM(f.quantity) AS net_units_sold,
    ROUND(SUM(CASE WHEN NOT f.is_return THEN f.net_sales ELSE 0 END), 2) AS gross_sales,
    ROUND(SUM(f.net_sales), 2) AS net_sales
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;

-- KPI 2, 3 & 4: Inventory Coverage, Rule-Based Stockout Risk & Estimated Revenue at Risk
CREATE OR REPLACE VIEW gold_kpi_inventory_risk AS
WITH max_dates AS (
    SELECT
        (SELECT MAX(snapshot_date) FROM fact_inventory_snapshot) AS latest_snapshot_date,
        (SELECT MAX(sale_date) FROM fact_sales) AS latest_sale_date
),
latest_stock AS (
    SELECT
        i.store_id,
        i.product_id,
        i.snapshot_date,
        i.on_hand_qty,
        i.reorder_level
    FROM fact_inventory_snapshot i
    CROSS JOIN max_dates m
    WHERE i.snapshot_date = m.latest_snapshot_date
),
recent_demand AS (
    SELECT
        f.store_id,
        f.product_id,
        -- 14-day trailing window average daily demand (positive sales only)
        ROUND(SUM(CASE WHEN NOT f.is_return THEN f.quantity ELSE 0 END) / 14.0, 2) AS avg_daily_units,
        ROUND(SUM(CASE WHEN NOT f.is_return THEN f.net_sales ELSE 0 END) / 14.0, 2) AS avg_daily_sales_value
    FROM fact_sales f
    CROSS JOIN max_dates m
    WHERE f.sale_date > m.latest_sale_date - INTERVAL 14 DAY
    GROUP BY f.store_id, f.product_id
)
SELECT
    ls.snapshot_date,
    st.store_id,
    st.store_name,
    st.region,
    p.product_id,
    p.product_name,
    p.category,
    sup.supplier_id,
    sup.supplier_name,
    sup.lead_time_days,
    ls.on_hand_qty,
    ls.reorder_level,
    COALESCE(rd.avg_daily_units, 0.0) AS avg_daily_units,
    COALESCE(rd.avg_daily_sales_value, 0.0) AS avg_daily_sales_value,
    CASE
        WHEN COALESCE(rd.avg_daily_units, 0) <= 0 THEN 999.0
        ELSE ROUND(ls.on_hand_qty / rd.avg_daily_units, 1)
    END AS days_of_cover,
    -- Heuristic Risk Classification: Lead time + 3-day safety buffer
    CASE
        WHEN COALESCE(rd.avg_daily_units, 0) <= 0 THEN 'Low'
        WHEN (ls.on_hand_qty / rd.avg_daily_units) < sup.lead_time_days THEN 'High'
        WHEN (ls.on_hand_qty / rd.avg_daily_units) < (sup.lead_time_days + 3) THEN 'Medium'
        ELSE 'Low'
    END AS stockout_risk,
    -- Exposure days = shortfall between lead time and available days of cover
    CASE
        WHEN COALESCE(rd.avg_daily_units, 0) <= 0 THEN 0.0
        ELSE ROUND(GREATEST(0.0, sup.lead_time_days - (ls.on_hand_qty / rd.avg_daily_units)), 1)
    END AS risk_exposure_days,
    -- Estimated Revenue at Risk = avg_daily_sales_value * risk_exposure_days
    CASE
        WHEN COALESCE(rd.avg_daily_units, 0) <= 0 THEN 0.0
        ELSE ROUND(
            COALESCE(rd.avg_daily_sales_value, 0.0) *
            GREATEST(0.0, sup.lead_time_days - (ls.on_hand_qty / rd.avg_daily_units)),
            2
        )
    END AS estimated_revenue_at_risk
FROM latest_stock ls
JOIN dim_store st ON ls.store_id = st.store_id
JOIN dim_product p ON ls.product_id = p.product_id
LEFT JOIN dim_supplier sup ON p.supplier_id = sup.supplier_id
LEFT JOIN recent_demand rd ON ls.store_id = rd.store_id AND ls.product_id = rd.product_id;

-- KPI 5: Supplier Delivery Reliability
CREATE OR REPLACE VIEW gold_kpi_supplier_performance AS
SELECT
    sup.supplier_id,
    sup.supplier_name,
    sup.lead_time_days AS contracted_lead_time_days,
    COUNT(d.delivery_id) AS total_orders,
    COUNT(d.actual_date) AS completed_deliveries,
    SUM(COALESCE(d.is_on_time, 0)) AS on_time_deliveries,
    SUM(CASE WHEN d.is_on_time = 0 THEN 1 ELSE 0 END) AS late_deliveries,
    ROUND(
        100.0 * SUM(COALESCE(d.is_on_time, 0)) / NULLIF(COUNT(d.actual_date), 0),
        1
    ) AS on_time_rate_pct,
    ROUND(COALESCE(AVG(d.delay_days), 0.0), 1) AS avg_delay_days,
    ROUND(
        100.0 * SUM(d.qty_received) / NULLIF(SUM(d.qty_ordered), 0),
        1
    ) AS fill_rate_pct
FROM dim_supplier sup
LEFT JOIN fact_supplier_delivery d ON sup.supplier_id = d.supplier_id
GROUP BY 1, 2, 3;

-- KPI 6: Store Week-over-Week (WoW) Sales Change
CREATE OR REPLACE VIEW gold_kpi_store_wow_sales AS
WITH weekly_store AS (
    SELECT
        s.store_id,
        s.store_name,
        s.region,
        d.week_start_date,
        ROUND(SUM(f.net_sales), 2) AS weekly_net_sales,
        SUM(f.quantity) AS weekly_net_units
    FROM fact_sales f
    JOIN dim_store s ON f.store_id = s.store_id
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY 1, 2, 3, 4
)
SELECT
    store_id,
    store_name,
    region,
    week_start_date,
    weekly_net_sales,
    LAG(weekly_net_sales) OVER (PARTITION BY store_id ORDER BY week_start_date) AS prev_week_sales,
    ROUND(
        weekly_net_sales - LAG(weekly_net_sales) OVER (PARTITION BY store_id ORDER BY week_start_date),
        2
    ) AS wow_abs_change,
    ROUND(
        100.0 * (weekly_net_sales - LAG(weekly_net_sales) OVER (PARTITION BY store_id ORDER BY week_start_date))
        / NULLIF(ABS(LAG(weekly_net_sales) OVER (PARTITION BY store_id ORDER BY week_start_date)), 0),
        1
    ) AS wow_pct_change
FROM weekly_store;

-- KPI 7: Rolling Daily Sales & Return Anomaly Detection
CREATE OR REPLACE VIEW gold_kpi_sales_anomalies AS
WITH daily_cat AS (
    SELECT
        f.sale_date,
        p.category,
        ROUND(SUM(CASE WHEN NOT f.is_return THEN f.net_sales ELSE 0 END), 2) AS daily_gross_sales,
        ROUND(SUM(CASE WHEN f.is_return THEN ABS(f.net_sales) ELSE 0 END), 2) AS daily_return_value,
        ROUND(
            100.0 * SUM(CASE WHEN f.is_return THEN ABS(f.net_sales) ELSE 0 END)
            / NULLIF(SUM(CASE WHEN NOT f.is_return THEN f.net_sales ELSE 0 END), 0),
            1
        ) AS return_rate_pct
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY 1, 2
),
stats AS (
    SELECT
        *,
        AVG(daily_gross_sales) OVER (
            PARTITION BY category ORDER BY sale_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS rolling_7d_avg_sales,
        STDDEV_POP(daily_gross_sales) OVER (
            PARTITION BY category ORDER BY sale_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS rolling_7d_std_sales
    FROM daily_cat
)
SELECT
    sale_date,
    category,
    daily_gross_sales,
    daily_return_value,
    COALESCE(return_rate_pct, 0.0) AS return_rate_pct,
    ROUND(COALESCE(rolling_7d_avg_sales, daily_gross_sales), 2) AS baseline_7d_sales,
    CASE
        WHEN rolling_7d_std_sales > 0
             AND ABS(daily_gross_sales - rolling_7d_avg_sales) > (2.0 * rolling_7d_std_sales)
            THEN 'Sales Spike/Drop'
        WHEN COALESCE(return_rate_pct, 0.0) > 25.0
            THEN 'High Return Rate'
        ELSE 'Normal'
    END AS anomaly_flag
FROM stats;