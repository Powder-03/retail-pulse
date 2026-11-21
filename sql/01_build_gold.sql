-- ============================================================================
-- 01_build_gold.sql
-- Builds the Gold Star Schema in DuckDB from Silver Parquet files
-- ============================================================================

-- 1. Dimension: Suppliers
CREATE OR REPLACE TABLE dim_supplier AS
SELECT DISTINCT
    TRIM(supplier_id) AS supplier_id,
    COALESCE(NULLIF(TRIM(supplier_name), ''), 'Unknown Supplier') AS supplier_name,
    COALESCE(CAST(lead_time_days AS INTEGER), 7) AS lead_time_days,
    COALESCE(NULLIF(TRIM(region), ''), 'National') AS supplier_region
FROM read_parquet('data/silver/supplier_deliveries.parquet')
WHERE supplier_id IS NOT NULL;

-- 2. Dimension: Products
CREATE OR REPLACE TABLE dim_product AS
SELECT DISTINCT
    TRIM(product_id) AS product_id,
    COALESCE(NULLIF(TRIM(product_name), ''), 'Unknown Product') AS product_name,
    COALESCE(NULLIF(TRIM(category), ''), 'Uncategorized') AS category,
    ROUND(COALESCE(CAST(unit_price AS DOUBLE), 0.0), 2) AS catalog_unit_price,
    TRIM(supplier_id) AS supplier_id
FROM read_parquet('data/silver/products.parquet')
WHERE product_id IS NOT NULL;

-- 3. Dimension: Stores
CREATE OR REPLACE TABLE dim_store AS
SELECT DISTINCT
    TRIM(store_id) AS store_id,
    COALESCE(NULLIF(TRIM(store_name), ''), 'Unknown Store') AS store_name,
    COALESCE(NULLIF(TRIM(region), ''), 'General') AS region
FROM read_parquet('data/silver/stores.parquet')
WHERE store_id IS NOT NULL;

-- 4. Dimension: Date (derived across all fact activity dates)
CREATE OR REPLACE TABLE dim_date AS
WITH all_dates AS (
    SELECT CAST(invoice_date AS DATE) AS full_date FROM read_parquet('data/silver/sales.parquet')
    UNION
    SELECT CAST(snapshot_date AS DATE) AS full_date FROM read_parquet('data/silver/inventory_snapshots.parquet')
    UNION
    SELECT CAST(order_date AS DATE) AS full_date FROM read_parquet('data/silver/supplier_deliveries.parquet')
)
SELECT DISTINCT
    CAST(STRFTIME(full_date, '%Y%m%d') AS INTEGER) AS date_key,
    full_date,
    EXTRACT(DAY FROM full_date) AS day,
    EXTRACT(MONTH FROM full_date) AS month,
    STRFTIME(full_date, '%B') AS month_name,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    EXTRACT(YEAR FROM full_date) AS year,
    DATE_TRUNC('week', full_date)::DATE AS week_start_date
FROM all_dates
WHERE full_date IS NOT NULL
ORDER BY full_date;

-- 5. Fact: Sales
CREATE OR REPLACE TABLE fact_sales AS
SELECT
    ROW_NUMBER() OVER (ORDER BY invoice_date, invoice_id, product_id) AS sales_line_id,
    TRIM(invoice_id) AS invoice_id,
    CAST(STRFTIME(CAST(invoice_date AS DATE), '%Y%m%d') AS INTEGER) AS date_key,
    CAST(invoice_date AS DATE) AS sale_date,
    TRIM(product_id) AS product_id,
    TRIM(store_id) AS store_id,
    CAST(quantity AS INTEGER) AS quantity,
    ROUND(CAST(unit_price AS DOUBLE), 2) AS unit_price,
    ROUND(CAST(quantity AS DOUBLE) * CAST(unit_price AS DOUBLE), 2) AS net_sales,
    COALESCE(CAST(is_return AS BOOLEAN), quantity < 0) AS is_return
FROM read_parquet('data/silver/sales.parquet')
WHERE product_id IS NOT NULL
  AND store_id IS NOT NULL
  AND invoice_date IS NOT NULL;

-- 6. Fact: Inventory Snapshots
CREATE OR REPLACE TABLE fact_inventory_snapshot AS
SELECT
    ROW_NUMBER() OVER (ORDER BY snapshot_date, store_id, product_id) AS snapshot_id,
    CAST(snapshot_date AS DATE) AS snapshot_date,
    CAST(STRFTIME(CAST(snapshot_date AS DATE), '%Y%m%d') AS INTEGER) AS date_key,
    TRIM(product_id) AS product_id,
    TRIM(store_id) AS store_id,
    CAST(on_hand_qty AS INTEGER) AS on_hand_qty,
    COALESCE(CAST(reorder_level AS INTEGER), 20) AS reorder_level
FROM read_parquet('data/silver/inventory_snapshots.parquet')
WHERE product_id IS NOT NULL
  AND store_id IS NOT NULL
  AND snapshot_date IS NOT NULL
  AND on_hand_qty >= 0;

-- 7. Fact: Supplier Deliveries
CREATE OR REPLACE TABLE fact_supplier_delivery AS
SELECT
    TRIM(delivery_id) AS delivery_id,
    TRIM(supplier_id) AS supplier_id,
    TRIM(product_id) AS product_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(expected_date AS DATE) AS expected_date,
    CAST(actual_date AS DATE) AS actual_date,
    CAST(qty_ordered AS INTEGER) AS qty_ordered,
    CAST(qty_received AS INTEGER) AS qty_received,
    CASE
        WHEN actual_date IS NULL THEN NULL
        WHEN CAST(actual_date AS DATE) <= CAST(expected_date AS DATE) THEN 1
        ELSE 0
    END AS is_on_time,
    CASE
        WHEN actual_date IS NULL THEN NULL
        ELSE GREATEST(0, DATE_DIFF('day', CAST(expected_date AS DATE), CAST(actual_date AS DATE)))
    END AS delay_days
FROM read_parquet('data/silver/supplier_deliveries.parquet')
WHERE supplier_id IS NOT NULL
  AND product_id IS NOT NULL;