# Power BI Desktop Setup & Dashboard Guide

This guide walks through connecting **Power BI Desktop** (Windows) to the containerized **PostgreSQL Gold Serving Layer (`retail_db`)** and building a three-page executive retail intelligence report.

---

## 1. Prerequisites

1. **Running Docker Stack:** Confirm `retail_postgres` is healthy and exposing port `5432` to `localhost`:
   ```powershell
   docker compose ps
   ```
   Verify `retail_postgres` shows `Up ... (healthy)` with `0.0.0.0:5432->5432/tcp`.
2. **Populated Gold Layer:** Ensure the Medallion ETL pipeline has completed at least one run (either via Airflow DAG `smart_retail_medallion_etl` or the Streamlit **Run Full ETL** button).
3. **Power BI Desktop & Npgsql Driver:** Install **Power BI Desktop** on Windows. If prompted for a PostgreSQL connector, download and install the official **Npgsql** MSI driver (enable **Npgsql GAC Installation** during setup) and restart Power BI Desktop.

---

## 2. Connect Power BI Desktop to PostgreSQL

1. Open **Power BI Desktop**.
2. Go to **Home** > **Get Data** > **More...**
3. Select **Database** > **PostgreSQL database** and click **Connect**.
4. Enter the connection parameters:
   * **Server:** `localhost:5432`
   * **Database:** `retail_db`
   * **Data Connectivity mode:** `Import`
5. Click **OK**.
6. In the authentication dialog, select **Database** from the left menu and enter:
   * **User name:** `retail_admin`
   * **Password:** `retail_secret_pass`
7. Click **Connect** (if prompted about unencrypted local connections, click **OK**).

---

## 3. Select Tables & Analytical Views

In the **Navigator** window, select the following star-schema tables and pre-built analytical views, then click **Load**:

### Dimension Tables
* `gold_dim_date`
* `gold_dim_product`
* `gold_dim_store`
* `gold_dim_supplier`

### Fact Tables
* `gold_fact_sales`
* `gold_fact_inventory_snapshot`
* `gold_fact_supplier_delivery`

### Pre-Built Analytical Views
* `vw_pbi_executive_summary`
* `vw_pbi_inventory_action_list`
* `vw_pbi_supplier_scorecard`

---

## 4. Configure Star-Schema Relationships

Open **Model View** (third icon on the left navigation rail) and verify that Power BI configured these **Many-to-One (`*:1`, Single cross-filter direction)** relationships. Drag and drop matching keys to create any missing links:

| Fact Table | Fact Column | Dimension Table | Dimension Column | Cardinality |
| :--- | :--- | :--- | :--- | :--- |
| `gold_fact_sales` | `date_key` | `gold_dim_date` | `date_key` | Many-to-One (`*:1`) |
| `gold_fact_sales` | `product_id` | `gold_dim_product` | `product_id` | Many-to-One (`*:1`) |
| `gold_fact_sales` | `store_id` | `gold_dim_store` | `store_id` | Many-to-One (`*:1`) |
| `gold_fact_inventory_snapshot` | `date_key` | `gold_dim_date` | `date_key` | Many-to-One (`*:1`) |
| `gold_fact_inventory_snapshot` | `product_id` | `gold_dim_product` | `product_id` | Many-to-One (`*:1`) |
| `gold_fact_inventory_snapshot` | `store_id` | `gold_dim_store` | `store_id` | Many-to-One (`*:1`) |
| `gold_fact_supplier_delivery` | `product_id` | `gold_dim_product` | `product_id` | Many-to-One (`*:1`) |
| `gold_fact_supplier_delivery` | `supplier_id` | `gold_dim_supplier` | `supplier_id` | Many-to-One (`*:1`) |

> **Tip:** Mark `gold_dim_date` as a Date Table (**Table tools** > **Mark as date table** > select `full_date`) to enable native time-intelligence calculations.

---

## 5. Create Core DAX Measures

1. Switch to **Report View** and click **Home** > **Enter Data**.
2. Name the table `_Measures` and click **Load**.
3. Right-click `_Measures` in the **Data** pane > **New measure** and add each measure below:

```dax
Total Net Sales = SUM(gold_fact_sales[net_sales])
```

```dax
Gross Sales = 
CALCULATE(
    SUM(gold_fact_sales[net_sales]),
    gold_fact_sales[is_return] = FALSE()
)
```

```dax
Return Value = 
CALCULATE(
    ABS(SUM(gold_fact_sales[net_sales])),
    gold_fact_sales[is_return] = TRUE()
)
```

```dax
Return Rate % = DIVIDE([Return Value], [Gross Sales], 0)
```

```dax
Total Units Sold = 
CALCULATE(
    SUM(gold_fact_sales[quantity]),
    gold_fact_sales[is_return] = FALSE()
)
```

```dax
Est. Revenue at Risk = SUM(vw_pbi_inventory_action_list[estimated_revenue_at_risk])
```

```dax
High Risk SKU-Stores = 
CALCULATE(
    COUNTROWS(vw_pbi_inventory_action_list),
    vw_pbi_inventory_action_list[stockout_risk] = "High"
)
```

```dax
Supplier On-Time Delivery % = AVERAGE(vw_pbi_supplier_scorecard[on_time_rate_pct])
```

4. Right-click `Column1` inside `_Measures` and select **Hide** so `_Measures` displays cleanly as a dedicated measure group at the top of the **Data** pane.

---

## 6. Build the 3-Page Report Layout

### Page 1: Executive Sales & Risk Overview
* **Top Slicers:** Add **Slicer** visuals for `gold_dim_store[region]`, `gold_dim_store[store_name]`, and `gold_dim_product[category]`.
* **KPI Cards (Top Row):** Add four **Card** visuals mapped to `Total Net Sales`, `Est. Revenue at Risk`, `High Risk SKU-Stores`, and `Supplier On-Time Delivery %`.
* **Daily Revenue Trend (Bottom Left):** Add a **Line chart** with `gold_dim_date[full_date]` on the **X-axis** and `Total Net Sales` on the **Y-axis**.
* **Revenue by Category (Bottom Right):** Add a **Clustered bar chart** with `gold_dim_product[category]` on the **Y-axis** and `Total Net Sales` on the **X-axis**.

### Page 2: Inventory Coverage & Stockout Action
* **Stockout Risk Matrix (Top):** Add a **Scatter chart** from `vw_pbi_inventory_action_list` with:
  * **X-axis:** `lead_time_days`
  * **Y-axis:** `days_of_cover`
  * **Legend:** `stockout_risk` (format `High` as red, `Medium` as amber, `Low` as green)
  * **Size:** `estimated_revenue_at_risk`
* **Replenishment Action Table (Bottom):** Add a **Table** visual displaying `stockout_risk`, `store_name`, `product_name`, `supplier_name`, `on_hand_qty`, `days_of_cover`, `lead_time_days`, and `estimated_revenue_at_risk`, sorted descending by `estimated_revenue_at_risk`.

### Page 3: Supplier Reliability Scorecard
* **On-Time Rate by Supplier (Left):** Add a **Clustered bar chart** from `vw_pbi_supplier_scorecard` with `supplier_name` on the **Y-axis** and `on_time_rate_pct` on the **X-axis**.
* **Vendor Performance Table (Right):** Add a **Table** visual displaying `supplier_name`, `supplier_tier`, `completed_deliveries`, `on_time_deliveries`, `late_deliveries`, `avg_delay_days`, and `fill_rate_pct`.

---

## 7. Refreshing & Saving

* Whenever your Airflow DAG or Streamlit pipeline ingests new data, click **Home** > **Refresh** in Power BI Desktop to reload the updated Gold tables and views from PostgreSQL.
* Save your completed report file as `powerbi/Smart_Retail_Intelligence.pbix`.