import random
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import requests
from faker import Faker

from src.config import SOURCE_DIR

fake = Faker()
Faker.seed(42)
random.seed(42)

UCI_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"

CATEGORIES = [
    "Home & Kitchen",
    "Electronics & Gadgets",
    "Apparel & Accessories",
    "Health & Personal Care",
    "Office & Stationery",
    "Seasonal & Gifts",
]

REGIONS = ["North", "South", "East", "West", "Central"]


def generate_suppliers(num_suppliers: int = 20) -> pd.DataFrame:
    rows = []
    for i in range(1, num_suppliers + 1):
        sup_id = f"SUP-{i:03d}"
        # Make a few suppliers intentionally slow for realistic risk analytics
        lead_time = random.choice([3, 4, 5, 7, 10, 14])
        rows.append(
            {
                "supplier_id": sup_id,
                "supplier_name": f"{fake.company()} Supply Co.",
                "lead_time_days": lead_time,
                "region": random.choice(REGIONS),
            }
        )
    return pd.DataFrame(rows)


def generate_stores(num_stores: int = 10) -> pd.DataFrame:
    rows = []
    for i in range(1, num_stores + 1):
        rows.append(
            {
                "store_id": f"STR-{i:02d}",
                "store_name": f"SmartRetail {fake.city()} #{i}",
                "region": REGIONS[(i - 1) % len(REGIONS)],
            }
        )
    return pd.DataFrame(rows)


def generate_products(suppliers_df: pd.DataFrame, num_products: int = 200) -> pd.DataFrame:
    sup_ids = suppliers_df["supplier_id"].tolist()
    adjectives = ["Pro", "Eco", "Ultra", "Compact", "Classic", "Smart", "Deluxe", "Essential"]
    nouns = [
        "Organizer", "Lamp", "Charger", "Bottle", "Backpack", "Notebook",
        "Kettle", "Speaker", "Watch", "Mug", "Chair", "Stand", "Kit", "Sensor",
    ]
    rows = []
    for i in range(1, num_products + 1):
        prod_id = f"SKU-{1000 + i}"
        cat = random.choice(CATEGORIES)
        name = f"{random.choice(adjectives)} {random.choice(nouns)} {i}"
        price = round(random.uniform(4.99, 149.99), 2)
        rows.append(
            {
                "product_id": prod_id,
                "product_name": name,
                "category": cat,
                "unit_price": price,
                "supplier_id": random.choice(sup_ids),
            }
        )
    return pd.DataFrame(rows)


def _try_fetch_uci_transactions(
    products_df: pd.DataFrame, stores_df: pd.DataFrame, max_rows: int = 35000
) -> pd.DataFrame | None:
    """Attempts to download UCI Online Retail and align product/store keys."""
    try:
        resp = requests.get(UCI_URL, timeout=15)
        resp.raise_for_status()
        tmp_xlsx = SOURCE_DIR / "sales" / "_uci_raw_cache.xlsx"
        tmp_xlsx.write_bytes(resp.content)

        raw = pd.read_excel(tmp_xlsx, nrows=max_rows)
        tmp_xlsx.unlink(missing_ok=True)

        prod_ids = products_df["product_id"].tolist()
        price_map = dict(zip(products_df["product_id"], products_df["unit_price"]))
        store_ids = stores_df["store_id"].tolist()

        # Map UCI StockCode deterministically onto our product catalog
        mapped_products = [
            prod_ids[abs(hash(str(code))) % len(prod_ids)] for code in raw["StockCode"]
        ]
        # Shift historical 2010-2011 dates to recent 45-day window ending today
        max_dt = pd.to_datetime(raw["InvoiceDate"]).max()
        day_offset = (pd.Timestamp.now().normalize() - max_dt.normalize()).days
        shifted_dates = pd.to_datetime(raw["InvoiceDate"]) + pd.to_timedelta(day_offset, unit="D")

        df = pd.DataFrame(
            {
                "invoice_id": raw["InvoiceNo"].astype(str),
                "invoice_date": shifted_dates.dt.strftime("%Y-%m-%d %H:%M:%S"),
                "product_id": mapped_products,
                "store_id": [random.choice(store_ids) for _ in range(len(raw))],
                "quantity": raw["Quantity"],
                "unit_price": [price_map[p] for p in mapped_products],
            }
        )
        return df
    except Exception:
        return None


def generate_sales(
    products_df: pd.DataFrame,
    stores_df: pd.DataFrame,
    num_rows: int = 40000,
    days_back: int = 45,
) -> pd.DataFrame:
    uci_df = _try_fetch_uci_transactions(products_df, stores_df, max_rows=num_rows)
    if uci_df is not None and len(uci_df) > 1000:
        df = uci_df
    else:
        end_date = datetime.now().replace(hour=18, minute=0, second=0, microsecond=0)
        prod_records = products_df[["product_id", "unit_price"]].to_dict("records")
        store_ids = stores_df["store_id"].tolist()

        rows = []
        for idx in range(1, num_rows + 1):
            prod = random.choice(prod_records)
            inv_id = f"INV-{100000 + (idx // 3)}"
            dt = end_date - timedelta(
                days=random.randint(0, days_back),
                hours=random.randint(0, 10),
                minutes=random.randint(0, 59),
            )
            # ~5% customer returns (negative quantity)
            is_ret = random.random() < 0.05
            qty = -random.randint(1, 4) if is_ret else random.randint(1, 12)
            rows.append(
                {
                    "invoice_id": inv_id,
                    "invoice_date": dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "product_id": prod["product_id"],
                    "store_id": random.choice(store_ids),
                    "quantity": qty,
                    "unit_price": prod["unit_price"],
                }
            )
        df = pd.DataFrame(rows)

    # Inject ~1% dirty rows to exercise Silver DQ quarantine & deduplication
    dirty_rows = df.sample(n=min(150, len(df)), random_state=42).copy()  # exact duplicates
    bad_key_rows = df.sample(n=min(50, len(df)), random_state=7).copy()
    bad_key_rows["product_id"] = None
    return pd.concat([df, dirty_rows, bad_key_rows], ignore_index=True)


def generate_inventory_snapshots(
    products_df: pd.DataFrame, stores_df: pd.DataFrame, days: int = 30
) -> pd.DataFrame:
    end_date = datetime.now().date()
    prod_ids = products_df["product_id"].tolist()
    store_ids = stores_df["store_id"].tolist()

    # Designate 15% of SKUs as low-stock / stockout-risk items for rich dashboard insights
    high_risk_skus = set(random.sample(prod_ids, k=max(1, int(len(prod_ids) * 0.15))))

    rows = []
    for d_offset in range(days - 1, -1, -1):
        snap_date = (end_date - timedelta(days=d_offset)).strftime("%Y-%m-%d")
        for store_id in store_ids:
            for prod_id in prod_ids:
                if prod_id in high_risk_skus and d_offset == 0:
                    on_hand = random.randint(0, 8)
                else:
                    on_hand = random.randint(15, 160)
                rows.append(
                    {
                        "snapshot_date": snap_date,
                        "store_id": store_id,
                        "product_id": prod_id,
                        "on_hand_qty": on_hand,
                        "reorder_level": 25,
                    }
                )

    df = pd.DataFrame(rows)
    # Inject a few invalid negative stock rows for DQ rule verification
    if len(df) > 20:
        df.loc[df.sample(n=15, random_state=99).index, "on_hand_qty"] = -5
    return df


def generate_supplier_deliveries(
    products_df: pd.DataFrame, suppliers_df: pd.DataFrame, num_orders: int = 1200
) -> pd.DataFrame:
    end_date = datetime.now().date()
    lead_map = dict(zip(suppliers_df["supplier_id"], suppliers_df["lead_time_days"]))
    prod_sup = products_df[["product_id", "supplier_id"]].to_dict("records")

    # Pick 4 chronic late suppliers so supplier scorecard shows clear differentiation
    slow_suppliers = set(suppliers_df["supplier_id"].head(4).tolist())

    rows = []
    for i in range(1, num_orders + 1):
        item = random.choice(prod_sup)
        sup_id = item["supplier_id"]
        lead_days = int(lead_map.get(sup_id, 7))
        order_dt = end_date - timedelta(days=random.randint(5, 60))
        expected_dt = order_dt + timedelta(days=lead_days)

        if sup_id in slow_suppliers:
            delay = random.choice([-1, 0, 2, 4, 6, 8])
        else:
            delay = random.choice([-2, -1, 0, 0, 0, 1, 3])

        actual_dt = expected_dt + timedelta(days=delay)
        qty_ordered = random.randint(50, 300)
        qty_received = (
            qty_ordered if delay <= 1 else max(0, qty_ordered - random.randint(0, 15))
        )

        rows.append(
            {
                "delivery_id": f"DEL-{50000 + i}",
                "supplier_id": sup_id,
                "product_id": item["product_id"],
                "order_date": order_dt.strftime("%Y-%m-%d"),
                "expected_date": expected_dt.strftime("%Y-%m-%d"),
                "actual_date": actual_dt.strftime("%Y-%m-%d") if actual_dt <= end_date else None,
                "qty_ordered": qty_ordered,
                "qty_received": qty_received if actual_dt <= end_date else 0,
            }
        )
    return pd.DataFrame(rows)


def generate_all_datasets(include_incremental: bool = False) -> dict[str, Path]:
    """Generates all baseline datasets (and optional incremental batch) in data/source/."""
    suppliers_df = generate_suppliers(num_suppliers=20)
    stores_df = generate_stores(num_stores=10)
    products_df = generate_products(suppliers_df, num_products=200)
    sales_df = generate_sales(products_df, stores_df, num_rows=40000, days_back=45)
    inventory_df = generate_inventory_snapshots(products_df, stores_df, days=30)
    deliveries_df = generate_supplier_deliveries(products_df, suppliers_df, num_orders=1200)

    # Merge supplier metadata into deliveries so dim_supplier has full context
    deliveries_enriched = deliveries_df.merge(
        suppliers_df[["supplier_id", "supplier_name", "lead_time_days", "region"]],
        on="supplier_id",
        how="left",
    )

    outputs = {
        "products": SOURCE_DIR / "products" / "products.csv",
        "stores": SOURCE_DIR / "stores" / "stores.csv",
        "sales": SOURCE_DIR / "sales" / "transactions_base.csv",
        "inventory": SOURCE_DIR / "inventory" / "inventory_snapshots.csv",
        "suppliers": SOURCE_DIR / "suppliers" / "supplier_deliveries.csv",
    }

    products_df.to_csv(outputs["products"], index=False)
    stores_df.to_csv(outputs["stores"], index=False)
    sales_df.to_csv(outputs["sales"], index=False)
    inventory_df.to_csv(outputs["inventory"], index=False)
    deliveries_enriched.to_csv(outputs["suppliers"], index=False)

    if include_incremental:
        inc_df = generate_sales(products_df, stores_df, num_rows=2500, days_back=3)
        inc_path = SOURCE_DIR / "sales" / f"incremental_sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        inc_df.to_csv(inc_path, index=False)
        outputs["incremental_sales"] = inc_path

    return outputs


if __name__ == "__main__":
    paths = generate_all_datasets(include_incremental=False)
    for name, path in paths.items():
        print(f"Generated {name}: {path}")