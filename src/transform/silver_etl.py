import json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.config import BRONZE_DIR, DQ_REPORT_PATH, MIN_DQ_PASS_RATE_PCT, SILVER_DIR


def get_spark_session(app_name: str = "SmartRetailSilverETL") -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _load_bronze_dataset(dataset: str, bronze_root: Path = BRONZE_DIR) -> pd.DataFrame:
    dataset_dir = bronze_root / dataset
    if not dataset_dir.exists():
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for fpath in sorted(dataset_dir.rglob("*")):
        if not fpath.is_file() or fpath.name.startswith("."):
            continue
        ext = fpath.suffix.lower()
        if ext == ".csv":
            frames.append(pd.read_csv(fpath))
        elif ext == ".xlsx":
            frames.append(pd.read_excel(fpath))
        elif ext == ".json":
            frames.append(pd.read_json(fpath))

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _write_clean_parquet(spark_df: DataFrame, output_path: Path) -> int:
    """Converts validated Spark DataFrame to a single clean Parquet file."""
    pdf = spark_df.toPandas()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pdf.to_parquet(output_path, index=False, engine="pyarrow")
    return len(pdf)


def _transform_products(spark: SparkSession, raw_pdf: pd.DataFrame, silver_root: Path) -> dict:
    raw_count = len(raw_pdf)
    sdf = spark.createDataFrame(raw_pdf.astype(str))

    cleaned = (
        sdf.select(
            F.trim(F.col("product_id")).alias("product_id"),
            F.coalesce(F.nullif(F.trim(F.col("product_name")), F.lit("")), F.lit("Unknown")).alias("product_name"),
            F.coalesce(F.nullif(F.trim(F.col("category")), F.lit("")), F.lit("Unknown")).alias("category"),
            F.col("unit_price").cast(T.DoubleType()).alias("unit_price"),
            F.trim(F.col("supplier_id")).alias("supplier_id"),
        )
        .filter(
            F.col("product_id").isNotNull()
            & (F.col("product_id") != "")
            & (F.col("product_id") != "None")
            & (F.col("product_id") != "nan")
            & F.col("unit_price").isNotNull()
            & (F.col("unit_price") >= 0)
        )
        .dropDuplicates(["product_id"])
    )

    clean_count = _write_clean_parquet(cleaned, silver_root / "products.parquet")
    rejected = raw_count - clean_count
    pass_rate = round((clean_count / raw_count) * 100, 2) if raw_count else 100.0
    return {
        "raw_rows": raw_count,
        "clean_rows": clean_count,
        "rejected_or_deduped": rejected,
        "pass_rate_pct": pass_rate,
        "meets_threshold": pass_rate >= MIN_DQ_PASS_RATE_PCT,
    }


def _transform_stores(spark: SparkSession, raw_pdf: pd.DataFrame, silver_root: Path) -> dict:
    raw_count = len(raw_pdf)
    sdf = spark.createDataFrame(raw_pdf.astype(str))

    cleaned = (
        sdf.select(
            F.trim(F.col("store_id")).alias("store_id"),
            F.coalesce(F.nullif(F.trim(F.col("store_name")), F.lit("")), F.lit("Unknown")).alias("store_name"),
            F.coalesce(F.nullif(F.trim(F.col("region")), F.lit("")), F.lit("Unknown")).alias("region"),
        )
        .filter(
            F.col("store_id").isNotNull()
            & (F.col("store_id") != "")
            & (F.col("store_id") != "None")
            & (F.col("store_id") != "nan")
        )
        .dropDuplicates(["store_id"])
    )

    clean_count = _write_clean_parquet(cleaned, silver_root / "stores.parquet")
    rejected = raw_count - clean_count
    pass_rate = round((clean_count / raw_count) * 100, 2) if raw_count else 100.0
    return {
        "raw_rows": raw_count,
        "clean_rows": clean_count,
        "rejected_or_deduped": rejected,
        "pass_rate_pct": pass_rate,
        "meets_threshold": pass_rate >= MIN_DQ_PASS_RATE_PCT,
    }


def _transform_sales(spark: SparkSession, raw_pdf: pd.DataFrame, silver_root: Path) -> dict:
    raw_count = len(raw_pdf)
    sdf = spark.createDataFrame(raw_pdf.astype(str))

    cleaned = (
        sdf.select(
            F.trim(F.col("invoice_id")).alias("invoice_id"),
            F.to_timestamp(F.trim(F.col("invoice_date"))).alias("invoice_date"),
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("store_id")).alias("store_id"),
            F.col("quantity").cast(T.DoubleType()).cast(T.IntegerType()).alias("quantity"),
            F.col("unit_price").cast(T.DoubleType()).alias("unit_price"),
        )
        .filter(
            F.col("invoice_id").isNotNull()
            & (F.col("invoice_id") != "")
            & F.col("invoice_date").isNotNull()
            & F.col("product_id").isNotNull()
            & (~F.col("product_id").isin("", "None", "nan", "NaN"))
            & F.col("store_id").isNotNull()
            & (~F.col("store_id").isin("", "None", "nan", "NaN"))
            & F.col("quantity").isNotNull()
            & (F.col("quantity") != 0)
            & F.col("unit_price").isNotNull()
            & (F.col("unit_price") >= 0)
        )
        .dropDuplicates(["invoice_id", "invoice_date", "product_id", "store_id", "quantity"])
        .withColumn("is_return", (F.col("quantity") < 0) | F.col("invoice_id").startswith("C"))
        .withColumn("net_sales", F.round(F.col("quantity") * F.col("unit_price"), 2))
    )

    clean_count = _write_clean_parquet(cleaned, silver_root / "sales.parquet")
    rejected = raw_count - clean_count
    pass_rate = round((clean_count / raw_count) * 100, 2) if raw_count else 100.0
    return {
        "raw_rows": raw_count,
        "clean_rows": clean_count,
        "rejected_or_deduped": rejected,
        "pass_rate_pct": pass_rate,
        "meets_threshold": pass_rate >= MIN_DQ_PASS_RATE_PCT,
    }


def _transform_inventory(spark: SparkSession, raw_pdf: pd.DataFrame, silver_root: Path) -> dict:
    raw_count = len(raw_pdf)
    sdf = spark.createDataFrame(raw_pdf.astype(str))

    cleaned = (
        sdf.select(
            F.to_date(F.trim(F.col("snapshot_date"))).alias("snapshot_date"),
            F.trim(F.col("store_id")).alias("store_id"),
            F.trim(F.col("product_id")).alias("product_id"),
            F.col("on_hand_qty").cast(T.DoubleType()).cast(T.IntegerType()).alias("on_hand_qty"),
            F.coalesce(F.col("reorder_level").cast(T.DoubleType()).cast(T.IntegerType()), F.lit(20)).alias("reorder_level"),
        )
        .filter(
            F.col("snapshot_date").isNotNull()
            & F.col("store_id").isNotNull()
            & (~F.col("store_id").isin("", "None", "nan"))
            & F.col("product_id").isNotNull()
            & (~F.col("product_id").isin("", "None", "nan"))
            & F.col("on_hand_qty").isNotNull()
            & (F.col("on_hand_qty") >= 0)  # Reject negative stock snapshots per DQ rule 6.1
        )
        .dropDuplicates(["snapshot_date", "store_id", "product_id"])
    )

    clean_count = _write_clean_parquet(cleaned, silver_root / "inventory_snapshots.parquet")
    rejected = raw_count - clean_count
    pass_rate = round((clean_count / raw_count) * 100, 2) if raw_count else 100.0
    return {
        "raw_rows": raw_count,
        "clean_rows": clean_count,
        "rejected_or_deduped": rejected,
        "pass_rate_pct": pass_rate,
        "meets_threshold": pass_rate >= MIN_DQ_PASS_RATE_PCT,
    }


def _transform_suppliers(spark: SparkSession, raw_pdf: pd.DataFrame, silver_root: Path) -> dict:
    raw_count = len(raw_pdf)
    if "supplier_name" not in raw_pdf.columns:
        raw_pdf["supplier_name"] = "Unknown Supplier"
    if "lead_time_days" not in raw_pdf.columns:
        raw_pdf["lead_time_days"] = 7
    if "region" not in raw_pdf.columns:
        raw_pdf["region"] = "National"

    sdf = spark.createDataFrame(raw_pdf.astype(str))

    cleaned = (
        sdf.select(
            F.trim(F.col("delivery_id")).alias("delivery_id"),
            F.trim(F.col("supplier_id")).alias("supplier_id"),
            F.coalesce(F.nullif(F.trim(F.col("supplier_name")), F.lit("")), F.lit("Unknown")).alias("supplier_name"),
            F.coalesce(F.col("lead_time_days").cast(T.DoubleType()).cast(T.IntegerType()), F.lit(7)).alias("lead_time_days"),
            F.coalesce(F.nullif(F.trim(F.col("region")), F.lit("")), F.lit("National")).alias("region"),
            F.trim(F.col("product_id")).alias("product_id"),
            F.to_date(F.trim(F.col("order_date"))).alias("order_date"),
            F.to_date(F.trim(F.col("expected_date"))).alias("expected_date"),
            F.to_date(
                F.when(F.col("actual_date").isin("", "None", "nan", "NaT"), F.lit(None)).otherwise(F.trim(F.col("actual_date")))
            ).alias("actual_date"),
            F.col("qty_ordered").cast(T.DoubleType()).cast(T.IntegerType()).alias("qty_ordered"),
            F.coalesce(F.col("qty_received").cast(T.DoubleType()).cast(T.IntegerType()), F.lit(0)).alias("qty_received"),
        )
        .filter(
            F.col("delivery_id").isNotNull()
            & (F.col("delivery_id") != "")
            & F.col("supplier_id").isNotNull()
            & (~F.col("supplier_id").isin("", "None", "nan"))
            & F.col("product_id").isNotNull()
            & (~F.col("product_id").isin("", "None", "nan"))
            & F.col("order_date").isNotNull()
            & F.col("expected_date").isNotNull()
            & (F.col("qty_ordered") > 0)
        )
        .dropDuplicates(["delivery_id"])
    )

    clean_count = _write_clean_parquet(cleaned, silver_root / "supplier_deliveries.parquet")
    rejected = raw_count - clean_count
    pass_rate = round((clean_count / raw_count) * 100, 2) if raw_count else 100.0
    return {
        "raw_rows": raw_count,
        "clean_rows": clean_count,
        "rejected_or_deduped": rejected,
        "pass_rate_pct": pass_rate,
        "meets_threshold": pass_rate >= MIN_DQ_PASS_RATE_PCT,
    }


def run_silver_etl(
    bronze_root: Path = BRONZE_DIR,
    silver_root: Path = SILVER_DIR,
    report_path: Path = DQ_REPORT_PATH,
) -> dict:
    """Runs PySpark + Pandas cleaning across all Bronze datasets and writes Silver Parquet + DQ report."""
    spark = get_spark_session()
    transformers = {
        "products": _transform_products,
        "stores": _transform_stores,
        "sales": _transform_sales,
        "inventory": _transform_inventory,
        "suppliers": _transform_suppliers,
    }

    report: dict = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "min_pass_rate_threshold_pct": MIN_DQ_PASS_RATE_PCT,
        "datasets": {},
    }

    try:
        for dataset, transform_fn in transformers.items():
            raw_pdf = _load_bronze_dataset(dataset, bronze_root=bronze_root)
            if raw_pdf.empty:
                report["datasets"][dataset] = {
                    "raw_rows": 0,
                    "clean_rows": 0,
                    "rejected_or_deduped": 0,
                    "pass_rate_pct": 0.0,
                    "meets_threshold": False,
                }
                continue
            report["datasets"][dataset] = transform_fn(spark, raw_pdf, silver_root)
    finally:
        spark.stop()

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


if __name__ == "__main__":
    summary = run_silver_etl()
    print(json.dumps(summary, indent=2))