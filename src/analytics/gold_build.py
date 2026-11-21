from pathlib import Path
import duckdb
from sqlalchemy import create_engine, text

from src.config import DUCKDB_PATH, POSTGRES_URI, SILVER_DIR, SQL_DIR

GOLD_OBJECTS_TO_SYNC = [
    "dim_product",
    "dim_store",
    "dim_supplier",
    "dim_date",
    "fact_sales",
    "fact_inventory_snapshot",
    "fact_supplier_delivery",
    "gold_kpi_product_sales",
    "gold_kpi_inventory_risk",
    "gold_kpi_supplier_performance",
    "gold_kpi_store_wow_sales",
    "gold_kpi_sales_anomalies",
]


def _load_sql_with_silver_path(sql_file: Path, silver_dir: Path) -> str:
    raw_sql = sql_file.read_text(encoding="utf-8")
    normalized_silver = silver_dir.resolve().as_posix()
    return raw_sql.replace("data/silver/", f"{normalized_silver}/")


def build_duckdb_gold(
    duckdb_path: Path = DUCKDB_PATH,
    silver_dir: Path = SILVER_DIR,
    sql_dir: Path = SQL_DIR,
) -> dict[str, int]:
    """Builds Gold star schema tables and KPI views in DuckDB from Silver Parquet files."""
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(duckdb_path))

    try:
        build_sql = _load_sql_with_silver_path(sql_dir / "01_build_gold.sql", silver_dir)
        kpi_sql = _load_sql_with_silver_path(sql_dir / "02_business_kpis.sql", silver_dir)

        con.execute(build_sql)
        con.execute(kpi_sql)

        row_counts: dict[str, int] = {}
        for obj_name in GOLD_OBJECTS_TO_SYNC:
            count = con.execute(f"SELECT COUNT(*) FROM {obj_name}").fetchone()[0]
            row_counts[obj_name] = int(count)
        return row_counts
    finally:
        con.close()


def sync_gold_to_postgres(
    duckdb_path: Path = DUCKDB_PATH,
    sql_dir: Path = SQL_DIR,
    db_uri: str | None = None,
) -> None:
    """Exports DuckDB Gold tables/views into PostgreSQL and creates Power BI views."""
    engine = create_engine(db_uri or POSTGRES_URI, future=True)
    con = duckdb.connect(str(duckdb_path), read_only=True)

    try:
        with engine.begin() as pg_conn:
            for obj_name in GOLD_OBJECTS_TO_SYNC:
                pg_table = obj_name if obj_name.startswith("gold_") else f"gold_{obj_name}"
                df = con.execute(f"SELECT * FROM {obj_name}").fetchdf()
                # Drop dependent Power BI views temporarily if replacing base tables
                pg_conn.execute(text(f"DROP TABLE IF EXISTS {pg_table} CASCADE"))
                df.to_sql(pg_table, pg_conn, if_exists="replace", index=False, chunksize=5000)

            pbi_sql_file = sql_dir / "03_powerbi_views.sql"
            if pbi_sql_file.exists():
                pbi_sql = pbi_sql_file.read_text(encoding="utf-8")
                pg_conn.execute(text(pbi_sql))
    finally:
        con.close()


def run_gold_pipeline(
    duckdb_path: Path = DUCKDB_PATH,
    silver_dir: Path = SILVER_DIR,
    sql_dir: Path = SQL_DIR,
    db_uri: str | None = None,
    sync_postgres: bool = True,
) -> dict[str, int]:
    """Executes full Gold build in DuckDB and optionally syncs serving layer to PostgreSQL."""
    counts = build_duckdb_gold(duckdb_path=duckdb_path, silver_dir=silver_dir, sql_dir=sql_dir)
    if sync_postgres:
        sync_gold_to_postgres(duckdb_path=duckdb_path, sql_dir=sql_dir, db_uri=db_uri)
    return counts


if __name__ == "__main__":
    summary = run_gold_pipeline()
    print("Gold build & PostgreSQL sync complete:")
    for table_name, cnt in summary.items():
        print(f"  - {table_name}: {cnt:,} rows")