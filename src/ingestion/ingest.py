import hashlib
import shutil
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    select,
)
from sqlalchemy.engine import Engine

from src.config import AWS_S3_BUCKET, BRONZE_DIR, POSTGRES_URI, SOURCE_DIR, sync_to_s3

EXPECTED_SCHEMAS: dict[str, set[str]] = {
    "sales": {"invoice_id", "invoice_date", "product_id", "store_id", "quantity", "unit_price"},
    "products": {"product_id", "product_name", "category", "unit_price", "supplier_id"},
    "stores": {"store_id", "store_name", "region"},
    "inventory": {"snapshot_date", "store_id", "product_id", "on_hand_qty", "reorder_level"},
    "suppliers": {
        "delivery_id",
        "supplier_id",
        "product_id",
        "order_date",
        "expected_date",
        "actual_date",
        "qty_ordered",
        "qty_received",
    },
}

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".json"}

metadata = MetaData()

ingestion_runs = Table(
    "ingestion_runs",
    metadata,
    Column("run_id", Integer, primary_key=True, autoincrement=True),
    Column("dataset", String(50), nullable=False),
    Column("source_path", Text, nullable=False),
    Column("bronze_path", Text, nullable=True),
    Column("checksum", String(64), nullable=False, index=True),
    Column("file_size_bytes", Integer, nullable=False),
    Column("row_count", Integer, nullable=True),
    Column("status", String(20), nullable=False),  # SUCCESS | SKIPPED | FAILED
    Column("error_message", Text, nullable=True),
    Column("ingested_at", DateTime(timezone=True), nullable=False),
)


def get_engine(db_uri: str | None = None) -> Engine:
    engine = create_engine(db_uri or POSTGRES_URI, future=True)
    metadata.create_all(engine)
    return engine


def compute_sha256(file_path: Path) -> str:
    hasher = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _read_tabular_file(file_path: Path) -> pd.DataFrame:
    ext = file_path.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(file_path)
    if ext == ".xlsx":
        return pd.read_excel(file_path)
    if ext == ".json":
        return pd.read_json(file_path)
    raise ValueError(f"Unsupported file extension: {ext}")


def _is_already_ingested(engine: Engine, dataset: str, checksum: str) -> bool:
    stmt = select(ingestion_runs.c.run_id).where(
        ingestion_runs.c.dataset == dataset,
        ingestion_runs.c.checksum == checksum,
        ingestion_runs.c.status == "SUCCESS",
    )
    with engine.connect() as conn:
        return conn.execute(stmt).first() is not None


def _log_run(
    engine: Engine,
    *,
    dataset: str,
    source_path: Path,
    bronze_path: Path | None,
    checksum: str,
    file_size_bytes: int,
    row_count: int | None,
    status: str,
    error_message: str | None = None,
) -> None:
    stmt = ingestion_runs.insert().values(
        dataset=dataset,
        source_path=str(source_path),
        bronze_path=str(bronze_path) if bronze_path else None,
        checksum=checksum,
        file_size_bytes=file_size_bytes,
        row_count=row_count,
        status=status,
        error_message=error_message,
        ingested_at=datetime.now(timezone.utc),
    )
    with engine.begin() as conn:
        conn.execute(stmt)


def ingest_file(
    file_path: Path,
    dataset: str,
    engine: Engine,
    bronze_root: Path = BRONZE_DIR,
) -> str:
    """Validates and copies a single file into Bronze with checksum idempotency."""
    file_size = file_path.stat().st_size
    checksum = compute_sha256(file_path)

    if _is_already_ingested(engine, dataset, checksum):
        _log_run(
            engine,
            dataset=dataset,
            source_path=file_path,
            bronze_path=None,
            checksum=checksum,
            file_size_bytes=file_size,
            row_count=None,
            status="SKIPPED",
            error_message="Duplicate checksum already ingested",
        )
        return "SKIPPED"

    try:
        if file_path.suffix.lower() not in ALLOWED_EXTENSIONS:
            raise ValueError(f"Extension {file_path.suffix} not allowed")

        df = _read_tabular_file(file_path)
        if df.empty:
            raise ValueError("Source file contains zero data rows")

        actual_cols = {c.strip() for c in df.columns}
        required_cols = EXPECTED_SCHEMAS.get(dataset, set())
        missing_cols = required_cols - actual_cols
        if missing_cols:
            raise ValueError(f"Missing required columns for '{dataset}': {sorted(missing_cols)}")

        date_partition = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        dest_dir = bronze_root / dataset / f"ingestion_date={date_partition}"
        dest_dir.mkdir(parents=True, exist_ok=True)

        # Stage to temporary file first, then atomically rename into place
        final_dest = dest_dir / f"{checksum[:8]}_{file_path.name}"
        tmp_dest = dest_dir / f".tmp_{checksum[:8]}_{file_path.name}"
        shutil.copy2(file_path, tmp_dest)
        tmp_dest.replace(final_dest)

        _log_run(
            engine,
            dataset=dataset,
            source_path=file_path,
            bronze_path=final_dest,
            checksum=checksum,
            file_size_bytes=file_size,
            row_count=len(df),
            status="SUCCESS",
        )
        return "SUCCESS"

    except Exception as exc:
        _log_run(
            engine,
            dataset=dataset,
            source_path=file_path,
            bronze_path=None,
            checksum=checksum,
            file_size_bytes=file_size,
            row_count=None,
            status="FAILED",
            error_message=str(exc),
        )
        return "FAILED"


def run_ingestion(
    source_root: Path = SOURCE_DIR,
    bronze_root: Path = BRONZE_DIR,
    db_uri: str | None = None,
) -> dict[str, int]:
    """Discovers files across all dataset drop folders and ingests them into Bronze."""
    engine = get_engine(db_uri)
    counts = {"SUCCESS": 0, "SKIPPED": 0, "FAILED": 0}

    for dataset in EXPECTED_SCHEMAS:
        dataset_dir = source_root / dataset
        if not dataset_dir.exists():
            continue
        for file_path in sorted(dataset_dir.iterdir()):
            if file_path.is_file() and not file_path.name.startswith("."):
                status = ingest_file(file_path, dataset, engine, bronze_root=bronze_root)
                counts[status] += 1

    if AWS_S3_BUCKET:
        sync_to_s3(bronze_root, "bronze")

    return counts


if __name__ == "__main__":
    result = run_ingestion()
    print(f"Bronze ingestion complete: {result}")