import os
from pathlib import Path
import boto3
from dotenv import load_dotenv

load_dotenv()

# Project Root & Directory Layout
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SOURCE_DIR = DATA_DIR / "source"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
SQL_DIR = BASE_DIR / "sql"

# Ensure lakehouse folders exist at runtime
for _dir in (
    SOURCE_DIR / "sales",
    SOURCE_DIR / "products",
    SOURCE_DIR / "stores",
    SOURCE_DIR / "inventory",
    SOURCE_DIR / "suppliers",
    BRONZE_DIR,
    SILVER_DIR,
    GOLD_DIR,
):
    _dir.mkdir(parents=True, exist_ok=True)

# AWS Environment & S3 Lakehouse Configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "").strip()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
AWS_S3_ENDPOINT_URL = os.getenv("AWS_S3_ENDPOINT_URL", "").strip() or None


def get_s3_client():
    """Returns a configured boto3 S3 client supporting AWS and LocalStack."""
    kwargs = {"region_name": AWS_REGION}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"] = AWS_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
    if AWS_S3_ENDPOINT_URL:
        kwargs["endpoint_url"] = AWS_S3_ENDPOINT_URL
    return boto3.client("s3", **kwargs)


def sync_to_s3(local_dir: Path, s3_prefix: str, bucket_name: str | None = None) -> int:
    """Uploads local lakehouse directory partitions to AWS S3 if a bucket is configured."""
    bucket = bucket_name or AWS_S3_BUCKET
    if not bucket or not local_dir.exists():
        return 0
    s3 = get_s3_client()
    uploaded = 0
    for root, _, files in os.walk(local_dir):
        for file in files:
            full_path = Path(root) / file
            rel_path = full_path.relative_to(local_dir)
            s3_key = f"{s3_prefix.rstrip('/')}/{rel_path}".lstrip("/")
            s3.upload_file(str(full_path), bucket, s3_key)
            uploaded += 1
    return uploaded


# Analytical & Metadata Databases (DuckDB + AWS RDS / PostgreSQL)
DUCKDB_PATH = GOLD_DIR / "retail_analytics.duckdb"
DQ_REPORT_PATH = SILVER_DIR / "dq_report.json"

POSTGRES_USER = os.getenv("POSTGRES_USER", "retail_admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "retail_secret_pass")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "retail_db")
POSTGRES_SSLMODE = os.getenv("POSTGRES_SSLMODE", "").strip()

ssl_param = f"?sslmode={POSTGRES_SSLMODE}" if POSTGRES_SSLMODE else ""
POSTGRES_URI = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}{ssl_param}"
)

# Generative AI (Google Cloud Vertex AI / Gemini) Settings
VERTEX_PROJECT_ID = os.getenv(
    "VERTEX_PROJECT_ID",
    os.getenv("GCP_PROJECT_ID", os.getenv("GOOGLE_CLOUD_PROJECT", "")),
).strip()
VERTEX_LOCATION = os.getenv("VERTEX_LOCATION", "us-central1").strip()
VERTEX_API_KEY = os.getenv("VERTEX_API_KEY", os.getenv("GEMINI_API_KEY", "")).strip()
VERTEX_MODEL = os.getenv("VERTEX_MODEL", "gemini-1.5-flash").strip()

# Retail Risk & Quality Thresholds
SAFETY_BUFFER_DAYS = 3
MIN_DQ_PASS_RATE_PCT = 95.0
