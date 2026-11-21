import os
from pathlib import Path
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

# Analytical & Metadata Databases
DUCKDB_PATH = GOLD_DIR / "retail_analytics.duckdb"
DQ_REPORT_PATH = SILVER_DIR / "dq_report.json"

POSTGRES_USER = os.getenv("POSTGRES_USER", "retail_admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "retail_secret_pass")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "retail_db")

POSTGRES_URI = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Generative AI (Groq) Settings
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

# Retail Risk & Quality Thresholds
SAFETY_BUFFER_DAYS = 3
MIN_DQ_PASS_RATE_PCT = 95.0