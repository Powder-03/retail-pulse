# Stage 1: Application & ETL Image (Streamlit + PySpark + DuckDB + uv)
FROM python:3.11-slim AS app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1 \
    PYTHONPATH=/app \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    curl \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml README.md* ./
RUN uv sync --no-dev

COPY src/ ./src/
COPY sql/ ./sql/

EXPOSE 8501

CMD ["uv", "run", "streamlit", "run", "src/dashboard/app.py", "--server.address", "0.0.0.0", "--server.port", "8501"]

# Stage 2: Apache Airflow Image (with JRE + Project Dependencies)
FROM apache/airflow:2.10.2-python3.11 AS airflow

USER root

ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    openpyxl>=3.1.2 \
    faker>=24.0.0 \
    "pyspark>=3.5.1,<4.0" \
    pyarrow>=15.0.0 \
    duckdb>=1.1.0 \
    plotly>=5.22.0 \
    groq>=0.9.0 \
    python-dotenv>=1.0.1