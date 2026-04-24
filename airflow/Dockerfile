FROM apache/airflow:2.8.0-python3.11

USER airflow

RUN pip install --no-cache-dir \
    "numpy<2" \
    "pandas==2.2.3" \
    "pyarrow>=16" \
    langgraph \
    litellm \
    faiss-cpu \
    rapidfuzz \
    structlog \
    prometheus_client \
    mlxtend \
    rank-bm25 \
    scikit-learn \
    chromadb \
    sentence-transformers \
    networkx \
    redis \
    psycopg2-binary \
    kafka-python \
    google-cloud-storage \
    google-cloud-bigquery \
    sqlalchemy
