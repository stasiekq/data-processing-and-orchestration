# Reproducible runtime: Spark 3.5 + Java 17 (Spark + Hadoop UGI are not compatible with JDK 24+ on many setups).
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java-home

ENV JAVA_HOME=/opt/java-home
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt pyproject.toml ./
COPY ecommerce_pipeline ./ecommerce_pipeline
COPY dbt ./dbt
COPY run_pipeline.py ./

RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -e .

# Processed Parquet + DuckDB land in /app/data and /app/dbt/target inside the container
CMD ["python", "run_pipeline.py"]
