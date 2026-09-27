FROM python:3.10-slim-bookworm AS pipeline

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 185 spark \
    && useradd --uid 185 --gid spark --create-home spark

COPY requirements.txt /tmp/requirements.txt

RUN --mount=type=cache,target=/root/.cache/pip \
    python3 -m pip install \
        -r /tmp/requirements.txt \
        pyspark==3.4.1 \
    && mkdir -p /opt/spark/work-dir /opt/spark/checkpoints /opt/spark/ivy \
    && chown -R spark:spark /opt/spark

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONPATH=/opt/spark/work-dir

USER spark

WORKDIR /opt/spark/work-dir


FROM apache/superset:5.0.0 AS superset

USER root

RUN . /app/.venv/bin/activate \
    && uv pip install \
        clickhouse-connect==1.8.0 \
        psycopg2-binary==2.9.10

COPY configs/superset_config.py /app/pythonpath/superset_config.py
COPY scripts/init_superset.py /app/docker/init_superset.py

USER superset
