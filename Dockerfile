FROM python:3.10-slim-bookworm

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 sparkapp \
    && useradd --uid 10001 --gid sparkapp --create-home sparkapp

COPY requirements.txt /tmp/requirements.txt

RUN --mount=type=cache,target=/root/.cache/pip \
    python3 -m pip install \
        -r /tmp/requirements.txt \
        pyspark==3.4.1 \
    && mkdir -p /opt/spark/work-dir /opt/spark/checkpoints /opt/spark/ivy \
    && chown -R sparkapp:sparkapp /opt/spark

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONPATH=/opt/spark/work-dir

USER sparkapp

WORKDIR /opt/spark/work-dir
