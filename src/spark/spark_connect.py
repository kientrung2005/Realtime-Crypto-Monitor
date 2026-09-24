from __future__ import annotations

import logging
import os
import sys

import pyspark
from pyspark.sql import SparkSession

from configs.settings import settings


logger = logging.getLogger(__name__)

KAFKA_CONNECTOR_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:"
    f"{pyspark.__version__}"
)


class SparkConnect:
    @staticmethod
    def create_spark_session(
        app_name: str = "RealtimeCryptoAggTrades",
    ) -> SparkSession:
        driver_python = sys.executable
        executor_python = (
            settings.SPARK_EXECUTOR_PYTHON
            if settings.IN_DOCKER
            else driver_python
        )
        os.environ["PYSPARK_PYTHON"] = executor_python
        os.environ["PYSPARK_DRIVER_PYTHON"] = driver_python

        builder = (
            SparkSession.builder
            .appName(app_name)
            .master(settings.SPARK_MASTER_URL)
            .config(
                "spark.jars.ivy",
                settings.SPARK_IVY_CACHE_LOCATION,
            )
            .config("spark.jars.packages", KAFKA_CONNECTOR_PACKAGE)
            .config("spark.pyspark.python", executor_python)
            .config("spark.pyspark.driver.python", driver_python)
            .config("spark.driver.memory", settings.SPARK_DRIVER_MEMORY)
            .config("spark.executor.memory", settings.SPARK_EXECUTOR_MEMORY)
            .config(
                "spark.sql.shuffle.partitions",
                str(settings.KAFKA_NUM_PARTITIONS),
            )
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.caseSensitive", "true")
            .config("spark.sql.ansi.enabled", "false")
        )

        if settings.IN_DOCKER:
            builder = (
                builder
                .config("spark.driver.host", settings.SPARK_DRIVER_HOST)
                .config("spark.driver.bindAddress", "0.0.0.0")
            )

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        logger.info(
            "Spark session started with master %s",
            settings.SPARK_MASTER_URL,
        )
        return spark
