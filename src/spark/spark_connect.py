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
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"
SPARK_PACKAGES = f"{KAFKA_CONNECTOR_PACKAGE},{HADOOP_AWS_PACKAGE}"


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
            .config("spark.jars.packages", SPARK_PACKAGES)
            .config("spark.pyspark.python", executor_python)
            .config("spark.pyspark.driver.python", driver_python)
            .config("spark.driver.memory", settings.SPARK_DRIVER_MEMORY)
            .config("spark.executor.memory", settings.SPARK_EXECUTOR_MEMORY)
            .config(
                "spark.sql.shuffle.partitions",
                str(settings.SPARK_SQL_SHUFFLE_PARTITIONS),
            )
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.caseSensitive", "true")
            .config("spark.sql.ansi.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                (
                    f"{'https' if settings.MINIO_SECURE else 'http'}://"
                    f"{settings.MINIO_ENDPOINT}"
                ),
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint.region",
                settings.MINIO_REGION,
            )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(settings.MINIO_SECURE).lower(),
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config(
                "spark.hadoop.fs.s3a.access.key",
                settings.MINIO_ACCESS_KEY,
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                settings.MINIO_SECRET_KEY,
            )
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
