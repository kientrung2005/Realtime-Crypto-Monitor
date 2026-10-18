import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format

from configs.settings import settings


logger = logging.getLogger(__name__)


def write_parquet_batch(
    batch_df: DataFrame,
    batch_id: int,
    dataset: str,
    columns: tuple[str, ...],
    event_time_column: str,
) -> None:
    batch_path = (
        f"s3a://{settings.MINIO_BUCKET}/"
        f"{settings.MINIO_ARCHIVE_PREFIX.strip('/')}/{dataset}/"
        f"batch_id={batch_id:020d}"
    )
    archive_df = (
        batch_df.select(*columns)
        .withColumn("date", date_format(col(event_time_column), "yyyy-MM-dd"))
        .withColumn("hour", date_format(col(event_time_column), "HH"))
    )
    (
        archive_df.coalesce(settings.SPARK_SQL_SHUFFLE_PARTITIONS)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("exchange", "date", "hour")
        .parquet(batch_path)
    )
    logger.info(
        "Spark batch %d archived %s to MinIO path %s",
        batch_id,
        dataset,
        batch_path,
    )
