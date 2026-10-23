import logging
import re
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event, Thread

import duckdb
from minio.deleteobjects import DeleteObject
from minio.error import S3Error
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format

from configs.settings import settings
from src.storage.minio_client import MinIOStorageClient


logger = logging.getLogger(__name__)

ARCHIVE_DATASETS = {"trades", "quotes"}


@dataclass(frozen=True, order=True)
class ArchivePartition:
    dataset: str
    exchange: str
    date: str
    hour: str

    @property
    def end_time(self) -> datetime:
        start_time = datetime.strptime(
            f"{self.date} {self.hour}",
            "%Y-%m-%d %H",
        ).replace(tzinfo=timezone.utc)
        return start_time + timedelta(hours=1)


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
        archive_df.coalesce(settings.MINIO_ARCHIVE_OUTPUT_PARTITIONS)
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


def _archive_pattern() -> re.Pattern[str]:
    archive_root = settings.MINIO_ARCHIVE_PREFIX.strip("/")
    return re.compile(
        rf"^{re.escape(archive_root)}/"
        r"(?P<dataset>[^/]+)/batch_id=\d+/"
        r"exchange=(?P<exchange>[^/]+)/"
        r"date=(?P<date>\d{4}-\d{2}-\d{2})/"
        r"hour=(?P<hour>\d{2})/[^/]+\.parquet$"
    )


def _compacted_object_name(partition: ArchivePartition) -> str:
    archive_root = settings.MINIO_ARCHIVE_PREFIX.strip("/")
    return (
        f"{archive_root}/{partition.dataset}/compacted/"
        f"exchange={partition.exchange}/date={partition.date}/"
        f"hour={partition.hour}/data.parquet"
    )


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _deduplicated_query(dataset: str, source_sql: str) -> str:
    if dataset == "trades":
        key_columns = "market_type, symbol, agg_trade_id, trade_time"
    else:
        key_columns = "market_type, symbol, update_id, snapshot_time"
    return (
        "SELECT * EXCLUDE (_row_number) FROM ("
        "SELECT *, row_number() OVER (PARTITION BY "
        f"{key_columns} ORDER BY processed_at DESC) AS _row_number "
        f"FROM read_parquet([{source_sql}], union_by_name=true)"
        ") WHERE _row_number = 1"
    )


class MinioArchiveCompactor:
    def __init__(self) -> None:
        self.storage = MinIOStorageClient()
        self._stop_event = Event()
        self._thread: Thread | None = None

    def _source_partitions(self) -> dict[ArchivePartition, list[str]]:
        archive_root = settings.MINIO_ARCHIVE_PREFIX.strip("/")
        pattern = _archive_pattern()
        partitions: dict[ArchivePartition, list[str]] = defaultdict(list)
        objects = self.storage.client.list_objects(
            settings.MINIO_BUCKET,
            prefix=f"{archive_root}/",
            recursive=True,
        )
        for item in objects:
            match = pattern.match(item.object_name)
            if match is None:
                continue
            values = match.groupdict()
            if values["dataset"] not in ARCHIVE_DATASETS:
                continue
            partition = ArchivePartition(
                dataset=values["dataset"],
                exchange=values["exchange"],
                date=values["date"],
                hour=values["hour"],
            )
            partitions[partition].append(item.object_name)
        return partitions

    def _object_exists(self, object_name: str) -> bool:
        try:
            self.storage.client.stat_object(
                settings.MINIO_BUCKET,
                object_name,
            )
            return True
        except S3Error as error:
            if error.code in {"NoSuchKey", "NoSuchObject"}:
                return False
            raise

    def _remove_objects(self, object_names: list[str]) -> None:
        if not object_names:
            return
        errors = list(
            self.storage.client.remove_objects(
                settings.MINIO_BUCKET,
                (DeleteObject(name) for name in object_names),
            )
        )
        if errors:
            details = ", ".join(
                f"{error.object_name}: {error.message}"
                for error in errors
            )
            raise RuntimeError(f"MinIO object deletion failed: {details}")

    def _cleanup_batch_markers(self, source_objects: list[str]) -> None:
        batch_prefixes = {
            object_name.split("/exchange=", 1)[0] + "/"
            for object_name in source_objects
        }
        for batch_prefix in batch_prefixes:
            remaining = list(
                self.storage.client.list_objects(
                    settings.MINIO_BUCKET,
                    prefix=batch_prefix,
                    recursive=True,
                )
            )
            if any(item.object_name.endswith(".parquet") for item in remaining):
                continue
            self._remove_objects([item.object_name for item in remaining])

    def _compact_partition(
        self,
        partition: ArchivePartition,
        source_objects: list[str],
    ) -> bool:
        final_object = _compacted_object_name(partition)
        final_exists = self._object_exists(final_object)
        if (
            len(source_objects) < settings.MINIO_COMPACTION_MIN_FILES
            and not final_exists
        ):
            return False

        with tempfile.TemporaryDirectory(prefix="minio-compact-") as temp_dir:
            local_files: list[str] = []
            for index, object_name in enumerate(source_objects):
                local_path = str(Path(temp_dir) / f"source-{index:06d}.parquet")
                self.storage.client.fget_object(
                    settings.MINIO_BUCKET,
                    object_name,
                    local_path,
                )
                local_files.append(local_path)

            if final_exists:
                current_path = str(Path(temp_dir) / "current.parquet")
                self.storage.client.fget_object(
                    settings.MINIO_BUCKET,
                    final_object,
                    current_path,
                )
                local_files.append(current_path)

            source_sql = ", ".join(_sql_string(path) for path in local_files)
            compacted_query = _deduplicated_query(
                partition.dataset,
                source_sql,
            )
            output_path = str(Path(temp_dir) / "compacted.parquet")
            connection = duckdb.connect()
            try:
                row_count = connection.execute(
                    f"SELECT count(*) FROM ({compacted_query})"
                ).fetchone()[0]
                connection.execute(
                    f"COPY ({compacted_query}) "
                    f"TO {_sql_string(output_path)} "
                    "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
                )
            finally:
                connection.close()

            self.storage.client.fput_object(
                settings.MINIO_BUCKET,
                final_object,
                output_path,
                content_type="application/octet-stream",
            )
            self._remove_objects(source_objects)
            self._cleanup_batch_markers(source_objects)
            logger.info(
                "Compacted %d MinIO file(s) into %s with %d row(s)",
                len(source_objects),
                final_object,
                row_count,
            )
        return True

    def compact_once(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(
            minutes=settings.MINIO_COMPACTION_MIN_AGE_MINUTES
        )
        compacted_partitions = 0
        for partition, source_objects in sorted(
            self._source_partitions().items()
        ):
            if partition.end_time > cutoff:
                continue
            if self._compact_partition(partition, source_objects):
                compacted_partitions += 1
        logger.info(
            "MinIO compaction scan completed: %d partition(s) compacted",
            compacted_partitions,
        )

    def _run(self) -> None:
        if self._stop_event.wait(
            settings.MINIO_COMPACTION_INITIAL_DELAY_SECONDS
        ):
            return
        while not self._stop_event.is_set():
            try:
                self.compact_once()
            except Exception:
                logger.exception("MinIO archive compaction failed")
            if self._stop_event.wait(
                settings.MINIO_COMPACTION_INTERVAL_SECONDS
            ):
                return

    def start(self) -> None:
        if not settings.MINIO_COMPACTION_ENABLED:
            logger.info("MinIO archive compaction is disabled")
            return
        self._thread = Thread(
            target=self._run,
            name="minio-archive-compactor",
            daemon=True,
        )
        self._thread.start()
        logger.info("MinIO archive compactor started")

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
