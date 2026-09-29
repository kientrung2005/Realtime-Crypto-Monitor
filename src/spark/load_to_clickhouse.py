from __future__ import annotations

import logging
import sys
from collections.abc import Iterator
from pathlib import Path

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.streaming import StreamingQuery


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.settings import settings
from src.database.clickhouse_connect import ClickHouseConnect
from src.spark.spark_connect import SparkConnect
from src.spark.transformation import (
    aggregate_market_candles,
    deduplicate_agg_trades,
    format_dead_letter_records,
    transform_agg_trades,
)


logger = logging.getLogger(__name__)

AGG_TRADES_TABLE = "fact_agg_trades"
AGG_TRADES_COLUMNS = (
    "schema_version",
    "exchange",
    "market_type",
    "symbol",
    "agg_trade_id",
    "event_time",
    "trade_time",
    "price",
    "quantity",
    "quote_quantity",
    "taker_side",
    "buyer_is_market_maker",
    "first_trade_id",
    "last_trade_id",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "processed_at",
)
CANDLES_TABLE = "agg_market_candles"
CANDLES_COLUMNS = (
    "symbol",
    "interval",
    "window_start",
    "window_end",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "base_volume",
    "quote_volume",
    "trade_count",
    "vwap",
    "buy_volume",
    "sell_volume",
    "buy_pressure",
    "sell_pressure",
    "volatility",
    "updated_at",
)
CLICKHOUSE_INSERT_BATCH_SIZE = 5_000


def _checkpoint_location(query_name: str) -> str:
    checkpoint_root = settings.SPARK_CHECKPOINT_LOCATION.rstrip("/")
    return f"{checkpoint_root}/{query_name}"


def _row_batches(
    rows: Iterator[Row],
    columns: tuple[str, ...],
    batch_size: int,
) -> Iterator[list[list[object]]]:
    batch: list[list[object]] = []

    for row in rows:
        batch.append([row[column_name] for column_name in columns])
        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch


def _write_table_batch(
    batch_df: DataFrame,
    batch_id: int,
    table: str,
    columns: tuple[str, ...],
) -> None:
    rows = batch_df.select(*columns).toLocalIterator()
    inserted_rows = 0

    with ClickHouseConnect(
        host=settings.CLICKHOUSE_HOST,
        port=settings.CLICKHOUSE_HTTP_PORT,
        username=settings.CLICKHOUSE_USER,
        password=settings.CLICKHOUSE_PASSWORD,
        database=settings.CLICKHOUSE_DATABASE,
    ) as client:
        for row_batch in _row_batches(
            rows,
            columns,
            CLICKHOUSE_INSERT_BATCH_SIZE,
        ):
            client.insert(
                table=table,
                data=row_batch,
                column_names=list(columns),
            )
            inserted_rows += len(row_batch)

    logger.info(
        "Spark batch %d inserted %d row(s) into ClickHouse table %s",
        batch_id,
        inserted_rows,
        table,
    )


def _write_agg_trades_batch(batch_df: DataFrame, batch_id: int) -> None:
    _write_table_batch(
        batch_df,
        batch_id,
        AGG_TRADES_TABLE,
        AGG_TRADES_COLUMNS,
    )


def _write_market_candles_batch(batch_df: DataFrame, batch_id: int) -> None:
    _write_table_batch(
        batch_df,
        batch_id,
        CANDLES_TABLE,
        CANDLES_COLUMNS,
    )


def _read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", settings.KAFKA_AGG_TRADES_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )


def _start_queries(spark: SparkSession) -> list[StreamingQuery]:
    kafka_df = _read_kafka_stream(spark)
    valid_df, invalid_df = transform_agg_trades(kafka_df)
    deduplicated_valid_df = deduplicate_agg_trades(valid_df)

    clickhouse_query = (
        deduplicated_valid_df.writeStream
        .queryName("agg_trades_to_clickhouse")
        .outputMode("append")
        .foreachBatch(_write_agg_trades_batch)
        .option(
            "checkpointLocation",
            _checkpoint_location("agg_trades_to_clickhouse"),
        )
        .trigger(processingTime="10 seconds")
        .start()
    )

    market_candles_df = aggregate_market_candles(valid_df)
    market_candles_query = (
        market_candles_df.writeStream
        .queryName("agg_market_candles_1m_to_clickhouse")
        .outputMode("update")
        .foreachBatch(_write_market_candles_batch)
        .option(
            "checkpointLocation",
            _checkpoint_location("agg_market_candles_1m_to_clickhouse"),
        )
        .trigger(processingTime="10 seconds")
        .start()
    )

    dead_letter_df = format_dead_letter_records(
        invalid_df,
        settings.KAFKA_AGG_TRADES_TOPIC,
    )
    dead_letter_query = (
        dead_letter_df.writeStream
        .queryName("invalid_agg_trades_to_dlq")
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", settings.KAFKA_DEAD_LETTER_TOPIC)
        .option(
            "checkpointLocation",
            _checkpoint_location("invalid_agg_trades_to_dlq"),
        )
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return [clickhouse_query, market_candles_query, dead_letter_query]


def run() -> None:
    spark = SparkConnect.create_spark_session()
    queries: list[StreamingQuery] = []

    try:
        queries = _start_queries(spark)
        logger.info(
            "Spark is consuming %s and writing trades to %s and 1-minute "
            "candles to %s",
            settings.KAFKA_AGG_TRADES_TOPIC,
            AGG_TRADES_TABLE,
            CANDLES_TABLE,
        )
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    try:
        run()
    except KeyboardInterrupt:
        logger.info("Spark streaming job stopped by user.")


if __name__ == "__main__":
    main()
