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
    aggregate_quote_snapshots,
    deduplicate_agg_trades,
    format_dead_letter_records,
    format_market_alert_records,
    generate_market_alerts,
    transform_agg_trades,
    transform_book_tickers,
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
QUOTE_SNAPSHOTS_TABLE = "fact_quote_snapshots"
QUOTE_SNAPSHOTS_COLUMNS = (
    "exchange",
    "market_type",
    "symbol",
    "update_id",
    "snapshot_time",
    "best_bid_price",
    "best_bid_quantity",
    "best_ask_price",
    "best_ask_quantity",
    "mid_price",
    "spread",
    "spread_percentage",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "processed_at",
)
MARKET_ALERTS_TABLE = "fact_market_alerts"
MARKET_ALERTS_COLUMNS = (
    "symbol",
    "alert_type",
    "severity",
    "event_time",
    "interval",
    "metric_name",
    "metric_value",
    "threshold_value",
    "message",
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


def _write_quote_snapshots_batch(batch_df: DataFrame, batch_id: int) -> None:
    _write_table_batch(
        batch_df,
        batch_id,
        QUOTE_SNAPSHOTS_TABLE,
        QUOTE_SNAPSHOTS_COLUMNS,
    )


def _write_market_alerts_batch(batch_df: DataFrame, batch_id: int) -> None:
    _write_table_batch(
        batch_df,
        batch_id,
        MARKET_ALERTS_TABLE,
        MARKET_ALERTS_COLUMNS,
    )


def _read_kafka_stream(
    spark: SparkSession,
    topic: str,
    starting_offsets: str = "earliest",
) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "true")
        .load()
    )


def _start_queries(spark: SparkSession) -> list[StreamingQuery]:
    agg_trades_kafka_df = _read_kafka_stream(
        spark,
        settings.KAFKA_AGG_TRADES_TOPIC,
    )
    valid_df, invalid_df = transform_agg_trades(agg_trades_kafka_df)
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

    alert_agg_trades_kafka_df = _read_kafka_stream(
        spark,
        settings.KAFKA_AGG_TRADES_TOPIC,
        starting_offsets="latest",
    )
    alert_valid_df, _ = transform_agg_trades(alert_agg_trades_kafka_df)
    completed_candles_df = aggregate_market_candles(
        alert_valid_df,
        watermark_delay=(
            f"{settings.ALERT_CANDLE_WATERMARK_SECONDS} seconds"
        ),
    )
    market_alerts_df = generate_market_alerts(
        completed_candles_df,
        volatility_threshold_percent=(
            settings.ALERT_VOLATILITY_THRESHOLD_PERCENT
        ),
        pressure_threshold_percent=settings.ALERT_PRESSURE_THRESHOLD_PERCENT,
        quote_volume_threshold_usdt=(
            settings.ALERT_QUOTE_VOLUME_THRESHOLD_USDT
        ),
    )
    market_alerts_clickhouse_query = (
        market_alerts_df.writeStream
        .queryName("market_alerts_to_clickhouse")
        .outputMode("append")
        .foreachBatch(_write_market_alerts_batch)
        .option(
            "checkpointLocation",
            _checkpoint_location("market_alerts_to_clickhouse"),
        )
        .trigger(processingTime="10 seconds")
        .start()
    )

    market_alerts_kafka_query = (
        format_market_alert_records(market_alerts_df)
        .writeStream
        .queryName("market_alerts_to_kafka")
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", settings.KAFKA_ALERTS_TOPIC)
        .option(
            "checkpointLocation",
            _checkpoint_location("market_alerts_to_kafka"),
        )
        .outputMode("append")
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

    book_ticker_kafka_df = _read_kafka_stream(
        spark,
        settings.KAFKA_BOOK_TICKER_TOPIC,
    )
    valid_book_ticker_df, invalid_book_ticker_df = transform_book_tickers(
        book_ticker_kafka_df
    )
    quote_snapshots_df = aggregate_quote_snapshots(
        valid_book_ticker_df,
        window_duration=(
            f"{settings.QUOTE_SNAPSHOT_INTERVAL_SECONDS} seconds"
        ),
        watermark_delay=(
            f"{settings.QUOTE_SNAPSHOT_WATERMARK_SECONDS} seconds"
        ),
    )
    quote_snapshots_query = (
        quote_snapshots_df.writeStream
        .queryName("quote_snapshots_to_clickhouse")
        .outputMode("append")
        .foreachBatch(_write_quote_snapshots_batch)
        .option(
            "checkpointLocation",
            _checkpoint_location("quote_snapshots_to_clickhouse"),
        )
        .trigger(processingTime="10 seconds")
        .start()
    )

    invalid_book_ticker_dead_letter_df = format_dead_letter_records(
        invalid_book_ticker_df,
        settings.KAFKA_BOOK_TICKER_TOPIC,
    )
    invalid_book_ticker_query = (
        invalid_book_ticker_dead_letter_df.writeStream
        .queryName("invalid_book_tickers_to_dlq")
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", settings.KAFKA_DEAD_LETTER_TOPIC)
        .option(
            "checkpointLocation",
            _checkpoint_location("invalid_book_tickers_to_dlq"),
        )
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return [
        clickhouse_query,
        market_candles_query,
        market_alerts_clickhouse_query,
        market_alerts_kafka_query,
        dead_letter_query,
        quote_snapshots_query,
        invalid_book_ticker_query,
    ]


def run() -> None:
    spark = SparkConnect.create_spark_session()
    queries: list[StreamingQuery] = []

    try:
        queries = _start_queries(spark)
        logger.info(
            "Spark is consuming %s and %s; writing trades to %s, "
            "1-minute candles to %s, quote snapshots to %s, and alerts to %s",
            settings.KAFKA_AGG_TRADES_TOPIC,
            settings.KAFKA_BOOK_TICKER_TOPIC,
            AGG_TRADES_TABLE,
            CANDLES_TABLE,
            QUOTE_SNAPSHOTS_TABLE,
            MARKET_ALERTS_TABLE,
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
