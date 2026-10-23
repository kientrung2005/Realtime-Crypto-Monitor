import logging
import math
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.streaming import StreamingQuery


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.settings import settings
from src.database.clickhouse_connect import ClickHouseConnect
from src.database.create_tables import create_tables
from src.spark.spark_connect import SparkConnect
from src.spark.transformation import (
    SUPPORTED_EXCHANGES,
    aggregate_market_candles,
    aggregate_quote_snapshots,
    deduplicate_agg_trades,
    format_dead_letter_records,
    format_market_alert_records,
    generate_market_alerts,
    transform_agg_trades,
    transform_book_tickers,
)
from src.storage.minio_writer import MinioArchiveCompactor, write_parquet_batch


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
    "exchange",
    "market_type",
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
EXCHANGE_SPREADS_TABLE = "fact_exchange_price_spreads"
EXCHANGE_SPREADS_COLUMNS = (
    "market_type",
    "symbol",
    "comparison_time",
    "exchange_a",
    "exchange_b",
    "trigger_exchange",
    "trigger_update_id",
    "quote_time_a",
    "quote_time_b",
    "mid_price_a",
    "mid_price_b",
    "spread_usdt",
    "spread_percentage",
    "recorded_at",
)
MARKET_ALERTS_TABLE = "fact_market_alerts"
MARKET_ALERTS_COLUMNS = (
    "exchange",
    "market_type",
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
CLICKHOUSE_RECORD = "clickhouse"
DEAD_LETTER_RECORD = "dead_letter"
TRADES_ARCHIVE = "trades"
QUOTES_ARCHIVE = "quotes"
STREAMING_TRIGGER = f"{settings.SPARK_STREAMING_TRIGGER_SECONDS} seconds"


@dataclass
class BatchWriteStats:
    inserted_rows: int = 0
    earliest_event_time: datetime | None = None
    latest_event_time: datetime | None = None


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
    event_time_column: str | None = None,
) -> BatchWriteStats:
    rows = batch_df.select(*columns).toLocalIterator()
    stats = BatchWriteStats()
    time_index = columns.index(event_time_column) if event_time_column else None

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
            if time_index is not None:
                for row in row_batch:
                    event_time = row[time_index]
                    if (
                        stats.earliest_event_time is None
                        or event_time < stats.earliest_event_time
                    ):
                        stats.earliest_event_time = event_time
                    if (
                        stats.latest_event_time is None
                        or event_time > stats.latest_event_time
                    ):
                        stats.latest_event_time = event_time
            client.insert(
                table=table,
                data=row_batch,
                column_names=list(columns),
            )
            stats.inserted_rows += len(row_batch)

    logger.info(
        "Spark batch %d inserted %d row(s) into ClickHouse table %s",
        batch_id,
        stats.inserted_rows,
        table,
    )
    return stats


def _exchange_spread_query(start: datetime, end: datetime) -> str:
    max_skew_seconds = settings.EXCHANGE_SPREAD_MAX_SKEW_SECONDS
    min_percent = settings.EXCHANGE_SPREAD_MIN_PERCENT
    if max_skew_seconds <= 0:
        raise ValueError("Exchange spread max skew must be positive")
    if not math.isfinite(min_percent) or not 0 <= min_percent <= 100:
        raise ValueError("Exchange spread minimum percent must be between 0 and 100")

    start_sql = start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    end_sql = end.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    lookup_start_sql = (start - timedelta(seconds=max_skew_seconds)).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )[:-3]
    exchanges_sql = ", ".join(f"'{exchange}'" for exchange in SUPPORTED_EXCHANGES)
    return f"""
        SELECT
            market_type,
            symbol,
            comparison_time,
            exchange_a,
            exchange_b,
            trigger_exchange,
            trigger_update_id,
            quote_time_a,
            quote_time_b,
            mid_price_a,
            mid_price_b,
            abs(mid_price_a - mid_price_b) AS spread_usdt,
            toDecimal128(
                abs(toFloat64(mid_price_a) - toFloat64(mid_price_b))
                * 100 / least(toFloat64(mid_price_a), toFloat64(mid_price_b)),
                18
            ) AS spread_percentage,
            now64(3) AS recorded_at
        FROM (
            SELECT
                a.market_type AS market_type,
                a.symbol AS symbol,
                a.snapshot_time AS comparison_time,
                least(a.exchange, a.other_exchange) AS exchange_a,
                greatest(a.exchange, a.other_exchange) AS exchange_b,
                a.exchange AS trigger_exchange,
                a.update_id AS trigger_update_id,
                if(a.exchange < a.other_exchange,
                    a.snapshot_time, b.snapshot_time) AS quote_time_a,
                if(a.exchange < a.other_exchange,
                    b.snapshot_time, a.snapshot_time) AS quote_time_b,
                if(a.exchange < a.other_exchange,
                    a.mid_price, b.mid_price) AS mid_price_a,
                if(a.exchange < a.other_exchange,
                    b.mid_price, a.mid_price) AS mid_price_b
            FROM (
                SELECT
                    market_type, symbol, exchange, snapshot_time,
                    update_id, mid_price,
                    arrayJoin([{exchanges_sql}]) AS other_exchange
                FROM {settings.CLICKHOUSE_DATABASE}.{QUOTE_SNAPSHOTS_TABLE} FINAL
                WHERE market_type = 'spot'
                  AND snapshot_time >= toDateTime64('{start_sql}', 3, 'UTC')
                  AND snapshot_time < toDateTime64('{end_sql}', 3, 'UTC')
            ) AS a
            ASOF LEFT JOIN (
                SELECT market_type, symbol, exchange, snapshot_time, mid_price
                FROM {settings.CLICKHOUSE_DATABASE}.{QUOTE_SNAPSHOTS_TABLE} FINAL
                WHERE market_type = 'spot'
                  AND snapshot_time >= toDateTime64('{lookup_start_sql}', 3, 'UTC')
                  AND snapshot_time < toDateTime64('{end_sql}', 3, 'UTC')
            ) AS b
            ON a.market_type = b.market_type
               AND a.symbol = b.symbol
               AND a.other_exchange = b.exchange
               AND a.snapshot_time >= b.snapshot_time
            WHERE a.exchange != a.other_exchange
              AND dateDiff('millisecond', b.snapshot_time, a.snapshot_time)
                  BETWEEN 0 AND {max_skew_seconds * 1000}
        )
        WHERE abs(toFloat64(mid_price_a) - toFloat64(mid_price_b))
              * 100 / least(toFloat64(mid_price_a), toFloat64(mid_price_b))
              >= {min_percent}
    """


def _write_exchange_spreads_batch(stats: BatchWriteStats, batch_id: int) -> None:
    if stats.earliest_event_time is None or stats.latest_event_time is None:
        return

    start = stats.earliest_event_time
    end = stats.latest_event_time + timedelta(milliseconds=1)

    with ClickHouseConnect(
        host=settings.CLICKHOUSE_HOST,
        port=settings.CLICKHOUSE_HTTP_PORT,
        username=settings.CLICKHOUSE_USER,
        password=settings.CLICKHOUSE_PASSWORD,
        database=settings.CLICKHOUSE_DATABASE,
    ) as client:
        rows = client.query(_exchange_spread_query(start, end)).result_rows
        if rows:
            client.insert(
                table=EXCHANGE_SPREADS_TABLE,
                data=rows,
                column_names=list(EXCHANGE_SPREADS_COLUMNS),
            )
    logger.info(
        "Spark batch %d recorded %d significant cross-exchange comparison(s)",
        batch_id,
        len(rows),
    )


def _write_market_candles_batch(batch_df: DataFrame, batch_id: int) -> None:
    _write_table_batch(
        batch_df,
        batch_id,
        CANDLES_TABLE,
        CANDLES_COLUMNS,
    )


def _write_market_alerts_batch(batch_df: DataFrame, batch_id: int) -> None:
    _write_table_batch(
        batch_df,
        batch_id,
        MARKET_ALERTS_TABLE,
        MARKET_ALERTS_COLUMNS,
    )


def _write_kafka_batch(
    batch_df: DataFrame,
    batch_id: int,
    topic: str,
) -> None:
    if batch_df.isEmpty():
        logger.info(
            "Spark batch %d has no row(s) for Kafka topic %s",
            batch_id,
            topic,
        )
        return

    (
        batch_df.select(
            col("key").cast("string").alias("key"),
            col("value").cast("string").alias("value"),
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", topic)
        .mode("append")
        .save()
    )
    logger.info(
        "Spark batch %d wrote row(s) to Kafka topic %s",
        batch_id,
        topic,
    )


def _combine_clickhouse_and_dead_letter_records(
    valid_df: DataFrame,
    invalid_df: DataFrame,
    columns: tuple[str, ...],
    source_topic: str,
) -> DataFrame:
    clickhouse_df = (
        valid_df.select(*columns)
        .withColumn("_record_target", lit(CLICKHOUSE_RECORD))
        .withColumn("key", lit(None).cast("string"))
        .withColumn("value", lit(None).cast("string"))
    )
    dead_letter_df = (
        format_dead_letter_records(invalid_df, source_topic)
        .withColumn("_record_target", lit(DEAD_LETTER_RECORD))
    )
    return clickhouse_df.unionByName(dead_letter_df, allowMissingColumns=True)


def _write_clickhouse_and_dead_letter_batch(
    batch_df: DataFrame,
    batch_id: int,
    table: str,
    columns: tuple[str, ...],
    archive_dataset: str,
    archive_event_time_column: str,
) -> BatchWriteStats:
    batch_df.persist()
    try:
        clickhouse_df = batch_df.filter(
            col("_record_target") == CLICKHOUSE_RECORD
        )
        stats = _write_table_batch(
            clickhouse_df,
            batch_id,
            table,
            columns,
            archive_event_time_column,
        )
        if stats.inserted_rows > 0:
            write_parquet_batch(
                clickhouse_df,
                batch_id,
                archive_dataset,
                columns,
                archive_event_time_column,
            )
        _write_kafka_batch(
            batch_df.filter(col("_record_target") == DEAD_LETTER_RECORD),
            batch_id,
            settings.KAFKA_DEAD_LETTER_TOPIC,
        )
        return stats
    finally:
        batch_df.unpersist()


def _write_trade_outputs_batch(batch_df: DataFrame, batch_id: int) -> None:
    _write_clickhouse_and_dead_letter_batch(
        batch_df,
        batch_id,
        AGG_TRADES_TABLE,
        AGG_TRADES_COLUMNS,
        TRADES_ARCHIVE,
        "trade_time",
    )


def _write_quote_outputs_batch(batch_df: DataFrame, batch_id: int) -> None:
    stats = _write_clickhouse_and_dead_letter_batch(
        batch_df,
        batch_id,
        QUOTE_SNAPSHOTS_TABLE,
        QUOTE_SNAPSHOTS_COLUMNS,
        QUOTES_ARCHIVE,
        "snapshot_time",
    )
    _write_exchange_spreads_batch(stats, batch_id)


def _write_market_alert_outputs_batch(
    batch_df: DataFrame,
    batch_id: int,
) -> None:
    batch_df.persist()
    try:
        _write_market_alerts_batch(batch_df, batch_id)
        _write_kafka_batch(
            format_market_alert_records(batch_df),
            batch_id,
            settings.KAFKA_ALERTS_TOPIC,
        )
    finally:
        batch_df.unpersist()


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
        .option(
            "maxOffsetsPerTrigger",
            str(settings.SPARK_MAX_OFFSETS_PER_TRIGGER),
        )
        .option("failOnDataLoss", "true")
        .load()
    )


def _start_queries(spark: SparkSession) -> list[StreamingQuery]:
    agg_trades_kafka_df = _read_kafka_stream(
        spark,
        settings.KAFKA_SPOT_TRADES_TOPIC,
    )
    valid_df, invalid_df = transform_agg_trades(agg_trades_kafka_df)
    deduplicated_valid_df = deduplicate_agg_trades(valid_df)
    trade_outputs_df = _combine_clickhouse_and_dead_letter_records(
        deduplicated_valid_df,
        invalid_df,
        AGG_TRADES_COLUMNS,
        settings.KAFKA_SPOT_TRADES_TOPIC,
    )

    trade_outputs_query = (
        trade_outputs_df.writeStream
        .queryName("spot_trade_outputs_v3")
        .outputMode("append")
        .foreachBatch(_write_trade_outputs_batch)
        .option(
            "checkpointLocation",
            _checkpoint_location("spot_trade_outputs_v3"),
        )
        .trigger(processingTime=STREAMING_TRIGGER)
        .start()
    )

    market_candles_df = aggregate_market_candles(valid_df)
    market_candles_query = (
        market_candles_df.writeStream
        .queryName("spot_market_candles_1m_to_clickhouse_v3")
        .outputMode("update")
        .foreachBatch(_write_market_candles_batch)
        .option(
            "checkpointLocation",
            _checkpoint_location("spot_market_candles_1m_to_clickhouse_v3"),
        )
        .trigger(processingTime=STREAMING_TRIGGER)
        .start()
    )

    alert_agg_trades_kafka_df = _read_kafka_stream(
        spark,
        settings.KAFKA_SPOT_TRADES_TOPIC,
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
    market_alerts_outputs_query = (
        market_alerts_df.writeStream
        .queryName("spot_market_alert_outputs_v3")
        .outputMode("append")
        .foreachBatch(_write_market_alert_outputs_batch)
        .option(
            "checkpointLocation",
            _checkpoint_location("spot_market_alert_outputs_v3"),
        )
        .trigger(processingTime=STREAMING_TRIGGER)
        .start()
    )

    book_ticker_kafka_df = _read_kafka_stream(
        spark,
        settings.KAFKA_SPOT_QUOTES_TOPIC,
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
    quote_outputs_df = _combine_clickhouse_and_dead_letter_records(
        quote_snapshots_df,
        invalid_book_ticker_df,
        QUOTE_SNAPSHOTS_COLUMNS,
        settings.KAFKA_SPOT_QUOTES_TOPIC,
    )
    quote_outputs_query = (
        quote_outputs_df.writeStream
        .queryName("spot_quote_outputs_v3")
        .outputMode("append")
        .foreachBatch(_write_quote_outputs_batch)
        .option(
            "checkpointLocation",
            _checkpoint_location("spot_quote_outputs_v3"),
        )
        .trigger(processingTime=STREAMING_TRIGGER)
        .start()
    )

    return [
        trade_outputs_query,
        market_candles_query,
        market_alerts_outputs_query,
        quote_outputs_query,
    ]


def run() -> None:
    create_tables()
    spark = SparkConnect.create_spark_session()
    queries: list[StreamingQuery] = []
    compactor = MinioArchiveCompactor()

    try:
        queries = _start_queries(spark)
        compactor.start()
        logger.info(
            "Spark is consuming %s and %s; writing trades to %s, "
            "1-minute candles to %s, quote snapshots to %s, "
            "cross-exchange spreads to %s, and alerts to %s",
            settings.KAFKA_SPOT_TRADES_TOPIC,
            settings.KAFKA_SPOT_QUOTES_TOPIC,
            AGG_TRADES_TABLE,
            CANDLES_TABLE,
            QUOTE_SNAPSHOTS_TABLE,
            EXCHANGE_SPREADS_TABLE,
            MARKET_ALERTS_TABLE,
        )
        spark.streams.awaitAnyTermination()
    finally:
        compactor.stop()
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
