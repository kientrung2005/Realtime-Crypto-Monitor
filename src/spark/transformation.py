from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array,
    coalesce,
    col,
    concat_ws,
    count,
    current_timestamp,
    date_format,
    explode,
    expr,
    from_json,
    length,
    lit,
    lower,
    max as spark_max,
    max_by,
    min as spark_min,
    min_by,
    struct,
    sum as spark_sum,
    to_json,
    trim,
    upper,
    when,
    window,
)
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)


DECIMAL_TYPE = DecimalType(38, 18)
SUPPORTED_EXCHANGES = ("binance", "bybit", "okx")

TRADE_EVENT_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), True),
        StructField("exchange", StringType(), True),
        StructField("market_type", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("trade_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("trade_time", StringType(), True),
        StructField("price", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("taker_side", StringType(), True),
        StructField("buyer_is_market_maker", BooleanType(), True),
        StructField("first_trade_id", StringType(), True),
        StructField("last_trade_id", StringType(), True),
    ]
)

QUOTE_EVENT_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), True),
        StructField("exchange", StringType(), True),
        StructField("market_type", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("update_id", StringType(), True),
        StructField("snapshot_time", StringType(), True),
        StructField("best_bid_price", StringType(), True),
        StructField("best_bid_quantity", StringType(), True),
        StructField("best_ask_price", StringType(), True),
        StructField("best_ask_quantity", StringType(), True),
    ]
)


def transform_agg_trades(kafka_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
        col("partition").cast("int").alias("kafka_partition"),
        col("offset").cast("long").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), TRADE_EVENT_SCHEMA).alias("event"),
    )
    normalized_df = parsed_df.select(
        "kafka_key",
        "raw_value",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        lower(trim(col("event.event_type"))).alias("event_type"),
        lower(trim(col("event.exchange"))).alias("exchange"),
        lower(trim(col("event.market_type"))).alias("market_type"),
        upper(trim(col("event.symbol"))).alias("symbol"),
        col("event.trade_id").cast("long").alias("agg_trade_id"),
        expr("timestamp_millis(CAST(event.event_time AS BIGINT))").alias("event_time"),
        expr("timestamp_millis(CAST(event.trade_time AS BIGINT))").alias("trade_time"),
        col("event.price").cast(DECIMAL_TYPE).alias("price"),
        col("event.quantity").cast(DECIMAL_TYPE).alias("quantity"),
        upper(trim(col("event.taker_side"))).alias("taker_side"),
        col("event.buyer_is_market_maker").cast("boolean").alias(
            "buyer_is_market_maker"
        ),
        col("event.first_trade_id").cast("long").alias("first_trade_id"),
        col("event.last_trade_id").cast("long").alias("last_trade_id"),
    )
    error_reason = (
        when(col("raw_value").isNull() | (length(trim(col("raw_value"))) == 0), lit("empty_payload"))
        .when(col("event_type") != "trade", lit("unsupported_event_type"))
        .when(~col("exchange").isin(*SUPPORTED_EXCHANGES), lit("unsupported_exchange"))
        .when(col("market_type") != "spot", lit("unsupported_market_type"))
        .when(col("symbol").isNull() | (length(col("symbol")) == 0), lit("missing_symbol"))
        .when(
            col("kafka_key").isNotNull()
            & (upper(trim(col("kafka_key"))) != col("symbol")),
            lit("kafka_key_symbol_mismatch"),
        )
        .when(col("agg_trade_id").isNull(), lit("missing_trade_id"))
        .when(col("price").isNull() | (col("price") <= 0), lit("invalid_price"))
        .when(col("quantity").isNull() | (col("quantity") <= 0), lit("invalid_quantity"))
        .when(col("event_time").isNull() | col("trade_time").isNull(), lit("invalid_timestamp"))
        .when(~col("taker_side").isin("BUY", "SELL"), lit("invalid_taker_side"))
        .when(col("first_trade_id").isNull(), lit("missing_first_trade_id"))
        .when(col("last_trade_id").isNull(), lit("missing_last_trade_id"))
        .when(col("buyer_is_market_maker").isNull(), lit("missing_market_maker_flag"))
    )
    validated_df = normalized_df.withColumn("error_reason", error_reason)
    valid_df = validated_df.filter(col("error_reason").isNull()).select(
        lit(2).cast("short").alias("schema_version"),
        "exchange",
        "market_type",
        "symbol",
        "agg_trade_id",
        "event_time",
        "trade_time",
        "price",
        "quantity",
        (col("price") * col("quantity")).cast(DECIMAL_TYPE).alias("quote_quantity"),
        "taker_side",
        "buyer_is_market_maker",
        "first_trade_id",
        "last_trade_id",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        current_timestamp().alias("processed_at"),
    )
    return valid_df, validated_df.filter(col("error_reason").isNotNull())


def deduplicate_agg_trades(valid_df: DataFrame) -> DataFrame:
    key_columns = ["exchange", "market_type", "symbol", "agg_trade_id", "trade_time"]
    if valid_df.isStreaming:
        return valid_df.withWatermark("trade_time", "2 minutes").dropDuplicates(key_columns)
    return valid_df.dropDuplicates(key_columns[:-1])


def transform_book_tickers(kafka_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
        col("partition").cast("int").alias("kafka_partition"),
        col("offset").cast("long").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), QUOTE_EVENT_SCHEMA).alias("event"),
    )
    normalized_df = parsed_df.select(
        "kafka_key",
        "raw_value",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        lower(trim(col("event.event_type"))).alias("event_type"),
        lower(trim(col("event.exchange"))).alias("exchange"),
        lower(trim(col("event.market_type"))).alias("market_type"),
        upper(trim(col("event.symbol"))).alias("symbol"),
        col("event.update_id").cast("long").alias("update_id"),
        expr("timestamp_millis(CAST(event.snapshot_time AS BIGINT))").alias("snapshot_time"),
        col("event.best_bid_price").cast(DECIMAL_TYPE).alias("best_bid_price"),
        col("event.best_bid_quantity").cast(DECIMAL_TYPE).alias("best_bid_quantity"),
        col("event.best_ask_price").cast(DECIMAL_TYPE).alias("best_ask_price"),
        col("event.best_ask_quantity").cast(DECIMAL_TYPE).alias("best_ask_quantity"),
    )
    error_reason = (
        when(col("raw_value").isNull() | (length(trim(col("raw_value"))) == 0), lit("empty_payload"))
        .when(col("event_type") != "quote", lit("unsupported_event_type"))
        .when(~col("exchange").isin(*SUPPORTED_EXCHANGES), lit("unsupported_exchange"))
        .when(col("market_type") != "spot", lit("unsupported_market_type"))
        .when(col("update_id").isNull(), lit("missing_update_id"))
        .when(col("snapshot_time").isNull(), lit("invalid_snapshot_timestamp"))
        .when(col("symbol").isNull() | (length(col("symbol")) == 0), lit("missing_symbol"))
        .when(
            col("kafka_key").isNotNull()
            & (upper(trim(col("kafka_key"))) != col("symbol")),
            lit("kafka_key_symbol_mismatch"),
        )
        .when(col("best_bid_price").isNull() | (col("best_bid_price") <= 0), lit("invalid_best_bid_price"))
        .when(col("best_ask_price").isNull() | (col("best_ask_price") <= 0), lit("invalid_best_ask_price"))
        .when(col("best_bid_quantity").isNull() | (col("best_bid_quantity") < 0), lit("invalid_best_bid_quantity"))
        .when(col("best_ask_quantity").isNull() | (col("best_ask_quantity") < 0), lit("invalid_best_ask_quantity"))
        .when(col("best_ask_price") < col("best_bid_price"), lit("crossed_quote"))
        .when(col("kafka_timestamp").isNull(), lit("missing_kafka_timestamp"))
    )
    validated_df = normalized_df.withColumn("error_reason", error_reason)
    valid_df = validated_df.filter(col("error_reason").isNull()).select(
        "exchange",
        "market_type",
        "symbol",
        "update_id",
        "snapshot_time",
        "best_bid_price",
        "best_bid_quantity",
        "best_ask_price",
        "best_ask_quantity",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )
    return valid_df, validated_df.filter(col("error_reason").isNotNull())


def aggregate_quote_snapshots(
    valid_df: DataFrame,
    window_duration: str = "1 second",
    watermark_delay: str = "5 seconds",
) -> DataFrame:
    quote_order = struct(
        col("snapshot_time"), col("update_id"), col("kafka_partition"), col("kafka_offset")
    )
    quote_value = struct(
        "update_id", "snapshot_time", "best_bid_price", "best_bid_quantity",
        "best_ask_price", "best_ask_quantity", "kafka_partition", "kafka_offset",
        "kafka_timestamp",
    )
    aggregated_df = (
        valid_df.withWatermark("snapshot_time", watermark_delay)
        .groupBy(
            "exchange", "market_type", "symbol",
            window(col("snapshot_time"), window_duration).alias("snapshot_window"),
        )
        .agg(max_by(quote_value, quote_order).alias("latest"))
    )
    best_bid_price = col("latest.best_bid_price")
    best_ask_price = col("latest.best_ask_price")
    mid_price = ((best_bid_price + best_ask_price) / lit(2)).cast(DECIMAL_TYPE)
    spread = (best_ask_price - best_bid_price).cast(DECIMAL_TYPE)
    return aggregated_df.select(
        "exchange", "market_type", "symbol",
        col("latest.update_id").alias("update_id"),
        col("latest.snapshot_time").alias("snapshot_time"),
        best_bid_price.cast(DECIMAL_TYPE).alias("best_bid_price"),
        col("latest.best_bid_quantity").cast(DECIMAL_TYPE).alias("best_bid_quantity"),
        best_ask_price.cast(DECIMAL_TYPE).alias("best_ask_price"),
        col("latest.best_ask_quantity").cast(DECIMAL_TYPE).alias("best_ask_quantity"),
        mid_price.alias("mid_price"),
        spread.alias("spread"),
        when(mid_price > 0, spread * 100 / mid_price).otherwise(lit(0)).cast(DECIMAL_TYPE).alias("spread_percentage"),
        col("latest.kafka_partition").alias("kafka_partition"),
        col("latest.kafka_offset").alias("kafka_offset"),
        col("latest.kafka_timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("processed_at"),
    )


def aggregate_market_candles(
    valid_df: DataFrame,
    window_duration: str = "1 minute",
    interval: str = "1m",
    watermark_delay: str = "2 minutes",
) -> DataFrame:
    zero = lit(0).cast(DECIMAL_TYPE)
    trade_order = struct(col("trade_time"), col("agg_trade_id"))
    aggregated_df = (
        valid_df.withWatermark("trade_time", watermark_delay)
        .groupBy(
            "exchange", "market_type", "symbol",
            window(col("trade_time"), window_duration).alias("trade_window"),
        )
        .agg(
            min_by(col("price"), trade_order).alias("open_price"),
            spark_max("price").alias("high_price"),
            spark_min("price").alias("low_price"),
            max_by(col("price"), trade_order).alias("close_price"),
            spark_sum("quantity").alias("base_volume"),
            spark_sum("quote_quantity").alias("quote_volume"),
            count(lit(1)).alias("trade_count"),
            spark_sum(when(col("taker_side") == "BUY", col("quantity")).otherwise(zero)).alias("buy_volume"),
            spark_sum(when(col("taker_side") == "SELL", col("quantity")).otherwise(zero)).alias("sell_volume"),
        )
    )
    return aggregated_df.select(
        "exchange", "market_type", "symbol", lit(interval).alias("interval"),
        col("trade_window.start").alias("window_start"),
        col("trade_window.end").alias("window_end"),
        col("open_price").cast(DECIMAL_TYPE), col("high_price").cast(DECIMAL_TYPE),
        col("low_price").cast(DECIMAL_TYPE), col("close_price").cast(DECIMAL_TYPE),
        col("base_volume").cast(DECIMAL_TYPE), col("quote_volume").cast(DECIMAL_TYPE),
        "trade_count",
        when(col("base_volume") > 0, col("quote_volume") / col("base_volume")).otherwise(zero).cast(DECIMAL_TYPE).alias("vwap"),
        col("buy_volume").cast(DECIMAL_TYPE), col("sell_volume").cast(DECIMAL_TYPE),
        when(col("base_volume") > 0, col("buy_volume") * 100 / col("base_volume")).otherwise(zero).cast(DECIMAL_TYPE).alias("buy_pressure"),
        when(col("base_volume") > 0, col("sell_volume") * 100 / col("base_volume")).otherwise(zero).cast(DECIMAL_TYPE).alias("sell_pressure"),
        when(col("open_price") > 0, (col("high_price") - col("low_price")) * 100 / col("open_price")).otherwise(zero).cast(DECIMAL_TYPE).alias("volatility"),
        current_timestamp().alias("updated_at"),
    )


def generate_market_alerts(
    candles_df: DataFrame,
    volatility_threshold_percent: float,
    pressure_threshold_percent: float,
    quote_volume_threshold_usdt: float,
) -> DataFrame:
    volatility_threshold = lit(volatility_threshold_percent).cast(DECIMAL_TYPE)
    pressure_threshold = lit(pressure_threshold_percent).cast(DECIMAL_TYPE)
    volume_threshold = lit(quote_volume_threshold_usdt).cast(DECIMAL_TYPE)

    def candidate(condition: object, alert_type: str, metric_name: str, value: object, threshold: object, message: str, critical: object) -> object:
        return when(
            condition,
            struct(
                lit(alert_type).alias("alert_type"),
                when(critical, lit("CRITICAL")).otherwise(lit("HIGH")).alias("severity"),
                lit(metric_name).alias("metric_name"),
                value.cast(DECIMAL_TYPE).alias("metric_value"),
                threshold.cast(DECIMAL_TYPE).alias("threshold_value"),
                lit(message).alias("message"),
            ),
        )

    candidates = array(
        candidate(col("volatility") >= volatility_threshold, "HIGH_VOLATILITY", "volatility_percent", col("volatility"), volatility_threshold, "1-minute price volatility exceeded the configured threshold.", col("volatility") >= volatility_threshold * lit(2)),
        candidate(col("buy_pressure") >= pressure_threshold, "BUY_PRESSURE", "buy_pressure_percent", col("buy_pressure"), pressure_threshold, "1-minute buy pressure exceeded the configured threshold.", col("buy_pressure") >= pressure_threshold + lit(15)),
        candidate(col("sell_pressure") >= pressure_threshold, "SELL_PRESSURE", "sell_pressure_percent", col("sell_pressure"), pressure_threshold, "1-minute sell pressure exceeded the configured threshold.", col("sell_pressure") >= pressure_threshold + lit(15)),
        candidate(col("quote_volume") >= volume_threshold, "HIGH_VOLUME", "quote_volume_usdt", col("quote_volume"), volume_threshold, "1-minute quote volume exceeded the configured threshold.", col("quote_volume") >= volume_threshold * lit(2)),
    )
    return (
        candles_df.select(
            "exchange", "market_type", "symbol", col("window_end").alias("event_time"),
            "interval", explode(candidates).alias("alert"),
        )
        .filter(col("alert").isNotNull())
        .select(
            "exchange", "market_type", "symbol",
            col("alert.alert_type").alias("alert_type"),
            col("alert.severity").alias("severity"), "event_time", "interval",
            col("alert.metric_name").alias("metric_name"),
            col("alert.metric_value").alias("metric_value"),
            col("alert.threshold_value").alias("threshold_value"),
            col("alert.message").alias("message"),
        )
    )


def format_market_alert_records(alerts_df: DataFrame) -> DataFrame:
    return alerts_df.select(
        concat_ws("|", "exchange", "market_type", "symbol", "alert_type", date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")),
        to_json(struct("exchange", "market_type", "symbol", "alert_type", "severity", "event_time", "interval", "metric_name", "metric_value", "threshold_value", "message")).alias("value"),
    ).toDF("key", "value")


def format_dead_letter_records(invalid_df: DataFrame, source_topic: str) -> DataFrame:
    return invalid_df.select(
        coalesce(col("kafka_key"), col("symbol"), lit("unknown")).alias("key"),
        to_json(
            struct(
                lit(source_topic).alias("source_topic"),
                col("kafka_partition").alias("source_partition"),
                col("kafka_offset").alias("source_offset"),
                col("kafka_timestamp").alias("source_timestamp"),
                col("error_reason"), col("raw_value").alias("payload"),
                current_timestamp().alias("failed_at"),
            )
        ).alias("value"),
    )
