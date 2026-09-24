from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    expr,
    from_json,
    length,
    lit,
    struct,
    to_json,
    trim,
    upper,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)


DECIMAL_TYPE = DecimalType(38, 18)

AGG_TRADE_SCHEMA = StructType(
    [
        StructField("e", StringType(), True),
        StructField("E", LongType(), True),
        StructField("s", StringType(), True),
        StructField("a", LongType(), True),
        StructField("p", StringType(), True),
        StructField("q", StringType(), True),
        StructField("f", LongType(), True),
        StructField("l", LongType(), True),
        StructField("T", LongType(), True),
        StructField("m", BooleanType(), True),
    ]
)


def transform_agg_trades(
    kafka_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
        col("partition").cast("int").alias("kafka_partition"),
        col("offset").cast("long").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(
            col("value").cast("string"),
            AGG_TRADE_SCHEMA,
            {"mode": "PERMISSIVE"},
        ).alias("event"),
    )

    normalized_df = parsed_df.select(
        "kafka_key",
        "raw_value",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        col("event.e").alias("event_type"),
        expr("timestamp_millis(event.E)").alias("event_time"),
        upper(trim(col("event.s"))).alias("symbol"),
        col("event.a").cast("long").alias("agg_trade_id"),
        col("event.p").cast(DECIMAL_TYPE).alias("price"),
        col("event.q").cast(DECIMAL_TYPE).alias("quantity"),
        col("event.f").cast("long").alias("first_trade_id"),
        col("event.l").cast("long").alias("last_trade_id"),
        expr("timestamp_millis(event.T)").alias("trade_time"),
        col("event.m").cast("boolean").alias("buyer_is_market_maker"),
    )

    error_reason = (
        when(
            col("raw_value").isNull()
            | (length(trim(col("raw_value"))) == 0),
            lit("empty_payload"),
        )
        .when(
            col("event_type").isNull(),
            lit("invalid_json_or_missing_event_type"),
        )
        .when(col("event_type") != "aggTrade", lit("unsupported_event_type"))
        .when(
            col("symbol").isNull() | (length(col("symbol")) == 0),
            lit("missing_symbol"),
        )
        .when(
            col("kafka_key").isNotNull()
            & (upper(trim(col("kafka_key"))) != col("symbol")),
            lit("kafka_key_symbol_mismatch"),
        )
        .when(col("agg_trade_id").isNull(), lit("missing_agg_trade_id"))
        .when(
            col("price").isNull() | (col("price") <= 0),
            lit("invalid_price"),
        )
        .when(
            col("quantity").isNull() | (col("quantity") <= 0),
            lit("invalid_quantity"),
        )
        .when(
            col("event_time").isNull() | col("trade_time").isNull(),
            lit("invalid_timestamp"),
        )
        .when(col("first_trade_id").isNull(), lit("missing_first_trade_id"))
        .when(col("last_trade_id").isNull(), lit("missing_last_trade_id"))
        .when(
            col("buyer_is_market_maker").isNull(),
            lit("missing_market_maker_flag"),
        )
    )

    validated_df = normalized_df.withColumn("error_reason", error_reason)

    valid_df = (
        validated_df
        .filter(col("error_reason").isNull())
        .select(
            lit(1).cast("short").alias("schema_version"),
            lit("binance").alias("exchange"),
            lit("spot").alias("market_type"),
            "symbol",
            "agg_trade_id",
            "event_time",
            "trade_time",
            "price",
            "quantity",
            (col("price") * col("quantity"))
            .cast(DECIMAL_TYPE)
            .alias("quote_quantity"),
            when(col("buyer_is_market_maker"), lit("SELL"))
            .otherwise(lit("BUY"))
            .alias("taker_side"),
            "buyer_is_market_maker",
            "first_trade_id",
            "last_trade_id",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            current_timestamp().alias("processed_at"),
        )
    )

    if kafka_df.isStreaming:
        valid_df = (
            valid_df
            .withWatermark("trade_time", "2 minutes")
            .dropDuplicates(["symbol", "agg_trade_id", "trade_time"])
        )
    else:
        valid_df = valid_df.dropDuplicates(["symbol", "agg_trade_id"])

    invalid_df = validated_df.filter(col("error_reason").isNotNull())
    return valid_df, invalid_df


def format_dead_letter_records(
    invalid_df: DataFrame,
    source_topic: str,
) -> DataFrame:
    return invalid_df.select(
        coalesce(col("kafka_key"), col("symbol"), lit("unknown")).alias("key"),
        to_json(
            struct(
                lit(source_topic).alias("source_topic"),
                col("kafka_partition").alias("source_partition"),
                col("kafka_offset").alias("source_offset"),
                col("kafka_timestamp").alias("source_timestamp"),
                col("error_reason"),
                col("raw_value").alias("payload"),
                current_timestamp().alias("failed_at"),
            )
        ).alias("value"),
    )
