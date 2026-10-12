import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def _get_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _get_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _get_symbols(name: str, default: str) -> tuple[str, ...]:
    raw_symbols = os.getenv(name, default)
    return tuple(
        symbol.strip().upper()
        for symbol in raw_symbols.split(",")
        if symbol.strip()
    )


class Settings:
    IN_DOCKER: bool = Path("/.dockerenv").exists()

    BINANCE_WEBSOCKET_URL: str = os.getenv(
        "BINANCE_WEBSOCKET_URL",
        "wss://stream.binance.com:9443/ws",
    )
    BINANCE_SYMBOLS: tuple[str, ...] = _get_symbols(
        "BINANCE_SYMBOLS", "btcusdt,ethusdt,solusdt"
    )
    BINANCE_AGG_TRADES_STREAM: str = os.getenv(
        "BINANCE_AGG_TRADES_STREAM",
        os.getenv("BINANCE_STREAM", "aggTrade"),
    )
    BINANCE_BOOK_TICKER_STREAM: str = os.getenv(
        "BINANCE_BOOK_TICKER_STREAM",
        "bookTicker",
    )

    BYBIT_WEBSOCKET_URL: str = os.getenv(
        "BYBIT_WEBSOCKET_URL",
        "wss://stream.bybit.com/v5/public/spot",
    )
    BYBIT_SYMBOLS: tuple[str, ...] = _get_symbols(
        "BYBIT_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT"
    )

    OKX_WEBSOCKET_URL: str = os.getenv(
        "OKX_WEBSOCKET_URL",
        "wss://ws.okx.com:8443/ws/v5/public",
    )
    OKX_SYMBOLS: tuple[str, ...] = _get_symbols(
        "OKX_SYMBOLS", "BTC-USDT,ETH-USDT,SOL-USDT"
    )

    KAFKA_BOOTSTRAP_SERVERS_HOST: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9094",
    )
    KAFKA_BOOTSTRAP_SERVERS_DOCKER: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS_DOCKER",
        "kafka:9092",
    )
    KAFKA_BOOTSTRAP_SERVERS: str = (
        KAFKA_BOOTSTRAP_SERVERS_DOCKER
        if IN_DOCKER
        else KAFKA_BOOTSTRAP_SERVERS_HOST
    )
    KAFKA_NUM_PARTITIONS: int = _get_int("KAFKA_NUM_PARTITIONS", 6)
    KAFKA_REPLICATION_FACTOR: int = _get_int("KAFKA_REPLICATION_FACTOR", 1)
    KAFKA_RETENTION_HOURS: int = _get_int("KAFKA_RETENTION_HOURS", 24)
    KAFKA_BOOK_TICKER_RETENTION_HOURS: int = _get_int(
        "KAFKA_BOOK_TICKER_RETENTION_HOURS",
        6,
    )
    KAFKA_SPOT_TRADES_TOPIC: str = os.getenv(
        "KAFKA_SPOT_TRADES_TOPIC",
        "crypto.spot.trades.v1",
    )
    KAFKA_SPOT_QUOTES_TOPIC: str = os.getenv(
        "KAFKA_SPOT_QUOTES_TOPIC",
        "crypto.spot.quotes.v1",
    )
    KAFKA_ALERTS_TOPIC: str = os.getenv(
        "KAFKA_ALERTS_TOPIC",
        "crypto.market-alerts.v1",
    )
    KAFKA_DEAD_LETTER_TOPIC: str = os.getenv(
        "KAFKA_DEAD_LETTER_TOPIC",
        "crypto.dead-letter.v1",
    )
    QUOTE_SNAPSHOT_INTERVAL_SECONDS: int = _get_int(
        "QUOTE_SNAPSHOT_INTERVAL_SECONDS",
        1,
    )
    QUOTE_SNAPSHOT_WATERMARK_SECONDS: int = _get_int(
        "QUOTE_SNAPSHOT_WATERMARK_SECONDS",
        5,
    )
    ALERT_CANDLE_WATERMARK_SECONDS: int = _get_int(
        "ALERT_CANDLE_WATERMARK_SECONDS",
        10,
    )
    ALERT_VOLATILITY_THRESHOLD_PERCENT: float = _get_float(
        "ALERT_VOLATILITY_THRESHOLD_PERCENT",
        0.5,
    )
    ALERT_PRESSURE_THRESHOLD_PERCENT: float = _get_float(
        "ALERT_PRESSURE_THRESHOLD_PERCENT",
        75.0,
    )
    ALERT_QUOTE_VOLUME_THRESHOLD_USDT: float = _get_float(
        "ALERT_QUOTE_VOLUME_THRESHOLD_USDT",
        5_000_000.0,
    )

    SPARK_MASTER_URL_HOST: str = os.getenv("SPARK_MASTER_URL_HOST", "local[*]")
    SPARK_MASTER_URL_DOCKER: str = os.getenv(
        "SPARK_MASTER_URL",
        "spark://spark-master:7077",
    )
    SPARK_MASTER_URL: str = (
        SPARK_MASTER_URL_DOCKER if IN_DOCKER else SPARK_MASTER_URL_HOST
    )
    SPARK_WORKER_CORES: int = _get_int("SPARK_WORKER_CORES", 2)
    SPARK_WORKER_MEMORY: str = os.getenv("SPARK_WORKER_MEMORY", "2g")
    SPARK_DRIVER_MEMORY: str = os.getenv("SPARK_DRIVER_MEMORY", "1g")
    SPARK_EXECUTOR_MEMORY: str = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
    SPARK_SQL_SHUFFLE_PARTITIONS: int = _get_int(
        "SPARK_SQL_SHUFFLE_PARTITIONS",
        SPARK_WORKER_CORES,
    )
    SPARK_MAX_OFFSETS_PER_TRIGGER: int = _get_int(
        "SPARK_MAX_OFFSETS_PER_TRIGGER",
        50_000,
    )
    SPARK_CHECKPOINT_LOCATION: str = os.getenv(
        "SPARK_CHECKPOINT_LOCATION",
        "/opt/spark/checkpoints",
    )
    SPARK_IVY_CACHE_LOCATION: str = os.getenv(
        "SPARK_IVY_CACHE_LOCATION",
        "/opt/spark/ivy",
    )
    SPARK_DRIVER_HOST: str = os.getenv(
        "SPARK_DRIVER_HOST",
        "spark-streaming",
    )
    SPARK_EXECUTOR_PYTHON: str = os.getenv(
        "SPARK_EXECUTOR_PYTHON",
        "/usr/bin/python3",
    )

    CLICKHOUSE_HOST_LOCAL: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    CLICKHOUSE_HOST_DOCKER: str = os.getenv(
        "CLICKHOUSE_HOST_DOCKER",
        "clickhouse",
    )
    CLICKHOUSE_HOST: str = (
        CLICKHOUSE_HOST_DOCKER if IN_DOCKER else CLICKHOUSE_HOST_LOCAL
    )
    CLICKHOUSE_HTTP_PORT: int = _get_int("CLICKHOUSE_HTTP_PORT", 8123)
    CLICKHOUSE_NATIVE_PORT: int = _get_int("CLICKHOUSE_NATIVE_PORT", 9000)
    CLICKHOUSE_DATABASE: str = os.getenv(
        "CLICKHOUSE_DATABASE",
        "crypto_market",
    )
    CLICKHOUSE_USER: str = os.getenv("CLICKHOUSE_USER", "crypto_user")
    CLICKHOUSE_PASSWORD: str = os.getenv("CLICKHOUSE_PASSWORD", "")


settings = Settings()
