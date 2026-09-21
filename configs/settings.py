import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def _get_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _get_symbols() -> tuple[str, ...]:
    raw_symbols = os.getenv("BINANCE_SYMBOLS", "btcusdt,ethusdt,solusdt")
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
    BINANCE_SYMBOLS: tuple[str, ...] = _get_symbols()
    BINANCE_STREAM: str = os.getenv("BINANCE_STREAM", "aggTrade")

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
    KAFKA_AGG_TRADES_TOPIC: str = os.getenv(
        "KAFKA_AGG_TRADES_TOPIC",
        "binance.spot.agg-trades.v1",
    )
    KAFKA_BOOK_TICKER_TOPIC: str = os.getenv(
        "KAFKA_BOOK_TICKER_TOPIC",
        "binance.spot.book-ticker.v1",
    )
    KAFKA_ALERTS_TOPIC: str = os.getenv(
        "KAFKA_ALERTS_TOPIC",
        "binance.market-alerts.v1",
    )
    KAFKA_DEAD_LETTER_TOPIC: str = os.getenv(
        "KAFKA_DEAD_LETTER_TOPIC",
        "binance.dead-letter.v1",
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
    SPARK_CHECKPOINT_LOCATION: str = os.getenv(
        "SPARK_CHECKPOINT_LOCATION",
        "/opt/spark/checkpoints",
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
