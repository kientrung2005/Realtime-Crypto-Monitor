import asyncio
import logging
from collections.abc import Sequence

from confluent_kafka import KafkaError, Message, Producer
from websockets.asyncio.client import connect

from configs.settings import settings


logger = logging.getLogger(__name__)


class BinanceProducer:
    def __init__(self) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "client.id": "binance-agg-trades-producer",
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "gzip",
                "linger.ms": 20,
                "message.timeout.ms": 30_000,
            }
        )

    @staticmethod
    def _delivery_report(error: KafkaError | None, message: Message) -> None:
        if error is not None:
            logger.error(
                "Failed to deliver message to Kafka topic %s: %s",
                message.topic(),
                error,
            )

    async def publish(self, symbol: str, payload: str | bytes) -> None:
        while True:
            try:
                self._producer.produce(
                    topic=settings.KAFKA_AGG_TRADES_TOPIC,
                    key=symbol,
                    value=payload,
                    headers={
                        "source": "binance",
                        "event_type": settings.BINANCE_STREAM,
                        "schema_version": "1",
                    },
                    on_delivery=self._delivery_report,
                )
                self._producer.poll(0)
                return
            except BufferError:
                logger.warning("Kafka producer queue is full; waiting for delivery.")
                self._producer.poll(0)
                await asyncio.sleep(0.1)

    def close(self) -> None:
        remaining_messages = self._producer.flush(timeout=10)
        if remaining_messages:
            logger.warning(
                "%d Kafka message(s) were not delivered before shutdown.",
                remaining_messages,
            )
        else:
            logger.info("Kafka producer stopped after delivering all queued messages.")


def _stream_url(symbol: str) -> str:
    base_url = settings.BINANCE_WEBSOCKET_URL.rstrip("/")
    stream_name = f"{symbol.lower()}@{settings.BINANCE_STREAM}"
    return f"{base_url}/{stream_name}"


async def _stream_symbol(symbol: str, producer: BinanceProducer) -> None:
    reconnect_delay = 1
    url = _stream_url(symbol)

    while True:
        try:
            logger.info("Connecting to Binance stream for %s", symbol)
            async with connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_queue=1_000,
            ) as websocket:
                logger.info("Connected to Binance stream for %s", symbol)
                reconnect_delay = 1

                async for payload in websocket:
                    await producer.publish(symbol, payload)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning(
                "Binance stream for %s disconnected: %s. Reconnecting in %d second(s).",
                symbol,
                error,
                reconnect_delay,
            )
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30)


async def run(symbols: Sequence[str] | None = None) -> None:
    selected_symbols = tuple(symbols or settings.BINANCE_SYMBOLS)
    if not selected_symbols:
        raise ValueError("BINANCE_SYMBOLS must contain at least one symbol.")

    producer = BinanceProducer()
    logger.info(
        "Starting Binance producer for %s; Kafka topic: %s",
        ", ".join(selected_symbols),
        settings.KAFKA_AGG_TRADES_TOPIC,
    )

    try:
        await asyncio.gather(
            *(
                _stream_symbol(symbol, producer)
                for symbol in selected_symbols
            )
        )
    finally:
        producer.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Binance producer stopped by user.")


if __name__ == "__main__":
    main()
