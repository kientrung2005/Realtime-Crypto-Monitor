from __future__ import annotations

import asyncio
import json
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
                "client.id": "binance-market-data-producer",
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

    async def publish(
        self,
        topic: str,
        event_type: str,
        symbol: str,
        payload: str | bytes,
    ) -> None:
        while True:
            try:
                self._producer.produce(
                    topic=topic,
                    key=symbol,
                    value=payload,
                    headers={
                        "source": "binance",
                        "event_type": event_type,
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


def _subscription_streams(symbols: Sequence[str]) -> tuple[str, ...]:
    return tuple(
        stream
        for symbol in symbols
        for stream in (
            f"{symbol.lower()}@{settings.BINANCE_AGG_TRADES_STREAM}",
            f"{symbol.lower()}@{settings.BINANCE_BOOK_TICKER_STREAM}",
        )
    )


def _route_payload(payload: str | bytes) -> tuple[str, str, str] | None:
    try:
        event = json.loads(payload)
    except (json.JSONDecodeError, TypeError):
        logger.warning("Received invalid JSON from Binance WebSocket.")
        return None

    if not isinstance(event, dict):
        logger.warning("Received unsupported Binance payload type.")
        return None

    symbol = str(event.get("s", "")).strip().upper()
    if not symbol:
        # Subscription acknowledgements do not contain a symbol.
        return None

    if event.get("e") == "aggTrade":
        return (
            settings.KAFKA_AGG_TRADES_TOPIC,
            settings.BINANCE_AGG_TRADES_STREAM,
            symbol,
        )

    if all(field in event for field in ("u", "b", "B", "a", "A")):
        return (
            settings.KAFKA_BOOK_TICKER_TOPIC,
            settings.BINANCE_BOOK_TICKER_STREAM,
            symbol,
        )

    logger.debug("Ignoring unsupported Binance event for %s.", symbol)
    return None


async def _stream_market_data(
    symbols: Sequence[str],
    producer: BinanceProducer,
) -> None:
    reconnect_delay = 1
    streams = _subscription_streams(symbols)
    subscription = json.dumps(
        {
            "method": "SUBSCRIBE",
            "params": list(streams),
            "id": 1,
        }
    )

    while True:
        try:
            logger.info(
                "Connecting to Binance WebSocket for %d market stream(s)",
                len(streams),
            )
            async with connect(
                settings.BINANCE_WEBSOCKET_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_queue=1_000,
            ) as websocket:
                await websocket.send(subscription)
                logger.info(
                    "Subscribed to Binance streams: %s",
                    ", ".join(streams),
                )
                reconnect_delay = 1

                async for payload in websocket:
                    route = _route_payload(payload)
                    if route is None:
                        continue

                    topic, event_type, symbol = route
                    await producer.publish(
                        topic,
                        event_type,
                        symbol,
                        payload,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning(
                "Binance WebSocket disconnected: %s. Reconnecting in %d second(s).",
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
        "Starting Binance producer for %s; Kafka topics: %s, %s",
        ", ".join(selected_symbols),
        settings.KAFKA_AGG_TRADES_TOPIC,
        settings.KAFKA_BOOK_TICKER_TOPIC,
    )

    try:
        await _stream_market_data(selected_symbols, producer)
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
