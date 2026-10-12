import asyncio
import json
import logging
from collections.abc import Mapping
from time import monotonic

from confluent_kafka import KafkaError, Message, Producer

from configs.settings import settings


logger = logging.getLogger(__name__)


class MarketDataPublisher:
    def __init__(self, exchange: str) -> None:
        self.exchange = exchange
        self._last_quote_published_at: dict[str, float] = {}
        self._producer = Producer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "client.id": f"{exchange}-market-data-producer",
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
        payload: Mapping[str, object],
    ) -> None:
        if event_type == "quote":
            now = monotonic()
            last_published_at = self._last_quote_published_at.get(symbol)
            if (
                last_published_at is not None
                and now - last_published_at
                < settings.QUOTE_SNAPSHOT_INTERVAL_SECONDS
            ):
                return
            self._last_quote_published_at[symbol] = now

        serialized_payload = json.dumps(payload, separators=(",", ":"))

        while True:
            try:
                self._producer.produce(
                    topic=topic,
                    key=symbol,
                    value=serialized_payload,
                    headers={
                        "source": self.exchange,
                        "event_type": event_type,
                        "schema_version": "2",
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
            logger.info(
                "%s producer stopped after delivering all queued messages.",
                self.exchange.title(),
            )
