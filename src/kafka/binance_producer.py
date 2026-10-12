import asyncio
import json
import logging
import time
from collections.abc import Mapping, Sequence

from websockets.asyncio.client import connect

from configs.settings import settings
from src.kafka.market_data import MarketDataPublisher


logger = logging.getLogger(__name__)


def _subscription_streams(symbols: Sequence[str]) -> tuple[str, ...]:
    return tuple(
        stream
        for symbol in symbols
        for stream in (
            f"{symbol.lower()}@{settings.BINANCE_AGG_TRADES_STREAM}",
            f"{symbol.lower()}@{settings.BINANCE_BOOK_TICKER_STREAM}",
        )
    )


def _normalized_trade(event: Mapping[str, object]) -> dict[str, object]:
    symbol = str(event["s"]).upper()
    buyer_is_market_maker = bool(event["m"])
    return {
        "event_type": "trade",
        "exchange": "binance",
        "market_type": "spot",
        "symbol": symbol,
        "trade_id": event["a"],
        "event_time": event["E"],
        "trade_time": event["T"],
        "price": event["p"],
        "quantity": event["q"],
        "taker_side": "SELL" if buyer_is_market_maker else "BUY",
        "buyer_is_market_maker": buyer_is_market_maker,
        "first_trade_id": event["f"],
        "last_trade_id": event["l"],
    }


def _normalized_quote(event: Mapping[str, object]) -> dict[str, object]:
    return {
        "event_type": "quote",
        "exchange": "binance",
        "market_type": "spot",
        "symbol": str(event["s"]).upper(),
        "update_id": event["u"],
        "snapshot_time": event.get("E") or event.get("T") or int(time.time() * 1000),
        "best_bid_price": event["b"],
        "best_bid_quantity": event["B"],
        "best_ask_price": event["a"],
        "best_ask_quantity": event["A"],
    }


def _route_payload(
    payload: str | bytes,
) -> tuple[str, str, str, dict[str, object]] | None:
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
        return None

    if event.get("e") == "aggTrade":
        return (
            settings.KAFKA_SPOT_TRADES_TOPIC,
            "trade",
            symbol,
            _normalized_trade(event),
        )

    if all(field in event for field in ("u", "b", "B", "a", "A")):
        return (
            settings.KAFKA_SPOT_QUOTES_TOPIC,
            "quote",
            symbol,
            _normalized_quote(event),
        )

    logger.debug("Ignoring unsupported Binance event for %s.", symbol)
    return None


async def _stream_market_data(
    symbols: Sequence[str],
    publisher: MarketDataPublisher,
) -> None:
    reconnect_delay = 1
    streams = _subscription_streams(symbols)
    subscription = json.dumps(
        {"method": "SUBSCRIBE", "params": list(streams), "id": 1}
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
                logger.info("Subscribed to Binance streams: %s", ", ".join(streams))
                reconnect_delay = 1

                async for payload in websocket:
                    route = _route_payload(payload)
                    if route is None:
                        continue

                    topic, event_type, symbol, normalized_payload = route
                    await publisher.publish(
                        topic,
                        event_type,
                        symbol,
                        normalized_payload,
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

    publisher = MarketDataPublisher("binance")
    logger.info(
        "Starting Binance Spot producer for %s; Kafka topics: %s, %s",
        ", ".join(selected_symbols),
        settings.KAFKA_SPOT_TRADES_TOPIC,
        settings.KAFKA_SPOT_QUOTES_TOPIC,
    )

    try:
        await _stream_market_data(selected_symbols, publisher)
    finally:
        publisher.close()


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
