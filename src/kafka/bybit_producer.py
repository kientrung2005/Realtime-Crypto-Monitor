import asyncio
import json
import logging
from collections.abc import Mapping, Sequence

from websockets.asyncio.client import connect

from configs.settings import settings
from src.kafka.market_data import MarketDataPublisher


logger = logging.getLogger(__name__)


def _trade_event(trade: Mapping[str, object]) -> dict[str, object] | None:
    symbol = str(trade.get("s", "")).strip().upper()
    side = str(trade.get("S", "")).strip().upper()
    trade_id = trade.get("i")
    if not symbol or side not in {"BUY", "SELL"} or trade_id is None:
        return None

    return {
        "event_type": "trade",
        "exchange": "bybit",
        "market_type": "spot",
        "symbol": symbol,
        "trade_id": trade_id,
        "event_time": trade.get("T"),
        "trade_time": trade.get("T"),
        "price": trade.get("p"),
        "quantity": trade.get("v"),
        "taker_side": side,
        "buyer_is_market_maker": side == "SELL",
        "first_trade_id": trade_id,
        "last_trade_id": trade_id,
    }


def _quote_event(message: Mapping[str, object]) -> dict[str, object] | None:
    topic = str(message.get("topic", ""))
    symbol = topic.removeprefix("orderbook.1.").upper()
    data = message.get("data")
    if not symbol or not isinstance(data, Mapping):
        return None

    bids = data.get("b")
    asks = data.get("a")
    if not isinstance(bids, list) or not isinstance(asks, list) or not bids or not asks:
        return None

    best_bid = bids[0]
    best_ask = asks[0]
    if not isinstance(best_bid, list) or not isinstance(best_ask, list):
        return None
    if len(best_bid) < 2 or len(best_ask) < 2:
        return None

    return {
        "event_type": "quote",
        "exchange": "bybit",
        "market_type": "spot",
        "symbol": symbol,
        "update_id": data.get("u"),
        "snapshot_time": data.get("cts") or message.get("ts"),
        "best_bid_price": best_bid[0],
        "best_bid_quantity": best_bid[1],
        "best_ask_price": best_ask[0],
        "best_ask_quantity": best_ask[1],
    }


async def _stream_market_data(
    symbols: Sequence[str],
    publisher: MarketDataPublisher,
) -> None:
    reconnect_delay = 1
    topics = [
        topic
        for symbol in symbols
        for topic in (f"publicTrade.{symbol}", f"orderbook.1.{symbol}")
    ]
    subscription = json.dumps({"op": "subscribe", "args": topics})

    while True:
        try:
            logger.info(
                "Connecting to Bybit WebSocket for %d market stream(s)",
                len(topics),
            )
            async with connect(
                settings.BYBIT_WEBSOCKET_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_queue=1_000,
            ) as websocket:
                await websocket.send(subscription)
                logger.info("Subscribed to Bybit streams: %s", ", ".join(topics))
                reconnect_delay = 1

                async for payload in websocket:
                    try:
                        message = json.loads(payload)
                    except (json.JSONDecodeError, TypeError):
                        logger.warning("Received invalid JSON from Bybit WebSocket.")
                        continue

                    if not isinstance(message, dict):
                        continue

                    topic = str(message.get("topic", ""))
                    if topic.startswith("publicTrade."):
                        trades = message.get("data")
                        if not isinstance(trades, list):
                            continue
                        for trade in trades:
                            if not isinstance(trade, Mapping):
                                continue
                            event = _trade_event(trade)
                            if event is not None:
                                await publisher.publish(
                                    settings.KAFKA_SPOT_TRADES_TOPIC,
                                    "trade",
                                    str(event["symbol"]),
                                    event,
                                )
                    elif topic.startswith("orderbook.1."):
                        event = _quote_event(message)
                        if event is not None:
                            await publisher.publish(
                                settings.KAFKA_SPOT_QUOTES_TOPIC,
                                "quote",
                                str(event["symbol"]),
                                event,
                            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning(
                "Bybit WebSocket disconnected: %s. Reconnecting in %d second(s).",
                error,
                reconnect_delay,
            )
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30)


async def run(symbols: Sequence[str] | None = None) -> None:
    selected_symbols = tuple(symbols or settings.BYBIT_SYMBOLS)
    if not selected_symbols:
        raise ValueError("BYBIT_SYMBOLS must contain at least one symbol.")

    publisher = MarketDataPublisher("bybit")
    logger.info(
        "Starting Bybit Spot producer for %s; Kafka topics: %s, %s",
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
        logger.info("Bybit producer stopped by user.")


if __name__ == "__main__":
    main()
