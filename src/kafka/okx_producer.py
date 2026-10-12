import asyncio
import json
import logging
from collections.abc import Mapping, Sequence

from websockets.asyncio.client import connect

from configs.settings import settings
from src.kafka.market_data import MarketDataPublisher


logger = logging.getLogger(__name__)


def _canonical_symbol(symbol: object) -> str:
    return str(symbol).strip().upper().replace("-", "")


def _trade_event(trade: Mapping[str, object]) -> dict[str, object] | None:
    symbol = _canonical_symbol(trade.get("instId", ""))
    side = str(trade.get("side", "")).strip().upper()
    trade_id = trade.get("tradeId")
    if not symbol or side not in {"BUY", "SELL"} or trade_id is None:
        return None

    return {
        "event_type": "trade",
        "exchange": "okx",
        "market_type": "spot",
        "symbol": symbol,
        "trade_id": trade_id,
        "event_time": trade.get("ts"),
        "trade_time": trade.get("ts"),
        "price": trade.get("px"),
        "quantity": trade.get("sz"),
        "taker_side": side,
        "buyer_is_market_maker": side == "SELL",
        "first_trade_id": trade_id,
        "last_trade_id": trade_id,
    }


def _quote_event(message: Mapping[str, object]) -> dict[str, object] | None:
    argument = message.get("arg")
    data = message.get("data")
    if not isinstance(argument, Mapping) or not isinstance(data, list) or not data:
        return None

    snapshot = data[0]
    if not isinstance(snapshot, Mapping):
        return None
    symbol = _canonical_symbol(argument.get("instId", ""))
    bids = snapshot.get("bids")
    asks = snapshot.get("asks")
    if not symbol or not isinstance(bids, list) or not isinstance(asks, list):
        return None
    if not bids or not asks or len(bids[0]) < 2 or len(asks[0]) < 2:
        return None

    return {
        "event_type": "quote",
        "exchange": "okx",
        "market_type": "spot",
        "symbol": symbol,
        "update_id": snapshot.get("seqId"),
        "snapshot_time": snapshot.get("ts"),
        "best_bid_price": bids[0][0],
        "best_bid_quantity": bids[0][1],
        "best_ask_price": asks[0][0],
        "best_ask_quantity": asks[0][1],
    }


async def _stream_market_data(
    symbols: Sequence[str],
    publisher: MarketDataPublisher,
) -> None:
    reconnect_delay = 1
    subscriptions = [
        {"channel": channel, "instId": symbol}
        for symbol in symbols
        for channel in ("trades", "books5")
    ]
    subscription = json.dumps({"op": "subscribe", "args": subscriptions})

    while True:
        try:
            logger.info(
                "Connecting to OKX WebSocket for %d market stream(s)",
                len(subscriptions),
            )
            async with connect(
                settings.OKX_WEBSOCKET_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_queue=1_000,
            ) as websocket:
                await websocket.send(subscription)
                logger.info("Subscribed to OKX streams for: %s", ", ".join(symbols))
                reconnect_delay = 1

                async for payload in websocket:
                    try:
                        message = json.loads(payload)
                    except (json.JSONDecodeError, TypeError):
                        logger.warning("Received invalid JSON from OKX WebSocket.")
                        continue

                    if not isinstance(message, dict):
                        continue
                    argument = message.get("arg")
                    channel = (
                        str(argument.get("channel", ""))
                        if isinstance(argument, Mapping)
                        else ""
                    )
                    if channel == "trades":
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
                    elif channel == "books5":
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
                "OKX WebSocket disconnected: %s. Reconnecting in %d second(s).",
                error,
                reconnect_delay,
            )
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30)


async def run(symbols: Sequence[str] | None = None) -> None:
    selected_symbols = tuple(symbols or settings.OKX_SYMBOLS)
    if not selected_symbols:
        raise ValueError("OKX_SYMBOLS must contain at least one symbol.")

    publisher = MarketDataPublisher("okx")
    logger.info(
        "Starting OKX Spot producer for %s; Kafka topics: %s, %s",
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
        logger.info("OKX producer stopped by user.")


if __name__ == "__main__":
    main()
