import logging
from datetime import UTC, datetime
from src.ml import enqueue_chain_snapshot_refresh, enqueue_trade_for_inference
from src.db import get_db, insert_trade
from src.tradier_stuff import parse_occ_symbol

logger = logging.getLogger(__name__)


TRADE_STATE_TTL_SECONDS = 20 * 60


async def handle_timesale(data: dict) -> None:
    """
    Handle the trade data received from the Tradier API.
    Identify the trade type (buy/sell) and store it in the database.

    Args:
        data (dict): The trade data received from the Tradier API.
    """
    symbol = data.get("symbol")
    if not symbol:
        return

    try:
        parse_occ_symbol(symbol)
    except ValueError:
        logger.debug("Skipping non-option timesale for symbol %s", symbol)
        return

    db = get_db()
    ask, bid = float(data.get("ask")), float(data.get("bid"))
    midpoint = (ask + bid) / 2
    trade_price = float(data.get("last"))

    # Lee ready rule
    if trade_price > ask:
        trade_type = "strong_buy"
    elif trade_price < bid:
        trade_type = "strong_sell"
    elif abs(trade_price - midpoint) > abs(trade_price - ask):
        trade_type = "buy"
    elif abs(trade_price - midpoint) > abs(trade_price - bid):
        trade_type = "sell"
    else:
        prev_price = await db.get(f"tradeprice:{symbol}")
        prev_class = await db.get(f"tradeclass:{symbol}")
        if prev_price and prev_class:
            trade_type = await tick_test(data, prev_price, prev_class)
        else:
            trade_type = "unknown"

    await db.set(f"tradeprice:{symbol}", trade_price, ex=TRADE_STATE_TTL_SECONDS)
    await db.set(f"tradeclass:{symbol}", trade_type, ex=TRADE_STATE_TTL_SECONDS)
    data["date"] = datetime.fromtimestamp(int(data.get("date")) / 1000, UTC)
    data["trade_type"] = trade_type
    data["ask"] = float(data["ask"])
    data["bid"] = float(data["bid"])
    data["last"] = float(data["last"])
    data["size"] = int(data["size"])
    trade_id = await insert_trade(data)
    data["trade_id"] = trade_id
    await enqueue_chain_snapshot_refresh(data)
    await enqueue_trade_for_inference(data)


async def tick_test(data: dict, prev_price: float, prev_class: str) -> str:
    """ """
    if data.get("last") > prev_price:
        return "buy"
    elif data.get("last") < prev_price:
        return "sell"
    else:
        if prev_class:
            return prev_class
        else:
            return "unknown"
