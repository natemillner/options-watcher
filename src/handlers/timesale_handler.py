import logging

from src.db import get_db, insert_trade

logger = logging.getLogger(__name__)


async def handle_timesale(data: dict) -> None:
    """
    Handle the trade data received from the Tradier API.
    Identify the trade type (buy/sell) and store it in the database.

    Args:
        data (dict): The trade data received from the Tradier API.
    """
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
        prev_price = await db.get(f"tradeprice:{data.get('symbol')}")
        prev_class = await db.get(f"tradeclass:{data.get('symbol')}")
        if prev_price and prev_class:
            trade_type = await tick_test(data, prev_price, prev_class)
        else:
            trade_type = "unknown"

    await db.set(f"tradeprice:{data.get('symbol')}", trade_price)
    await db.set(f"tradeclass:{data.get('symbol')}", trade_type)
    data["trade_type"] = trade_type
    await insert_trade(data)


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
