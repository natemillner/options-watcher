from datetime import datetime

from src.db import get_db


QUOTE_TTL_SECONDS = 20 * 60
PREV_QUOTE_TTL_SECONDS = 10 * 60


async def handle_quote(data: dict) -> None:
    """
    Handle the quote data received from the Tradier API.

    Args:
        data (dict): The quote data received from the Tradier API.
    """
    db = get_db()
    symbol = data.get("symbol")
    if not symbol:
        return

    previous_quote = await db.get_json(f"quote:{symbol}")
    if previous_quote is not None:
        await db.set_json(
            f"quote:prev:{symbol}",
            previous_quote,
            ex=PREV_QUOTE_TTL_SECONDS,
        )

    normalized = {
        "symbol": symbol,
        "bid": float(data.get("bid") or 0.0),
        "ask": float(data.get("ask") or 0.0),
        "last": float(data.get("last") or data.get("price") or 0.0),
        "updated_at": datetime.utcnow().isoformat(),
    }
    await db.set_json(f"quote:{symbol}", normalized, ex=QUOTE_TTL_SECONDS)
