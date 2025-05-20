from src.db import get_db


async def handle_quote(data: dict) -> None:
    """
    Handle the quote data received from the Tradier API.

    Args:
        data (dict): The quote data received from the Tradier API.
    """
    db = get_db()
    # Save prev quote to quote2
    await db.set(f"quote2:{db.get(f"quote:{data.get('symbol')}")}", data)
    # Save quote to quote
    await db.set(f"quote:{data.get('symbol')}", data)
