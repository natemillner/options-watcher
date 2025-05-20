import asyncio
import json
from datetime import datetime, timedelta
import logging
import websockets

from .db import create_db, init_db_pool
from .handlers import handle_timesale
from .tradier_stuff import (
    filter_options_symbols,
    get_options_symbols,
    get_price,
    get_session_id,
)

logger = logging.getLogger(__name__)
TICKERS = [
    {
        "ticker": "SPY",
        "expiration": (datetime.now() + timedelta(days=30)).date(),
        "strike_range": 10,
    }
]


async def ws_connect():
    await create_db()
    await init_db_pool()
    uri = "wss://ws.tradier.com/v1/markets/events"
    async with websockets.connect(uri, ssl=True, compression=None) as websocket:
        payload = {
            "symbols": [
                filter_options_symbols(
                    get_options_symbols(ticker["ticker"]),
                    ticker["expiration"],
                    ticker["strike_range"],
                    get_price(ticker["ticker"]),
                )
                for ticker in TICKERS
            ][0],
            "sessionid": get_session_id(),
            "filter": ["timesale"],
            "validOnly": True,
        }
        logger.info("Connecting to Tradier WebSocket")
        await websocket.send(json.dumps(payload))
        logger.info("Connected to Tradier WebSocket")
        async for message in websocket:
            data = json.loads(message)
            msg_type = data.get("type")
            if msg_type == "timesale":
                if (
                    data.get("cancel", False) is False
                    and data.get("correction", False) is False
                ):
                    asyncio.create_task(handle_timesale(data))

        await asyncio.Future()  # Keep the connection open
