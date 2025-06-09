import asyncio
import json
from datetime import datetime, timedelta
import logging
import websockets

import itertools

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
        "expiration": (datetime.now() + timedelta(days=8)).date(),
        "strike_range": 10,
    },
    {
        "ticker": "QQQ",
        "expiration": (datetime.now() + timedelta(days=8)).date(),
        "strike_range": 10,
    },
    {
        "ticker": "NVDA",
        "expiration": (datetime.now() + timedelta(days=8)).date(),
        "strike_range": 10,
    },
    {
        "ticker": "RIOT",
        "expiration": (datetime.now() + timedelta(days=8)).date(),
        "strike_range": 4,
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
            ],
            "sessionid": get_session_id(),
            "filter": ["timesale"],
            "validOnly": True,
        }
        payload["symbols"] = list(itertools.chain.from_iterable(payload["symbols"]))

        logger.info(f"Sending payload for {len(payload['symbols'])} tickers")
        await websocket.send(json.dumps(payload))
        logger.info("Connected to Tradier WebSocket")

        async def watchdog():
            try:
                await asyncio.sleep(600)
                logger.warning("No message for 10 minutes—closing WebSocket.")
                await websocket.close()
            except asyncio.CancelledError:
                # expected when we reset the timer
                return

        watchdog_task = asyncio.create_task(watchdog())
        try:
            async for message in websocket:
                watchdog_task.cancel()
                data = json.loads(message)
                msg_type = data.get("type")
                if msg_type == "timesale":
                    if (
                        data.get("cancel", False) is False
                        and data.get("correction", False) is False
                    ):
                        asyncio.create_task(handle_timesale(data))
                watchdog_task = asyncio.create_task(watchdog())
        except websockets.ConnectionClosed as e:
            logger.error(f"WebSocket connection closed: {e}")
        finally:
            watchdog_task.cancel()
            logger.info("WebSocket connection closed, exiting.")
