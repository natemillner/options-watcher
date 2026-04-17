import asyncio
import json
from datetime import datetime, timedelta
import logging
import websockets

import itertools

from .db import create_db, init_db_pool
from .handlers import handle_quote, handle_timesale
from .ml import initialize_chain_snapshot_runtime, initialize_inference_runtime
from .tradier_stuff import (
    filter_options_symbols,
    get_options_symbols,
    parse_occ_symbol,
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
    },
    {
        "ticker":"CAVA",
        "expiration": (datetime.now() + timedelta(days=12)).date(),
        "strike_range": 5,
    }
]


async def ws_connect():
    await create_db()
    await init_db_pool()
    await initialize_chain_snapshot_runtime()
    await initialize_inference_runtime()
    uri = "wss://ws.tradier.com/v1/markets/events"
    async with websockets.connect(uri, ssl=True, compression=None) as websocket:
        option_symbols = [
            filter_options_symbols(
                get_options_symbols(ticker["ticker"]),
                ticker["expiration"],
                ticker["strike_range"],
                get_price(ticker["ticker"]),
            )
            for ticker in TICKERS
        ]
        underlying_symbols = [ticker["ticker"] for ticker in TICKERS]
        underlying_symbol_set = set(underlying_symbols)
        payload = {
            "symbols": option_symbols + [underlying_symbols],
            "sessionid": get_session_id(),
            "filter": ["timesale", "quote"],
            "validOnly": True,
        }
        payload["symbols"] = list(itertools.chain.from_iterable(payload["symbols"]))

        logger.info(f"Sending payload for {len(payload['symbols'])} tickers")
        await websocket.send(json.dumps(payload))
        logger.info("Connected to Tradier WebSocket")

        def spawn_handler(coro: asyncio.Future, label: str) -> None:
            task = asyncio.create_task(coro)

            def _done_callback(done_task: asyncio.Task) -> None:
                try:
                    done_task.result()
                except Exception:
                    logger.exception("Unhandled error in %s task", label)

            task.add_done_callback(_done_callback)

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
                        try:
                            parse_occ_symbol(data.get("symbol", ""))
                        except ValueError:
                            continue
                        spawn_handler(handle_timesale(data), "timesale")
                elif msg_type == "quote":
                    if data.get("symbol") not in underlying_symbol_set:
                        continue
                    spawn_handler(handle_quote(data), "quote")
                watchdog_task = asyncio.create_task(watchdog())
        except websockets.ConnectionClosed as e:
            logger.error(f"WebSocket connection closed: {e}")
        finally:
            watchdog_task.cancel()
            logger.info("WebSocket connection closed, exiting.")
