import asyncio
import logging
from datetime import UTC, datetime

from src.db import get_db, insert_option_chain_snapshot
from src.tradier_stuff import get_quote, parse_occ_symbol

logger = logging.getLogger(__name__)

UNDERLYING_QUOTE_TTL_SECONDS = 20 * 60

_snapshot_queue: asyncio.Queue[dict] | None = None
_snapshot_workers: list[asyncio.Task] = []
_latest_requested_minute: dict[str, datetime] = {}
_runtime_initialized = False


async def initialize_chain_snapshot_runtime(worker_count: int = 1) -> None:
    global _snapshot_queue, _snapshot_workers, _runtime_initialized
    if _runtime_initialized:
        return

    _snapshot_queue = asyncio.Queue(maxsize=10000)
    _snapshot_workers = [
        asyncio.create_task(_snapshot_worker(worker_id)) for worker_id in range(worker_count)
    ]
    _runtime_initialized = True
    logger.info("Chain snapshot runtime initialized with %s worker(s)", worker_count)


async def enqueue_chain_snapshot_refresh(trade: dict) -> None:
    if _snapshot_queue is None:
        return

    symbol = trade.get("symbol")
    if not symbol:
        return

    try:
        parse_occ_symbol(symbol)
    except ValueError:
        return

    snapshot_minute = _snapshot_floor_minute(trade.get("date"))
    if snapshot_minute is None:
        return

    if _latest_requested_minute.get(symbol) == snapshot_minute:
        return

    _latest_requested_minute[symbol] = snapshot_minute

    try:
        _snapshot_queue.put_nowait({
            "symbol": symbol,
            "snapshot_minute": snapshot_minute,
        })
    except asyncio.QueueFull:
        logger.warning("Chain snapshot queue full; dropping refresh for %s", symbol)


async def _snapshot_worker(worker_id: int) -> None:
    assert _snapshot_queue is not None
    while True:
        payload = await _snapshot_queue.get()
        try:
            await _capture_snapshot(payload)
        except Exception:
            logger.exception(
                "Chain snapshot worker %s failed for symbol %s",
                worker_id,
                payload.get("symbol"),
            )
        finally:
            _snapshot_queue.task_done()


async def _capture_snapshot(payload: dict) -> None:
    symbol = payload["symbol"]
    symbol_meta = parse_occ_symbol(symbol)
    root = symbol_meta["root"]

    quote = await asyncio.to_thread(get_quote, symbol, True)
    if quote is None:
        return

    underlying_price = await _resolve_underlying_price(root, quote)
    bid = _as_float(quote.get("bid"))
    ask = _as_float(quote.get("ask"))
    last_price = _as_float(quote.get("last") or quote.get("close"))
    greeks = quote.get("greeks") or {}
    implied_volatility = _first_float(
        greeks.get("mid_iv"),
        greeks.get("smv_vol"),
        greeks.get("iv"),
        quote.get("greeks.mid_iv"),
        quote.get("iv"),
    )

    intrinsic_value, extrinsic_value = _derive_option_values(
        call_putt=symbol_meta["call_putt"],
        strike=float(symbol_meta["strike"]),
        underlying_price=underlying_price,
        option_price=last_price,
    )

    snapshot = {
        "symbol": symbol,
        "root": root,
        "expiration": datetime.combine(symbol_meta["expiration"], datetime.min.time()),
        "call_putt": symbol_meta["call_putt"],
        "strike": float(symbol_meta["strike"]),
        "snapshot_minute": payload["snapshot_minute"],
        "underlying_price": underlying_price,
        "bid": bid,
        "ask": ask,
        "last_price": last_price,
        "mark_price": _midpoint(bid, ask, last_price),
        "bid_size": _as_int(quote.get("bidsize") or quote.get("bid_size")),
        "ask_size": _as_int(quote.get("asksize") or quote.get("ask_size")),
        "volume": _as_int(quote.get("volume")),
        "open_interest": _as_int(quote.get("open_interest") or quote.get("openInterest")),
        "implied_volatility": implied_volatility,
        "delta": _first_float(greeks.get("delta"), quote.get("delta")),
        "gamma": _first_float(greeks.get("gamma"), quote.get("gamma")),
        "theta": _first_float(greeks.get("theta"), quote.get("theta")),
        "vega": _first_float(greeks.get("vega"), quote.get("vega")),
        "rho": _first_float(greeks.get("rho"), quote.get("rho")),
        "intrinsic_value": intrinsic_value,
        "extrinsic_value": extrinsic_value,
        "updated_at": datetime.now(UTC),
    }
    await insert_option_chain_snapshot(snapshot)


async def _resolve_underlying_price(root: str, option_quote: dict) -> float | None:
    direct_underlying = _first_float(
        option_quote.get("underlying"),
        option_quote.get("underlying_price"),
        option_quote.get("underlyingPrice"),
    )
    if direct_underlying is not None:
        return direct_underlying

    db = get_db()
    cached_quote = await db.get_json(f"quote:{root}")
    if cached_quote and cached_quote.get("last") is not None:
        return _as_float(cached_quote.get("last"))

    root_quote = await asyncio.to_thread(get_quote, root, False)
    if root_quote is None:
        return None

    normalized = {
        "symbol": root,
        "bid": float(root_quote.get("bid") or 0.0),
        "ask": float(root_quote.get("ask") or 0.0),
        "last": float(root_quote.get("last") or root_quote.get("close") or 0.0),
        "updated_at": datetime.now(UTC).isoformat(),
    }
    await db.set_json(f"quote:{root}", normalized, ex=UNDERLYING_QUOTE_TTL_SECONDS)
    return normalized["last"] or None


def _snapshot_floor_minute(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.replace(second=0, microsecond=0)


def _derive_option_values(
    call_putt: str,
    strike: float,
    underlying_price: float | None,
    option_price: float | None,
) -> tuple[float | None, float | None]:
    if underlying_price is None or option_price is None:
        return None, None

    if call_putt == "call":
        intrinsic_value = max(underlying_price - strike, 0.0)
    else:
        intrinsic_value = max(strike - underlying_price, 0.0)
    extrinsic_value = max(option_price - intrinsic_value, 0.0)
    return intrinsic_value, extrinsic_value


def _midpoint(bid: float | None, ask: float | None, last_price: float | None) -> float | None:
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    return last_price


def _first_float(*values: object) -> float | None:
    for value in values:
        parsed = _as_float(value)
        if parsed is not None:
            return parsed
    return None


def _as_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None