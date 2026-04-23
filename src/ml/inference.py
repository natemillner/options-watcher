import asyncio
import logging
import math
from collections import defaultdict, deque
from datetime import UTC, datetime, timedelta
from pathlib import Path

import joblib
import numpy as np
from scipy.stats import norm

from src.db import get_db, insert_anomaly_score, insert_trade_event
from src.tradier_stuff import get_quote, parse_occ_symbol

logger = logging.getLogger(__name__)

FEATURE_COLS = [
    "spread",
    "midpoint",
    "trade_price",
    "trade_size",
    "dte_days",
    "dte_years",
    "is_call",
    "tc_buy",
    "tc_sell",
    "tc_strong_buy",
    "tc_strong_sell",
    "moneyness",
    "iv",
    "delta",
    "gamma",
    "theta",
    "aggression_ratio",
]
SUPPORTED_ROOTS = {"SPY", "QQQ", "NVDA"}
MODELS_DIR = Path("models")
ROLLING_WINDOW = timedelta(minutes=15)
EVENT_GAP = timedelta(minutes=3)
UNDERLYING_QUOTE_TTL_SECONDS = 20 * 60
TRADE_QUEUE_MAXSIZE = 5000

_trade_queue: asyncio.Queue[dict] | None = None
_worker_tasks: list[asyncio.Task] = []
_event_sweeper_task: asyncio.Task | None = None
_models: dict[str, dict[str, object]] = {}
_aggression_state: dict[str, deque[tuple[datetime, float, float]]] = defaultdict(deque)
_pending_events: dict[tuple[str, str], dict] = {}
_runtime_initialized = False
_dropped_trade_inference = 0


def _latest_model_path(root: str) -> Path | None:
    candidates = sorted(MODELS_DIR.glob(f"{root}_*.joblib"))
    return candidates[-1] if candidates else None


def _load_models() -> dict[str, dict[str, object]]:
    loaded: dict[str, dict[str, object]] = {}
    for root in SUPPORTED_ROOTS:
        model_path = _latest_model_path(root)
        if model_path is None:
            logger.warning("No trained model found for %s in %s", root, MODELS_DIR)
            continue
        loaded[root] = {
            "model": joblib.load(model_path),
            "version": model_path.stem,
        }
    return loaded


async def initialize_inference_runtime(worker_count: int = 2) -> None:
    global _trade_queue, _worker_tasks, _event_sweeper_task, _models, _runtime_initialized
    if _runtime_initialized:
        return

    _models = _load_models()
    _trade_queue = asyncio.Queue(maxsize=TRADE_QUEUE_MAXSIZE)
    _worker_tasks = [
        asyncio.create_task(_inference_worker(worker_id))
        for worker_id in range(worker_count)
    ]
    _event_sweeper_task = asyncio.create_task(_event_sweeper())
    _runtime_initialized = True
    logger.info("Inference runtime initialized with %s models", len(_models))


async def enqueue_trade_for_inference(trade: dict) -> None:
    global _dropped_trade_inference
    if _trade_queue is None:
        logger.debug("Inference runtime not initialized; skipping trade enqueue")
        return

    try:
        symbol_meta = parse_occ_symbol(trade["symbol"])
    except ValueError:
        logger.debug("Skipping inference for unparsable symbol %s", trade.get("symbol"))
        return

    if symbol_meta["root"] not in _models:
        return

    try:
        _trade_queue.put_nowait(dict(trade))
    except asyncio.QueueFull:
        _dropped_trade_inference += 1
        if _dropped_trade_inference == 1 or _dropped_trade_inference % 100 == 0:
            logger.warning(
                "Inference queue full; dropped %s trades so far",
                _dropped_trade_inference,
            )


async def _inference_worker(worker_id: int) -> None:
    assert _trade_queue is not None
    while True:
        trade = await _trade_queue.get()
        try:
            await _score_trade(trade)
        except Exception:
            logger.exception("Inference worker %s failed for trade %s", worker_id, trade.get("trade_id"))
        finally:
            _trade_queue.task_done()


async def _event_sweeper() -> None:
    while True:
        await asyncio.sleep(30)
        cutoff = datetime.now(UTC) - EVENT_GAP
        expired_keys = [
            key for key, event in _pending_events.items() if event["event_end"] <= cutoff
        ]
        for key in expired_keys:
            event = _pending_events.pop(key, None)
            if event is not None:
                await insert_trade_event(_finalize_event(event))


async def _score_trade(trade: dict) -> None:
    symbol_meta = parse_occ_symbol(trade["symbol"])
    root = symbol_meta["root"]
    model_info = _models.get(root)
    if model_info is None:
        return

    trade_time = _normalize_trade_time(trade["date"])
    underlying_price = await _resolve_underlying_price(root)
    if underlying_price is None or underlying_price <= 0:
        logger.debug("Missing underlying quote for %s; skipping trade %s", root, trade["trade_id"])
        return

    features = _build_feature_vector(trade, symbol_meta, underlying_price, trade_time)
    if features is None:
        return

    model = model_info["model"]
    feature_array = np.array([[features[column] for column in FEATURE_COLS]], dtype=np.float32)
    anomaly_label = int(model.predict(feature_array)[0])
    anomaly_score = float(model.decision_function(feature_array)[0])

    await insert_anomaly_score(
        {
            "trade_id": int(trade["trade_id"]),
            "symbol": trade["symbol"],
            "root": root,
            "anomaly_label": anomaly_label == -1,
            "anomaly_score": anomaly_score,
            "model_version": str(model_info["version"]),
            "inferred_at": datetime.now(UTC),
        }
    )

    if anomaly_label == -1:
        await _update_live_event(trade, symbol_meta, anomaly_score, model_info["version"], trade_time)


def _normalize_trade_time(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


async def _resolve_underlying_price(root: str) -> float | None:
    db = get_db()
    cached_quote = await db.get_json(f"quote:{root}")
    if cached_quote and cached_quote.get("last"):
        return float(cached_quote["last"])

    quote = await asyncio.to_thread(get_quote, root, False)
    if quote is None:
        return None

    normalized = {
        "symbol": root,
        "bid": float(quote.get("bid") or 0.0),
        "ask": float(quote.get("ask") or 0.0),
        "last": float(quote.get("last") or quote.get("close") or 0.0),
        "updated_at": datetime.now(UTC).isoformat(),
    }
    await db.set_json(f"quote:{root}", normalized, ex=UNDERLYING_QUOTE_TTL_SECONDS)
    return normalized["last"] or None


def _build_feature_vector(
    trade: dict,
    symbol_meta: dict,
    underlying_price: float,
    trade_time: datetime,
) -> dict[str, float] | None:
    bid = float(trade["bid"])
    ask = float(trade["ask"])
    trade_price = float(trade["last"])
    trade_size = float(trade["size"])
    midpoint = (ask + bid) / 2.0
    spread = ask - bid

    expiration_value = symbol_meta["expiration"]
    expiration_dt = datetime.combine(expiration_value, datetime.max.time(), tzinfo=UTC)
    dte_days = max((expiration_dt - trade_time).total_seconds() / 86400.0, 1e-6)
    dte_years = max(dte_days / 365.25, 1e-6)
    is_call = 1.0 if symbol_meta["call_putt"] == "call" else 0.0
    strike = float(symbol_meta["strike"])
    moneyness = strike / underlying_price if underlying_price > 0 else math.nan

    if midpoint <= 0 or strike <= 0 or not math.isfinite(moneyness):
        return None

    iv = _bs_iv(midpoint, underlying_price, strike, dte_years, 0.05, bool(is_call))
    delta = _bs_delta(underlying_price, strike, dte_years, 0.05, iv, bool(is_call))
    gamma = _bs_gamma(underlying_price, strike, dte_years, 0.05, iv)
    theta = _bs_theta(underlying_price, strike, dte_years, 0.05, iv, bool(is_call))
    aggression_ratio = _update_aggression_ratio(trade["symbol"], trade_time, trade["trade_type"], trade_size)

    return {
        "spread": spread,
        "midpoint": midpoint,
        "trade_price": trade_price,
        "trade_size": trade_size,
        "dte_days": dte_days,
        "dte_years": dte_years,
        "is_call": is_call,
        "tc_buy": 1.0 if trade["trade_type"] == "buy" else 0.0,
        "tc_sell": 1.0 if trade["trade_type"] == "sell" else 0.0,
        "tc_strong_buy": 1.0 if trade["trade_type"] == "strong_buy" else 0.0,
        "tc_strong_sell": 1.0 if trade["trade_type"] == "strong_sell" else 0.0,
        "moneyness": moneyness,
        "iv": iv,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "aggression_ratio": aggression_ratio,
    }


def _update_aggression_ratio(
    symbol: str,
    trade_time: datetime,
    trade_type: str,
    trade_size: float,
) -> float:
    series = _aggression_state[symbol]
    strong_buy_volume = trade_size if trade_type == "strong_buy" else 0.0
    series.append((trade_time, strong_buy_volume, trade_size))

    cutoff = trade_time - ROLLING_WINDOW
    while series and series[0][0] < cutoff:
        series.popleft()

    total_strong_buy = sum(item[1] for item in series)
    total_volume = sum(item[2] for item in series)
    return total_strong_buy / max(total_volume, 1.0)


async def _update_live_event(
    trade: dict,
    symbol_meta: dict,
    anomaly_score: float,
    model_version: str,
    trade_time: datetime,
) -> None:
    key = (trade["symbol"], model_version)
    pending = _pending_events.get(key)
    if pending is None or trade_time - pending["event_end"] > EVENT_GAP:
        if pending is not None:
            await insert_trade_event(_finalize_event(pending))
        _pending_events[key] = {
            "symbol": trade["symbol"],
            "root": symbol_meta["root"],
            "expiration": datetime.combine(symbol_meta["expiration"], datetime.min.time()),
            "call_putt": symbol_meta["call_putt"],
            "strike": float(symbol_meta["strike"]),
            "event_start": trade_time,
            "event_end": trade_time,
            "scores": [anomaly_score],
            "trade_conditions": [trade["trade_type"]],
            "n_trades": 1,
            "total_size": int(trade["size"]),
            "model_version": str(model_version),
        }
        return

    pending["event_end"] = trade_time
    pending["scores"].append(anomaly_score)
    pending["trade_conditions"].append(trade["trade_type"])
    pending["n_trades"] += 1
    pending["total_size"] += int(trade["size"])


def _finalize_event(pending: dict) -> dict:
    dominant_trade_condition = max(
        set(pending["trade_conditions"]),
        key=pending["trade_conditions"].count,
    )
    return {
        "symbol": pending["symbol"],
        "root": pending["root"],
        "expiration": pending["expiration"],
        "call_putt": pending["call_putt"],
        "strike": pending["strike"],
        "event_start": pending["event_start"],
        "event_end": pending["event_end"],
        "duration_seconds": int((pending["event_end"] - pending["event_start"]).total_seconds()),
        "n_trades": pending["n_trades"],
        "total_size": pending["total_size"],
        "mean_anomaly_score": float(sum(pending["scores"]) / len(pending["scores"])),
        "worst_anomaly_score": float(min(pending["scores"])),
        "dominant_trade_condition": dominant_trade_condition,
        "model_version": pending["model_version"],
        "finalized_at": datetime.now(UTC),
    }


def _d1d2(S: float, K: float, T: float, r: float, sigma: float) -> tuple[float, float]:
    T = max(T, 1e-10)
    sigma = max(sigma, 1e-10)
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return d1, d2


def _bs_price(S: float, K: float, T: float, r: float, sigma: float, is_call: bool) -> float:
    d1, d2 = _d1d2(S, K, T, r, sigma)
    if is_call:
        return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def _bs_iv(
    market_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    is_call: bool,
    tol: float = 1e-6,
    max_iter: int = 100,
) -> float:
    sigma = 0.3
    for _ in range(max_iter):
        price = _bs_price(S, K, T, r, sigma, is_call)
        d1, _ = _d1d2(S, K, T, r, sigma)
        vega = max(S * norm.pdf(d1) * math.sqrt(max(T, 1e-10)), 1e-10)
        diff = price - market_price
        sigma = min(max(sigma - diff / vega, 1e-6), 10.0)
        if abs(diff) < tol:
            break
    return sigma


def _bs_delta(S: float, K: float, T: float, r: float, sigma: float, is_call: bool) -> float:
    d1, _ = _d1d2(S, K, T, r, sigma)
    return norm.cdf(d1) if is_call else norm.cdf(d1) - 1.0


def _bs_gamma(S: float, K: float, T: float, r: float, sigma: float) -> float:
    d1, _ = _d1d2(S, K, T, r, sigma)
    return norm.pdf(d1) / (S * sigma * math.sqrt(max(T, 1e-10)))


def _bs_theta(S: float, K: float, T: float, r: float, sigma: float, is_call: bool) -> float:
    d1, d2 = _d1d2(S, K, T, r, sigma)
    term1 = -(S * norm.pdf(d1) * sigma) / (2 * math.sqrt(max(T, 1e-10)))
    if is_call:
        theta = term1 - r * K * math.exp(-r * T) * norm.cdf(d2)
    else:
        theta = term1 + r * K * math.exp(-r * T) * norm.cdf(-d2)
    return theta / 365.0