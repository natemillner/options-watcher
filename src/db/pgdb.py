import logging
import os
from collections.abc import Mapping
from datetime import UTC, datetime

import asyncpg
from asyncpg import ForeignKeyViolationError

from src.tradier_stuff import parse_occ_symbol

from .sql import INSERT_ANOMALY_SCORE_SQL, INSERT_OPTION_CHAIN_SNAPSHOT_SQL, INSERT_SYMBOL_SQL, INSERT_TRADE_EVENT_SQL, INSERT_TRADE_SQL

logger = logging.getLogger(__name__)
DB_DSN = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_NAME')}"

pool = None


def _to_db_timestamp(value: object) -> object:
    """
    Convert timezone-aware datetimes to naive UTC for TIMESTAMP columns.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


async def init_db_pool() -> None:
    """
    Initialize the database connection pool.
    """
    global pool
    logger.info("Initializing database connection pool")
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=10, max_size=20)


async def insert_trade(trade: dict) -> None:
    """
    Insert a trade into the database.
    """
    global pool
    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                trade_id = await conn.fetchval(
                    INSERT_TRADE_SQL,
                    trade["symbol"],
                    _to_db_timestamp(trade["date"]),
                    trade["bid"],
                    trade["ask"],
                    trade["last"],
                    trade["size"],
                    trade["trade_type"],
                )
                return int(trade_id)
        except ForeignKeyViolationError:
            logger.info(
                f"Foreign key violation for symbol {trade['symbol']}. Inserting symbol."
            )
            symbol_parsed = parse_occ_symbol(trade["symbol"])
            async with conn.transaction():
                await conn.execute(
                    INSERT_SYMBOL_SQL,
                    symbol_parsed.get("symbol"),
                    symbol_parsed.get("root"),
                    symbol_parsed.get("expiration"),
                    symbol_parsed.get("call_putt"),
                    symbol_parsed.get("strike"),
                )
            async with conn.transaction():
                trade_id = await conn.fetchval(
                    INSERT_TRADE_SQL,
                    trade["symbol"],
                    _to_db_timestamp(trade["date"]),
                    trade["bid"],
                    trade["ask"],
                    trade["last"],
                    trade["size"],
                    trade["trade_type"],
                )
                return int(trade_id)


async def insert_anomaly_score(score: Mapping[str, object]) -> None:
    """
    Persist the model output for a scored trade.
    """
    global pool
    async with pool.acquire() as conn:
        await conn.execute(
            INSERT_ANOMALY_SCORE_SQL,
            score["trade_id"],
            score["symbol"],
            score["root"],
            score["anomaly_label"],
            score["anomaly_score"],
            score["model_version"],
            _to_db_timestamp(score.get("inferred_at", datetime.utcnow())),
        )


async def insert_trade_event(event: Mapping[str, object]) -> None:
    """
    Persist a finalized live anomaly event.
    """
    global pool
    async with pool.acquire() as conn:
        await conn.execute(
            INSERT_TRADE_EVENT_SQL,
            event["symbol"],
            event["root"],
            _to_db_timestamp(event["expiration"]),
            event["call_putt"],
            event["strike"],
            _to_db_timestamp(event["event_start"]),
            _to_db_timestamp(event["event_end"]),
            event["duration_seconds"],
            event["n_trades"],
            event["total_size"],
            event["mean_anomaly_score"],
            event["worst_anomaly_score"],
            event["dominant_trade_condition"],
            event["model_version"],
            _to_db_timestamp(event.get("finalized_at", datetime.utcnow())),
        )


async def insert_option_chain_snapshot(snapshot: Mapping[str, object]) -> None:
    """
    Persist or update a per-minute option chain snapshot.
    """
    global pool
    async with pool.acquire() as conn:
        await conn.execute(
            INSERT_OPTION_CHAIN_SNAPSHOT_SQL,
            snapshot["symbol"],
            snapshot["root"],
            _to_db_timestamp(snapshot["expiration"]),
            snapshot["call_putt"],
            snapshot["strike"],
            _to_db_timestamp(snapshot["snapshot_minute"]),
            snapshot.get("underlying_price"),
            snapshot.get("bid"),
            snapshot.get("ask"),
            snapshot.get("last_price"),
            snapshot.get("mark_price"),
            snapshot.get("bid_size"),
            snapshot.get("ask_size"),
            snapshot.get("volume"),
            snapshot.get("open_interest"),
            snapshot.get("implied_volatility"),
            snapshot.get("delta"),
            snapshot.get("gamma"),
            snapshot.get("theta"),
            snapshot.get("vega"),
            snapshot.get("rho"),
            snapshot.get("intrinsic_value"),
            snapshot.get("extrinsic_value"),
            _to_db_timestamp(snapshot.get("updated_at", datetime.utcnow())),
        )
