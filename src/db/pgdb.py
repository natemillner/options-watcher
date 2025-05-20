import logging
import os

import asyncpg
from asyncpg import ForeignKeyViolationError

from src.tradier_stuff import parse_occ_symbol

from .sql import INSERT_SYMBOL_SQL, INSERT_TRADE_SQL

logger = logging.getLogger(__name__)
DB_DSN = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_NAME')}"

pool = None


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
        async with conn.transaction():
            try:
                await conn.execute(
                    INSERT_TRADE_SQL,
                    trade["symbol"],
                    trade["date"],
                    trade["bid"],
                    trade["ask"],
                    trade["last"],
                    trade["size"],
                    trade["trade_type"],
                )
            except ForeignKeyViolationError:
                logger.info(
                    f"Foreign key violation for symbol {trade['symbol']}. Inserting symbol."
                )
                symbol_parsed = parse_occ_symbol(trade["symbol"])
                await conn.execute(
                    INSERT_SYMBOL_SQL,
                    symbol_parsed.get("symbol"),
                    symbol_parsed.get("root"),
                    symbol_parsed.get("expiration"),
                    symbol_parsed.get("call_putt"),
                    symbol_parsed.get("strike"),
                )
                await conn.commit()
                await conn.execute(
                    INSERT_TRADE_SQL,
                    trade["symbol"],
                    trade["date"],
                    trade["bid"],
                    trade["ask"],
                    trade["last"],
                    trade["size"],
                    trade["trade_type"],
                )
