import logging
import os

import redis.asyncio as redis

logger = logging.getLogger(__name__)

db = None


class InMemoryDB:
    def __init__(self, host: str, port: int, password: str, db: int):
        self.redis = redis.Redis(host=host, port=port, password=password, db=db)

    @classmethod
    async def create(
        cls,
        host=os.getenv("REDIS_HOST"),
        port=os.getenv("REDIS_PORT"),
        password=os.getenv("REDIS_PASSWORD"),
        db=0,
    ):
        self = cls(host, port, password, db)
        if not await self.redis.ping():
            raise ConnectionError("Could not connect to Redis server.")
        logger.info("Connected to Redis server.")
        await self.redis.flushdb()
        logger.info("Flushed Redis database.")
        return self

    async def set(self, key: str, value: str) -> None:
        await self.redis.set(key, value)

    async def get(self, key: str) -> str:
        v = await self.redis.get(key)
        return v.decode("utf-8") if v else None


async def create_db() -> None:
    global db
    if db is None:
        db = await InMemoryDB.create()


def get_db() -> InMemoryDB:
    global db
    if db is None:
        raise ValueError("Database not initialized. Call create_db() first.")
    return db
