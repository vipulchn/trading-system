import logging
from typing import Optional
from redis.asyncio import Redis
from app.config import settings
from app.constants import (
    REDIS_KEY_CAPITAL, REDIS_KEY_HALT, REDIS_KEY_TRADING_ACTIVE,
    REDIS_QUEUE_SIGNALS, REDIS_QUEUE_EXECUTION, REDIS_KEY_DEDUP_PREFIX,
    REDIS_KEY_LAST_TV_WEBHOOK, REDIS_CAPITAL_TTL_SECONDS, SIGNAL_DEDUP_TTL_SECONDS,
)

logger = logging.getLogger(__name__)
_redis: Optional[Redis] = None


async def init_redis() -> Redis:
    global _redis
    logger.info("Connecting to Redis…")
    _redis = Redis.from_url(
        settings.redis_url, encoding="utf-8", decode_responses=True,
        socket_connect_timeout=5, socket_timeout=5,
        retry_on_timeout=True, health_check_interval=30,
    )
    await _redis.ping()
    logger.info("Redis connected.")
    return _redis


async def close_redis() -> None:
    global _redis
    if _redis:
        await _redis.aclose()
        _redis = None


async def get_redis() -> Redis:
    if _redis is None:
        raise RuntimeError("Redis not initialised — call init_redis() first.")
    return _redis


class RedisKeys:
    CAPITAL        = REDIS_KEY_CAPITAL
    HALT           = REDIS_KEY_HALT
    TRADING_ACTIVE = REDIS_KEY_TRADING_ACTIVE
    QUEUE_SIGNALS  = REDIS_QUEUE_SIGNALS
    QUEUE_EXECUTION = REDIS_QUEUE_EXECUTION
    LAST_TV_WEBHOOK = REDIS_KEY_LAST_TV_WEBHOOK

    @staticmethod
    def dedup(symbol: str) -> str:
        return f"{REDIS_KEY_DEDUP_PREFIX}{symbol.upper()}"


async def get_capital() -> float:
    r = await get_redis()
    val = await r.get(RedisKeys.CAPITAL)
    return float(val) if val else 0.0


async def set_capital(amount: float) -> None:
    r = await get_redis()
    await r.set(RedisKeys.CAPITAL, str(amount), ex=REDIS_CAPITAL_TTL_SECONDS)


async def is_halted() -> bool:
    r = await get_redis()
    return await r.get(RedisKeys.HALT) == "true"


async def set_halt(halted: bool) -> None:
    r = await get_redis()
    await r.set(RedisKeys.HALT, "true" if halted else "false")


async def is_trading_active() -> bool:
    r = await get_redis()
    return await r.get(RedisKeys.TRADING_ACTIVE) == "true"


async def set_trading_active(active: bool) -> None:
    r = await get_redis()
    await r.set(RedisKeys.TRADING_ACTIVE, "true" if active else "false")


async def set_dedup(symbol: str) -> None:
    r = await get_redis()
    await r.set(RedisKeys.dedup(symbol), "1", ex=SIGNAL_DEDUP_TTL_SECONDS)


async def check_dedup(symbol: str) -> bool:
    r = await get_redis()
    return await r.exists(RedisKeys.dedup(symbol)) == 1


async def push_signal(payload: str) -> None:
    r = await get_redis()
    await r.rpush(RedisKeys.QUEUE_SIGNALS, payload)


async def push_execution(payload: str) -> None:
    r = await get_redis()
    await r.rpush(RedisKeys.QUEUE_EXECUTION, payload)
