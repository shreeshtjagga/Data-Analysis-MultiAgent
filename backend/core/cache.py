
import logging

import os

import threading

import time

import orjson

from typing import Any, Optional

import redis.asyncio as aioredis

from .utils import rewrite_local_dev_host, sanitize_for_json

logger = logging.getLogger(__name__)

CACHE_TTL_ANALYSIS: int = int(os.getenv('CACHE_TTL_ANALYSIS', str(3 * 24 * 3600)))

_redis_client: Optional[aioredis.Redis] = None

_redis_lock = threading.Lock()

_redis_disabled = False

_memory_cache: dict[str, tuple[Any, float]] = {}

_memory_counters: dict[str, tuple[int, float]] = {}

_mem_lock = threading.Lock()

def _get_client() -> Optional[aioredis.Redis]:

    global _redis_client, _redis_disabled

    if _redis_disabled:

        return None

    if _redis_client is not None:

        return _redis_client

    with _redis_lock:

        if _redis_client is None and not _redis_disabled:

            redis_url = os.getenv('REDIS_URL')

            if not redis_url:

                _redis_disabled = True

                return None

            try:

                redis_url = rewrite_local_dev_host(redis_url, service_name='redis')

                _redis_client = aioredis.from_url(redis_url, encoding='utf-8', decode_responses=True, health_check_interval=30, socket_connect_timeout=3, socket_keepalive=True, retry_on_timeout=True, max_connections=10)

                logger.info('Redis client initialised (%s)', redis_url.split('@')[-1])

            except Exception as e:

                logger.warning('Redis disabled; using in-memory cache fallback: %s', e)

                _redis_disabled = True

    return _redis_client

async def close() -> None:

    global _redis_client

    if _redis_client is not None:

        await _redis_client.aclose()

        _redis_client = None

        logger.info('Redis connection closed')

def analysis_key(user_id: int, file_hash: str) -> str:

    return f'analysis:{user_id}:{file_hash}'

async def get(key: str) -> Optional[Any]:

    try:

        client = _get_client()

        if client is not None:

            raw = await client.get(key)

            if raw is not None:

                return orjson.loads(raw)

    except Exception as exc:

        logger.warning("Redis GET failed for key '%s': %s (using memory fallback)", key, exc)

    with _mem_lock:

        if key in _memory_cache:

            val, exp = _memory_cache[key]

            if time.time() < exp:

                return val

            else:

                del _memory_cache[key]

    return None

async def set(key: str, value: Any, ttl: int=CACHE_TTL_ANALYSIS) -> bool:

    cleaned = sanitize_for_json(value)

    now = time.time()

    with _mem_lock:

        _memory_cache[key] = (cleaned, now + ttl)

    try:

        client = _get_client()

        if client is not None:

            serialised = orjson.dumps(cleaned, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY).decode('utf-8')

            if len(serialised) <= 4 * 1024 * 1024:

                await client.setex(key, ttl, serialised)

    except Exception as exc:

        logger.debug("Redis SET failed for key '%s': %s", key, exc)

    return True

async def delete(key: str) -> bool:

    with _mem_lock:

        _memory_cache.pop(key, None)

    try:

        client = _get_client()

        if client is not None:

            await client.delete(key)

    except Exception:

        pass

    return True

async def ping() -> bool:

    try:

        client = _get_client()

        if client is not None:

            return await client.ping()

    except Exception:

        pass

    return True

async def increment_with_ttl(key: str, ttl_seconds: int) -> int:

    try:

        client = _get_client()

        if client is not None:

            script = "\n            local current = redis.call('INCR', KEYS[1])\n            if current == 1 then\n                redis.call('EXPIRE', KEYS[1], ARGV[1])\n            end\n            return current\n            "

            count = await client.eval(script, 1, key, ttl_seconds)

            return int(count)

    except Exception:

        pass

    now = time.time()

    with _mem_lock:

        if key in _memory_counters:

            cnt, exp = _memory_counters[key]

            if now < exp:

                cnt += 1

                _memory_counters[key] = (cnt, exp)

                return cnt

        _memory_counters[key] = (1, now + ttl_seconds)

        return 1
