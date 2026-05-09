import logging
import os
import threading
import orjson
from typing import Any, Optional
import redis.asyncio as aioredis
from .utils import rewrite_local_dev_host, sanitize_for_json
logger = logging.getLogger(__name__)
CACHE_TTL_ANALYSIS: int = int(os.getenv('CACHE_TTL_ANALYSIS', str(3 * 24 * 3600)))
_redis_client: Optional[aioredis.Redis] = None
_redis_lock = threading.Lock()

def _get_client() -> aioredis.Redis:
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    with _redis_lock:
        if _redis_client is None:
            redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
            redis_url = rewrite_local_dev_host(redis_url, service_name='redis')
            _redis_client = aioredis.from_url(redis_url, encoding='utf-8', decode_responses=True, health_check_interval=30, socket_connect_timeout=3, socket_keepalive=True, retry_on_timeout=True, max_connections=10)
            logger.info('Redis client initialised (%s)', redis_url.split('@')[-1])
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
        raw = await client.get(key)
        if raw is None:
            return None
        return orjson.loads(raw)
    except Exception as exc:
        logger.warning("Cache GET failed for key '%s': %s", key, exc)
        return None

async def set(key: str, value: Any, ttl: int=CACHE_TTL_ANALYSIS) -> bool:
    try:
        client = _get_client()
        cleaned = sanitize_for_json(value)
        serialised = orjson.dumps(cleaned, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY).decode('utf-8')
        if len(serialised) > 4 * 1024 * 1024:
            logger.warning('Cache payload too large (%d bytes), skipping Redis SET', len(serialised))
            return False
        await client.setex(key, ttl, serialised)
        logger.debug("Cache SET key='%s' ttl=%ds", key, ttl)
        return True
    except Exception as exc:
        logger.warning("Cache SET failed for key '%s': %s", key, exc)
        return False

async def delete(key: str) -> bool:
    try:
        client = _get_client()
        deleted = await client.delete(key)
        return bool(deleted)
    except Exception as exc:
        logger.warning("Cache DELETE failed for key '%s': %s", key, exc)
        return False

async def ping() -> bool:
    try:
        client = _get_client()
        return await client.ping()
    except Exception as exc:
        logger.warning('Cache ping failed: %s', exc)
        return False

async def increment_with_ttl(key: str, ttl_seconds: int) -> int:
    client = _get_client()
    try:
        script = "\n        local current = redis.call('INCR', KEYS[1])\n        if current == 1 then\n            redis.call('EXPIRE', KEYS[1], ARGV[1])\n        end\n        return current\n        "
        count = await client.eval(script, 1, key, ttl_seconds)
        return int(count)
    except Exception as exc:
        logger.warning("Cache INCR+EXPIRE eval failed for key '%s': %s", key, exc)
        count = await client.incr(key)
        if count == 1:
            await client.expire(key, ttl_seconds)
        return int(count)