

import json
import logging
import os
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse

import redis.asyncio as aioredis

from .utils import json_default, sanitize_for_json

logger = logging.getLogger(__name__)


CACHE_TTL_ANALYSIS: int = int(os.getenv("CACHE_TTL_ANALYSIS", str(3 * 24 * 3600)))  # 3 days
CACHE_TTL_SESSION: int = int(os.getenv("CACHE_TTL_SESSION", str(24 * 3600)))         # 1 day



_redis_client: Optional[aioredis.Redis] = None


def _running_in_container() -> bool:
    return (
        os.getenv("RUNNING_IN_DOCKER", "false").lower() == "true"
        or os.path.exists("/.dockerenv")
    )


def _rewrite_local_dev_redis_host(redis_url: str) -> str:
    app_env = os.getenv("APP_ENV", "production")
    if app_env != "development" or _running_in_container():
        return redis_url

    parsed = urlparse(redis_url)
    if parsed.hostname != "redis":
        return redis_url

    netloc = parsed.netloc
    if "@" in netloc:
        auth, host_port = netloc.rsplit("@", 1)
        if host_port.startswith("redis:"):
            netloc = f"{auth}@localhost:{host_port.split(':', 1)[1]}"
        elif host_port == "redis":
            netloc = f"{auth}@localhost"
    else:
        if netloc.startswith("redis:"):
            netloc = f"localhost:{netloc.split(':', 1)[1]}"
        elif netloc == "redis":
            netloc = "localhost"

    rewritten = urlunparse(parsed._replace(netloc=netloc))
    logger.warning("REDIS_URL host 'redis' detected in local dev; using localhost instead")
    return rewritten


def _get_client() -> aioredis.Redis:
    global _redis_client
    if _redis_client is None:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        redis_url = _rewrite_local_dev_redis_host(redis_url)
        _redis_client = aioredis.from_url(
            redis_url,
            encoding="utf-8",
            decode_responses=True,
            health_check_interval=30,
            socket_connect_timeout=3,      # fail fast if Upstash is unreachable
            socket_keepalive=True,         # keeps idle connection alive; avoids reconnect cost
            retry_on_timeout=True,
            max_connections=10,            # cap connections; Upstash free tier has limits
        )
        logger.info("Redis client initialised (%s)", redis_url.split("@")[-1])
    return _redis_client


async def close() -> None:
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
        logger.info("Redis connection closed")



def analysis_key(user_id: int, file_hash: str) -> str:
    return f"analysis:{user_id}:{file_hash}"


def session_key(user_id: int) -> str:
    return f"session:{user_id}"



async def get(key: str) -> Optional[Any]:
    try:
        client = _get_client()
        raw = await client.get(key)
        if raw is None:
            return None
        return json.loads(raw)
    except Exception as exc:
        logger.warning("Cache GET failed for key '%s': %s", key, exc)
        return None


async def set(key: str, value: Any, ttl: int = CACHE_TTL_ANALYSIS) -> bool:
    try:
        client = _get_client()
        cleaned = sanitize_for_json(value)
        serialised = json.dumps(cleaned, default=json_default, allow_nan=False)
        if len(serialised) > 4 * 1024 * 1024:
            logger.warning("Cache payload too large (%d bytes), skipping Redis SET", len(serialised))
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


async def exists(key: str) -> bool:
    try:
        client = _get_client()
        return bool(await client.exists(key))
    except Exception as exc:
        logger.warning("Cache EXISTS failed for key '%s': %s", key, exc)
        return False


async def flush_user(user_id: int) -> int:
    try:
        client = _get_client()
        pattern = f"analysis:{user_id}:*"
        keys = await client.keys(pattern)
        if not keys:
            return 0
        deleted = await client.delete(*keys)
        logger.info("Flushed %d cache keys for user %d", deleted, user_id)
        return deleted
    except Exception as exc:
        logger.warning("Cache flush failed for user %d: %s", user_id, exc)
        return 0


async def ping() -> bool:
    try:
        client = _get_client()
        return await client.ping()
    except Exception:
        return False


async def increment_with_ttl(key: str, ttl_seconds: int) -> int:
    """Atomically increment a key and set its expiry on first use."""
    try:
        client = _get_client()
        script = """
        local current = redis.call('INCR', KEYS[1])
        if current == 1 then
            redis.call('EXPIRE', KEYS[1], ARGV[1])
        end
        return current
        """
        count = await client.eval(script, 1, key, ttl_seconds)
        return int(count)
    except Exception as exc:
        logger.warning("Cache INCR+EXPIRE failed for key '%s': %s", key, exc)
        raise
