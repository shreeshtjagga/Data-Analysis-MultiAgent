"""
core/cache.py
─────────────
Redis client singleton and helper functions for the DataPulse cache layer.

Key design choices
------------------
• Single shared connection pool created at import time (lazy-connects on first use).
• All public functions are async and swallow Redis errors gracefully — a cache
  miss should never crash an analysis pipeline.
• Keys are namespaced: ``analysis:<user_id>:<file_hash>``
• TTL constants come from env vars so they are easy to tune without code changes.
"""

import json
import logging
import math
import os
from datetime import datetime, date
from typing import Any, Optional

import numpy as np
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        val = float(obj)
        return val if math.isfinite(val) else None
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, set):
        return list(obj)
    return str(obj)


def _sanitize_for_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_for_json(v) for v in value]
    if isinstance(value, tuple):
        return [_sanitize_for_json(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (np.floating,)):
        val = float(value)
        return val if math.isfinite(val) else None
    if isinstance(value, np.ndarray):
        return _sanitize_for_json(value.tolist())
    return value

# ── TTL constants (seconds) ───────────────────────────────────────────────────
CACHE_TTL_ANALYSIS: int = int(os.getenv("CACHE_TTL_ANALYSIS", str(3 * 24 * 3600)))  # 3 days
CACHE_TTL_SESSION: int = int(os.getenv("CACHE_TTL_SESSION", str(24 * 3600)))         # 1 day

# ── Redis client (module-level singleton) ─────────────────────────────────────
_redis_client: Optional[aioredis.Redis] = None


def _get_client() -> aioredis.Redis:
    """
    Return (or lazily create) the shared Redis connection pool.
    Uses a connection pool with health-check on each command.
    """
    global _redis_client
    if _redis_client is None:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        _redis_client = aioredis.from_url(
            redis_url,
            encoding="utf-8",
            decode_responses=True,
            health_check_interval=30,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )
        logger.info("Redis client initialised (%s)", redis_url)
    return _redis_client


async def close() -> None:
    """Close the Redis connection pool. Call this on application shutdown."""
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
        logger.info("Redis connection closed")


# ── Key builders ──────────────────────────────────────────────────────────────

def analysis_key(user_id: int, file_hash: str) -> str:
    """Namespaced key for a cached analysis result."""
    return f"analysis:{user_id}:{file_hash}"


def session_key(user_id: int) -> str:
    """Namespaced key for lightweight session data."""
    return f"session:{user_id}"


# ── Core helpers ──────────────────────────────────────────────────────────────

async def get(key: str) -> Optional[Any]:
    """
    Retrieve and JSON-deserialise a cached value.

    Returns
    -------
    The deserialised Python object, or ``None`` on cache miss / error.
    """
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
    """
    JSON-serialise and store *value* under *key* with the given TTL.

    Returns
    -------
    True on success, False if the write failed (logs a warning).
    """
    try:
        client = _get_client()
        cleaned = _sanitize_for_json(value)
        serialised = json.dumps(cleaned, default=_json_default, allow_nan=False)
        await client.setex(key, ttl, serialised)
        logger.debug("Cache SET key='%s' ttl=%ds", key, ttl)
        return True
    except Exception as exc:
        logger.warning("Cache SET failed for key '%s': %s", key, exc)
        return False


async def delete(key: str) -> bool:
    """
    Remove a key from the cache.

    Returns
    -------
    True if the key existed and was deleted, False otherwise.
    """
    try:
        client = _get_client()
        deleted = await client.delete(key)
        return bool(deleted)
    except Exception as exc:
        logger.warning("Cache DELETE failed for key '%s': %s", key, exc)
        return False


async def exists(key: str) -> bool:
    """Return True if *key* currently exists in Redis."""
    try:
        client = _get_client()
        return bool(await client.exists(key))
    except Exception as exc:
        logger.warning("Cache EXISTS failed for key '%s': %s", key, exc)
        return False


async def flush_user(user_id: int) -> int:
    """
    Delete all cache entries for a given user.

    Returns
    -------
    Number of keys deleted.
    """
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
    """Health-check the Redis connection. Returns True if reachable."""
    try:
        client = _get_client()
        return await client.ping()
    except Exception:
        return False