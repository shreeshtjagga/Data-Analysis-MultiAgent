"""
analysis_history.py
────────────────────
Async CRUD for analysis history.

Changes vs the original sqlite3 version
────────────────────────────────────────
• All DB access uses SQLAlchemy AsyncSession (PostgreSQL).
• get_analysis_by_hash()  checks Redis first; only hits Postgres on a miss.
• save_analysis()         writes to Postgres *and* warms the Redis cache.
• delete_analysis()       removes both the DB row and the Redis entry.
• Cache policy (max 5 per user, 3-day TTL) is enforced inside save_analysis().
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from .core import cache as redis_cache
from .db import AnalysisHistory, AnalysisMetadata

logger = logging.getLogger(__name__)

MAX_CACHE_FILES_PER_USER = 5
CACHE_TTL_DAYS = 3


# ── Serialisation helpers ─────────────────────────────────────────────────────

def _json_default(obj):
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if isinstance(obj, set):
        return list(obj)
    return str(obj)


def _to_json(value) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, default=_json_default)


def _serialize_charts(charts: dict) -> dict:
    if not charts:
        return {}
    out = {}
    for key, fig in charts.items():
        try:
            if hasattr(fig, "to_plotly_json"):
                out[key] = fig.to_plotly_json()
            elif isinstance(fig, dict):
                out[key] = fig
        except Exception as exc:
            logger.warning("Failed to serialise chart '%s': %s", key, exc)
    return out


def _deserialize_charts(charts: dict) -> dict:
    if not charts:
        return {}
    out = {}
    for key, fig_data in charts.items():
        try:
            out[key] = go.Figure(fig_data)
        except Exception as exc:
            logger.warning("Failed to restore chart '%s': %s", key, exc)
    return out


def compute_file_hash(file_bytes: bytes) -> str:
    return hashlib.sha256(file_bytes).hexdigest()


# ── Cache-policy enforcement ──────────────────────────────────────────────────

async def _enforce_cache_policy(user_id: int, db: AsyncSession) -> None:
    """Keep only the 5 most-recent analyses per user, delete the rest."""
    result = await db.execute(
        select(AnalysisHistory.id)
        .where(AnalysisHistory.user_id == user_id)
        .order_by(AnalysisHistory.analysis_date.desc())
    )
    all_ids = [row[0] for row in result.fetchall()]
    overflow_ids = all_ids[MAX_CACHE_FILES_PER_USER:]

    if overflow_ids:
        await db.execute(
            delete(AnalysisMetadata).where(AnalysisMetadata.analysis_id.in_(overflow_ids))
        )
        await db.execute(
            delete(AnalysisHistory).where(AnalysisHistory.id.in_(overflow_ids))
        )
        logger.info("Evicted %d overflow analyses for user %d", len(overflow_ids), user_id)


# ── Save ─────────────────────────────────────────────────────────────────────

async def save_analysis(
    db: AsyncSession,
    user_id: int,
    file_name: str,
    file_hash: str,
    file_size: int,
    analysis_result: dict,
) -> dict:
    """
    Upsert an analysis result into PostgreSQL and warm the Redis cache.

    Returns
    -------
    dict with ``success``, ``message``, ``analysis_id``
    """
    try:
        stats = analysis_result.get("stats_summary") or {}
        row_count = stats.get("row_count")
        column_count = stats.get("column_count")
        completeness = (stats.get("data_quality") or {}).get("completeness")

        # Serialise DataFrames to plain dicts
        raw_data = analysis_result.get("raw_df")
        clean_data = analysis_result.get("clean_df")
        if isinstance(raw_data, pd.DataFrame):
            raw_data = raw_data.head(100).to_dict(orient="records")
        if isinstance(clean_data, pd.DataFrame):
            clean_data = clean_data.head(100).to_dict(orient="records")

        charts_json = _to_json(_serialize_charts(analysis_result.get("charts") or {}))

        # Upsert: check if file_hash already exists for this user
        result = await db.execute(
            select(AnalysisHistory).where(
                AnalysisHistory.user_id == user_id,
                AnalysisHistory.file_hash == file_hash,
            )
        )
        existing: Optional[AnalysisHistory] = result.scalar_one_or_none()

        if existing:
            existing.raw_data = _to_json(raw_data)
            existing.clean_data = _to_json(clean_data)
            existing.stats_summary = _to_json(stats)
            existing.charts = charts_json
            existing.insights = _to_json(analysis_result.get("insights"))
            existing.errors = _to_json(analysis_result.get("errors", []))
            existing.completed_agents = _to_json(analysis_result.get("completed_agents", []))
            existing.analysis_date = datetime.now(tz=timezone.utc)
            analysis_id = existing.id
            message = "Analysis updated (same file detected)"
        else:
            row = AnalysisHistory(
                user_id=user_id,
                file_name=file_name,
                file_hash=file_hash,
                raw_data=_to_json(raw_data),
                clean_data=_to_json(clean_data),
                stats_summary=_to_json(stats),
                charts=charts_json,
                insights=_to_json(analysis_result.get("insights")),
                errors=_to_json(analysis_result.get("errors", [])),
                completed_agents=_to_json(analysis_result.get("completed_agents", [])),
            )
            db.add(row)
            await db.flush()
            await db.refresh(row)
            analysis_id = row.id
            message = "Analysis saved successfully"

        # Upsert metadata row
        meta_result = await db.execute(
            select(AnalysisMetadata).where(AnalysisMetadata.analysis_id == analysis_id)
        )
        meta: Optional[AnalysisMetadata] = meta_result.scalar_one_or_none()

        if meta:
            meta.file_size = file_size
            meta.row_count = row_count
            meta.column_count = column_count
            meta.completeness = completeness
            meta.analyzed_at = datetime.now(tz=timezone.utc)
        else:
            db.add(
                AnalysisMetadata(
                    analysis_id=analysis_id,
                    user_id=user_id,
                    file_name=file_name,
                    file_size=file_size,
                    row_count=row_count,
                    column_count=column_count,
                    completeness=completeness,
                )
            )

        await db.flush()

        # Enforce per-user cache limit
        await _enforce_cache_policy(user_id, db)

        # Warm Redis cache
        cache_key = redis_cache.analysis_key(user_id, file_hash)
        cacheable = {
            "id": analysis_id,
            "file_name": file_name,
            "file_hash": file_hash,
            "raw_df": raw_data,
            "clean_df": clean_data,
            "stats_summary": stats,
            "charts": _serialize_charts(analysis_result.get("charts") or {}),
            "insights": analysis_result.get("insights", {}),
            "errors": analysis_result.get("errors", []),
            "completed_agents": analysis_result.get("completed_agents", []),
            "from_cache": True,
        }
        await redis_cache.set(cache_key, cacheable, ttl=redis_cache.CACHE_TTL_ANALYSIS)

        logger.info("Saved analysis id=%d for user %d (%s)", analysis_id, user_id, file_name)
        return {"success": True, "message": message, "analysis_id": analysis_id}

    except Exception as exc:
        logger.exception("save_analysis error: %s", exc)
        return {"success": False, "message": f"Failed to save analysis: {exc}"}


# ── Fetch by hash ─────────────────────────────────────────────────────────────

async def get_analysis_by_hash(
    db: AsyncSession, user_id: int, file_hash: str
) -> Optional[dict]:
    """
    Return a cached analysis, checking Redis first then PostgreSQL.
    Charts are NOT deserialised here (they live as plain dicts for JSON transport).
    """
    # 1. Redis fast path
    cache_key = redis_cache.analysis_key(user_id, file_hash)
    cached = await redis_cache.get(cache_key)
    if cached is not None:
        logger.info("Cache HIT for user %d / hash %s", user_id, file_hash[:8])
        return cached

    # 2. Database fallback
    result = await db.execute(
        select(AnalysisHistory).where(
            AnalysisHistory.user_id == user_id,
            AnalysisHistory.file_hash == file_hash,
        )
    )
    row: Optional[AnalysisHistory] = result.scalar_one_or_none()
    if row is None:
        return None

    analysis = {
        "id": row.id,
        "file_name": row.file_name,
        "file_hash": file_hash,
        "raw_df": json.loads(row.raw_data) if row.raw_data else None,
        "clean_df": json.loads(row.clean_data) if row.clean_data else None,
        "stats_summary": json.loads(row.stats_summary) if row.stats_summary else {},
        "charts": json.loads(row.charts) if row.charts else {},
        "insights": json.loads(row.insights) if row.insights else {},
        "errors": json.loads(row.errors) if row.errors else [],
        "completed_agents": json.loads(row.completed_agents) if row.completed_agents else [],
        "analysis_date": row.analysis_date.isoformat() if row.analysis_date else None,
        "from_cache": True,
    }

    # Back-fill Redis from DB
    await redis_cache.set(cache_key, analysis, ttl=redis_cache.CACHE_TTL_ANALYSIS)
    logger.info("Cache MISS — loaded from DB for user %d / hash %s", user_id, file_hash[:8])
    return analysis


# ── History listing ───────────────────────────────────────────────────────────

async def get_user_analysis_history(
    db: AsyncSession, user_id: int, limit: int = 20
) -> list[dict]:
    """Return a list of lightweight metadata records for the user's analyses."""
    result = await db.execute(
        select(AnalysisMetadata, AnalysisHistory.id, AnalysisHistory.file_hash)
        .join(AnalysisHistory, AnalysisMetadata.analysis_id == AnalysisHistory.id)
        .where(AnalysisMetadata.user_id == user_id)
        .order_by(AnalysisMetadata.analyzed_at.desc())
        .limit(limit)
    )
    rows = result.fetchall()

    return [
        {
            "analysis_id": meta.analysis_id,
            "file_name": meta.file_name,
            "file_hash": file_hash,
            "row_count": meta.row_count,
            "column_count": meta.column_count,
            "completeness": meta.completeness,
            "analyzed_at": meta.analyzed_at.isoformat() if meta.analyzed_at else None,
        }
        for meta, _id, file_hash in rows
    ]


# ── Delete ────────────────────────────────────────────────────────────────────

async def delete_analysis(
    db: AsyncSession, user_id: int, analysis_id: int
) -> dict:
    """Delete an analysis row (+ its metadata via cascade) and purge Redis."""
    result = await db.execute(
        select(AnalysisHistory).where(
            AnalysisHistory.id == analysis_id,
            AnalysisHistory.user_id == user_id,
        )
    )
    row: Optional[AnalysisHistory] = result.scalar_one_or_none()

    if row is None:
        return {"success": False, "message": "Analysis not found or access denied"}

    file_hash = row.file_hash
    await db.delete(row)
    await db.flush()

    # Purge Redis entry
    cache_key = redis_cache.analysis_key(user_id, file_hash)
    await redis_cache.delete(cache_key)

    logger.info("Deleted analysis id=%d for user %d", analysis_id, user_id)
    return {"success": True, "message": "Analysis deleted successfully"}