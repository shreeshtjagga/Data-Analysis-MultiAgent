"""
DataPulse Database — Supabase PostgreSQL
==========================================
REMOVED (dead after migration):
  - _strip_unsupported_params_from_url() — Supabase connection string is clean
  - Neon-specific pool comments — replaced with Supabase pooler settings

CHANGED:
  - DATABASE_URL now points to Supabase PostgreSQL
  - User model: added supabase_id (UUID from auth.users), password_hash made nullable
    because Supabase manages auth — we no longer store hashed passwords locally
"""

import logging
import os
import ssl
from datetime import datetime
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Float,
    ForeignKey, Integer, String, Text, UniqueConstraint, func,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, relationship

from .core.utils import rewrite_local_dev_host

logger = logging.getLogger(__name__)


def _running_in_container() -> bool:
    return (
        os.getenv("RUNNING_IN_DOCKER", "false").lower() == "true"
        or os.path.exists("/.dockerenv")
    )


def _rewrite_local_dev_db_host(database_url: str) -> str:
    """Map Docker-compose DB hostname to localhost for non-container dev runs."""
    app_env = os.getenv("APP_ENV", "production")
    if app_env != "development" or _running_in_container():
        return database_url

    parsed = urlparse(database_url)
    if parsed.hostname != "db":
        return database_url

    netloc = parsed.netloc
    if "@" in netloc:
        auth, host_port = netloc.rsplit("@", 1)
        if host_port.startswith("db:"):
            netloc = f"{auth}@localhost:{host_port.split(':', 1)[1]}"
        elif host_port == "db":
            netloc = f"{auth}@localhost"
    else:
        if netloc.startswith("db:"):
            netloc = f"localhost:{netloc.split(':', 1)[1]}"
        elif netloc == "db":
            netloc = "localhost"

    rewritten = urlunparse(parsed._replace(netloc=netloc))
    logger.warning("DATABASE_URL host 'db' detected in local dev; using localhost instead")
    return rewritten


# ── Connection setup ───────────────────────────────────────────────────────────

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres",
)

# Normalise scheme — Supabase connection strings use postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

DATABASE_URL = _rewrite_local_dev_db_host(DATABASE_URL)

# Strip asyncpg-incompatible params (sslmode, channel_binding, gssencmode)
# that Supabase pooler URLs sometimes include
def _strip_url_params(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.query:
        return url
    params = parse_qs(parsed.query, keep_blank_values=True)
    for p in ("sslmode", "channel_binding", "gssencmode"):
        params.pop(p, None)
    new_query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=new_query))

DATABASE_URL = _strip_url_params(DATABASE_URL)

_db_ssl_mode = os.getenv("DB_SSL", "false").lower()
_connect_args: dict = {
    # Disables asyncpg prepared statement cache — required for Supabase pgbouncer
    "statement_cache_size": 0,
}
if _db_ssl_mode == "require":
    # Supabase direct connection — asyncpg accepts the plain string "require"
    _connect_args["ssl"] = "require"
elif _db_ssl_mode == "true":
    # Self-signed / local TLS — skip certificate verification
    _ssl_ctx = ssl.create_default_context()
    _ssl_ctx.check_hostname = False
    _ssl_ctx.verify_mode = ssl.CERT_NONE
    _connect_args["ssl"] = _ssl_ctx

engine = create_async_engine(
    DATABASE_URL,
    echo=os.getenv("APP_ENV", "production") == "development",
    # Supabase pgbouncer (transaction mode) — keep pool small
    pool_pre_ping=True,
    pool_size=3,
    max_overflow=5,
    pool_recycle=300,
    pool_timeout=30,
    connect_args=_connect_args,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


# ── Models ─────────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


class User(Base):
    """
    Local user profile — synced from Supabase auth.users on first login.
    
    CHANGED: Added supabase_id (UUID string) — links to Supabase auth.users.id.
    CHANGED: password_hash is now nullable — Supabase manages passwords, not us.
    """
    __tablename__ = "users"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    # Supabase auth.users UUID — set on first login, used for identity linking
    supabase_id = Column(String(64), unique=True, nullable=True, index=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=True)
    # nullable=True: Supabase manages password hashing, we do NOT store it
    password_hash = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    analyses = relationship(
        "AnalysisHistory",
        back_populates="user",
        cascade="all, delete-orphan",
        lazy="select",
    )


class AnalysisHistory(Base):
    __tablename__ = "analysis_history"
    __table_args__ = (
        UniqueConstraint("user_id", "file_hash", name="uq_user_file_hash"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    file_name = Column(String(512), nullable=False)
    file_hash = Column(String(64), nullable=False, index=True)

    raw_data = Column(Text, nullable=True)
    clean_data = Column(Text, nullable=True)
    stats_summary = Column(Text, nullable=True)
    charts = Column(Text, nullable=True)
    insights = Column(Text, nullable=True)
    errors = Column(Text, nullable=True)
    completed_agents = Column(Text, nullable=True)

    analysis_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    user = relationship("User", back_populates="analyses")
    metadata_row = relationship(
        "AnalysisMetadata",
        back_populates="analysis",
        cascade="all, delete-orphan",
        uselist=False,
        lazy="select",
    )


class AnalysisMetadata(Base):
    __tablename__ = "analysis_metadata"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    analysis_id = Column(
        BigInteger,
        ForeignKey("analysis_history.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    file_name = Column(String(512), nullable=False)
    file_size = Column(BigInteger, nullable=True)
    row_count = Column(Integer, nullable=True)
    column_count = Column(Integer, nullable=True)
    completeness = Column(Float, nullable=True)
    analyzed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    analysis = relationship("AnalysisHistory", back_populates="metadata_row")


# ── Session helpers ────────────────────────────────────────────────────────────

async def get_db() -> AsyncSession:
    session = AsyncSessionLocal()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def init_db() -> None:
    """Create all tables if they do not exist. Called once at application startup."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables initialised (Supabase PostgreSQL)")


async def drop_all() -> None:
    """Drop all tables — for use in tests only."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    logger.warning("All database tables dropped")
