import logging
import os
import ssl
from datetime import datetime

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


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://datapulse:datapulse_secret@localhost:5432/datapulse",
)

if DATABASE_URL.startswith("postgresql://") or DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

DATABASE_URL = rewrite_local_dev_host(DATABASE_URL, service_name="db")


_db_ssl = os.getenv("DB_SSL", "false").lower() == "true"
_connect_args: dict = {}
if _db_ssl:
    _ssl_ctx = ssl.create_default_context()
    _ssl_ctx.check_hostname = False
    _ssl_ctx.verify_mode = ssl.CERT_NONE
    _connect_args["ssl"] = _ssl_ctx

engine = create_async_engine(
    DATABASE_URL,
    echo=os.getenv("APP_ENV", "production") == "development",
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    connect_args=_connect_args,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)



class Base(DeclarativeBase):
    pass



class User(Base):
    __tablename__ = "users"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=True)
    password_hash = Column(String(255), nullable=False)
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
    logger.info("Database tables initialised (PostgreSQL)")


async def drop_all() -> None:
    """Drop all tables — for use in tests only."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    logger.warning("All database tables dropped")
