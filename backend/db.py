import logging
import os
import ssl
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from dotenv import load_dotenv
load_dotenv()
from sqlalchemy import BigInteger, Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint, func
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, relationship
logger = logging.getLogger(__name__)

def _running_in_container() -> bool:
    return os.getenv('RUNNING_IN_DOCKER', 'false').lower() == 'true' or os.path.exists('/.dockerenv')

def _rewrite_local_dev_db_host(database_url: str) -> str:
    app_env = os.getenv('APP_ENV', 'production')
    if app_env != 'development' or _running_in_container():
        return database_url
    parsed = urlparse(database_url)
    if parsed.hostname != 'db':
        return database_url
    netloc = parsed.netloc
    if '@' in netloc:
        (auth, host_port) = netloc.rsplit('@', 1)
        if host_port.startswith('db:'):
            netloc = f"{auth}@localhost:{host_port.split(':', 1)[1]}"
        elif host_port == 'db':
            netloc = f'{auth}@localhost'
    elif netloc.startswith('db:'):
        netloc = f"localhost:{netloc.split(':', 1)[1]}"
    elif netloc == 'db':
        netloc = 'localhost'
    rewritten = urlunparse(parsed._replace(netloc=netloc))
    logger.warning("DATABASE_URL host 'db' detected in local dev; using localhost instead")
    return rewritten
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+asyncpg://postgres:postgres@localhost:5432/postgres')
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql+asyncpg://', 1)
elif DATABASE_URL.startswith('postgresql://'):
    DATABASE_URL = DATABASE_URL.replace('postgresql://', 'postgresql+asyncpg://', 1)

# asyncpg mis-parses usernames with a dot (e.g. postgres.projectref from Supabase pooler).
# URL-encode the dot in the username portion so it is passed through correctly.
try:
    from urllib.parse import urlparse as _up, urlunparse as _uu
    _parsed = _up(DATABASE_URL)
    if _parsed.username and '.' in _parsed.username:
        _encoded_user = _parsed.username.replace('.', '%2E')
        _netloc = _parsed.netloc.replace(
            _parsed.username,
            _encoded_user,
            1
        )
        DATABASE_URL = _uu(_parsed._replace(netloc=_netloc))
        logger.debug('Encoded pooler username: %s', _encoded_user)
except Exception as _e:
    logger.warning('Could not encode pooler username: %s', _e)

DATABASE_URL = _rewrite_local_dev_db_host(DATABASE_URL)


def _strip_url_params(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.query:
        return url
    params = parse_qs(parsed.query, keep_blank_values=True)
    for p in ('sslmode', 'channel_binding', 'gssencmode'):
        params.pop(p, None)
    new_query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=new_query))
DATABASE_URL = _strip_url_params(DATABASE_URL)
_db_ssl_mode = os.getenv('DB_SSL', 'false').lower()
_connect_args: dict = {'statement_cache_size': 0, 'timeout': 5}
if _db_ssl_mode == 'require':
    _connect_args['ssl'] = 'require'
elif _db_ssl_mode == 'true':
    _ssl_ctx = ssl.create_default_context()
    _ssl_ctx.check_hostname = False
    _ssl_ctx.verify_mode = ssl.CERT_NONE
    _connect_args['ssl'] = _ssl_ctx
engine = create_async_engine(DATABASE_URL, echo=os.getenv('APP_ENV', 'production') == 'development', pool_pre_ping=True, pool_size=3, max_overflow=5, pool_recycle=300, pool_timeout=5, connect_args=_connect_args)
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False, autoflush=False, autocommit=False)

class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = 'users'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    supabase_id = Column(String(64), unique=True, nullable=True, index=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=True)
    password_hash = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    analyses = relationship('AnalysisHistory', back_populates='user', cascade='all, delete-orphan', lazy='select')

class AnalysisHistory(Base):
    __tablename__ = 'analysis_history'
    __table_args__ = (UniqueConstraint('user_id', 'file_hash', name='uq_user_file_hash'),)
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)
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
    user = relationship('User', back_populates='analyses')
    metadata_row = relationship('AnalysisMetadata', back_populates='analysis', cascade='all, delete-orphan', uselist=False, lazy='select')

class AnalysisMetadata(Base):
    __tablename__ = 'analysis_metadata'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    analysis_id = Column(BigInteger, ForeignKey('analysis_history.id', ondelete='CASCADE'), nullable=False, unique=True, index=True)
    user_id = Column(BigInteger, ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)
    file_name = Column(String(512), nullable=False)
    file_size = Column(BigInteger, nullable=True)
    row_count = Column(Integer, nullable=True)
    column_count = Column(Integer, nullable=True)
    completeness = Column(Float, nullable=True)
    analyzed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    analysis = relationship('AnalysisHistory', back_populates='metadata_row')

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    session = AsyncSessionLocal()
    try:
        yield session
        await session.commit()
    except Exception:
        logger.exception('get_db failed; rolling back session')
        await session.rollback()
        raise
    finally:
        await session.close()

async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info('Database tables initialised (Supabase PostgreSQL)')

async def drop_all() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    logger.warning('All database tables dropped')