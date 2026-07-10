import io
import asyncio
import logging
import os
import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import pandas as pd
import orjson
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status, Request, Response, Query, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.gzip import GZipMiddleware
import traceback
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select as _sa_select, or_
from pydantic import BaseModel, field_validator
from typing import Any, Annotated
import re as _re
_INJECTION_PATTERNS = ['ignore\\s+(all\\s+)?previous\\s+instructions', 'you\\s+are\\s+now\\s+a', 'act\\s+as\\s+(if\\s+you\\s+are\\s+)?a', 'disregard\\s+(all\\s+)?prior', 'system\\s{0,5}:\\s{0,5}', '<\\s*system\\s*>', '<\\s*/?inst\\s*>', '\\[INST\\]', '###\\s*instruction', 'forget\\s+(all\\s+)?previous', 'new\\s+persona', 'pretend\\s+(you\\s+are|to\\s+be)', 'bypassing', 'jailbreak', 'from\\s+now\\s+on', 'do\\s+not\\s+obey']
_INJECTION_RE = _re.compile('|'.join(_INJECTION_PATTERNS), _re.IGNORECASE)

def sanitize_chat_input(text: str) -> str:
    text = text.replace('\x00', '').replace('\r', ' ')
    text = _INJECTION_RE.sub('[removed]', text)
    return text.strip()
from .core import cache as redis_cache
from .core.cache import _get_client as _get_cache_client
from .analysis_history import _serialize_charts, compute_file_hash, delete_analysis, get_analysis_by_id, get_analysis_by_hash, get_user_analysis_history, save_analysis
from .auth import get_user_by_id, login_user, register_user, request_password_reset, reset_password_with_token, verify_access_token, refresh_session
from .core.constants import APP_VERSION, PIPELINE_VERSION, ENABLE_DISK_CACHE, PARQUET_STORAGE_DIR as _PARQUET_STORAGE_DIR
from .core.graph import run_pipeline
from .core.logging_config import configure_logging
from .core.upload_parsing import read_csv_with_fallback, validate_upload_magic
from .core.utils import truncate_stats_for_llm, build_chat_context_pack
from .core.data_agent import run_data_query
from .rag.indexer import build_rag_index, retrieve_chunks
from .rag.chat_engine import answer_question, answer_chart_explanation
from .rag.pinecone_client import ping as pinecone_ping
from .core.llm_client import get_groq_client
from .db import get_db, init_db, AsyncSessionLocal, User as _User, AnalysisHistory as _AnalysisHistory
from .models.schemas import AnalysisListResponse, AuthResponse, ChatRequest, DeleteResponse, ForgotPasswordRequest, ForgotPasswordResponse, SyncSessionRequest, HealthResponse, ResetPasswordRequest, TokenResponse, UserLogin, UserRegister, UserResponse
configure_logging()
logger = logging.getLogger(__name__)
APP_ENV = os.getenv('APP_ENV', 'production')
MAX_UPLOAD_BYTES = int(os.getenv('MAX_UPLOAD_BYTES', str(10 * 1024 * 1024)))
MAX_ANALYZE_ROWS = int(os.getenv('MAX_ANALYZE_ROWS', '15000'))
MAX_ANALYZE_COLUMNS = int(os.getenv('MAX_ANALYZE_COLUMNS', '150'))
MAX_EXCEL_SHEETS = int(os.getenv('MAX_EXCEL_SHEETS', '5'))
MAX_QUESTION_CHARS = int(os.getenv('CHAT_MAX_QUESTION_CHARS', '1200'))
MAX_CONTEXT_BYTES = int(os.getenv('CHAT_MAX_CONTEXT_BYTES', str(4 * 1024 * 1024)))
READ_CHUNK_BYTES = 1024 * 1024
CHAT_RATE_LIMIT = int(os.getenv('CHAT_RATE_LIMIT', '10'))
CHAT_RATE_WINDOW = int(os.getenv('CHAT_RATE_WINDOW_SECONDS', '60'))
INTENT_MODEL = os.getenv('GROQ_INTENT_MODEL', 'llama-3.1-8b-instant')
SYNTHESIS_MODEL = os.getenv('GROQ_SYNTHESIS_MODEL', 'llama-3.3-70b-versatile')
_raw_origins = os.getenv('CORS_ORIGINS', 'http://localhost:5173,http://localhost:3000')
origins = [o.strip() for o in _raw_origins.split(',') if o.strip()]

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info('Starting DataPulse API v2 (pipeline %s)', PIPELINE_VERSION)
    if not os.getenv('GROQ_API_KEY'):
        raise RuntimeError('GROQ_API_KEY environment variable is not set. The application cannot start without it. Set it in your .env file or environment.')
    if APP_ENV == 'production' and '*' in origins:
        raise RuntimeError("CORS_ORIGINS cannot contain '*' in production")
    try:
        await init_db()
        logger.info('Database tables ready')
    except Exception as exc:
        logger.error('DATABASE CONNECTION FAILED — server is starting without DB. Fix DATABASE_URL / DB password in .env and restart. Error: %s', exc)
    try:
        from sqlalchemy import text
        from .db import engine
        async with engine.connect() as conn:
            await conn.execute(text('SELECT 1'))
        logger.info('DB connection pool warmed up')
    except Exception as exc:
        logger.warning('DB warmup failed (non-fatal): %s', exc)
    try:
        redis_ok = await redis_cache.ping()
        logger.info('Redis warmed up (reachable=%s)', redis_ok)
    except Exception as exc:
        logger.warning('Redis warmup failed (non-fatal): %s', exc)
    try:
        pc_ok = await asyncio.to_thread(pinecone_ping)
        logger.info('Pinecone warmed up (reachable=%s)', pc_ok)
    except Exception as exc:
        logger.warning('Pinecone warmup failed (non-fatal): %s', exc)
    yield
    await redis_cache.close()
    logger.info('DataPulse API shutdown complete')
app = FastAPI(title='DataPulse API', description='Multi-agent CSV analysis API', version=APP_VERSION, lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=['*'], allow_headers=['*'])

@app.middleware('http')
async def add_security_headers(request: Request, call_next):
    # Strip /api prefix — frontend sends /api/... but FastAPI routes are at /...
    # This allows both local dev (Vite proxy strips /api) and production to work
    path = request.scope.get('path', '')
    if path.startswith('/api/'):
        request.scope['path'] = path[4:]       # e.g. /api/history → /history
        request.scope['raw_path'] = path[4:].encode()
    elif path == '/api':
        request.scope['path'] = '/'
        request.scope['raw_path'] = b'/'

    response = await call_next(request)

    # ── Core security headers ──
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    response.headers['Permissions-Policy'] = 'camera=(), microphone=(), geolocation=(), payment=()'
    response.headers['X-Permitted-Cross-Domain-Policies'] = 'none'
    # ── Content Security Policy ──
    csp_directives = [
        "default-src 'self'",
        "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.plot.ly",
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
        "font-src 'self' https://fonts.gstatic.com",
        "connect-src 'self' https://*.supabase.co wss://*.supabase.co",
        "img-src 'self' data: blob: https:",
        "frame-ancestors 'none'",
        "base-uri 'self'",
        "form-action 'self'",
    ]
    response.headers['Content-Security-Policy'] = '; '.join(csp_directives)
    # ── HSTS (only in production to avoid localhost issues) ──
    if APP_ENV == 'production':
        response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains; preload'
    return response

@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request: Request, exc: HTTPException):
    headers = getattr(exc, 'headers', None)
    if headers:
        response = JSONResponse(status_code=exc.status_code, content={'detail': exc.detail}, headers=headers)
    else:
        response = JSONResponse(status_code=exc.status_code, content={'detail': exc.detail})
    
    # Manually add CORS headers to prevent browser hiding the error
    origin = request.headers.get('origin')
    if origin:
        response.headers['Access-Control-Allow-Origin'] = origin
        response.headers['Access-Control-Allow-Credentials'] = 'true'
    return response

@app.exception_handler(Exception)
async def catch_all_exception_handler(request: Request, exc: Exception):
    logger.error('Unhandled error on %s %s', request.method, request.url.path, exc_info=exc)
    content = {'detail': 'Internal Server Error'}
    if os.getenv('APP_ENV', 'production') == 'development':
        error_trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        content = {'detail': str(exc), 'trace': error_trace}
    
    response = JSONResponse(status_code=500, content=content)
    
    # Manually add CORS headers
    origin = request.headers.get('origin')
    if origin:
        response.headers['Access-Control-Allow-Origin'] = origin
        response.headers['Access-Control-Allow-Credentials'] = 'true'
    return response

security = HTTPBearer()

async def get_current_user_id(credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)], db: AsyncSession=Depends(get_db)) -> int:
    token = credentials.credentials
    payload = await verify_access_token(token)
    if payload is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid or expired token', headers={'WWW-Authenticate': 'Bearer'})
    try:
        result = await asyncio.wait_for(
            db.execute(_sa_select(_User).where(or_(_User.supabase_id == payload['sub'], _User.email == payload['email']))),
            timeout=5.0
        )
        user = result.scalar_one_or_none()
        if user is None:
            try:
                user = _User(supabase_id=payload['sub'], email=payload['email'], name=payload['email'].split('@')[0])
                db.add(user)
                await asyncio.wait_for(db.commit(), timeout=5.0)
                await asyncio.wait_for(db.refresh(user), timeout=5.0)
                logger.info('Auto-created local profile for Supabase user: %s', payload['email'])
            except Exception as exc:
                await db.rollback()
                logger.error('Failed to auto-create profile for %s: %s', payload['email'], exc)
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail='Database temporarily unavailable. Please try again.')
        elif not user.supabase_id:
            try:
                user.supabase_id = payload['sub']
                await asyncio.wait_for(db.commit(), timeout=5.0)
            except Exception:
                await db.rollback()
        return user.id
    except HTTPException:
        raise
    except (asyncio.TimeoutError, Exception) as exc:
        logger.error('get_current_user_id DB error (token=%s...): %s', token[:20], exc)
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail='Database temporarily unavailable. Please try again in a moment.')

async def check_ip_rate_limit(request: Request):
    client_ip = request.client.host if request.client else 'unknown'
    key = f'ratelimit:ip:{client_ip}'
    try:
        count = await redis_cache.increment_with_ttl(key, 60)
        if count > 20:
            raise HTTPException(status_code=429, detail='Too many requests from this IP. Please try again in a minute.')
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise exc
        logger.warning('Rate limiting failed for %s: %s', key, exc)

async def check_user_rate_limit(user_id: int=Depends(get_current_user_id)):
    key = f'ratelimit:user_analyze:{user_id}'
    try:
        count = await redis_cache.increment_with_ttl(key, 60)
        if count > 5:
            raise HTTPException(status_code=429, detail='Too many analysis requests. Please try again in a minute.')
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise exc
        logger.warning('Rate limiting failed for %s: %s', key, exc)
    return user_id

@app.get('/health', response_model=HealthResponse, tags=['system'])
async def health_check(db: AsyncSession=Depends(get_db)):
    pg_ok = False
    try:
        await db.execute(__import__('sqlalchemy').text('SELECT 1'))
        pg_ok = True
    except Exception:
        pass
    redis_ok = await redis_cache.ping()
    return HealthResponse(status='ok' if pg_ok and redis_ok else 'degraded', postgres=pg_ok, redis=redis_ok)

@app.post('/auth/register', response_model=AuthResponse, status_code=status.HTTP_201_CREATED, tags=['auth'], dependencies=[Depends(check_ip_rate_limit)])
async def register(body: UserRegister, db: AsyncSession=Depends(get_db)):
    result = await register_user(db, body.email, body.password, body.name)
    if not result['success']:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result['message'])
    user_response = UserResponse(id=result['user_id'], name=result.get('name'), email=result['email'], created_at=result.get('created_at') or datetime.now(tz=timezone.utc), updated_at=result.get('updated_at') or datetime.now(tz=timezone.utc))
    return AuthResponse(success=True, message=result['message'], user=user_response)

@app.post('/auth/login', response_model=TokenResponse, tags=['auth'], dependencies=[Depends(check_ip_rate_limit)])
async def login(body: UserLogin, db: AsyncSession=Depends(get_db), response: Response=None):
    result = await login_user(db, body.email, body.password)
    if not result['success']:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=result['message'], headers={'WWW-Authenticate': 'Bearer'})
    user_obj = result['user']
    if response is not None and result.get('refresh_token'):
        response.set_cookie(key='datapulse_refresh', value=result['refresh_token'], httponly=True, secure=APP_ENV == 'production', samesite='lax', max_age=7 * 24 * 3600, path='/')
    return TokenResponse(access_token=result['access_token'], token_type='bearer', user=UserResponse(id=user_obj['id'], name=user_obj.get('name'), email=user_obj['email'], created_at=user_obj['created_at'], updated_at=user_obj['updated_at']))

@app.post('/auth/forgot-password', response_model=ForgotPasswordResponse, tags=['auth'], dependencies=[Depends(check_ip_rate_limit)])
async def forgot_password(body: ForgotPasswordRequest, db: AsyncSession=Depends(get_db)):
    result = await request_password_reset(db, body.email)
    return ForgotPasswordResponse(success=True, message=result.get('message', 'If an account exists for that email, a password reset link has been sent.'))

@app.post('/auth/reset-password', response_model=AuthResponse, tags=['auth'], dependencies=[Depends(check_ip_rate_limit)])
async def reset_password(body: ResetPasswordRequest, db: AsyncSession=Depends(get_db)):
    result = await reset_password_with_token(db, body.token, body.new_password)
    if not result.get('success'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result.get('message', 'Password reset failed'))
    return AuthResponse(success=True, message=result['message'])

@app.post('/auth/sync-session', response_model=TokenResponse, tags=['auth'])
async def sync_session(body: SyncSessionRequest, response: Response=None):
    # 1. Verify the Supabase token first — this is the critical step
    payload = await verify_access_token(body.access_token)
    if payload is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid or expired access token')

    # 2. Try to sync with local DB — but don't block login if DB is unavailable
    user_id = 0
    user_name = payload['email'].split('@')[0]
    user_email = payload['email']
    user_created_at = datetime.now(tz=timezone.utc)
    user_updated_at = datetime.now(tz=timezone.utc)

    try:
        async with AsyncSessionLocal() as db:
            result = await asyncio.wait_for(
                db.execute(_sa_select(_User).where(or_(_User.supabase_id == payload['sub'], _User.email == payload['email']))),
                timeout=5.0
            )
            user = result.scalar_one_or_none()
            if user is None:
                user = _User(supabase_id=payload['sub'], email=payload['email'], name=payload['email'].split('@')[0])
                db.add(user)
                await asyncio.wait_for(db.commit(), timeout=5.0)
                await asyncio.wait_for(db.refresh(user), timeout=5.0)
            elif not user.supabase_id:
                user.supabase_id = payload['sub']
                await asyncio.wait_for(db.commit(), timeout=5.0)
                await asyncio.wait_for(db.refresh(user), timeout=5.0)
            user_id = user.id
            user_name = user.name
            user_email = user.email
            user_created_at = user.created_at
            user_updated_at = user.updated_at
    except Exception as exc:
        logger.warning('sync_session: DB sync failed (non-fatal, using Supabase data): %s', exc)

    if response is not None and body.refresh_token:
        response.set_cookie(key='datapulse_refresh', value=body.refresh_token, httponly=True, secure=APP_ENV == 'production', samesite='lax', max_age=7 * 24 * 3600, path='/')
    return TokenResponse(access_token=body.access_token, token_type='bearer', user=UserResponse(id=user_id, name=user_name, email=user_email, created_at=user_created_at, updated_at=user_updated_at))


@app.post('/auth/refresh', response_model=TokenResponse, tags=['auth'])
async def refresh_token_route(request: Request, response: Response):
    token = request.cookies.get('datapulse_refresh')
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Missing refresh token')
    new_session = await refresh_session(token)
    if new_session is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid or expired refresh token')
    if new_session.get('refresh_token') and response is not None:
        response.set_cookie(key='datapulse_refresh', value=new_session['refresh_token'], httponly=True, secure=APP_ENV == 'production', samesite='lax', max_age=7 * 24 * 3600, path='/')
    payload = await verify_access_token(new_session['access_token'])
    if payload is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Token verification failed after refresh')
    # Try DB lookup — fall back to token payload if DB is unavailable
    user_id, user_name, user_email = 0, payload['email'].split('@')[0], payload['email']
    user_created_at = user_updated_at = datetime.now(tz=timezone.utc)
    try:
        async with AsyncSessionLocal() as db:
            result = await asyncio.wait_for(
                db.execute(_sa_select(_User).where(_User.supabase_id == payload['sub'])),
                timeout=5.0
            )
            user = result.scalar_one_or_none()
            if user:
                user_id, user_name, user_email = user.id, user.name, user.email
                user_created_at, user_updated_at = user.created_at, user.updated_at
    except Exception as exc:
        logger.warning('refresh_token_route: DB lookup failed (non-fatal): %s', exc)
    return TokenResponse(access_token=new_session['access_token'], token_type='bearer', user=UserResponse(id=user_id, name=user_name, email=user_email, created_at=user_created_at, updated_at=user_updated_at))

@app.post('/auth/logout', tags=['auth'])
async def logout(response: Response):
    response.delete_cookie('datapulse_refresh', path='/')
    return {'success': True, 'message': 'Logged out'}

@app.get('/auth/me', response_model=UserResponse, tags=['auth'])
async def me(credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)]):
    """Returns current user — DB-resilient: falls back to token payload if DB is unavailable."""
    token = credentials.credentials
    payload = await verify_access_token(token)
    if payload is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid or expired token')
    # Defaults from token (always available, even if DB is down)
    user_id = 0
    user_name = payload['email'].split('@')[0]
    user_email = payload['email']
    user_created_at = user_updated_at = datetime.now(tz=timezone.utc)
    try:
        async with AsyncSessionLocal() as db:
            result = await asyncio.wait_for(
                db.execute(_sa_select(_User).where(or_(_User.supabase_id == payload['sub'], _User.email == payload['email']))),
                timeout=5.0
            )
            user = result.scalar_one_or_none()
            if user:
                user_id = user.id
                user_name = user.name
                user_email = user.email
                user_created_at = user.created_at
                user_updated_at = user.updated_at
    except Exception as exc:
        logger.warning('auth/me: DB lookup failed, using token payload (non-fatal): %s', exc)
    return UserResponse(id=user_id, name=user_name, email=user_email, created_at=user_created_at, updated_at=user_updated_at)


def persist_full_data_backend(df: pd.DataFrame, file_hash: str) -> None:
    if not ENABLE_DISK_CACHE:
        return
    try:
        assert _re.match('^[a-f0-9]{64}$', file_hash), 'Invalid file hash'
        os.makedirs(_PARQUET_STORAGE_DIR, exist_ok=True)
        storage_path = os.path.join(_PARQUET_STORAGE_DIR, f'{file_hash}.parquet')
        df.to_parquet(storage_path, index=False)
        logger.info('Parquet saved: %s', storage_path)
    except Exception as exc:
        logger.warning('Parquet persist failed for %s: %s', file_hash, exc)

def cleanup_old_parquet_files(retention_days: int=3):
    try:
        os.makedirs(_PARQUET_STORAGE_DIR, exist_ok=True)
        cutoff = datetime.now() - timedelta(days=max(0, retention_days))
        deleted = 0
        files_checked = 0
        MAX_CHECK = 500
        for name in os.listdir(_PARQUET_STORAGE_DIR):
            if files_checked > MAX_CHECK:
                break
            files_checked += 1
            if not name.lower().endswith('.parquet'):
                continue
            path = os.path.join(_PARQUET_STORAGE_DIR, name)
            try:
                if not os.path.isfile(path):
                    continue
                modified_at = datetime.fromtimestamp(os.path.getmtime(path))
                if modified_at < cutoff:
                    os.remove(path)
                    deleted += 1
            except Exception as file_exc:
                logger.warning('Parquet cleanup skipped %s: %s', path, file_exc)
        if deleted:
            logger.info('Parquet cleanup removed %d stale file(s)', deleted)
    except Exception as exc:
        logger.warning('Parquet cleanup failed: %s', exc)

# Threshold below which Parquet persist runs synchronously (above this it's backgrounded)
_SYNC_PARQUET_THRESHOLD_ROWS: int = 5000

def _stratified_preview(df: pd.DataFrame, n: int) -> list:
    if len(df) > n:
        step = max(1, len(df) // n)
        df = df.iloc[::step].head(n)
    df_preview = df.copy()
    for col in df_preview.select_dtypes(include=['datetime64']).columns:
        df_preview[col] = df_preview[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    return json.loads(df_preview.to_json(orient='records'))

@app.post('/analyze', tags=['analysis'])
async def analyze(background_tasks: BackgroundTasks, file: UploadFile=File(...), user_id: int=Depends(check_user_rate_limit), db: AsyncSession=Depends(get_db)):
    logger.debug('analyze endpoint invoked, has_filename=%s', bool(file.filename))
    filename = os.path.basename(file.filename or 'upload').strip()
    parsed_ext = filename.lower().split('.')[-1] if '.' in filename else ''
    if parsed_ext not in ('csv', 'xlsx', 'xls'):
        raise HTTPException(status_code=400, detail='Only CSV and Excel files are accepted')
    total = 0
    chunks: list[bytes] = []
    while True:
        chunk = await file.read(READ_CHUNK_BYTES)
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail=f'File too large. Max allowed size is {MAX_UPLOAD_BYTES // (1024 * 1024)} MB')
        chunks.append(chunk)
    file_bytes = b''.join(chunks)
    if len(file_bytes) == 0:
        raise HTTPException(status_code=400, detail='Uploaded file is empty')
    validate_upload_magic(parsed_ext, file_bytes)
    file_hash = compute_file_hash(file_bytes, filename)
    assert _re.match('^[a-f0-9]{64}$', file_hash), 'Invalid file hash'
    cached = await get_analysis_by_hash(db, user_id, file_hash)
    if cached and cached.get('stats_summary') and cached.get('insights') and (not (cached.get('errors') or [])) and (cached.get('pipeline_version') == PIPELINE_VERSION):
        cached['charts'] = {k: v for (k, v) in (cached.get('charts') or {}).items()}
        return {'from_cache': True, **cached}
    elif cached:
        logger.info('Cache not eligible for reuse for user %d / %s — re-running pipeline', user_id, filename)
    await db.rollback()
    try:
        if parsed_ext == 'csv':
            df = read_csv_with_fallback(file_bytes, max_analyze_rows=MAX_ANALYZE_ROWS, max_analyze_columns=MAX_ANALYZE_COLUMNS)
        elif parsed_ext == 'xlsx':
            from openpyxl import load_workbook
            wb = load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
            try:
                sheet_count = len(wb.sheetnames)
                if sheet_count > MAX_EXCEL_SHEETS:
                    raise HTTPException(status_code=413, detail=f'Excel file has {sheet_count} sheets. Maximum allowed is {MAX_EXCEL_SHEETS}.')
                active_sheet = wb[wb.sheetnames[0]]
                if (active_sheet.max_row or 0) > MAX_ANALYZE_ROWS:
                    raise HTTPException(status_code=413, detail=f'Excel file has {active_sheet.max_row} rows. Maximum allowed is {MAX_ANALYZE_ROWS}.')
                if (active_sheet.max_column or 0) > MAX_ANALYZE_COLUMNS:
                    raise HTTPException(status_code=413, detail=f'Excel file has {active_sheet.max_column} columns. Maximum allowed is {MAX_ANALYZE_COLUMNS}.')
            finally:
                wb.close()
            df = pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl', sheet_name=0, nrows=MAX_ANALYZE_ROWS + 1)
        else:
            try:
                import xlrd
            except Exception:
                raise HTTPException(status_code=422, detail="Legacy .xls upload requires the 'xlrd' package. Please upload CSV/XLSX or install xlrd on the backend.")
            df = pd.read_excel(io.BytesIO(file_bytes), engine='xlrd', sheet_name=0, nrows=MAX_ANALYZE_ROWS + 1)
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise exc
        _raw_exc = str(exc).lower()
        if 'codec' in _raw_exc or 'encoding' in _raw_exc or 'decode' in _raw_exc:
            raise HTTPException(status_code=422, detail='Could not read the file — the encoding is not supported. Try saving your file as UTF-8 CSV.')
        elif 'column' in _raw_exc or 'header' in _raw_exc:
            raise HTTPException(status_code=422, detail='Could not parse the file headers. Make sure the first row contains column names.')
        elif 'empty' in _raw_exc:
            raise HTTPException(status_code=422, detail='The uploaded file appears to be empty.')
        else:
            raise HTTPException(status_code=422, detail='Could not read the file. Make sure it is a valid CSV or Excel file with data.')
    (row_count, column_count) = df.shape
    if row_count > MAX_ANALYZE_ROWS:
        raise HTTPException(status_code=413, detail=f'Dataset has {row_count} rows. Maximum allowed is {MAX_ANALYZE_ROWS}.')
    if column_count > MAX_ANALYZE_COLUMNS:
        raise HTTPException(status_code=413, detail=f'Dataset has {column_count} columns. Maximum allowed is {MAX_ANALYZE_COLUMNS}.')
    state = await asyncio.to_thread(run_pipeline, df)
    if getattr(state, 'clean_df', None) is not None and ENABLE_DISK_CACHE:
        df_to_save = state.clean_df.copy()
        if len(df_to_save) <= _SYNC_PARQUET_THRESHOLD_ROWS:
            persist_full_data_backend(df_to_save, file_hash)
        else:
            background_tasks.add_task(persist_full_data_backend, df_to_save, file_hash)
        background_tasks.add_task(cleanup_old_parquet_files, 3)
    CHART_PREVIEW_ROWS = int(os.getenv('CHART_PREVIEW_ROWS', '500'))
    preview_raw = _stratified_preview(state.raw_df, CHART_PREVIEW_ROWS) if getattr(state, 'raw_df', None) is not None else []
    preview_clean = _stratified_preview(state.clean_df, CHART_PREVIEW_ROWS) if getattr(state, 'clean_df', None) is not None else []
    state.raw_df = None
    state.clean_df = None
    result = state.model_dump()
    result['raw_df'] = preview_raw
    result['clean_df'] = preview_clean
    result['file_hash'] = file_hash
    has_stats = bool(result.get('stats_summary') and result['stats_summary'].get('row_count'))
    has_insights = bool(result.get('insights') and result['insights'].get('findings'))
    is_fatal = not has_stats and (not has_insights)
    if is_fatal:
        logger.error('Pipeline critically failed for user %d / %s: %s', user_id, filename, state.errors)
        raise HTTPException(status_code=500, detail={'message': 'Analysis pipeline failed — no statistics or insights were produced. Please check the dataset format and try again.', 'errors': [str(e) for e in state.errors]})
    elif state.errors:
        logger.warning('Pipeline completed with non-fatal errors for user %d / %s: %s', user_id, filename, state.errors)
    serialized_charts = _serialize_charts(result.get('charts') or {})
    save_result = await save_analysis(db=db, user_id=user_id, file_name=filename, file_hash=file_hash, file_size=len(file_bytes), analysis_result=result, serialized_charts=serialized_charts)
    if not save_result['success']:
        logger.warning('Failed to persist analysis: %s', save_result['message'])
    else:
        result['analysis_id'] = save_result.get('analysis_id')
    _rag_stats = result.get('stats_summary') or {}
    _rag_insights = result.get('insights') or {}
    _rag_charts = serialized_charts or {}
    _rag_hash = file_hash

    async def _index_rag_background():
        try:
            _redis = _get_cache_client()
            n = await build_rag_index(file_hash=_rag_hash, stats=_rag_stats, insights=_rag_insights, charts=_rag_charts, redis_client=_redis)
            logger.info('RAG indexed %d chunks for %s', n, _rag_hash[:8])
        except Exception as _rag_exc:
            logger.warning('RAG indexing failed (non-fatal): %s', _rag_exc)
    background_tasks.add_task(_index_rag_background)
    result['chat_context_pack'] = build_chat_context_pack(result.get('stats_summary', {}), result.get('insights', {}))
    result['charts'] = serialized_charts
    result['partial'] = False
    if state.errors:
        result['warnings'] = state.errors
    safe_result = orjson.loads(orjson.dumps(result, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY))
    return {'from_cache': False, 'pipeline_version': PIPELINE_VERSION, **safe_result}

@app.get('/history', response_model=AnalysisListResponse, tags=['history'])
async def history(user_id: int=Depends(get_current_user_id), db: AsyncSession=Depends(get_db), limit: int=Query(default=20, ge=1, le=100)):
    items = await get_user_analysis_history(db, user_id, limit=limit)
    return AnalysisListResponse(success=True, message='History retrieved', total=len(items), analyses=items)

@app.get('/history/{analysis_id}', tags=['history'])
async def history_item(analysis_id: int, user_id: int=Depends(get_current_user_id), db: AsyncSession=Depends(get_db)):
    item = await get_analysis_by_id(db, user_id, analysis_id)
    if item is None:
        raise HTTPException(status_code=404, detail='Analysis not found or access denied')
    item['charts'] = {k: v for (k, v) in (item.get('charts') or {}).items()}
    return item

@app.delete('/history/{analysis_id}', response_model=DeleteResponse, tags=['history'])
async def remove_analysis(analysis_id: int, user_id: int=Depends(get_current_user_id), db: AsyncSession=Depends(get_db)):
    result = await delete_analysis(db, user_id, analysis_id)
    if not result['success']:
        raise HTTPException(status_code=404, detail=result['message'])
    return DeleteResponse(success=True, message=result['message'])

VALID_QUERY_TYPES = {'filter_lookup', 'value_counts', 'filter_group', 'filter_aggregate', 'group_aggregate', 'aggregate', 'top_n', 'bottom_n', 'lookup', 'row_count', 'distinct', 'search', 'correlation', 'percentile'}

class QueryPlan(BaseModel):
    type: str
    params: dict[str, Any] = {}

    @field_validator('type')
    @classmethod
    def validate_type(cls, v):
        if v not in VALID_QUERY_TYPES:
            raise ValueError(f'Unknown query type: {v}')
        return v

def _is_result_plausible(data_result: dict, stats: dict) -> tuple[bool, str]:
    if not data_result or 'error' in data_result:
        return (False, data_result.get('error', 'query failed'))
    result = data_result.get('result')
    if result == 'No rows found.' or result is None:
        return (False, 'empty result')
    if not isinstance(result, (int, float)):
        return (True, '')
    numeric_cols = stats.get('numeric_columns', {})
    sort_col = data_result.get('sort_column') or data_result.get('query', '')
    for (col, col_stats) in numeric_cols.items():
        if col.lower() in sort_col.lower():
            col_min = col_stats.get('min', float('-inf'))
            col_max = col_stats.get('max', float('inf'))
            if not col_min <= result <= col_max * 1.01:
                return (False, f'value {result} outside known range [{col_min}, {col_max}]')
    if data_result.get('query', '').startswith('Row count'):
        known_rows = stats.get('row_count', float('inf'))
        if isinstance(result, int) and result > known_rows:
            return (False, f'row count {result} exceeds dataset size {known_rows}')
    return (True, '')

def _classify_chat_intent(question: str) -> str:
    q = question.lower().strip()
    _OFF_TOPIC = ('write code', 'write python', 'write javascript', 'write java', 'how to program', 'how to code', 'write a function', 'help me code', 'write a script', 'write an app', 'build an app', 'build a website', 'recipe', 'how to cook', 'how to bake', 'how to make a cake', 'who is the president', 'capital of', 'weather in', 'sports', 'tell me a joke', 'tell me a story', 'write a poem', 'write a song')
    if any((ot in q for ot in _OFF_TOPIC)):
        return 'off_topic'
    _PURE_GREETINGS = {'hello', 'hi', 'hey', 'howdy', 'hiya', 'yo', 'bye', 'goodbye', 'see you', 'see ya', 'later', 'cya', 'thanks', 'thank you', 'thx', 'ty', 'thank', 'ok', 'okay', 'good', 'cool', 'great', 'nice', 'good morning', 'good afternoon', 'good evening', 'good night', 'how are you', 'how r u', "what's up", 'sup', 'whats up', 'who are you', 'what can you do', 'what can you help', 'help', 'hi there', 'hey there', 'hello there'}
    _DATA_INDICATOR = ('chart', 'graph', 'plot', 'column', 'row', 'data', 'value', 'average', 'mean', 'max', 'min', 'count', 'total', 'sum', 'trend', 'correlation', 'distribution', 'analysis', 'generate', 'create', 'show', 'display', 'visualize', 'what', 'which', 'how many', 'how much', 'when', 'where', 'why', 'does', 'did', 'is there', 'histogram', 'scatter', 'heatmap', 'violin', 'donut', 'pie', 'bar', 'line', 'frequency')
    if q in _PURE_GREETINGS:
        return 'greeting'
    if len(q.split()) <= 4 and (not any((dw in q for dw in _DATA_INDICATOR))):
        if any((g in q for g in ('hello', 'hi', 'bye', 'hey', 'thanks', 'thank', 'ok', 'okay'))):
            return 'greeting'
    _EXPLAIN = (
        'explain', 'what does this', 'what do these', 'tell me about this chart', 'tell me about the chart',
        'interpret', 'what can i see', 'what am i looking at', 'why is', 'why are', 'what patterns',
        'what trends', 'describe this chart', 'describe the chart', 'what does this chart',
        'what does the chart', 'analyse the chart', 'analyze the chart', 'insight from the chart',
        'insight from this', 'what does this plot', 'what does the plot', 'what does this graph',
        'what does the graph', 'what is shown in', 'what is shown on',
        # additional natural phrasings
        'above chart', 'above graph', 'above plot', 'last chart', 'this visualization',
        'about this chart', 'about the chart', 'from this chart', 'from the chart',
        'what chart shows', 'what graph shows', 'what plot shows',
        'chart mean', 'graph mean', 'chart tell', 'graph tell',
        'chart showing', 'chart show', 'reading the chart', 'read the chart',
    )
    if any((p in q for p in _EXPLAIN)):
        return 'explain_chart'
    _NEED_WANT = ('i need graph', 'i need a graph', 'i need chart', 'i need a chart', 'i need plot', 'i need a plot', 'i need visualization', 'i want graph', 'i want a graph', 'i want chart', 'i want a chart', 'i want plot', 'i want a plot', 'need graph', 'need chart', 'need plot', 'want graph', 'want chart', 'want plot', 'show graph', 'show chart', 'show plot', 'show a graph', 'show a chart', 'show a plot', 'new chart', 'new plot', 'new graph', 'another chart', 'another plot', 'another graph', 'different chart', 'different plot', 'one more chart', 'one more plot', 'more charts', 'more plots', 'can you plot', 'can you chart', 'can you make a', 'can you generate', 'can you create', 'can you show me a', 'can you visualize', 'can you visualise', 'generate chart', 'generate graph', 'generate plot', 'generate me chart', 'generate me graph', 'generate me plot', 'generate me a', 'create chart', 'create graph', 'create plot', 'make chart', 'make graph', 'make plot', 'make a chart', 'make a graph', 'make a plot', 'make me chart', 'make me graph', 'make me a', 'give me chart', 'give me graph', 'give me plot', 'draw chart', 'draw graph', 'draw plot', 'show me a new', 'show me chart', 'show me graph', 'show me plot', 'give me a chart', 'give me a plot', 'give me a graph', 'give me a scatter', 'give me a pie', 'give me a bar', 'give me a line', 'give me a histogram', 'give me a donut', 'give me a heatmap', 'show a pie', 'show a bar', 'show a scatter', 'show a line', 'show a histogram', 'show a donut', 'graph for', 'graph of', 'chart for', 'chart of', 'plot for', 'plot of', 'generate a', 'create a', 'build a', 'draw a', 'visualize ', 'visualise ')
    if any((p in q for p in _NEED_WANT)):
        return 'generate_chart'
    _GEN_VERBS = ('generate', 'create', 'make', 'build', 'draw', 'visualize', 'visualise')
    _CHART_NOUNS = ('a chart', 'a graph', 'a plot', 'a bar', 'a line', 'a scatter', 'a histogram', 'a pie', 'a donut', 'a heatmap', 'a box plot', 'a violin', 'scatter', 'histogram', 'heatmap', 'violin', 'chart', 'graph', 'plot')
    for verb in _GEN_VERBS:
        if verb in q:
            if any((noun in q for noun in _CHART_NOUNS)):
                return 'generate_chart'
            if verb in ('visualize', 'visualise'):
                return 'generate_chart'
    words = q.split()
    for (i_w, w) in enumerate(words):
        if w in ('plot', 'graph', 'chart') and i_w > 0 and words[i_w - 1] not in ('the', 'this', 'that', 'a', 'an', 'my', 'your'):
            return 'generate_chart'
    _BREAKDOWN = ('by year', 'by month', 'by quarter', 'by week', 'by day', 'over time', 'over the years', 'over the months', 'trend of', 'trend for', 'with year', 'with month', 'across years', 'across months', 'breakdown of', 'breakdown by', 'comparison of', 'by brand', 'by category', 'by region', 'by state', 'by type', 'by model', 'sales by', 'revenue by', 'count by', 'total by', 'average by', 'mean by')
    _VISUAL_WORD = ('graph', 'chart', 'plot', 'visualization', 'bar', 'line', 'scatter', 'histogram', 'pie', 'donut')
    _visual_re = _re.compile('\\b(' + '|'.join((_re.escape(w) for w in _VISUAL_WORD)) + ')\\b')
    if any((bd in q for bd in _BREAKDOWN)) and _visual_re.search(q):
        return 'generate_chart'
    _CHART_REFS = ('the scatter', 'the histogram', 'the bar chart', 'the bar', 'the heatmap', 'the line chart', 'the line graph', 'the box plot', 'the violin', 'the donut', 'the pie', 'this chart', 'this plot', 'this graph', 'that chart', 'that plot', 'the chart', 'the graph')
    if any((n in q for n in _CHART_REFS)):
        _OVERRIDE = ('generate', 'create', 'make', 'build', 'draw', 'another', 'new', 'different')
        if not any((v in q for v in _OVERRIDE)):
            return 'explain_chart'
    return 'data_question'

@app.post('/chat', tags=['analysis'])
async def chat_with_analysis(body: ChatRequest, user_id: int=Depends(get_current_user_id), db: AsyncSession=Depends(get_db)):
    try:
        key = f'ratelimit:chat:{user_id}'
        count = await redis_cache.increment_with_ttl(key, CHAT_RATE_WINDOW)
        if count > CHAT_RATE_LIMIT:
            raise HTTPException(status_code=429, detail=f'Too many chat requests. Please wait {CHAT_RATE_WINDOW} seconds.')
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise exc
        logger.warning('Redis rate limiter unavailable for user %s: %s', user_id, exc)
    question = sanitize_chat_input(body.question.strip())
    context = body.context or {}
    if not question:
        raise HTTPException(status_code=400, detail='Question is required')
    if len(question) > MAX_QUESTION_CHARS:
        raise HTTPException(status_code=413, detail=f'Question too long. Max {MAX_QUESTION_CHARS} characters.')
    _MAX_QUESTION_BYTES = MAX_QUESTION_CHARS * 4
    if len(question.encode('utf-8')) > _MAX_QUESTION_BYTES:
        raise HTTPException(status_code=413, detail=f'Question too long. Max {MAX_QUESTION_CHARS} characters.')
    try:
        context_blob = json.dumps(context, default=str)
    except Exception:
        raise HTTPException(status_code=400, detail='Invalid context payload format.')
    if len(context_blob.encode('utf-8')) > MAX_CONTEXT_BYTES:
        raise HTTPException(status_code=413, detail=f'Context too large. Max {MAX_CONTEXT_BYTES // 1024} KB.')
    chat_context_pack = context.get('chat_context_pack')
    if chat_context_pack:
        _pack_columns = chat_context_pack.get('columns') or {}
        _pack_numeric = {}
        _pack_categorical = {}
        for (_col_name, _col_info) in _pack_columns.items():
            _col_type = _col_info.get('type') or '' if isinstance(_col_info, dict) else ''
            if _col_type == 'numeric':
                _pack_numeric[_col_name] = {'mean': _col_info.get('mean'), 'median': _col_info.get('median'), 'min': _col_info.get('min'), 'max': _col_info.get('max'), 'std': _col_info.get('std'), 'skewness': _col_info.get('skew'), 'count': None}
            elif _col_type == 'categorical':
                _pack_categorical[_col_name] = {'unique_values': _col_info.get('unique_count'), 'most_common': _col_info.get('top_value'), 'top_5_values': _col_info.get('top_5') or {}, 'least_common': _col_info.get('least_common')}
        stats = {'row_count': chat_context_pack.get('row_count'), 'column_count': chat_context_pack.get('column_count'), 'numeric_columns': _pack_numeric, 'categorical_columns': _pack_categorical, 'dataset_profile': chat_context_pack.get('profile'), 'data_quality': chat_context_pack.get('quality'), 'strong_correlations': chat_context_pack.get('correlations') or []}
        insights = {'findings': chat_context_pack.get('key_findings') or [], 'headline': chat_context_pack.get('headline') or ''}
        file_name = context.get('fileName') or 'dataset'
        charts_data = context.get('charts') or {}
        file_hash = context.get('file_hash')
    else:
        stats = context.get('stats') or context.get('stats_summary') or {}
        insights = context.get('insights') or {}
        file_name = context.get('fileName') or 'dataset'
        charts_data = context.get('charts', {})
        file_hash = context.get('file_hash')
    if file_hash:
        try:
            _fh_row = await db.execute(_sa_select(_AnalysisHistory.id).where(_AnalysisHistory.user_id == user_id, _AnalysisHistory.file_hash == file_hash).limit(1))
            if _fh_row.scalar_one_or_none() is None:
                logger.warning('Unauthorized file_hash access: user_id=%s hash=%s', user_id, file_hash)
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='You do not have access to this analysis.')
        except HTTPException:
            raise
        except Exception as _fh_exc:
            logger.error('file_hash ownership check failed: %s', _fh_exc)
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail='Could not verify dataset access. Please try again.')
    df_records: list = []
    using_preview_only = False
    if file_hash and ENABLE_DISK_CACHE:
        try:
            assert _re.match('^[a-f0-9]{64}$', file_hash), 'Invalid file hash'
            storage_path = os.path.join(_PARQUET_STORAGE_DIR, f'{file_hash}.parquet')
            if os.path.exists(storage_path):
                df_full = pd.read_parquet(storage_path)
                df_records = df_full.to_dict('records')
                logger.info('Chat: Loaded %d rows from Parquet for on-demand chart gen', len(df_full))
            else:
                using_preview_only = True
        except Exception as load_exc:
            logger.warning('Chat: Failed to load full dataset from Parquet: %s', load_exc)
            using_preview_only = True

    # Production fallback: load full clean_data from DB when Parquet is unavailable
    if not df_records and file_hash:
        try:
            _db_row = await db.execute(
                _sa_select(_AnalysisHistory.clean_data).where(
                    _AnalysisHistory.user_id == user_id,
                    _AnalysisHistory.file_hash == file_hash,
                ).limit(1)
            )
            _clean_data_str = _db_row.scalar_one_or_none()
            if _clean_data_str:
                import orjson as _orjson
                _db_records = _orjson.loads(_clean_data_str)
                if isinstance(_db_records, list) and _db_records:
                    df_records = _db_records
                    using_preview_only = False
                    logger.info('Chat: Loaded %d rows from DB clean_data fallback', len(df_records))
        except Exception as _db_exc:
            logger.warning('Chat: DB clean_data fallback failed: %s', _db_exc)
    if not df_records:
        df_records = context.get('clean_df') or context.get('cleanDf') or context.get('clean_data') or []
    if not isinstance(df_records, list):
        df_records = []
    dashboard_keys = list(charts_data.keys()) if isinstance(charts_data, dict) else []
    generated_keys = context.get('generated_chart_keys') or []
    if not isinstance(generated_keys, list):
        generated_keys = []
    existing_chart_keys = list(dict.fromkeys(dashboard_keys + generated_keys))
    intent = _classify_chat_intent(question)
    logger.info("Chat intent classified as '%s' for question: %s", intent, question[:80])
    if intent == 'off_topic':
        return {'answer': "I'm a data analysis assistant focused on your dataset. I can help you analyze trends, summarize data, and build charts, but I can't answer off-topic questions or write general code.", 'data_queried': False, 'new_chart': None}
    if intent == 'greeting':
        _q = question.lower()
        if any((t in _q for t in ('bye', 'goodbye', 'see you', 'later', 'cya'))):
            msg = f'Goodbye! Your analysis of {file_name} is saved in History. Come back anytime.'
        elif any((t in _q for t in ('thanks', 'thank you', 'thx', 'ty', 'thank'))):
            msg = f'Happy to help! Let me know if you have more questions about {file_name}.'
        elif any((t in _q for t in ('who are you',))):
            msg = "I'm your AI data analyst. I answer questions about your dataset, find correlations, and generate charts on demand."
        elif any((t in _q for t in ('how are you',))):
            msg = 'Running smoothly! Ready to dig into your data whenever you are.'
        else:
            msg = f"Hello! I'm your AI analyst for '{file_name}'. Ask me about statistics, relationships, trends — or say 'generate a chart' to create a new visualization."
        return {'answer': msg, 'data_queried': False, 'new_chart': None}
    if intent == 'generate_chart':
        from .agents.plot_generator import generate_on_demand_chart, suggest_novel_chart
        chart_df_records = df_records
        filter_label = ''
        if df_records:
            q_lower = question.lower()
            _chart_df = pd.DataFrame(df_records)
            _year_match = _re.search('(?:in\\s+)?(?:year|yr)\\s*(\\d{4})', q_lower)
            if not _year_match:
                _year_match = _re.search('(?:for|of|from)\\s+(\\d{4})\\s*(?:only)?', q_lower)
            if _year_match:
                _target_year = int(_year_match.group(1))
                _year_col = None
                for _col in _chart_df.columns:
                    if _chart_df[_col].dtype in ('int64', 'float64', 'int32'):
                        _col_vals = _chart_df[_col].dropna()
                        if len(_col_vals) > 0:
                            (_mn, _mx) = (_col_vals.min(), _col_vals.max())
                            if 1900 <= _mn <= 2100 and 1900 <= _mx <= 2100:
                                _year_col = _col
                                break
                    if 'date' in str(_chart_df[_col].dtype).lower():
                        _year_col = _col
                        break
                if _year_col is not None:
                    try:
                        if _chart_df[_year_col].dtype in ('int64', 'float64', 'int32'):
                            _filtered = _chart_df[_chart_df[_year_col] == _target_year]
                        else:
                            _chart_df[_year_col] = pd.to_datetime(_chart_df[_year_col], errors='coerce')
                            _filtered = _chart_df[_chart_df[_year_col].dt.year == _target_year]
                        if len(_filtered) > 0:
                            chart_df_records = _filtered.to_dict('records')
                            filter_label = f' (filtered to year {_target_year})'
                            logger.info("Chart filter: %d rows for year %d from column '%s'", len(_filtered), _target_year, _year_col)
                    except Exception as _filt_exc:
                        logger.warning('Year filter failed: %s', _filt_exc)
            if not filter_label:
                _cat_cols = stats.get('categorical_columns') or {}
                for (_cat_name, _cat_info) in _cat_cols.items():
                    _top_vals = _cat_info.get('top_5_values') or _cat_info.get('top_values') or {}
                    for _val_name in _top_vals:
                        if str(_val_name).lower() in q_lower and len(str(_val_name)) > 2:
                            try:
                                _filtered = _chart_df[_chart_df[_cat_name].astype(str).str.lower() == str(_val_name).lower()]
                                if len(_filtered) > 5:
                                    chart_df_records = _filtered.to_dict('records')
                                    filter_label = f' (filtered to {_val_name})'
                                    logger.info("Chart filter: %d rows for %s='%s'", len(_filtered), _cat_name, _val_name)
                                    break
                            except Exception:
                                pass
                    if filter_label:
                        break
        novel = suggest_novel_chart(df_records=chart_df_records, existing_chart_keys=existing_chart_keys, user_request=question, stats_summary=stats)
        if novel.get('cannot_plot'):
            reason = novel.get('reason', "I've already plotted all the most useful column combinations for this dataset.")
            return {'answer': reason, 'data_queried': False, 'new_chart': None}
        chart_result = generate_on_demand_chart(spec=novel['spec'], df_records=chart_df_records, existing_chart_keys=existing_chart_keys)
        if chart_result.get('is_duplicate'):
            return {'answer': "You already have this chart on your dashboard! If you'd like to see something else, tell me which columns to plot.", 'data_queried': False, 'new_chart': None}
        if chart_result.get('error'):
            return {'answer': chart_result['error'], 'data_queried': False, 'new_chart': None}
        reasoning = novel.get('reasoning', '')
        answer = reasoning + filter_label if reasoning else f'Here is the chart you requested{filter_label}.'
        return {'answer': answer, 'data_queried': False, 'new_chart': chart_result}
    if intent == 'explain_chart':
        if not existing_chart_keys:
            return {'answer': "There aren't any charts on the dashboard yet. If you'd like me to generate one, just ask!", 'data_queried': False, 'new_chart': None}
        q_lower = question.lower()
        matched_key = None

        # Pass 1: exact key name substring match
        for key in existing_chart_keys:
            if key.lower() in q_lower:
                matched_key = key
                break

        # Pass 2: chart-type keyword → 'in' partial key match (not startswith — too strict)
        if not matched_key:
            chart_type_map = {
                'scatter':      ['scatter'],
                'histogram':    ['histogram', 'distribution'],
                'bar':          ['bar'],
                'heatmap':      ['heatmap'],
                'correlation':  ['heatmap', 'correlation'],
                'overview':     ['heatmap', 'correlation', 'overview'],
                'line':         ['line', 'trend'],
                'box':          ['box'],
                'violin':       ['violin'],
                'donut':        ['donut'],
                'pie':          ['donut', 'pie'],
                'distribution': ['histogram', 'distribution'],
            }
            for word, prefixes in chart_type_map.items():
                if word in q_lower:
                    for prefix in prefixes:
                        for key in existing_chart_keys:
                            if prefix in key.lower():
                                matched_key = key
                                break
                        if matched_key:
                            break
                if matched_key:
                    break

        # Pass 3: token overlap scoring — dataset-agnostic, handles any column name
        if not matched_key:
            q_tokens = set(_re.sub(r'[^\w]', ' ', q_lower).split())
            best_key, best_score = None, 0
            for key in existing_chart_keys:
                key_tokens = set(_re.sub(r'[^\w]', ' ', key.lower()).split())
                score = len(q_tokens & key_tokens)
                if score > best_score:
                    best_score, best_key = score, key
            if best_score > 0:
                matched_key = best_key

        # Pass 4: fallback to first chart (most prominent on dashboard)
        _explain_key = matched_key or existing_chart_keys[0]

        # Load chart data — try context first, then Redis once with the correct key
        _chart_raw = None
        if isinstance(charts_data, dict) and _explain_key in charts_data:
            _raw = charts_data[_explain_key]
            # True/None/bool means frontend sent a placeholder stub — need Redis
            if _raw and _raw is not True:
                _chart_raw = _raw

        if not _chart_raw and file_hash:
            try:
                _cache_key = redis_cache.analysis_key(user_id, file_hash)
                _cached = await redis_cache.get(_cache_key)
                if isinstance(_cached, dict):
                    _chart_raw = _cached.get('charts', {}).get(_explain_key)
            except Exception as _redis_exc:
                logger.warning('Redis lookup for explain_chart failed: %s', _redis_exc)

        _explain_client = get_groq_client()
        if _explain_client and file_hash:
            _expl_redis = _get_cache_client()
            explain_result = await answer_chart_explanation(
                question=question,
                chart_key=_explain_key,
                chart_data=_chart_raw,
                file_name=file_name,
                file_hash=file_hash,
                chart_keys=existing_chart_keys,
                stats=stats,
                insights=insights,
                conversation_history=body.history or [],
                groq_client=_explain_client,
                redis_client=_expl_redis,
            )
            return explain_result
        _tag = f'\n[CHART: {_explain_key}]' if _explain_key else ''
        return {'answer': f'Here is the chart from {file_name}.{_tag}', 'data_queried': False, 'new_chart': None}
    _data_client = get_groq_client()
    if _data_client and file_hash:
        _data_redis = _get_cache_client()
        ans_result = await answer_question(question=question, file_hash=file_hash, file_name=file_name, stats=stats, insights=insights, chart_keys=existing_chart_keys, conversation_history=body.history or [], groq_client=_data_client, redis_client=_data_redis)
        return ans_result
    return {'answer': f"The dataset '{file_name}' has {stats.get('row_count', '?')} rows and {stats.get('column_count', '?')} columns. I need a valid Groq API key to answer specific questions about it.", 'data_queried': False, 'new_chart': None}