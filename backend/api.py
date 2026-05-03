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
from sqlalchemy import select as _sa_select
from pydantic import BaseModel, field_validator
from typing import Any, Annotated

import re as _re

_INJECTION_PATTERNS = [
    r"ignore\s+(all\s+)?previous\s+instructions",
    r"you\s+are\s+now\s+a",
    r"act\s+as\s+(if\s+you\s+are\s+)?a",
    r"disregard\s+(all\s+)?prior",
    # FIX 40: Bound whitespace quantifiers to reduce regex backtracking risk
    r"system\s{0,5}:\s{0,5}",
    r"<\s*system\s*>",
    r"<\s*/?inst\s*>",
    r"\[INST\]",
    r"###\s*instruction",
    r"forget\s+(all\s+)?previous",
    r"new\s+persona",
    r"pretend\s+(you\s+are|to\s+be)",
    r"bypassing",
    r"jailbreak",
    r"from\s+now\s+on",
    r"do\s+not\s+obey",
]
_INJECTION_RE = _re.compile("|".join(_INJECTION_PATTERNS), _re.IGNORECASE)



def sanitize_chat_input(text: str) -> str:
    """Strip prompt injection patterns and null bytes from user input."""
    text = text.replace("\x00", "").replace("\r", " ")
    text = _INJECTION_RE.sub("[removed]", text)
    return text.strip()

from .core import cache as redis_cache
from .core.cache import _get_client as _get_cache_client
from .analysis_history import (
    _serialize_charts,
    compute_file_hash,
    delete_analysis,
    get_analysis_by_id,
    get_analysis_by_hash,
    get_user_analysis_history,
    save_analysis,
)
from .auth import (
    create_access_token,
    get_user_by_id,
    login_user,
    register_user,
    request_password_reset,
    reset_password_with_token,
    verify_access_token,
    verify_google_token,
    login_google_user,
    create_refresh_token,
    verify_refresh_token,
    REFRESH_EXPIRE_DAYS,
)
from .core.constants import APP_VERSION, PIPELINE_VERSION
from .core.graph import run_pipeline
from .core.logging_config import configure_logging
from .core.upload_parsing import read_csv_with_fallback, validate_upload_magic
from .core.utils import truncate_stats_for_llm, build_chat_context_pack
from .core.data_agent import run_data_query
from .rag.indexer import build_rag_index, retrieve_chunks
from .rag.chat_engine import answer_question, answer_chart_explanation
from .rag.pinecone_client import ping as pinecone_ping
from .core.llm_client import get_groq_client
from .db import get_db, init_db, AnalysisHistory as _AnalysisHistory
from .models.schemas import (
    AnalysisListResponse,
    AuthResponse,
    ChatRequest,
    DeleteResponse,
    ForgotPasswordRequest,
    ForgotPasswordResponse,
    GoogleLoginRequest,
    HealthResponse,
    ResetPasswordRequest,
    TokenResponse,
    UserLogin,
    UserRegister,
    UserResponse,
)

configure_logging()
logger = logging.getLogger(__name__)

APP_ENV = os.getenv("APP_ENV", "production")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()
FRONTEND_GOOGLE_CLIENT_ID = (
    os.getenv("FRONTEND_GOOGLE_CLIENT_ID", "").strip()
    or os.getenv("VITE_GOOGLE_CLIENT_ID", "").strip()
)
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(10 * 1024 * 1024))) # 10 MB limit for strict RAM bounds
MAX_ANALYZE_ROWS = int(os.getenv("MAX_ANALYZE_ROWS", "15000")) # Lowered to 15K rows for 500MB Render memory limit
MAX_ANALYZE_COLUMNS = int(os.getenv("MAX_ANALYZE_COLUMNS", "150"))
MAX_EXCEL_SHEETS = int(os.getenv("MAX_EXCEL_SHEETS", "5"))
MAX_QUESTION_CHARS = int(os.getenv("CHAT_MAX_QUESTION_CHARS", "1200"))
MAX_CONTEXT_BYTES = int(os.getenv("CHAT_MAX_CONTEXT_BYTES", str(4 * 1024 * 1024)))  # Increased to 4 MB to handle long histories
READ_CHUNK_BYTES = 1024 * 1024
CHAT_RATE_LIMIT = int(os.getenv("CHAT_RATE_LIMIT", "10"))
CHAT_RATE_WINDOW = int(os.getenv("CHAT_RATE_WINDOW_SECONDS", "60"))

INTENT_MODEL    = os.getenv("GROQ_INTENT_MODEL", "llama-3.1-8b-instant")   # fast, structured
SYNTHESIS_MODEL = os.getenv("GROQ_SYNTHESIS_MODEL", "llama-3.3-70b-versatile")  # smart, grounded

# Groq client is now a shared singleton in core.llm_client


_raw_origins = os.getenv("CORS_ORIGINS", "http://localhost:5173,http://localhost:3000")
origins = [o.strip() for o in _raw_origins.split(",") if o.strip()]


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting DataPulse API v2 (pipeline %s)", PIPELINE_VERSION)

    if not os.getenv("GROQ_API_KEY"):
        raise RuntimeError(
            "GROQ_API_KEY environment variable is not set. "
            "The application cannot start without it. "
            "Set it in your .env file or environment."
        )

    if APP_ENV == "production" and "*" in origins:
        raise RuntimeError("CORS_ORIGINS cannot contain '*' in production")

    if GOOGLE_CLIENT_ID and FRONTEND_GOOGLE_CLIENT_ID and GOOGLE_CLIENT_ID != FRONTEND_GOOGLE_CLIENT_ID:
        raise RuntimeError(
            "Google OAuth Client ID mismatch between backend and frontend configuration"
        )

    # ── Eagerly warm up connections so first request is instant ──────────────
    await init_db()

    # Warm DB pool: open one real connection now so asyncpg doesn't cold-start
    try:
        from sqlalchemy import text
        from .db import engine
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("DB connection pool warmed up")
    except Exception as exc:
        logger.warning("DB warmup failed (non-fatal): %s", exc)

    # Warm Redis: open the connection now instead of on first request
    try:
        redis_ok = await redis_cache.ping()
        logger.info("Redis warmed up (reachable=%s)", redis_ok)
    except Exception as exc:
        logger.warning("Redis warmup failed (non-fatal): %s", exc)

    # Warm up Pinecone connection (lazy init, non-blocking if key missing)
    try:
        pc_ok = await asyncio.to_thread(pinecone_ping)
        logger.info("Pinecone warmed up (reachable=%s)", pc_ok)
    except Exception as exc:
        logger.warning("Pinecone warmup failed (non-fatal): %s", exc)
    # ─────────────────────────────────────────────────────────────────────────

    yield
    await redis_cache.close()
    logger.info("DataPulse API shutdown complete")


app = FastAPI(
    title="DataPulse API",
    description="Multi-agent CSV analysis API",
    version=APP_VERSION,
    lifespan=lifespan,
)

app.add_middleware(GZipMiddleware, minimum_size=1024)  # compress responses > 1KB
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    return response

@app.exception_handler(Exception)
async def catch_all_exception_handler(request: Request, exc: Exception):
    logger.error("Unhandled error on %s %s", request.method, request.url.path, exc_info=exc)
    
    if os.getenv("APP_ENV", "production") == "development":
        error_trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        return JSONResponse(status_code=500, content={"detail": str(exc), "trace": error_trace})
        
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})



security = HTTPBearer()


async def get_current_user_id(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
) -> int:
    token = credentials.credentials
    payload = verify_access_token(token)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return int(payload["sub"])


async def check_ip_rate_limit(request: Request):
    client_ip = request.client.host if request.client else "unknown"
    key = f"ratelimit:ip:{client_ip}"
    try:
        count = await redis_cache.increment_with_ttl(key, 60)
        if count > 20:
            raise HTTPException(status_code=429, detail="Too many requests from this IP. Please try again in a minute.")
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise exc
        logger.warning(f"Rate limiting failed for {key}: {exc}")

async def check_user_rate_limit(user_id: int = Depends(get_current_user_id)):
    key = f"ratelimit:user_analyze:{user_id}"
    try:
        count = await redis_cache.increment_with_ttl(key, 60)
        if count > 5:
            raise HTTPException(status_code=429, detail="Too many analysis requests. Please try again in a minute.")
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise exc
        logger.warning(f"Rate limiting failed for {key}: {exc}")
    return user_id



@app.get("/health", response_model=HealthResponse, tags=["system"])
async def health_check(db: AsyncSession = Depends(get_db)):
    """Liveness probe — returns Postgres and Redis reachability."""
    pg_ok = False
    try:
        await db.execute(__import__("sqlalchemy").text("SELECT 1"))
        pg_ok = True
    except Exception:
        pass

    redis_ok = await redis_cache.ping()

    return HealthResponse(
        status="ok" if (pg_ok and redis_ok) else "degraded",
        postgres=pg_ok,
        redis=redis_ok,
    )




@app.post("/auth/register", response_model=AuthResponse, status_code=status.HTTP_201_CREATED, tags=["auth"], dependencies=[Depends(check_ip_rate_limit)])
async def register(body: UserRegister, db: AsyncSession = Depends(get_db)):
    result = await register_user(db, body.email, body.password, body.name)
    if not result["success"]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result["message"])
    
    # FIX: Use real DB timestamps from register_user instead of fabricating utcnow()
    user_response = UserResponse(
        id=result["user_id"],
        name=result.get("name"),
        email=result["email"],
        created_at=result.get("created_at") or datetime.now(tz=timezone.utc),
        updated_at=result.get("updated_at") or datetime.now(tz=timezone.utc),
    )
    
    return AuthResponse(
        success=True, 
        message=result["message"],
        user=user_response
    )


@app.post("/auth/login", response_model=TokenResponse, tags=["auth"], dependencies=[Depends(check_ip_rate_limit)])
async def login(body: UserLogin, db: AsyncSession = Depends(get_db), response: Response = None):
    result = await login_user(db, body.email, body.password)
    if not result["success"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=result["message"],
            headers={"WWW-Authenticate": "Bearer"},
        )
    user_obj = result["user"]
    # create an HttpOnly refresh cookie and return the short-lived access token
    try:
        refresh_token = create_refresh_token(user_obj["id"], user_obj["email"])

        if response is not None:
            response.set_cookie(
                key="datapulse_refresh",
                value=refresh_token,
                httponly=True,
                secure=(APP_ENV == "production"),
                samesite="lax",
                max_age=REFRESH_EXPIRE_DAYS * 24 * 3600,
                path="/",
            )
    except Exception:
        logger.exception("Failed to create refresh token")
    return TokenResponse(
        access_token=result["access_token"],
        token_type="bearer",
        user=UserResponse(
            id=user_obj["id"],
            name=user_obj.get("name"),
            email=user_obj["email"],
            created_at=user_obj["created_at"],
            updated_at=user_obj["updated_at"],
        ),
    )


@app.post("/auth/forgot-password", response_model=ForgotPasswordResponse, tags=["auth"], dependencies=[Depends(check_ip_rate_limit)])
async def forgot_password(body: ForgotPasswordRequest, db: AsyncSession = Depends(get_db)):
    result = await request_password_reset(db, body.email)
    return ForgotPasswordResponse(
        success=True,
        message=result.get("message", "If an account exists for that email, a password reset link has been sent."),
        debug_reset_token=result.get("debug_reset_token"),
    )


@app.post("/auth/reset-password", response_model=AuthResponse, tags=["auth"], dependencies=[Depends(check_ip_rate_limit)])
async def reset_password(body: ResetPasswordRequest, db: AsyncSession = Depends(get_db)):
    result = await reset_password_with_token(db, body.token, body.new_password)
    if not result.get("success"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result.get("message", "Password reset failed"))
    return AuthResponse(success=True, message=result["message"])


@app.post("/auth/google", response_model=TokenResponse, tags=["auth"])
async def login_with_google(body: GoogleLoginRequest, db: AsyncSession = Depends(get_db), response: Response = None):
    credential = body.credential
    if not credential:
        raise HTTPException(status_code=400, detail="Missing Google credential")

    frontend_client_id = (body.client_id or "").strip()
    if frontend_client_id and GOOGLE_CLIENT_ID and frontend_client_id != GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=400, detail="Google client ID mismatch")

    # FIX 43: Move blocking Google token verification off the async event loop
    idinfo = await asyncio.to_thread(verify_google_token, credential)
    if not idinfo:
        raise HTTPException(status_code=401, detail="Invalid Google token")
    if GOOGLE_CLIENT_ID and idinfo.get("aud") != GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=401, detail="Invalid Google token audience")

    email = idinfo.get("email")
    google_id = idinfo.get("sub")
    name = idinfo.get("name")
    if not email:
        raise HTTPException(status_code=400, detail="No email provided by Google")

    result = await login_google_user(db, email, google_id, name)
    if not result["success"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=result["message"],
        )
    
    user_obj = result["user"]
    try:
        refresh_token = create_refresh_token(user_obj["id"], user_obj["email"])
        if response is not None:
            response.set_cookie(
                key="datapulse_refresh",
                value=refresh_token,
                httponly=True,
                secure=(APP_ENV == "production"),
                samesite="lax",
                max_age=REFRESH_EXPIRE_DAYS * 24 * 3600,
                path="/",
            )
    except Exception:
        logger.exception("Failed to create refresh token for google login")

    return TokenResponse(
        access_token=result["access_token"],
        token_type="bearer",
        user=UserResponse(
            id=user_obj["id"],
            name=user_obj.get("name"),
            email=user_obj["email"],
            created_at=user_obj["created_at"],
            updated_at=user_obj["updated_at"],
        ),
    )



@app.post("/auth/refresh", response_model=TokenResponse, tags=["auth"])
async def refresh_token(request: Request, response: Response, db: AsyncSession = Depends(get_db)):
    token = request.cookies.get("datapulse_refresh")
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing refresh token")
    payload = verify_refresh_token(token)
    if payload is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired refresh token")
    user_id = int(payload["sub"])
    user = await get_user_by_id(db, user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    access_token = create_access_token(user.id, user.email)
    try:
        new_refresh = create_refresh_token(user.id, user.email)
        response.set_cookie(
            key="datapulse_refresh",
            value=new_refresh,
            httponly=True,
            secure=(APP_ENV == "production"),
            samesite="lax",
            max_age=REFRESH_EXPIRE_DAYS * 24 * 3600,
            path="/",
        )
    except Exception:
        logger.exception("Failed to rotate refresh token")

    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        user=UserResponse(
            id=user.id,
            name=user.name,
            email=user.email,
            created_at=user.created_at,
            updated_at=user.updated_at,
        ),
    )


@app.post("/auth/logout", tags=["auth"])
async def logout(response: Response):

    response.delete_cookie("datapulse_refresh", path="/")
    return {"success": True, "message": "Logged out"}


@app.get("/auth/me", response_model=UserResponse, tags=["auth"])
async def me(
    user_id: Annotated[int, Depends(get_current_user_id)],
    db: AsyncSession = Depends(get_db),
):
    user = await get_user_by_id(db, user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return UserResponse(
        id=user.id,
        name=user.name,
        email=user.email,
        created_at=user.created_at,
        updated_at=user.updated_at,
    )





# ── Utility for background storage ───────────────────────────────────────
# Use an absolute path so the file is always written to the same location
# that data_agent.py reads from, regardless of the server's working directory.
_API_FILE_DIR = os.path.dirname(os.path.abspath(__file__))   # .../backend/
_PARQUET_STORAGE_DIR = os.path.join(_API_FILE_DIR, "storage", "data")

def persist_full_data_backend(df: pd.DataFrame, file_hash: str):
    """Saves cleaned DataFrame to Parquet in the background."""
    try:
        # FIX 42: Enforce strict hash format before constructing parquet path.
        assert _re.match(r"^[a-f0-9]{64}$", file_hash), "Invalid file hash"
        os.makedirs(_PARQUET_STORAGE_DIR, exist_ok=True)
        storage_path = os.path.join(_PARQUET_STORAGE_DIR, f"{file_hash}.parquet")
        df.to_parquet(storage_path, index=False)
        logger.info("Background storage: Saved full data to %s", storage_path)
    except Exception as exc:
        logger.warning("Background storage failed for %s: %s", file_hash, exc)


def cleanup_old_parquet_files(retention_days: int = 3):
    """Delete stale parquet files from backend storage to limit disk growth."""
    try:
        os.makedirs(_PARQUET_STORAGE_DIR, exist_ok=True)
        cutoff = datetime.now() - timedelta(days=max(0, retention_days))

        deleted = 0
        # FIX 48: Cap per-run scan to avoid long cleanup loops.
        files_checked = 0
        MAX_CHECK = 500
        for name in os.listdir(_PARQUET_STORAGE_DIR):
            if files_checked > MAX_CHECK:
                break
            files_checked += 1
            if not name.lower().endswith(".parquet"):
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
                logger.warning("Parquet cleanup skipped %s: %s", path, file_exc)

        if deleted:
            logger.info("Parquet cleanup removed %d stale file(s)", deleted)
    except Exception as exc:
        logger.warning("Parquet cleanup failed: %s", exc)


def _stratified_preview(df: pd.DataFrame, n: int) -> list:
    if len(df) <= n:
        # FIX 49: Preserve datetime readability in preview payload.
        df_preview = df.copy()
        for col in df_preview.select_dtypes(include=["datetime64"]).columns:
            df_preview[col] = df_preview[col].dt.strftime("%Y-%m-%d %H:%M:%S")
        return json.loads(df_preview.to_json(orient="records"))
    # Sample uniformly across the index so edge values are represented
    step = max(1, len(df) // n)
    sampled = df.iloc[::step].head(n)
    df_preview = sampled.copy()
    for col in df_preview.select_dtypes(include=["datetime64"]).columns:
        df_preview[col] = df_preview[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    return json.loads(df_preview.to_json(orient="records"))


@app.post("/analyze", tags=["analysis"])
async def analyze(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    user_id: int = Depends(check_user_rate_limit), # check_user_rate_limit now returns user_id
    db: AsyncSession = Depends(get_db),
):
    logger.debug("analyze endpoint invoked, has_filename=%s", bool(file.filename))

    # FIX 41: Sanitize uploaded filename before hashing and downstream usage.
    filename = os.path.basename(file.filename or "upload").strip()
    parsed_ext = filename.lower().split('.')[-1] if '.' in filename else ""
    if parsed_ext not in ("csv", "xlsx", "xls"):
        raise HTTPException(status_code=400, detail="Only CSV and Excel files are accepted")

    total = 0
    chunks: list[bytes] = []
    while True:
        chunk = await file.read(READ_CHUNK_BYTES)
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_UPLOAD_BYTES:
            raise HTTPException(
                status_code=413,
                detail=f"File too large. Max allowed size is {MAX_UPLOAD_BYTES // (1024 * 1024)} MB",
            )
        chunks.append(chunk)
    file_bytes = b"".join(chunks)

    if len(file_bytes) == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    validate_upload_magic(parsed_ext, file_bytes)

    file_hash = compute_file_hash(file_bytes, filename)
    # Enforce strict hash format before any filesystem usage.
    assert _re.match(r"^[a-f0-9]{64}$", file_hash), "Invalid file hash"


    cached = await get_analysis_by_hash(db, user_id, file_hash)
    if (
        cached
        and cached.get("stats_summary")
        and cached.get("insights")
        and not (cached.get("errors") or [])
        and cached.get("pipeline_version") == PIPELINE_VERSION
    ):
        cached["charts"] = {k: v for k, v in (cached.get("charts") or {}).items()}
        return {"from_cache": True, **cached}
    elif cached:
        logger.info("Cache not eligible for reuse for user %d / %s — re-running pipeline", user_id, filename)

    await db.rollback()


    try:
        if parsed_ext == "csv":
            df = read_csv_with_fallback(
                file_bytes,
                max_analyze_rows=MAX_ANALYZE_ROWS,
                max_analyze_columns=MAX_ANALYZE_COLUMNS,
            )
        else:
            if parsed_ext == "xlsx":
                from openpyxl import load_workbook

                wb = load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
                try:
                    sheet_count = len(wb.sheetnames)
                    if sheet_count > MAX_EXCEL_SHEETS:
                        raise HTTPException(
                            status_code=413,
                            detail=f"Excel file has {sheet_count} sheets. Maximum allowed is {MAX_EXCEL_SHEETS}.",
                        )

                    active_sheet = wb[wb.sheetnames[0]]
                    if (active_sheet.max_row or 0) > MAX_ANALYZE_ROWS:
                        raise HTTPException(
                            status_code=413,
                            detail=f"Excel file has {active_sheet.max_row} rows. Maximum allowed is {MAX_ANALYZE_ROWS}.",
                        )
                    if (active_sheet.max_column or 0) > MAX_ANALYZE_COLUMNS:
                        raise HTTPException(
                            status_code=413,
                            detail=f"Excel file has {active_sheet.max_column} columns. Maximum allowed is {MAX_ANALYZE_COLUMNS}.",
                        )
                finally:
                    wb.close()

                df = pd.read_excel(
                    io.BytesIO(file_bytes),
                    engine="openpyxl",
                    sheet_name=0,
                    nrows=MAX_ANALYZE_ROWS + 1,
                )
            else:
                try:
                    import xlrd  # noqa: F401
                except Exception:
                    raise HTTPException(
                        status_code=422,
                        detail="Legacy .xls upload requires the 'xlrd' package. Please upload CSV/XLSX or install xlrd on the backend.",
                    )

                df = pd.read_excel(
                    io.BytesIO(file_bytes),
                    engine="xlrd",
                    sheet_name=0,
                    nrows=MAX_ANALYZE_ROWS + 1,
                )
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise exc
        raise HTTPException(status_code=422, detail=f"Could not parse file: {exc}")

    row_count, column_count = df.shape
    if row_count > MAX_ANALYZE_ROWS:
        raise HTTPException(
            status_code=413,
            detail=f"Dataset has {row_count} rows. Maximum allowed is {MAX_ANALYZE_ROWS}.",
        )
    if column_count > MAX_ANALYZE_COLUMNS:
        raise HTTPException(
            status_code=413,
            detail=f"Dataset has {column_count} columns. Maximum allowed is {MAX_ANALYZE_COLUMNS}.",
        )

    # Run the agentic pipeline
    state = await asyncio.to_thread(run_pipeline, df)

    # ── Full Data Persistence (Background) ───────────────────────────────────
    SYNC_PARQUET_THRESHOLD_ROWS = 5000

    if getattr(state, "clean_df", None) is not None:
        df_to_save = state.clean_df.copy()
        if len(df_to_save) <= SYNC_PARQUET_THRESHOLD_ROWS:
            # Small file — save synchronously so chat works immediately
            persist_full_data_backend(df_to_save, file_hash)
        else:
            # Large file — background is fine, user won't chat instantly
            background_tasks.add_task(persist_full_data_backend, df_to_save, file_hash)
        background_tasks.add_task(cleanup_old_parquet_files, 3)

    # ── Strip full DataFrames BEFORE model_dump to prevent serialising
    # 30k rows as Python dicts (≈500 MB RAM spike). ───────────────────────────
    CHART_PREVIEW_ROWS = int(os.getenv("CHART_PREVIEW_ROWS", "500"))

    preview_raw = _stratified_preview(state.raw_df, CHART_PREVIEW_ROWS) if getattr(state, "raw_df", None) is not None else []
    
    preview_clean = _stratified_preview(state.clean_df, CHART_PREVIEW_ROWS) if getattr(state, "clean_df", None) is not None else []
    
    state.raw_df   = None
    state.clean_df = None

    result = state.model_dump()
    result["raw_df"]   = preview_raw
    result["clean_df"] = preview_clean
    result["file_hash"] = file_hash  # Crucial for chat context

    # Only hard-fail if BOTH stats and insights are completely empty.
    # Partial data (e.g., architect succeeded but statistician failed) still yields a usable page.
    has_stats    = bool(result.get("stats_summary") and result["stats_summary"].get("row_count"))
    has_insights = bool(result.get("insights") and result["insights"].get("findings"))
    # True last-resort: literally nothing was produced
    is_fatal = not has_stats and not has_insights

    if is_fatal:
        logger.error("Pipeline critically failed for user %d / %s: %s", user_id, filename, state.errors)
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Analysis pipeline failed — no statistics or insights were produced. Please check the dataset format and try again.",
                "errors": [str(e) for e in state.errors],
            },
        )
    elif state.errors:
        # Non-fatal warnings: log them but continue — the result is still usable
        logger.warning(
            "Pipeline completed with non-fatal errors for user %d / %s: %s",
            user_id, filename, state.errors,
        )

    # Fix #6: compute serialized charts once, reuse for both DB save and response
    serialized_charts = _serialize_charts(result.get("charts") or {})

    save_result = await save_analysis(
        db=db,
        user_id=user_id,
        file_name=filename,
        file_hash=file_hash,
        file_size=len(file_bytes),
        analysis_result=result,
        serialized_charts=serialized_charts,
    )
    if not save_result["success"]:
        logger.warning("Failed to persist analysis: %s", save_result["message"])
    else:
        result["analysis_id"] = save_result.get("analysis_id")

    # ── Build RAG index in background ────────────────────────────────────────
    _rag_stats    = result.get("stats_summary") or {}
    _rag_insights = result.get("insights") or {}
    _rag_charts   = serialized_charts or {}
    _rag_hash     = file_hash

    async def _index_rag_background():
        try:
            _redis = _get_cache_client()
            n = await build_rag_index(
                file_hash=_rag_hash,
                stats=_rag_stats,
                insights=_rag_insights,
                charts=_rag_charts,
                redis_client=_redis,
            )
            logger.info("RAG indexed %d chunks for %s", n, _rag_hash[:8])
        except Exception as _rag_exc:
            logger.warning("RAG indexing failed (non-fatal): %s", _rag_exc)

    background_tasks.add_task(_index_rag_background)

    result["chat_context_pack"] = build_chat_context_pack(
        result.get("stats_summary", {}),
        result.get("insights", {}),
    )
    result["charts"] = serialized_charts
    result["partial"] = False
    if state.errors:
        result["warnings"] = state.errors   # surface non-fatal errors to frontend

    # orjson serialises NaN/Inf → null natively and is ~10x faster than
    # the manual recursive sanitize_floats walk on large result dicts.
    # Added OPT_SERIALIZE_NUMPY to prevent 'Type is not JSON serializable: numpy.float64' errors.
    safe_result = orjson.loads(orjson.dumps(result, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY))

    return {"from_cache": False, "pipeline_version": PIPELINE_VERSION, **safe_result}




@app.get("/history", response_model=AnalysisListResponse, tags=["history"])
async def history(
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
    limit: int = Query(default=20, ge=1, le=100),
):
    items = await get_user_analysis_history(db, user_id, limit=limit)
    return AnalysisListResponse(
        success=True,
        message="History retrieved",
        total=len(items),
        analyses=items,
    )


@app.get("/history/{analysis_id}", tags=["history"])
async def history_item(
    analysis_id: int,
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    item = await get_analysis_by_id(db, user_id, analysis_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Analysis not found or access denied")
    item["charts"] = {k: v for k, v in (item.get("charts") or {}).items()}
    return item


@app.delete("/history/{analysis_id}", response_model=DeleteResponse, tags=["history"])
async def remove_analysis(
    analysis_id: int,
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    result = await delete_analysis(db, user_id, analysis_id)
    if not result["success"]:
        raise HTTPException(status_code=404, detail=result["message"])
    return DeleteResponse(success=True, message=result["message"])


# ── Plot-intent keyword sets ──────────────────────────────────────────────────
_PLOT_GENERATE_KEYWORDS = frozenset({
    # explicit generation verbs
    "generate", "create", "make", "build", "draw",
    # show/give patterns
    "show me a", "give me a", "give me some", "give some",
    "show some", "show a", "show plots", "show charts",
    # new/another patterns
    "new chart", "new plot", "another chart", "another plot",
    "one more chart", "one more plot", "more charts", "more plots",
    "different chart", "different plot", "other chart", "other plot",
    "other plots", "other charts",
    # can you patterns
    "can you plot", "can you chart", "can you generate", "can you create",
    "can you make", "can you show",
    # what's possible patterns
    "what plots", "what charts", "possible plots", "possible charts",
    "what else can", "what other", "any other plot", "any other chart",
    "any more plot", "any more chart",
    # chart type names used as request
    "plot a", "chart a", "histogram", "scatter plot", "bar chart",
    "pie chart", "donut chart", "heatmap", "line chart", "box plot",
    "violin plot", "stacked bar", "frequency chart", "distribution chart",
    "correlation chart", "scatter", "visualize", "visualise",
})

_PLOT_EXPLAIN_KEYWORDS = frozenset({
    "explain", "describe", "what does", "what is", "tell me about",
    "interpret", "analyse", "analyze", "understand", "insight from",
    "what does the chart", "what does this chart", "what does that chart",
    "explain the chart", "explain the plot", "explain the graph",
    "explain the scatter", "explain the histogram", "explain the bar",
    "explain the heatmap", "explain the line", "explain the box",
    "what can i see", "what do i see",
})

_TYPE_DISPLAY = {
    "scatter": "Scatter plot", "histogram": "Histogram",
    "ranked_bar": "Ranked bar chart", "grouped_bar": "Grouped bar chart",
    "bar": "Bar chart", "box": "Box plot", "violin": "Violin plot",
    "donut": "Donut chart", "pie": "Pie chart", "line": "Line chart",
    "heatmap": "Heatmap", "freq_bar": "Frequency bar chart",
    "stacked_bar": "Stacked bar chart",
}

VALID_QUERY_TYPES = {
    "filter_lookup", "value_counts", "filter_group", "filter_aggregate",
    "group_aggregate", "aggregate", "top_n", "bottom_n", "lookup",
    "row_count", "distinct", "search", "correlation", "percentile"
}

class QueryPlan(BaseModel):
    type: str
    params: dict[str, Any] = {}

    @field_validator("type")
    @classmethod
    def validate_type(cls, v):
        if v not in VALID_QUERY_TYPES:
            raise ValueError(f"Unknown query type: {v}")
        return v


def _is_result_plausible(data_result: dict, stats: dict) -> tuple[bool, str]:
    """
    Cross-check data agent result against known stats to catch impossible values.
    Returns (is_plausible, reason_if_not).
    """
    if not data_result or "error" in data_result:
        return False, data_result.get("error", "query failed")
    
    result = data_result.get("result")
    if result == "No rows found." or result is None:
        return False, "empty result"
    
    # FIX 22: Skip plausibility range checks for non-numeric result payloads.
    if not isinstance(result, (int, float)):
        return True, ""

    # Check numeric results against known column ranges
    numeric_cols = stats.get("numeric_columns", {})
    
    # For top_n / aggregate results that return a single number,
    # verify it's within known min/max
    # Find which column was queried
    sort_col = data_result.get("sort_column") or data_result.get("query", "")
    for col, col_stats in numeric_cols.items():
        if col.lower() in sort_col.lower():
            col_min = col_stats.get("min", float("-inf"))
            col_max = col_stats.get("max", float("inf"))
            if not (col_min <= result <= col_max * 1.01):  # 1% tolerance
                return False, f"value {result} outside known range [{col_min}, {col_max}]"
    
    # Check row_count results don't exceed known row_count
    if data_result.get("query", "").startswith("Row count"):
        known_rows = stats.get("row_count", float("inf"))
        if isinstance(result, int) and result > known_rows:
            return False, f"row count {result} exceeds dataset size {known_rows}"
    
    return True, ""


def _classify_chat_intent(question: str) -> str:
    """
    Classify chat intent. Returns one of:
      "generate_chart" - user wants a new chart built
      "explain_chart"  - user wants an existing chart explained
      "data_question"  - user wants factual data answer (RAG handles this)
    """
    q = question.lower().strip()

    # ---- Off-Topic Guardrail --------------------------------------------
    _OFF_TOPIC = (
        "write code", "write python", "write javascript", "write java",
        "how to program", "how to code", "write a function", "help me code",
        "write a script", "write an app", "build an app", "build a website",
        "recipe", "how to cook", "how to bake", "how to make a cake",
        "who is the president", "capital of", "weather in", "sports",
        "tell me a joke", "tell me a story", "write a poem", "write a song",
    )
    if any(ot in q for ot in _OFF_TOPIC):
        return "off_topic"

    # ---- Greeting / small talk (intercept before any data logic) --------
    _PURE_GREETINGS = {
        "hello", "hi", "hey", "howdy", "hiya", "yo",
        "bye", "goodbye", "see you", "see ya", "later", "cya",
        "thanks", "thank you", "thx", "ty", "thank",
        "ok", "okay", "good", "cool", "great", "nice",
        "good morning", "good afternoon", "good evening", "good night",
        "how are you", "how r u", "what's up", "sup", "whats up",
        "who are you", "what can you do", "what can you help",
        "help", "hi there", "hey there", "hello there",
    }
    _DATA_INDICATOR = (
        "chart", "graph", "plot", "column", "row", "data", "value",
        "average", "mean", "max", "min", "count", "total", "sum",
        "trend", "correlation", "distribution", "analysis",
        "generate", "create", "show", "display", "visualize",
        "what", "which", "how many", "how much", "when", "where", "why",
        "does", "did", "is there",
        "histogram", "scatter", "heatmap", "violin", "donut", "pie",
        "bar", "line", "frequency",
    )
    if q in _PURE_GREETINGS:
        return "greeting"
    if len(q.split()) <= 4 and not any(dw in q for dw in _DATA_INDICATOR):
        if any(g in q for g in ("hello", "hi", "bye", "hey", "thanks", "thank", "ok", "okay")):
            return "greeting"

    # ---- Explain triggers (highest priority) ----------------------------
    _EXPLAIN = (
        "explain", "what does this", "what do these",
        "tell me about this chart", "tell me about the chart",
        "interpret", "what can i see", "what am i looking at",
        "why is", "why are", "what patterns", "what trends",
        "describe this chart", "describe the chart",
        "what does this chart", "what does the chart",
        "analyse the chart", "analyze the chart",
        "insight from the chart", "insight from this",
        "what does this plot", "what does the plot",
        "what does this graph", "what does the graph",
        "what is shown in", "what is shown on",
    )
    if any(p in q for p in _EXPLAIN):
        return "explain_chart"

    # ---- Generate triggers (wide net) -----------------------------------
    _NEED_WANT = (
        "i need graph", "i need a graph", "i need chart", "i need a chart",
        "i need plot", "i need a plot", "i need visualization",
        "i want graph", "i want a graph", "i want chart", "i want a chart",
        "i want plot", "i want a plot",
        "need graph", "need chart", "need plot",
        "want graph", "want chart", "want plot",
        "show graph", "show chart", "show plot",
        "show a graph", "show a chart", "show a plot",
        "new chart", "new plot", "new graph",
        "another chart", "another plot", "another graph",
        "different chart", "different plot",
        "one more chart", "one more plot",
        "more charts", "more plots",
        "can you plot", "can you chart", "can you make a",
        "can you generate", "can you create", "can you show me a",
        "can you visualize", "can you visualise",
        "generate chart", "generate graph", "generate plot",
        "create chart", "create graph", "create plot",
        "make chart", "make graph", "make plot",
        "make a chart", "make a graph", "make a plot",
        "draw chart", "draw graph", "draw plot",
        "show me a new", "give me a chart", "give me a plot",
        "give me a graph", "give me a scatter", "give me a pie",
        "give me a bar", "give me a line", "give me a histogram",
        "give me a donut", "give me a heatmap",
        "show a pie", "show a bar", "show a scatter",
        "show a line", "show a histogram", "show a donut",
        "graph for", "graph of",
        "chart for", "chart of", "plot for", "plot of",
        "generate a", "create a", "build a", "draw a",
        "visualize ", "visualise ",
    )
    if any(p in q for p in _NEED_WANT):
        return "generate_chart"

    # Action verb + chart noun
    _GEN_VERBS = (
        "generate", "create", "make", "build", "draw",
        "visualize", "visualise",
    )
    _CHART_NOUNS = (
        "a chart", "a graph", "a plot", "a bar", "a line",
        "a scatter", "a histogram", "a pie", "a donut",
        "a heatmap", "a box plot", "a violin",
        "scatter", "histogram", "heatmap", "violin",
    )
    for verb in _GEN_VERBS:
        if verb in q:
            if any(noun in q for noun in _CHART_NOUNS):
                return "generate_chart"
            # visualize/visualise alone with any column/dimension word is enough
            if verb in ("visualize", "visualise"):
                return "generate_chart"

    # "plot X" or "graph X" used as a verb
    words = q.split()
    for i_w, w in enumerate(words):
        if w in ("plot", "graph") and i_w + 1 < len(words):
            return "generate_chart"

    # Time/dimension breakdown + visual word implies chart
    _BREAKDOWN = (
        "by year", "by month", "by quarter", "by week", "by day",
        "over time", "over the years", "over the months",
        "trend of", "trend for", "with year", "with month",
        "across years", "across months",
        "breakdown of", "breakdown by", "comparison of",
        "by brand", "by category", "by region", "by state",
        "by type", "by model", "sales by", "revenue by",
        "count by", "total by", "average by", "mean by",
    )
    _VISUAL_WORD = (
        "graph", "chart", "plot", "visualization",
        "bar", "line", "scatter", "histogram", "pie", "donut",
    )
    if any(bd in q for bd in _BREAKDOWN) and any(vw in q for vw in _VISUAL_WORD):
        return "generate_chart"

    # ---- Existing chart reference (explain) -----------------------------
    _CHART_REFS = (
        "the scatter", "the histogram", "the bar chart", "the bar",
        "the heatmap", "the line chart", "the line graph",
        "the box plot", "the violin", "the donut", "the pie",
        "this chart", "this plot", "this graph",
        "that chart", "that plot", "the chart", "the graph",
    )
    if any(n in q for n in _CHART_REFS):
        _OVERRIDE = ("generate", "create", "make", "build", "draw", "another", "new", "different")
        if not any(v in q for v in _OVERRIDE):
            return "explain_chart"

    # ---- Default: data question (RAG + Parquet) -------------------------
    return "data_question"


@app.post("/chat", tags=["analysis"])
async def chat_with_analysis(
    body: ChatRequest,
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    """Answer a user question about the current analysis context."""

    # ── Rate limiting ─────────────────────────────────────────────────────────
    try:
        key = f"ratelimit:chat:{user_id}"
        count = await redis_cache.increment_with_ttl(key, CHAT_RATE_WINDOW)
        if count > CHAT_RATE_LIMIT:
            raise HTTPException(
                status_code=429,
                detail=f"Too many chat requests. Please wait {CHAT_RATE_WINDOW} seconds."
            )
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise exc
        logger.warning("Redis rate limiter unavailable for user %s: %s", user_id, exc)

    # FIX 18: Sanitize user input immediately before any question usage.
    question = sanitize_chat_input(body.question.strip())
    context = body.context or {}

    if not question:
        raise HTTPException(status_code=400, detail="Question is required")
    if len(question) > MAX_QUESTION_CHARS:
        raise HTTPException(status_code=413, detail=f"Question too long. Max {MAX_QUESTION_CHARS} characters.")
    # FIX 17: Also enforce a byte-length cap. len() counts Unicode code-points,
    # not bytes — a CJK / emoji string can pass the char check while sending
    # 3-4× more bytes to the LLM, bypassing the intended input budget.
    _MAX_QUESTION_BYTES = MAX_QUESTION_CHARS * 4  # worst-case UTF-8 expansion
    if len(question.encode("utf-8")) > _MAX_QUESTION_BYTES:
        raise HTTPException(status_code=413, detail=f"Question too long. Max {MAX_QUESTION_CHARS} characters.")

    try:
        context_blob = json.dumps(context, default=str)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid context payload format.")
    if len(context_blob.encode("utf-8")) > MAX_CONTEXT_BYTES:
        raise HTTPException(status_code=413, detail=f"Context too large. Max {MAX_CONTEXT_BYTES // 1024} KB.")

    # ── Extract context fields ────────────────────────────────────────────────
    # Check if frontend passed the optimized chat_context_pack directly
    chat_context_pack = context.get("chat_context_pack")
    if chat_context_pack:
        stats = chat_context_pack.get("stats") or {}
        insights = chat_context_pack.get("insights") or {}
        file_name = chat_context_pack.get("fileName") or "dataset"
        charts_data = chat_context_pack.get("charts") or {}
        file_hash = chat_context_pack.get("file_hash") or context.get("file_hash")
    else:
        stats = context.get("stats") or context.get("stats_summary") or {}
        insights = context.get("insights") or {}
        file_name = context.get("fileName") or "dataset"
        charts_data = context.get("charts", {})
        file_hash = context.get("file_hash")

    # ── Security: verify the authenticated user owns this file_hash ───────────
    # Without this check, any authenticated user could pass someone else's
    # file_hash in the context payload and query their private data.
    if file_hash:
        try:
            _fh_row = await db.execute(
                _sa_select(_AnalysisHistory.id).where(
                    _AnalysisHistory.user_id == user_id,
                    _AnalysisHistory.file_hash == file_hash,
                ).limit(1)
            )
            if _fh_row.scalar_one_or_none() is None:
                logger.warning(
                    "Unauthorized file_hash access: user_id=%s hash=%s",
                    user_id, file_hash,
                )
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You do not have access to this analysis.",
                )
        except HTTPException:
            raise
        except Exception as _fh_exc:
            # DB error on ownership check — log and deny to be safe
            logger.error("file_hash ownership check failed: %s", _fh_exc)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Could not verify dataset access. Please try again.",
            )


    df_records = []
    using_preview_only = False
    if file_hash:
        try:
            # FIX 42: Validate file hash before storage-path usage in chat branch.
            assert _re.match(r"^[a-f0-9]{64}$", file_hash), "Invalid file hash"
            storage_path = os.path.join(_PARQUET_STORAGE_DIR, f"{file_hash}.parquet")
            if os.path.exists(storage_path):
                df_full = pd.read_parquet(storage_path)
                df_records = df_full.to_dict("records")
                logger.info("Chat: Loaded %d rows from Parquet for on-demand chart gen", len(df_full))
            else:
                using_preview_only = True
        except Exception as load_exc:
            logger.warning("Chat: Failed to load full dataset from storage: %s", load_exc)
            using_preview_only = True

    if not df_records:
        df_records = (
            context.get("clean_df")
            or context.get("cleanDf")
            or context.get("clean_data")
            or []
        )
    
    if not isinstance(df_records, list):
        df_records = []

    # Merge existing dashboard chart keys + any already-generated chat charts.
    # The frontend sends context.generated_chart_keys so repeated "one more plot"
    # requests do not duplicate charts generated earlier in this chat session.
    dashboard_keys = list(charts_data.keys()) if isinstance(charts_data, dict) else []
    generated_keys = context.get("generated_chart_keys") or []
    if not isinstance(generated_keys, list):
        generated_keys = []
    existing_chart_keys = list(dict.fromkeys(dashboard_keys + generated_keys))

    # ── Classify intent ───────────────────────────────────────────────────────
    intent = _classify_chat_intent(question)
    logger.info("Chat intent classified as '%s' for question: %s", intent, question[:80])

    # ══════════════════════════════════════════════════════════════════════════
    # BRANCH -1 — Off-topic (non-analytical questions)
    # ══════════════════════════════════════════════════════════════════════════
    if intent == "off_topic":
        return {
            "answer": (
                "I'm a data analysis assistant focused on your dataset. "
                "I can help you analyze trends, summarize data, and build charts, "
                "but I can't answer off-topic questions or write general code."
            ),
            "data_queried": False,
            "new_chart": None,
        }

    # ══════════════════════════════════════════════════════════════════════════
    # BRANCH 0 — Greeting / small talk
    # ══════════════════════════════════════════════════════════════════════════
    if intent == "greeting":
        _q = question.lower()
        if any(t in _q for t in ("bye", "goodbye", "see you", "later", "cya")):
            msg = f"Goodbye! Your analysis of {file_name} is saved in History. Come back anytime."
        elif any(t in _q for t in ("thanks", "thank you", "thx", "ty", "thank")):
            msg = f"Happy to help! Let me know if you have more questions about {file_name}."
        elif any(t in _q for t in ("who are you",)):
            msg = ("I'm your AI data analyst. I answer questions about your dataset, "
                   "find correlations, and generate charts on demand.")
        elif any(t in _q for t in ("how are you",)):
            msg = "Running smoothly! Ready to dig into your data whenever you are."
        else:
            msg = (f"Hello! I'm your AI analyst for '{file_name}'. "
                   "Ask me about statistics, relationships, trends — "
                   "or say 'generate a chart' to create a new visualization.")
        return {"answer": msg, "data_queried": False, "new_chart": None}

    # ══════════════════════════════════════════════════════════════════════════
    # BRANCH A — Generate a new chart
    # ══════════════════════════════════════════════════════════════════════════
    if intent == "generate_chart":
        from .agents.plot_generator import generate_on_demand_chart, suggest_novel_chart

        novel = suggest_novel_chart(
            df_records=df_records,
            existing_chart_keys=existing_chart_keys,
            user_request=question,
            stats_summary=stats,
        )

        if novel.get("cannot_plot"):
            reason = novel.get("reason", "I've already plotted all the most useful column combinations for this dataset.")
            return {
                "answer": reason,
                "data_queried": False,
                "new_chart": None,
            }

        chart_result = generate_on_demand_chart(
            spec=novel["spec"],
            df_records=df_records,
            existing_chart_keys=existing_chart_keys,
        )

        if chart_result.get("is_duplicate"):
            return {
                "answer": "You already have this chart on your dashboard! If you'd like to see something else, tell me which columns to plot.",
                "data_queried": False,
                "new_chart": None,
            }

        if chart_result.get("error"):
            return {
                "answer": chart_result["error"],
                "data_queried": False,
                "new_chart": None,
            }

        # Build a natural answer describing what was generated
        reasoning = novel.get("reasoning", "")
        answer = reasoning if reasoning else "Here is the chart you requested."

        return {
            "answer": answer,
            "data_queried": False,
            "new_chart": chart_result,
        }

    # ══════════════════════════════════════════════════════════════════════════
    # BRANCH B — Explain an existing chart
    # ══════════════════════════════════════════════════════════════════════════
    if intent == "explain_chart":
        if not existing_chart_keys:
            return {
                "answer": "There aren't any charts on the dashboard yet. If you'd like me to generate one, just ask!",
                "data_queried": False,
                "new_chart": None,
            }

        # Find which chart they're talking about
        q_lower = question.lower()

        # Try to match a specific chart key they may have mentioned
        matched_key = None
        matched_chart_data = None

        # Check if they mentioned a specific chart key
        for key in existing_chart_keys:
            if key.lower() in q_lower:
                matched_key = key
                break

        # If no exact key match, infer from chart type words
        if not matched_key:
            chart_type_words = {
                "scatter": "scatter", "histogram": "histogram",
                "bar": "ranked_bar", "heatmap": "heatmap",
                "line": "line", "box": "box", "violin": "violin",
                "donut": "donut", "pie": "donut", "distribution": "histogram",
                "correlation": "heatmap",
            }
            for word, prefix in chart_type_words.items():
                if word in q_lower:
                    # Find first matching chart key with this prefix
                    for key in existing_chart_keys:
                        if key.lower().startswith(prefix):
                            matched_key = key
                            break
                if matched_key:
                    break

        # Extract chart data for context.
        # The frontend now sends only sentinel keys ({key: true}) to keep the
        # HTTP payload small. Try to load the real fig from Redis first;
        # fall back to whatever the context carries (legacy/full payload path).
        if matched_key and isinstance(charts_data, dict) and matched_key in charts_data:
            _raw_from_context = charts_data[matched_key]
            # Sentinel value from slim frontend: skip and load from cache
            _needs_redis = (
                _raw_from_context is True
                or _raw_from_context is None
                or isinstance(_raw_from_context, bool)
            )

            if _needs_redis and file_hash:
                try:
                    _cache_key = redis_cache.analysis_key(user_id, file_hash)
                    _cached_analysis = await redis_cache.get(_cache_key)
                    if isinstance(_cached_analysis, dict):
                        _raw_from_context = _cached_analysis.get("charts", {}).get(matched_key)
                except Exception as _redis_exc:
                    logger.warning("Could not load chart from Redis for explain_chart: %s", _redis_exc)

            if _raw_from_context and _raw_from_context is not True:
                try:
                    raw = _raw_from_context
                    logger.debug("Parsing chart data for key '%s' (type=%s)", matched_key, type(raw).__name__)
                    chart_fig = json.loads(raw) if isinstance(raw, str) else raw
                    # Build a readable summary of the chart's data
                    traces_info = []
                    for trace in chart_fig.get("data", [])[:3]:
                        ttype = trace.get("type", "unknown")
                        x_vals = list(trace.get("x") or [])[:8]
                        y_vals = list(trace.get("y") or [])[:8]
                        labels = list(trace.get("labels") or [])[:8]
                        values = list(trace.get("values") or [])[:8]
                        if labels and values:
                            traces_info.append(f"{ttype}: labels={labels}, values={values}")
                        elif x_vals or y_vals:
                            traces_info.append(f"{ttype}: x={x_vals}, y={y_vals}")

                    layout = chart_fig.get("layout", {})
                    title_raw = layout.get("title", {})
                    chart_title = (
                        title_raw.get("text") if isinstance(title_raw, dict)
                        else title_raw
                    ) or matched_key

                    matched_chart_data = {
                        "key": matched_key,
                        "title": chart_title,
                        "traces": traces_info,
                    }
                except Exception as e:
                    logger.warning("Could not parse chart data for key %s: %s", matched_key, e)

        # ── RAG-grounded chart explanation ──────────────────────────────────
        _explain_key = matched_key or (existing_chart_keys[0] if existing_chart_keys else "")
        _chart_raw = None

        # Load the real chart fig from Redis (frontend sends only sentinel keys now).
        if _explain_key and file_hash:
            try:
                _cache_key_expl = redis_cache.analysis_key(user_id, file_hash)
                _cached_expl = await redis_cache.get(_cache_key_expl)
                if isinstance(_cached_expl, dict):
                    _chart_raw = _cached_expl.get("charts", {}).get(_explain_key)
            except Exception as _expl_exc:
                logger.warning("Redis lookup for _chart_raw failed: %s", _expl_exc)

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
                conversation_history=body.history or [],
                groq_client=_explain_client,
                redis_client=_expl_redis,
            )
            return explain_result

        # Fallback: no LLM
        _tag = f"\n[CHART: {_explain_key}]" if _explain_key else ""
        return {
            "answer": f"Here is the chart from {file_name}.{_tag}",
            "data_queried": False,
            "new_chart": None,
        }

    # ══════════════════════════════════════════════════════════════════════════
    # BRANCH C — Data question (RAG + Parquet Query)
    # ══════════════════════════════════════════════════════════════════════════
    _data_client = get_groq_client()
    if _data_client and file_hash:
        _data_redis = _get_cache_client()
        ans_result = await answer_question(
            question=question,
            file_hash=file_hash,
            file_name=file_name,
            stats=stats,
            insights=insights,
            chart_keys=existing_chart_keys,
            conversation_history=body.history or [],
            groq_client=_data_client,
            redis_client=_data_redis,
        )
        return ans_result

    # Fallback if no LLM configured
    return {
        "answer": (
            f"The dataset '{file_name}' has {stats.get('row_count', '?')} rows "
            f"and {stats.get('column_count', '?')} columns. I need a valid Groq "
            f"API key to answer specific questions about it."
        ),
        "data_queried": False,
        "new_chart": None,
    }

