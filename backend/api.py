import io
import asyncio
import logging
import os
import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import pandas as pd
import orjson
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status, Request, Response, Query, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.gzip import GZipMiddleware
import traceback
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, field_validator
from typing import Any, Annotated

import re as _re

_INJECTION_PATTERNS = [
    r"ignore\s+(all\s+)?previous\s+instructions",
    r"you\s+are\s+now\s+a",
    r"act\s+as\s+(if\s+you\s+are\s+)?a",
    r"disregard\s+(all\s+)?prior",
    r"system\s*:\s*",
    r"<\s*system\s*>",
    r"<\s*/?inst\s*>",
    r"\[INST\]",
    r"###\s*instruction",
    r"forget\s+(all\s+)?previous",
    r"new\s+persona",
    r"pretend\s+(you\s+are|to\s+be)",
]
_INJECTION_RE = _re.compile("|".join(_INJECTION_PATTERNS), _re.IGNORECASE)

def sanitize_chat_input(text: str) -> str:
    """Strip prompt injection patterns and null bytes from user input."""
    text = text.replace("\x00", "").replace("\r", " ")
    text = _INJECTION_RE.sub("[removed]", text)
    return text.strip()

from .core import cache as redis_cache
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
from .core.llm_client import get_groq_client
from .db import get_db, init_db
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
    
    # Build the user response object
    now = datetime.utcnow()
    user_response = UserResponse(
        id=result["user_id"],
        name=result.get("name"),
        email=result["email"],
        created_at=now,
        updated_at=now
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

    idinfo = verify_google_token(credential)
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
        for name in os.listdir(_PARQUET_STORAGE_DIR):
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
        return json.loads(df.to_json(orient="records"))
    # Sample uniformly across the index so edge values are represented
    step = max(1, len(df) // n)
    sampled = df.iloc[::step].head(n)
    return json.loads(sampled.to_json(orient="records"))


@app.post("/analyze", tags=["analysis"])
async def analyze(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    user_id: int = Depends(check_user_rate_limit), # check_user_rate_limit now returns user_id
    db: AsyncSession = Depends(get_db),
):

    filename = file.filename or ""
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
    file_size = len(file_bytes)


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
        logger.info("Cache not eligible for reuse for user %d / %s — re-running pipeline", user_id, file.filename)

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
        del file_bytes
        chunks = None
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
        file_size=file_size,
        analysis_result=result,
        serialized_charts=serialized_charts,
    )
    if not save_result["success"]:
        logger.warning("Failed to persist analysis: %s", save_result["message"])
    else:
        result["analysis_id"] = save_result.get("analysis_id")

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
    
    # Check numeric results against known column ranges
    numeric_cols = stats.get("numeric_columns", {})
    
    # For top_n / aggregate results that return a single number,
    # verify it's within known min/max
    if isinstance(result, (int, float)):
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
    Returns: "generate_chart" | "explain_chart" | "data_question"

    Negative guards fire first — explain phrases override any chart noun.
    Generate triggers only fire when the sentence is explicitly asking for new output.
    """
    q = question.lower().strip()

    _EXPLAIN_OVERRIDES = (
        "what does", "what do", "what is shown", "tell me about",
        "explain", "interpret", "what can i", "why is", "why are",
        "summarise", "summarize", "describe", "what patterns",
        "what trends", "insight from", "insights from",
        "what does the", "what does this", "what does that",
        "analyse", "analyze", "understand", "what am i seeing",
    )
    if any(p in q for p in _EXPLAIN_OVERRIDES):
        return "explain_chart"

    _GENERATE_TRIGGERS = (
        "generate", "create", "make", "build", "draw",
        "show me a new", "give me a", "another chart", "another plot",
        "one more", "new chart", "new plot", "different chart",
        "can you plot", "can you chart", "can you make",
        "plot a", "chart a", "visualize", "visualise",
        "show some", "show a ", "give some",
    )
    if any(t in q for t in _GENERATE_TRIGGERS):
        return "generate_chart"

    # Bare chart-type nouns without a generate verb → explain intent
    _CHART_NOUNS = (
        "the scatter", "the histogram", "the bar chart", "the heatmap",
        "the line chart", "the box plot", "the violin", "the donut",
        "this chart", "this plot", "this graph", "that chart",
    )
    if any(n in q for n in _CHART_NOUNS):
        return "explain_chart"

    return "data_question"


@app.post("/chat", tags=["analysis"])
async def chat_with_analysis(
    body: ChatRequest,
    user_id: int = Depends(get_current_user_id),
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

    question = body.question.strip()
    question = sanitize_chat_input(question)
    context = body.context or {}

    if not question:
        raise HTTPException(status_code=400, detail="Question is required")
    if len(question) > MAX_QUESTION_CHARS:
        raise HTTPException(status_code=413, detail=f"Question too long. Max {MAX_QUESTION_CHARS} characters.")

    context_blob = json.dumps(context, default=str)
    if len(context_blob.encode("utf-8")) > MAX_CONTEXT_BYTES:
        raise HTTPException(status_code=413, detail=f"Context too large. Max {MAX_CONTEXT_BYTES // 1024} KB.")

    # ── Extract context fields ────────────────────────────────────────────────
    stats = context.get("stats") or context.get("stats_summary") or {}
    insights = context.get("insights") or {}
    file_name = context.get("fileName") or "dataset"
    charts_data = context.get("charts", {})
    file_hash = context.get("file_hash")

    df_records = []
    using_preview_only = False
    if file_hash:
        try:
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
            reason = novel.get("reason", "All useful column combinations are already visualized.")
            return {
                "answer": (
                    f"I've reviewed all possible chart combinations for {file_name}. "
                    f"{reason}"
                ),
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
                "answer": (
                    "That chart is already displayed on the dashboard. "
                    "All remaining column combinations have been visualized."
                ),
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
        spec = novel["spec"]
        ct = spec.get("chart_type", "chart")
        x_col = spec.get("x") or ""
        y_col = spec.get("y") or ""

        if x_col and y_col:
            col_desc = f" of {x_col} vs {y_col}"
        elif x_col:
            col_desc = f" of {x_col}"
        else:
            col_desc = ""

        answer = f"Here's a new {_TYPE_DISPLAY.get(ct, ct)}{col_desc}."
        if reasoning:
            answer += f" {reasoning}"

        return {
            "answer": answer,
            "data_queried": False,
            "new_chart": chart_result,
        }

    # ══════════════════════════════════════════════════════════════════════════
    # BRANCH B — Explain an existing chart
    # ══════════════════════════════════════════════════════════════════════════
    if intent == "explain_chart":
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

        # Extract chart data for context
        if matched_key and isinstance(charts_data, dict) and matched_key in charts_data:
            try:
                raw = charts_data[matched_key]
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

        # Build explanation via LLM
        client = get_groq_client()
        if client:
            try:
                chart_context_str = ""
                if matched_chart_data:
                    chart_context_str = (
                        f"\nChart being explained: '{matched_chart_data['title']}' (key: {matched_chart_data['key']})\n"
                        f"Chart data sample: {matched_chart_data['traces']}\n"
                    )
                elif existing_chart_keys:
                    chart_context_str = f"\nAvailable charts: {existing_chart_keys}\n"

                slim_stats = truncate_stats_for_llm(stats)

                explain_messages = [
                    {
                        "role": "system",
                        "content": (
                            "You are a Senior Data Analyst explaining charts to a non-technical user. "
                            "Be specific, mention actual values from the chart data, and give 2-3 sentences "
                            "of genuine insight. Never say 'I cannot see the chart' — use the data provided."
                        )
                    },
                    {
                        "role": "system",
                        "content": (
                            f"Dataset: {file_name}\n"
                            f"Stats summary: {json.dumps(slim_stats, default=str)[:1000]}\n"
                            f"{chart_context_str}"
                        )
                    },
                    {"role": "user", "content": question}
                ]

                completion = client.chat.completions.create(
                    model=SYNTHESIS_MODEL,
                    messages=explain_messages,
                    temperature=0.2,
                    max_tokens=300,
                )
                answer = (completion.choices[0].message.content or "").strip()
                if answer:
                    return {"answer": answer, "data_queried": False, "new_chart": None}
            except Exception as exc:
                logger.warning("LLM chart explanation failed: %s", exc)

        # Fallback explanation if LLM fails
        if matched_key:
            return {
                "answer": (
                    f"The {matched_key} chart shows the relationship between the "
                    f"dataset columns it visualizes. Look for patterns, clusters, or "
                    f"outliers in the data points to draw insights."
                ),
                "data_queried": False,
                "new_chart": None,
            }
        return {
            "answer": (
                "I can see the charts on your dashboard. Could you specify which chart "
                "you'd like me to explain? For example: 'explain the scatter plot' or "
                "'explain the bar chart'."
            ),
            "data_queried": False,
            "new_chart": None,
        }

    # ══════════════════════════════════════════════════════════════════════════
    # BRANCH C — Data question (original logic, preserved exactly)
    # ══════════════════════════════════════════════════════════════════════════
    slim_stats = truncate_stats_for_llm(stats)
    profile = slim_stats.get("dataset_profile") or {}
    outlier_counts = slim_stats.get("outlier_counts") or {}
    if not outlier_counts:
        outlier_counts = {
            k: int((v or {}).get("count", 0))
            for k, v in (stats.get("outliers") or {}).items()
        }
    top_outliers = sorted(outlier_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]
    correlations = context.get("correlations") or slim_stats.get("strong_correlations") or []
    quality = context.get("dataQuality") or slim_stats.get("data_quality") or {}

    # Build charts summary for LLM context
    charts_summary = {}
    if isinstance(charts_data, dict):
        for k, v in charts_data.items():
            try:
                c = json.loads(v) if isinstance(v, str) else v
                details = []
                for trace in c.get("data", []):
                    ttype = trace.get("type", "unknown")
                    if ttype in ("pie", "funnelarea"):
                        labels = trace.get("labels") or trace.get("x") or []
                        values = trace.get("values") or trace.get("y") or []
                        if isinstance(labels, list) and isinstance(values, list):
                            pairs = [f"{l}: {val}" for l, val in zip(labels[:10], values[:10])]
                            details.append(f"Pie/Donut: {', '.join(pairs)}")
                        else:
                            details.append("Pie/Donut chart")
                    elif ttype in ("bar", "scatter", "violin", "box"):
                        # Sample more values (16 instead of 8) for better LLM grounding
                        xv = (trace.get("x") or [])[:16]
                        yv = (trace.get("y") or [])[:16]
                        details.append(f"{ttype.capitalize()}: X={xv}, Y={yv}")
                    elif ttype == "histogram":
                        xv = (trace.get("x") or [])[:24]
                        details.append(f"Histogram of: {xv}")
                    else:
                        details.append(f"{ttype} chart")
                layout_title = c.get("layout", {}).get("title", {})
                if isinstance(layout_title, dict):
                    title_text = layout_title.get("text") or k
                elif isinstance(layout_title, str) and layout_title:
                    title_text = layout_title
                else:
                    title_text = k
                charts_summary[k] = f"[key='{k}'] Title '{title_text}' — " + (" | ".join(details) if details else "chart")
            except Exception:
                charts_summary[k] = f"[key='{k}'] Visual chart"

    exact_chart_keys = list(charts_summary.keys()) if isinstance(charts_summary, dict) else []
    outlier_summary = context.get("outlierSummary") or [
        {"column": col, "count": count} for col, count in top_outliers
    ]

    def _extract_recent_entity(history: list):
        import re
        _STOP = {"the", "a", "an", "is", "are", "was", "were", "has", "have", "had",
                 "in", "on", "at", "of", "for", "and", "or", "but", "with", "by",
                 "according", "to", "data", "dataset", "fetched", "per", "as"}
        for m in reversed(history or []):
            content = m.get("content", "")
            tokens = re.findall(r"[A-Z][a-z]+(?:\s[A-Z][a-z]+)*", content)
            for tok in reversed(tokens):
                if tok.lower() not in _STOP and len(tok) > 2:
                    return tok
        return None

    data_coverage_note = (
        "\nNOTE: Only a 500-row preview is available for data queries. "
        "For questions about exact counts, totals, or specific rows beyond the preview, "
        "say 'I can only see a preview of this dataset.'"
        if using_preview_only else ""
    )

    system_prompt = (
        "You are an elite Senior Data Analyst. Answer questions strictly grounded in the dataset context.\n"
        "Tone: Professional, authoritative, yet accessible. "
        "Brevity: Keep responses strictly between 1-4 sentences.\n"
        "Grounding Rules:\n"
        "1. MANDATORY: If you receive an 'ADDITIONAL DATA FROM FULL DATASET QUERY' block, you MUST use "
        "   those exact values to answer the question. Do NOT say 'I don't have that information'.\n"
        "2. For top_n / bottom_n results: look at 'top_entry' → state its name and value explicitly.\n"
        "3. For group_aggregate: 'result' is a dict of {entity: value}. Pick highest/lowest as needed.\n"
        "4. For search results: each item in 'result' is a full row dict — read the relevant field directly.\n"
        "5. NEVER say 'I don't have that data' if an ADDITIONAL DATA block is present.\n"
        "6. If truly no data available, say: 'I couldn't find that in the dataset.'\n"
        "7. If the user is chatty (hi, thanks), be polite but don't spontaneously analyze data.\n"
        f"8. Reference charts using exactly `[CHART: key]` with only these keys: {exact_chart_keys}.\n"
        "9. Use previous messages to maintain continuity. Resolve pronouns from conversation history. Do NOT use markdown bolding like **text** in your response.\n"
        "10. CRITICAL: Never invent values. If a fact is not in the data context or ADDITIONAL DATA block, "
        "    say exactly: 'That information isn't in the dataset.' Never estimate or assume.\n"
        f"{data_coverage_note}"
    )

    chat_pack  = context.get("chat_context_pack") or {}

    data_context = (
        f"Dataset: {file_name} | "
        f"{chat_pack.get('row_count') or stats.get('row_count')} rows, "
        f"{chat_pack.get('column_count') or stats.get('column_count')} columns\n"
        f"Profile: {chat_pack.get('profile') or slim_stats.get('dataset_profile', {})}\n"
        f"Quality: {chat_pack.get('quality') or slim_stats.get('data_quality', {})}\n"
        f"Column details:\n"
        f"{json.dumps(chat_pack.get('columns', {}), default=str)[:3000]}\n"
        f"Top correlations: {chat_pack.get('correlations') or slim_stats.get('strong_correlations', [])}\n"
        f"Key findings: {chat_pack.get('key_findings') or insights.get('findings', [])[:3]}\n"
        f"Charts on dashboard: {list(charts_data.keys()) if isinstance(charts_data, dict) else []}\n"
    )

    MAX_HISTORY_TURNS = 6  # last 6 turns = 12 messages, enough for continuity

    history_msgs = []
    if body.history:
        recent = body.history[-MAX_HISTORY_TURNS:]  # sliding window
        for m in recent:
            role = "assistant" if m.get("role") in ["assistant", "ai"] else "user"
            history_msgs.append({"role": role, "content": m.get("content", "")})

    messages = [
        {"role": "system", "content": system_prompt},          # instructions only
        {"role": "system", "content": f"DATASET CONTEXT:\n{data_context}"},  # data only
    ]
    messages.extend(history_msgs)
    messages.append({"role": "user", "content": question})

    client = get_groq_client()
    if client:
        try:
            col_types = {}
            for c in stats.get("numeric_columns", {}).keys():
                col_types[c] = "numeric"
            for c in stats.get("categorical_columns", {}).keys():
                col_types[c] = "categorical"
            for c in (stats.get("columns") or []):
                if c not in col_types:
                    col_types[c] = "unknown"

            data_result = None
            is_greeting = any(g in question.lower() for g in ["hello", "hi", "hey", "thanks", "thank you"])

            if file_hash and not is_greeting:
                try:
                    import re as _re
                    recent_history = ""
                    if body.history:
                        recent_turns = body.history[-8:]
                        recent_history = "\n".join(
                            f"{m.get('role','user').upper()}: {m.get('content','')}"
                            for m in recent_turns
                        )

                    _PRONOUNS = {"his", "her", "their", "its", "he", "she", "they",
                                 "him", "hers", "theirs", "this person", "that person"}
                    q_tokens = set(question.lower().split())
                    has_pronoun = bool(q_tokens & _PRONOUNS)
                    resolved_subject = _extract_recent_entity(body.history) if body.history else None
                    
                    planner_question = question
                    if resolved_subject and has_pronoun:
                        planner_question = f"{question} [Note: pronoun refers to '{resolved_subject}']"

                    intent_prompt = (
                        "You are a Data Query Planner. Analyze the user's question and decide if querying the FULL dataset is needed.\n"
                        "If YES, return ONLY a single valid JSON object. If NO, return exactly 'NONE'.\n\n"
                        "CRITICAL RULES:\n"
                        "- ALWAYS query for attribute questions: age, birthday, score, rank, salary, stats of a named person/item.\n"
                        "- Prefer 'search' when looking up a named entity's full row.\n"
                        "- Prefer 'filter_lookup' when mapping one column's value to another.\n"
                        "- NEVER return NONE for questions about a specific named person or item's attributes.\n\n"
                        "SUPPORTED QUERY TYPES:\n"
                        "0. filter_lookup — look up one column by matching another\n"
                        '   Example: {"type":"filter_lookup","params":{"filter_col":"name","filter_val":"John","result_col":"age"}}\n'
                        "1. top_n — highest N rows by numeric column\n"
                        '   Example: {"type":"top_n","params":{"column":"Price","n":1}}\n'
                        "2. bottom_n — lowest N rows\n"
                        '   Example: {"type":"bottom_n","params":{"column":"Price","n":1}}\n'
                        "3. group_aggregate — group by, aggregate numeric\n"
                        '   Example: {"type":"group_aggregate","params":{"group_by":"Brand","column":"Revenue","func":"sum","n":10}}\n'
                        "4. filter_group — filter then group+aggregate\n"
                        '   Example: {"type":"filter_group","params":{"group_by":"Brand","func":"count","n":5,"filters":[{"column":"Year","op":"year","value":2021}]}}\n'
                        "5. value_counts — count each unique category\n"
                        '   Example: {"type":"value_counts","params":{"column":"Category","n":15}}\n'
                        "6. aggregate — single stat\n"
                        '   Example: {"type":"aggregate","params":{"column":"Price","func":"mean"}}\n'
                        "7. distinct — list unique values\n"
                        '   Example: {"type":"distinct","params":{"column":"Brand"}}\n'
                        "8. row_count — count matching rows\n"
                        '   Example: {"type":"row_count","params":{"filters":[{"column":"Status","op":"eq","value":"Active"}]}}\n'
                        "9. search — text search\n"
                        '   Example: {"type":"search","params":{"value":"John","n":3}}\n'
                        "10. correlation — between two numeric columns\n"
                        '    Example: {"type":"correlation","params":{"column":"Price","column2":"Mileage"}}\n'
                        "11. percentile\n"
                        '    Example: {"type":"percentile","params":{"column":"Price","percentile":90}}\n\n'
                        f"Dataset columns: {col_types}\n\n"
                        + (f"Recent conversation:\n{recent_history}\n\n" if recent_history else "")
                        + f"Current question: {planner_question}\n\n"
                        "Return ONLY JSON or 'NONE'."
                    )

                    intent_resp = client.chat.completions.create(
                        model=INTENT_MODEL,
                        messages=[{"role": "user", "content": intent_prompt}],
                        max_tokens=350,
                        temperature=0,
                    )
                    intent_text = (intent_resp.choices[0].message.content or "").strip()
                    logger.info("Intent LLM response: %s", intent_text)

                    try:
                        if "{" in intent_text and "}" in intent_text:
                            raw_json = intent_text[intent_text.find("{"):intent_text.rfind("}")+1]
                            raw_plan = json.loads(raw_json)
                            query_plan = QueryPlan(**raw_plan)  # validate before running
                            
                            # Pre-check: verify columns exist before running
                            requested_cols = [
                                query_plan.params.get("column"),
                                query_plan.params.get("group_by"),
                                query_plan.params.get("filter_col"),
                                query_plan.params.get("result_col"),
                            ]
                            if df_records:
                                available_cols = set(df_records[0].keys()) if df_records else set()
                                for c in requested_cols:
                                    if c and c not in available_cols:
                                        logger.info("Intent plan col '%s' will need fuzzy resolve", c)
                            
                            data_result = await asyncio.to_thread(
                                run_data_query, file_hash, query_plan.type, query_plan.params
                            )
                            logger.info("Data Agent result: %s", str(data_result)[:500])
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.warning("Intent plan invalid (%s), skipping data query", e)
                        data_result = None

                    if "{" in intent_text and "}" in intent_text:
                        # (The above try/except handles the loading, this is for the fallback check)
                        result_is_empty = (
                            not data_result
                            or "error" in data_result
                            or (isinstance(data_result.get("result"), list) and len(data_result["result"]) == 0)
                            or data_result.get("result") == "No rows found."
                        )
                        if result_is_empty and resolved_subject:
                            data_result = await asyncio.to_thread(run_data_query, file_hash, "search", {"value": resolved_subject, "n": 3})
                    else:
                        _LOOKUP_SIGNALS = {
                            "birthday", "born", "dob", "date of birth", "age", "address",
                            "nationality", "country", "team", "salary", "height", "weight",
                        }
                        q_lower_check = question.lower()
                        is_lookup = any(sig in q_lower_check for sig in _LOOKUP_SIGNALS)
                        if (is_lookup or has_pronoun) and resolved_subject:
                            data_result = await asyncio.to_thread(run_data_query, file_hash, "search", {"value": resolved_subject, "n": 3})
                        elif is_lookup:
                            fallback_entity = _extract_recent_entity([{"content": question, "role": "user"}])
                            if fallback_entity:
                                data_result = await asyncio.to_thread(run_data_query, file_hash, "search", {"value": fallback_entity, "n": 3})

                except Exception as e:
                    logger.warning("Intent pass failed: %s", e)

            if data_result and "error" not in data_result:
                plausible, reason = _is_result_plausible(data_result, stats)
                if plausible:
                    subject_note = f" (subject: {resolved_subject})" if resolved_subject else ""
                    messages.append({
                        "role": "system",
                        "content": (
                            f"ADDITIONAL DATA FROM FULL DATASET QUERY{subject_note}:\n{data_result}\n"
                            "IMPORTANT: Use these exact values. Do NOT say 'I don't have that data'."
                        )
                    })
                else:
                    logger.warning("Rejecting implausible query result (%s): %s", reason, data_result)
            elif data_result:
                # If there's an error, don't label it as authoritative data
                logger.warning("Omitting query error from system prompt to avoid hallucination: %s", data_result.get("error"))

            synthesis_temp = 0.05 if (data_result and "error" not in data_result) else 0.15
            completion = client.chat.completions.create(
                model=SYNTHESIS_MODEL,
                messages=messages,
                temperature=synthesis_temp,
                max_tokens=450,
            )
            answer = (completion.choices[0].message.content or "").strip()
            if answer:
                return {"answer": answer, "data_queried": bool(data_result), "new_chart": None}

        except Exception as exc:
            logger.warning("Groq chat failed: %s", exc)

    # ── Rule-based fallback ────────────────────────────────────────────────────
    q_lower = question.lower()
    row_count = stats.get("row_count", "unknown")
    col_count = stats.get("column_count", "unknown")
    completeness = quality.get("completeness")
    missing_cells = quality.get("missing_cells")
    duplicate_rows = quality.get("duplicate_rows")
    findings = insights.get("findings") or []

    if "outlier" in q_lower:
        if top_outliers and top_outliers[0][1] > 0:
            summary = ", ".join([f"{col}: {cnt}" for col, cnt in top_outliers[:5]])
            return {"answer": f"Top outlier columns in {file_name}: {summary}.", "data_queried": False, "new_chart": None}
        return {"answer": f"No significant outliers detected in {file_name}.", "data_queried": False, "new_chart": None}

    if any(k in q_lower for k in ["quality", "missing", "duplicate", "completeness"]):
        parts = [f"{file_name} has {missing_cells or 0} missing cells and {duplicate_rows or 0} duplicate rows."]
        if completeness is not None:
            parts.append(f"Completeness: {float(completeness):.2f}%.")
        return {"answer": " ".join(parts), "data_queried": False, "new_chart": None}

    if "correlation" in q_lower:
        if correlations:
            top = correlations[0]
            return {
                "answer": f"Strongest correlation: {top.get('col1')} and {top.get('col2')} (r={float(top.get('correlation', 0)):.3f}).",
                "data_queried": False, "new_chart": None,
            }
        return {"answer": "No strong correlations found in the analysis.", "data_queried": False, "new_chart": None}

    is_greeting = any(g in q_lower for g in ["hello", "hi", "hey", "thanks", "thank you"])
    if is_greeting:
        return {"answer": "Hello! I'm your data analyst. Ask me anything about your dataset.", "data_queried": False, "new_chart": None}

    parts = [f"I analyzed {file_name} with {row_count} rows and {col_count} columns."]
    if completeness is not None:
        parts.append(f"Data completeness: {float(completeness):.2f}%.")
    if findings:
        parts.append(f"Key finding: {findings[0]}")
    return {"answer": " ".join(parts), "data_queried": False, "new_chart": None}