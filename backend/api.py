

import io
import csv
import asyncio
import logging
import os
import json
import zipfile
from contextlib import asynccontextmanager
from typing import Annotated, Optional

import pandas as pd
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status, Request, Response, Query
from fastapi.responses import JSONResponse
import traceback
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

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
from .core.utils import sanitize_floats, truncate_stats_for_llm
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
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(30 * 1024 * 1024)))
MAX_ANALYZE_ROWS = int(os.getenv("MAX_ANALYZE_ROWS", "250000"))
MAX_ANALYZE_COLUMNS = int(os.getenv("MAX_ANALYZE_COLUMNS", "300"))
MAX_EXCEL_SHEETS = int(os.getenv("MAX_EXCEL_SHEETS", "5"))
MAX_QUESTION_CHARS = int(os.getenv("CHAT_MAX_QUESTION_CHARS", "1200"))
MAX_CONTEXT_BYTES = int(os.getenv("CHAT_MAX_CONTEXT_BYTES", str(128 * 1024)))
READ_CHUNK_BYTES = 1024 * 1024
CHAT_RATE_LIMIT = int(os.getenv("CHAT_RATE_LIMIT", "10"))
CHAT_RATE_WINDOW = int(os.getenv("CHAT_RATE_WINDOW_SECONDS", "60"))


_raw_origins = os.getenv("CORS_ORIGINS", "http://localhost:5173,http://localhost:3000")
origins = [o.strip() for o in _raw_origins.split(",") if o.strip()]


def _looks_like_csv(file_bytes: bytes) -> bool:
    sample = file_bytes[:16384]
    if not sample:
        return False

    text: Optional[str] = None
    for encoding in ("utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "latin-1"):
        try:
            text = sample.decode(encoding)
            break
        except UnicodeDecodeError:
            continue

    if text is None:
        return False

    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) < 1:
        return False

    sniff_sample = "\n".join(lines[:10])
    try:
        csv.Sniffer().sniff(sniff_sample)
        return True
    except csv.Error:
        return any(delim in lines[0] for delim in (",", ";", "\t", "|"))


def _validate_upload_magic(parsed_ext: str, file_bytes: bytes) -> None:
    if parsed_ext == "csv":
        if not _looks_like_csv(file_bytes):
            raise HTTPException(status_code=400, detail="Invalid CSV content")
        return

    if parsed_ext == "xlsx":
        if not zipfile.is_zipfile(io.BytesIO(file_bytes)):
            raise HTTPException(status_code=400, detail="Invalid XLSX file signature")
        return

    if parsed_ext == "xls":
        if not file_bytes.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"):
            raise HTTPException(status_code=400, detail="Invalid XLS file signature")
        return

    raise HTTPException(status_code=400, detail="Unsupported file type")


def _detect_csv_delimiter(file_bytes: bytes) -> Optional[str]:
    sample = file_bytes[:16384]
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = sample.decode(encoding)
            break
        except UnicodeDecodeError:
            text = None
    if not text:
        return None

    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return None

    sniff_sample = "\n".join(lines[:20])
    try:
        dialect = csv.Sniffer().sniff(sniff_sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except csv.Error:
        return None


def _read_csv_with_fallback(file_bytes: bytes) -> pd.DataFrame:
    delimiter = _detect_csv_delimiter(file_bytes)

    attempts = []
    encodings = ["utf-8-sig", "utf-8", "utf-16", "latin-1"]
    for encoding in encodings:
        if delimiter:
            attempts.append({"engine": "python", "sep": delimiter, "encoding": encoding})
        attempts.append({"engine": "python", "sep": None, "encoding": encoding})
    attempts.append({})

    errors = []
    for csv_kwargs in attempts:
        try:
            header_df = pd.read_csv(io.BytesIO(file_bytes), nrows=0, **csv_kwargs)
            if header_df.shape[1] == 0:
                errors.append("No columns detected in CSV header")
                continue
            if header_df.shape[1] > MAX_ANALYZE_COLUMNS:
                raise HTTPException(
                    status_code=413,
                    detail=f"Dataset has {header_df.shape[1]} columns. Maximum allowed is {MAX_ANALYZE_COLUMNS}.",
                )

            return pd.read_csv(
                io.BytesIO(file_bytes),
                nrows=MAX_ANALYZE_ROWS + 1,
                **csv_kwargs,
            )
        except HTTPException:
            raise
        except Exception as exc:
            errors.append(str(exc))

    first_error = errors[0] if errors else "Unknown CSV parse error"
    raise HTTPException(status_code=422, detail=f"Could not parse file: {first_error}")




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

    await init_db()
    yield
    await redis_cache.close()
    logger.info("DataPulse API shutdown complete")




app = FastAPI(
    title="DataPulse API",
    description="Multi-agent CSV analysis API",
    version=APP_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(Exception)
async def catch_all_exception_handler(request: Request, exc: Exception):
    error_trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    logger.error(f"Unhandled error: {error_trace}")
    
    if os.getenv("APP_ENV", "production") == "development":
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




@app.post("/auth/register", response_model=AuthResponse, status_code=status.HTTP_201_CREATED, tags=["auth"])
async def register(body: UserRegister, db: AsyncSession = Depends(get_db)):
    result = await register_user(db, body.email, body.password, body.name)
    if not result["success"]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result["message"])
    return AuthResponse(success=True, message=result["message"])


@app.post("/auth/login", response_model=TokenResponse, tags=["auth"])
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
            email=user_obj["email"],
            created_at=user_obj["created_at"],
            updated_at=user_obj["updated_at"],
        ),
    )


@app.post("/auth/forgot-password", response_model=ForgotPasswordResponse, tags=["auth"])
async def forgot_password(body: ForgotPasswordRequest, db: AsyncSession = Depends(get_db)):
    result = await request_password_reset(db, body.email)
    return ForgotPasswordResponse(
        success=True,
        message=result.get("message", "If an account exists for that email, a password reset link has been sent."),
        debug_reset_token=result.get("debug_reset_token"),
    )


@app.post("/auth/reset-password", response_model=AuthResponse, tags=["auth"])
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




@app.post("/analyze", tags=["analysis"])
async def analyze(
    file: UploadFile = File(...),
    user_id: int = Depends(get_current_user_id),
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

    _validate_upload_magic(parsed_ext, file_bytes)

    file_hash = compute_file_hash(file_bytes, filename)


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
            df = _read_csv_with_fallback(file_bytes)
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

    state = await asyncio.to_thread(run_pipeline, df)
    result = state.model_dump()

    if state.errors or state.partial:
        logger.error("Pipeline failed for user %d / %s: %s", user_id, filename, state.errors)
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Analysis pipeline failed",
                "partial": True,
                "errors": state.errors,
                "completed_agents": state.completed_agents,
            },
        )


    save_result = await save_analysis(
        db=db,
        user_id=user_id,
        file_name=filename,
        file_hash=file_hash,
        file_size=len(file_bytes),
        analysis_result=result,
    )
    if not save_result["success"]:
        logger.warning("Failed to persist analysis: %s", save_result["message"])
    else:
        result["analysis_id"] = save_result.get("analysis_id")


    result["charts"] = _serialize_charts(result.get("charts") or {})
    result["partial"] = False


    for key in ("raw_df", "clean_df"):
        df_val = result.get(key)
        if hasattr(df_val, "to_json"):
            result[key] = json.loads(df_val.head(100).to_json(orient="records"))


    result = sanitize_floats(result)

    return {"from_cache": False, "pipeline_version": PIPELINE_VERSION, **result}




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


@app.post("/chat", tags=["analysis"])
async def chat_with_analysis(
    body: ChatRequest,
    user_id: int = Depends(get_current_user_id),
):
    """Answer a user question about the current analysis context."""

    try:
        key = f"ratelimit:chat:{user_id}"
        count = await redis_cache.increment_with_ttl(key, CHAT_RATE_WINDOW)
        if count > CHAT_RATE_LIMIT:
            raise HTTPException(status_code=429, detail=f"Too many chat requests. Please wait {CHAT_RATE_WINDOW} seconds.")
    except Exception as exc:
        # Graceful degradation: keep chat functional even if Redis is unavailable.
        logger.warning("Redis rate limiter unavailable for user %s, continuing without rate limit: %s", user_id, exc)

    question = body.question.strip()
    context = body.context or {}

    if not question:
        raise HTTPException(status_code=400, detail="Question is required")
    if len(question) > MAX_QUESTION_CHARS:
        raise HTTPException(
            status_code=413,
            detail=f"Question too long. Max length is {MAX_QUESTION_CHARS} characters",
        )

    context_blob = json.dumps(context, default=str)
    if len(context_blob.encode("utf-8")) > MAX_CONTEXT_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Context too large. Max size is {MAX_CONTEXT_BYTES // 1024} KB",
        )

    stats = context.get("stats") or context.get("stats_summary") or {}
    insights = context.get("insights") or {}
    file_name = context.get("fileName") or "dataset"


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
    outlier_summary = context.get("outlierSummary") or [
        {"column": col, "count": count} for col, count in top_outliers
    ]

    prompt = (
        "You are a data analyst assistant. "
        "Answer clearly in 2-4 short sentences based only on provided context. "
        "Never claim data is missing if it exists in context. "
        "If context is insufficient, say exactly which field is missing.\n\n"
        f"File: {file_name}\n"
        f"Dataset: {profile.get('label', 'unknown')} ({profile.get('domain', 'general')})\n"
        f"Data Quality: {quality}\n"
        f"Outlier Counts: {outlier_counts}\n"
        f"Outlier Summary: {outlier_summary}\n"
        f"Correlations: {correlations}\n"
        f"Stats: {slim_stats}\n"
        f"Insights: {insights}\n"
        f"Question: {question}"
    )

    api_key = os.getenv("GROQ_API_KEY")
    if api_key:
        try:
            from groq import Groq

            client = Groq(api_key=api_key)
            completion = client.chat.completions.create(
                model=os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"),
                messages=[
                    {"role": "system", "content": "You are a concise data analyst."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                max_tokens=220,
            )
            answer = (completion.choices[0].message.content or "").strip()
            if answer:
                return {"answer": answer}
        except Exception as exc:
            logger.warning("Groq chat failed, using fallback response: %s", exc)


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
            return {"answer": f"Top outlier columns in {file_name} are {summary}."}
        return {"answer": f"No outlier counts are present for {file_name}, or all detected counts are zero."}

    if any(k in q_lower for k in ["quality", "missing", "duplicate", "completeness"]):
        parts = [f"For {file_name}, data quality shows {missing_cells or 0} missing cells and {duplicate_rows or 0} duplicate rows."]
        if completeness is not None:
            parts.append(f"Completeness is {float(completeness):.2f}%.")
        return {"answer": " ".join(parts)}

    if "correlation" in q_lower:
        if correlations:
            top = correlations[0]
            return {"answer": f"The strongest reported correlation is {top.get('col1')} and {top.get('col2')} with r={float(top.get('correlation', 0)):.3f}."}
        return {"answer": "No strong correlations were provided in the current analysis context."}

    parts = [f"I analyzed {file_name} with {row_count} rows and {col_count} columns."]
    if completeness is not None:
        parts.append(f"Data completeness is {float(completeness):.2f}%.")
    if findings:
        parts.append(f"Key finding: {findings[0]}")
    return {"answer": " ".join(parts)}