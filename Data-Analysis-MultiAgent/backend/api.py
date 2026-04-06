"""
api.py
──────
Main FastAPI application.

Routes
------
  POST  /auth/register        — create account
  POST  /auth/login           — get JWT
  GET   /auth/me              — current user (protected)

  POST  /analyze              — upload CSV, run pipeline, return result (protected)
  GET   /history              — list past analyses (protected)
  DELETE /history/{id}        — delete one analysis (protected)

  GET   /health               — postgres + redis liveness check
"""

import io
import logging
import os
import json
from contextlib import asynccontextmanager
from typing import Annotated, Optional

import pandas as pd
from fastapi import Body, Depends, FastAPI, File, HTTPException, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from .core import cache as redis_cache
from .analysis_history import (
    compute_file_hash,
    delete_analysis,
    get_analysis_by_hash,
    get_user_analysis_history,
    save_analysis,
)
from .auth import (
    create_access_token,
    get_user_by_id,
    login_user,
    register_user,
    verify_access_token,
)
from .core.graph import run_pipeline
from .db import get_db, init_db
from .models.schemas import (
    AnalysisListResponse,
    AuthResponse,
    DeleteResponse,
    HealthResponse,
    TokenResponse,
    UserLogin,
    UserRegister,
    UserResponse,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")

MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(25 * 1024 * 1024)))
MAX_QUESTION_CHARS = int(os.getenv("CHAT_MAX_QUESTION_CHARS", "1200"))
MAX_CONTEXT_BYTES = int(os.getenv("CHAT_MAX_CONTEXT_BYTES", str(128 * 1024)))
READ_CHUNK_BYTES = 1024 * 1024


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting DataPulse API v2")
    await init_db()
    yield
    await redis_cache.close()
    logger.info("DataPulse API shutdown complete")


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="DataPulse API",
    description="Multi-agent CSV analysis API",
    version="2.0.0",
    lifespan=lifespan,
)

# CORS — allow the Vite dev server and any configured production origins
_raw_origins = os.getenv("CORS_ORIGINS", "http://localhost:5173,http://localhost:3000")
origins = [o.strip() for o in _raw_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Auth dependency ───────────────────────────────────────────────────────────

security = HTTPBearer()


async def get_current_user_id(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
) -> int:
    """
    FastAPI dependency.
    Validates the Bearer JWT and returns the integer user_id.
    Raises 401 if the token is missing, expired, or malformed.
    """
    token = credentials.credentials
    payload = verify_access_token(token)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return int(payload["sub"])


# ── Health ────────────────────────────────────────────────────────────────────

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


# ── Auth routes ───────────────────────────────────────────────────────────────

@app.post("/auth/register", response_model=AuthResponse, status_code=status.HTTP_201_CREATED, tags=["auth"])
async def register(body: UserRegister, db: AsyncSession = Depends(get_db)):
    result = await register_user(db, body.email, body.password)
    if not result["success"]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result["message"])
    return AuthResponse(success=True, message=result["message"])


@app.post("/auth/login", response_model=TokenResponse, tags=["auth"])
async def login(body: UserLogin, db: AsyncSession = Depends(get_db)):
    result = await login_user(db, body.email, body.password)
    if not result["success"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=result["message"],
            headers={"WWW-Authenticate": "Bearer"},
        )
    user_obj = result["user"]
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
        email=user.email,
        created_at=user.created_at,
        updated_at=user.updated_at,
    )


# ── Analysis routes ───────────────────────────────────────────────────────────

@app.post("/analyze", tags=["analysis"])
async def analyze(
    file: UploadFile = File(...),
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    """
    Accept a CSV upload, run the full multi-agent pipeline,
    persist the result, and return the JSON analysis.

    On repeated uploads of the same file (same SHA-256 hash),
    returns the cached result immediately without re-running the pipeline.
    """
    filename = file.filename or ""
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted")

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

    file_hash = compute_file_hash(file_bytes, filename)

    # Cache hit — return immediately
    cached = await get_analysis_by_hash(db, user_id, file_hash)
    if cached and cached.get("stats_summary") and cached.get("insights"):
        logger.info("Returning cached analysis for user %d / %s", user_id, file.filename)
        # Charts stored as plain dicts — safe for JSON serialisation
        cached["charts"] = {k: v for k, v in (cached.get("charts") or {}).items()}
        return {"from_cache": True, **cached}

    # Run full pipeline
    try:
        df = pd.read_csv(io.BytesIO(file_bytes))
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Could not parse CSV: {exc}")

    state = run_pipeline(df)
    result = state.model_dump()

    # Persist and warm cache (fire-and-forget style — errors are logged, not raised)
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

    # Serialise charts to plain dicts before returning
    from .analysis_history import _serialize_charts
    result["charts"] = _serialize_charts(result.get("charts") or {})

    # Make DataFrames JSON-safe
    for key in ("raw_df", "clean_df"):
        df_val = result.get(key)
        if hasattr(df_val, "to_dict"):
            result[key] = df_val.head(100).to_dict(orient="records")

    return {"from_cache": False, **result}


# ── History routes ────────────────────────────────────────────────────────────

@app.get("/history", response_model=AnalysisListResponse, tags=["history"])
async def history(
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
    limit: int = 20,
):
    items = await get_user_analysis_history(db, user_id, limit=limit)
    return AnalysisListResponse(
        success=True,
        message="History retrieved",
        total=len(items),
        analyses=items,
    )


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
    body: dict = Body(...),
    user_id: int = Depends(get_current_user_id),
):
    """Answer a user question about the current analysis context."""
    _ = user_id  # Reserved for per-user quotas/history in future.

    question = (body.get("question") or "").strip()
    context = body.get("context") or {}

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

    stats = context.get("stats") or {}
    insights = context.get("insights") or {}
    file_name = context.get("fileName") or "dataset"

    prompt = (
        "You are a data analyst assistant. "
        "Answer clearly in 2-4 short sentences based only on provided context. "
        "If context is insufficient, say what is missing.\n\n"
        f"File: {file_name}\n"
        f"Stats: {stats}\n"
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

    # Fallback when no LLM key is configured or provider fails.
    row_count = stats.get("row_count", "unknown")
    col_count = stats.get("column_count", "unknown")
    completeness = (stats.get("data_quality") or {}).get("completeness")
    findings = insights.get("findings") or []

    parts = [f"I analyzed {file_name} with {row_count} rows and {col_count} columns."]
    if completeness is not None:
        parts.append(f"Data completeness is {float(completeness):.2f}%.")
    if findings:
        parts.append(f"Key finding: {findings[0]}")
    parts.append("For a deeper answer, set GROQ_API_KEY in backend environment.")
    return {"answer": " ".join(parts)}