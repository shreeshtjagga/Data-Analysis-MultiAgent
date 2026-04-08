"""
models/schemas.py
─────────────────
Pydantic v2 request / response schemas for the FastAPI layer.

Changes vs original
────────────────────
• Added TokenResponse and TokenData for JWT auth flow.
• All models updated to Pydantic v2 syntax (model_config replaces inner Config).
• Stricter validators on email and password length.
"""

from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator


# ═══════════════════════════════════════════════════════════════════════════════
# AUTH SCHEMAS
# ═══════════════════════════════════════════════════════════════════════════════

class UserRegister(BaseModel):
    """Payload for POST /auth/register."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {"name": "John Doe", "email": "user@example.com", "password": "securepassword123"}
        }
    )

    name: Optional[str] = None
    email: EmailStr
    password: str = Field(..., min_length=6, description="At least 6 characters")

    @field_validator("password")
    @classmethod
    def password_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Password must not be blank")
        return v


class UserLogin(BaseModel):
    """Payload for POST /auth/login."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {"email": "user@example.com", "password": "securepassword123"}
        }
    )

    email: EmailStr
    password: str


class UserResponse(BaseModel):
    """Public user object — never includes password_hash."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    name: Optional[str] = None
    email: str
    created_at: datetime
    updated_at: datetime


class TokenResponse(BaseModel):
    """
    Response body for a successful login.

    ``access_token`` is a signed JWT; store it in memory (not localStorage).
    Send it as ``Authorization: Bearer <token>`` on every protected request.
    """

    access_token: str
    token_type: str = "bearer"
    user: UserResponse


class TokenData(BaseModel):
    """
    Decoded JWT payload — used internally by the FastAPI auth dependency.
    Not exposed as an API response.
    """

    sub: str          # str(user_id)
    email: str


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSIS HISTORY SCHEMAS
# ═══════════════════════════════════════════════════════════════════════════════

class AnalysisMetadata(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    file_name: str
    file_size: Optional[int] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    completeness: Optional[float] = None


class AnalysisHistoryList(BaseModel):
    """Lightweight record returned in the history list endpoint."""

    model_config = ConfigDict(from_attributes=True)

    analysis_id: int
    file_name: str
    file_hash: str
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    completeness: Optional[float] = None
    analyzed_at: Optional[datetime] = None


class AnalysisListResponse(BaseModel):
    success: bool
    message: str
    total: int
    analyses: List[AnalysisHistoryList]


class AnalysisStatsSummary(BaseModel):
    row_count: int
    column_count: int
    missing_cells: int
    duplicate_rows: int
    completeness: float
    outlier_cols: int
    strong_correlations: int


# ═══════════════════════════════════════════════════════════════════════════════
# GENERIC RESPONSE SCHEMAS
# ═══════════════════════════════════════════════════════════════════════════════

class AuthResponse(BaseModel):
    """Generic auth operation response (registration, etc.)."""

    success: bool
    message: str
    user: Optional[UserResponse] = None


class DeleteResponse(BaseModel):
    success: bool
    message: str


class HealthResponse(BaseModel):
    status: str
    postgres: bool
    redis: bool
    version: str = "2.0.0"