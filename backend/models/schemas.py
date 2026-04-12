from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

from ..core.constants import APP_VERSION


class UserRegister(BaseModel):
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
    model_config = ConfigDict(
        json_schema_extra={
            "example": {"email": "user@example.com", "password": "securepassword123"}
        }
    )

    email: EmailStr
    password: str


class GoogleLoginRequest(BaseModel):
    credential: str = Field(..., min_length=1)
    client_id: Optional[str] = None


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: Optional[str] = None
    email: str
    created_at: datetime
    updated_at: datetime


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


class TokenData(BaseModel):
    sub: str
    email: str


class AnalysisMetadata(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    file_name: str
    file_size: Optional[int] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    completeness: Optional[float] = None


class AnalysisHistoryList(BaseModel):
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


class ChatRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=1200)
    context: dict[str, Any] = Field(default_factory=dict)


class AuthResponse(BaseModel):
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
    version: str = APP_VERSION