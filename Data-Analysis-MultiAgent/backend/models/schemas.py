from pydantic import BaseModel, EmailStr, Field
from typing import Optional, Any, List
from datetime import datetime
import json


# ═══════════════════════════════════════════════════════════════════════════════
# USER SCHEMAS
# ═══════════════════════════════════════════════════════════════════════════════

class UserRegister(BaseModel):
    """Schema for user registration."""
    email: EmailStr
    password: str = Field(..., min_length=6, description="Password must be at least 6 characters")

    class Config:
        json_schema_extra = {
            "example": {
                "email": "user@example.com",
                "password": "securepassword123"
            }
        }


class UserLogin(BaseModel):
    """Schema for user login."""
    email: EmailStr
    password: str

    class Config:
        json_schema_extra = {
            "example": {
                "email": "user@example.com",
                "password": "securepassword123"
            }
        }


class UserResponse(BaseModel):
    """Schema for user response (without password)."""
    id: int
    email: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class UserInDB(BaseModel):
    """Schema for user stored in database."""
    id: int
    email: str
    password: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSIS HISTORY SCHEMAS
# ═══════════════════════════════════════════════════════════════════════════════

class AnalysisMetadata(BaseModel):
    """Metadata for quick lookups."""
    file_name: str
    file_size: Optional[int] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    completeness: Optional[float] = None

    class Config:
        json_schema_extra = {
            "example": {
                "file_name": "sales_data.csv",
                "file_size": 1024000,
                "row_count": 5000,
                "column_count": 15,
                "completeness": 95.5
            }
        }


class AnalysisHistoryCreate(BaseModel):
    """Schema for creating analysis history record."""
    file_name: str
    file_hash: str
    raw_data: Optional[dict] = None
    clean_data: Optional[dict] = None
    stats_summary: Optional[dict] = None
    charts: Optional[dict] = None
    insights: Optional[dict] = None
    errors: Optional[list] = None
    completed_agents: Optional[list] = None

    class Config:
        json_schema_extra = {
            "example": {
                "file_name": "sales_data.csv",
                "file_hash": "abc123def456",
                "stats_summary": {},
                "insights": {},
                "errors": []
            }
        }


class AnalysisHistoryResponse(BaseModel):
    """Schema for analysis history response."""
    id: int
    user_id: int
    file_name: str
    file_hash: str
    stats_summary: Optional[dict] = None
    insights: Optional[dict] = None
    errors: Optional[list] = None
    analysis_date: datetime
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    completeness: Optional[float] = None

    class Config:
        from_attributes = True


class AnalysisHistoryList(BaseModel):
    """Schema for listing analysis history (minimal info)."""
    id: int
    file_name: str
    file_hash: str
    analysis_date: datetime
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    completeness: Optional[float] = None

    class Config:
        from_attributes = True


class AnalysisStatsSummary(BaseModel):
    """Summary stats for a past analysis."""
    row_count: int
    column_count: int
    missing_cells: int
    duplicate_rows: int
    completeness: float
    outlier_cols: int
    strong_correlations: int


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION SCHEMAS
# ═══════════════════════════════════════════════════════════════════════════════

class SessionData(BaseModel):
    """Session data stored in Streamlit session state."""
    user_id: int
    email: str
    logged_in: bool
    login_time: datetime

    class Config:
        json_schema_extra = {
            "example": {
                "user_id": 1,
                "email": "user@example.com",
                "logged_in": True,
                "login_time": "2024-01-15T10:30:00"
            }
        }


# ═══════════════════════════════════════════════════════════════════════════════
# RESPONSE SCHEMAS
# ═══════════════════════════════════════════════════════════════════════════════

class AuthResponse(BaseModel):
    """Response for authentication operations."""
    success: bool
    message: str
    user: Optional[UserResponse] = None

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Login successful",
                "user": {
                    "id": 1,
                    "email": "user@example.com",
                    "created_at": "2024-01-15T10:30:00",
                    "updated_at": "2024-01-15T10:30:00"
                }
            }
        }


class AnalysisListResponse(BaseModel):
    """Response for analysis list."""
    success: bool
    message: str
    total: int
    analyses: List[AnalysisHistoryList]

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Analyses retrieved successfully",
                "total": 5,
                "analyses": []
            }
        }
