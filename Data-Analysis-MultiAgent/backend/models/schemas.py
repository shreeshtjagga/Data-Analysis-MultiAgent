from pydantic import BaseModel, Field
from typing import Any, Optional


class AnalysisRequest(BaseModel):
    """Request body for triggering an analysis."""
    file_path: str = Field(..., description="Path to the CSV file to analyze")


class AnalysisResponse(BaseModel):
    """Response body containing analysis results."""
    stats_summary: dict[str, Any] = Field(default_factory=dict)
    charts: dict[str, Any] = Field(default_factory=dict)
    insights: dict[str, Any] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    completed_agents: list[str] = Field(default_factory=list)


class HealthResponse(BaseModel):
    """Health check response."""
    status: str = "ok"
    version: str = "1.0.0"
