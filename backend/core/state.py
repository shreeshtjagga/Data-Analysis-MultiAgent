from pydantic import BaseModel, Field
from typing import Optional, Any
import pandas as pd


class AnalysisState(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    raw_df: Optional[pd.DataFrame] = None

    # Data processing
    clean_df: Optional[pd.DataFrame] = None
    column_types: dict[str, str] = Field(default_factory=dict)

    # Analysis outputs
    stats_summary: dict[str, Any] = Field(default_factory=dict)
    charts: dict[str, Any] = Field(default_factory=dict)
    insights: dict[str, Any] = Field(default_factory=dict)

    # Error tracking
    errors: list[str] = Field(default_factory=list)
    partial: bool = False

    # Agent tracking
    current_agent: Optional[str] = None
    completed_agents: list[str] = Field(default_factory=list)