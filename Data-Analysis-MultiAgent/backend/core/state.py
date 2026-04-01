from pydantic import BaseModel, Field
from typing import Optional, Any
import pandas as pd


class AnalysisState(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    file_path: Optional[str] = None
    raw_df: Optional[pd.DataFrame] = None
    clean_df: Optional[pd.DataFrame] = None
    column_types: dict[str, str] = Field(default_factory=dict)

    stats_summary: dict[str, Any] = Field(default_factory=dict)

    charts: dict[str, Any] = Field(default_factory=dict)

    insights: dict[str, Any] = Field(default_factory=dict)

    errors: list[str] = Field(default_factory=list)

    current_agent: Optional[str] = None
    completed_agents: list[str] = Field(default_factory=list)