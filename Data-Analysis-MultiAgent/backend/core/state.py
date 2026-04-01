from pydantic import BaseModel, Field
from typing import Optional, Any
import pandas as pd


class AnalysisState(BaseModel):
    """Shared state passed between agents in the analysis pipeline."""

    model_config = {"arbitrary_types_allowed": True}

    # Agent 1: Architect
    file_path: Optional[str] = None
    raw_df: Optional[pd.DataFrame] = None
    clean_df: Optional[pd.DataFrame] = None
    column_types: dict[str, str] = Field(default_factory=dict)

    # Agent 2: Statistician
    stats_summary: dict[str, Any] = Field(default_factory=dict)

    # Agent 3: Visualizer
    charts: dict[str, Any] = Field(default_factory=dict)

    # Agent 4: Insights
    insights: dict[str, Any] = Field(default_factory=dict)

    # Errors (any agent can write here)
    errors: list[str] = Field(default_factory=list)

    # Pipeline metadata
    current_agent: Optional[str] = None
    completed_agents: list[str] = Field(default_factory=list)
