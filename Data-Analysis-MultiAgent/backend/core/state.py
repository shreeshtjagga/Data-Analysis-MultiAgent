from pydantic import BaseModel, Field
from typing import Optional, Any

class AnalysisState(BaseModel):

    class Config:
        arbitrary_types_allowed = True

    # Agent 1: Architect
    file_path: Optional[str] = None
    raw_df: Optional[Any] = None
    clean_df: Optional[Any] = None

    # Agent 2: Statistician
    stats_summary: Optional[dict] = Field(default_factory=dict)

    # Agent 3: Visualizer
    charts: Optional[dict] = Field(default_factory=dict)

    # Agent 4: Insights
    insights: Optional[dict] = Field(default_factory=dict)

    # Errors (any agent can write here)
    errors: Optional[list] = Field(default_factory=list)