import logging
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from backend.core.state import AnalysisState

logger = logging.getLogger(__name__)


def visualizer_agent(state: AnalysisState) -> AnalysisState:
    """Generate Plotly charts based on the cleaned data and statistics.

    Produces histograms for numeric columns, bar charts for categorical columns,
    and a correlation heatmap when multiple numeric columns exist.
    """
    state.current_agent = "visualizer"
    logger.info("Visualizer agent started")

    try:
        df = state.clean_df
        if df is None:
            raise ValueError("No clean_df available — architect must run first")

        charts: dict = {}
        num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        cat_cols = df.select_dtypes(include=["object"]).columns.tolist()

        # Histograms for numeric columns
        for col in num_cols:
            fig = px.histogram(df, x=col, title=f"Distribution of {col}", marginal="box")
            fig.update_layout(template="plotly_white")
            charts[f"histogram_{col}"] = fig.to_json()

        # Bar charts for categorical columns (top 10 values)
        for col in cat_cols:
            top_values = df[col].value_counts().head(10)
            fig = px.bar(
                x=top_values.index,
                y=top_values.values,
                title=f"Top Values in {col}",
                labels={"x": col, "y": "Count"},
            )
            fig.update_layout(template="plotly_white")
            charts[f"bar_{col}"] = fig.to_json()

        # Correlation heatmap
        if len(num_cols) >= 2:
            corr = df[num_cols].corr()
            fig = go.Figure(
                data=go.Heatmap(
                    z=corr.values,
                    x=corr.columns.tolist(),
                    y=corr.columns.tolist(),
                    colorscale="RdBu_r",
                    zmin=-1,
                    zmax=1,
                    text=np.round(corr.values, 2),
                    texttemplate="%{text}",
                )
            )
            fig.update_layout(title="Correlation Heatmap", template="plotly_white")
            charts["correlation_heatmap"] = fig.to_json()

        state.charts = charts
        logger.info("Generated %d charts", len(charts))

    except Exception as e:
        error_msg = f"Visualizer error: {e}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    state.completed_agents.append("visualizer")
    return state
