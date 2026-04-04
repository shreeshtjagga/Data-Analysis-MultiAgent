import logging
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from core.state import AnalysisState

logger = logging.getLogger(__name__)


def visualizer_agent(state: AnalysisState) -> AnalysisState:
    """
    Visualizer Agent: Generates interactive Plotly charts based on
    the cleaned dataset and statistician findings.
    """
    state.current_agent = "visualizer"
    logger.info("Visualizer agent started")

    if state.clean_df is None or state.clean_df.empty:
        state.errors.append("Visualizer: No clean data available")
        state.completed_agents.append("visualizer")
        return state

    try:
        df = state.clean_df
        stats = state.stats_summary
        charts: dict = {}

        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        cat_cols = df.select_dtypes(include=["object"]).columns.tolist()

        # 1. Correlation heatmap (if >1 numeric columns)
        if len(numeric_cols) > 1:
            corr = df[numeric_cols].corr()
            fig = go.Figure(data=go.Heatmap(
                z=corr.values,
                x=corr.columns.tolist(),
                y=corr.columns.tolist(),
                colorscale="RdBu_r",
                zmin=-1,
                zmax=1,
                text=np.round(corr.values, 2),
                texttemplate="%{text}",
            ))
            fig.update_layout(title="Correlation Heatmap", height=500)
            charts["correlation_heatmap"] = fig

        # 2. Distribution histograms for top numeric columns
        for col in numeric_cols[:4]:
            fig = px.histogram(
                df, x=col,
                title=f"Distribution of {col}",
                marginal="box",
                opacity=0.7,
            )
            fig.update_layout(height=400)
            charts[f"dist_{col}"] = fig

        # 3. Box plots for numeric columns
        if numeric_cols:
            fig = go.Figure()
            for col in numeric_cols[:6]:
                fig.add_trace(go.Box(y=df[col], name=col))
            fig.update_layout(title="Box Plots — Numeric Columns", height=450)
            charts["box_plots"] = fig

        # 4. Category bar charts
        for col in cat_cols[:2]:
            vc = df[col].value_counts().head(10)
            fig = px.bar(
                x=vc.index.tolist(),
                y=vc.values.tolist(),
                labels={"x": col, "y": "Count"},
                title=f"Top Values — {col}",
            )
            fig.update_layout(height=400)
            charts[f"bar_{col}"] = fig

        # 5. Scatter plot for top correlated pair
        top_corr = stats.get("top_correlations", [])
        if top_corr:
            a, b, r = top_corr[0]
            fig = px.scatter(
                df, x=a, y=b,
                title=f"Scatter: {a} vs {b} (r={r:.2f})",
                opacity=0.6,
            )
            fig.update_layout(height=400)
            charts["scatter_top_corr"] = fig

        state.charts = charts
        logger.info("Visualizer complete. Generated %d charts.", len(charts))

    except Exception as e:
        error_msg = f"Visualizer error: {e}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    state.completed_agents.append("visualizer")
    return state
