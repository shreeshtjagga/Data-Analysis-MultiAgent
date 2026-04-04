"""
Visualizer Agent
================
Generates only RELEVANT charts based on actual data composition.
- If data has dates: time series charts
- If data has categories: bar/pie charts  
- If data has multiple numerics: correlation heatmap + scatter for top pair
- Always: distributions for key numeric columns
- Skips any chart type that doesn't apply to the data
"""

import logging
import pandas as pd
import numpy as np
from typing import Optional, Dict, List
import plotly.express as px
import plotly.graph_objects as go
from core.state import AnalysisState

logger = logging.getLogger(__name__)

_C = [
    "#7C3AED", "#2563EB", "#059669", "#DC2626",
    "#D97706", "#DB2777", "#0891B2", "#65A30D",
]

_DIVERGING = [
    [0.0, "#2563EB"],
    [0.5, "#0f172a"],
    [1.0, "#DC2626"],
]


def _base_layout(title: str, height: int = 420) -> dict:
    return dict(
        title=dict(
            text=title,
            font=dict(family="Plus Jakarta Sans, sans-serif", size=14, color="#e2e8f0"),
            x=0.0, xanchor="left", pad=dict(l=4),
        ),
        height=height,
        font=dict(family="Plus Jakarta Sans, sans-serif", size=12, color="#94a3b8"),
        paper_bgcolor="rgba(15,23,42,0)",
        plot_bgcolor="rgba(15,23,42,0.4)",
        margin=dict(l=50, r=20, t=52, b=50),
        xaxis=dict(
            gridcolor="rgba(148,163,184,0.07)",
            linecolor="rgba(148,163,184,0.15)",
            tickfont=dict(size=11),
        ),
        yaxis=dict(
            gridcolor="rgba(148,163,184,0.07)",
            linecolor="rgba(148,163,184,0.15)",
            tickfont=dict(size=11),
        ),
        colorway=_C,
        legend=dict(
            bgcolor="rgba(15,23,42,0.6)",
            bordercolor="rgba(148,163,184,0.15)",
            borderwidth=1,
            font=dict(size=11),
        ),
    )


def _build_histogram(df: pd.DataFrame, col: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=df[col].dropna(),
        nbinsx=35,
        name=col,
        marker=dict(color=_C[0], opacity=0.85, line=dict(width=0)),
    ))
    layout = _base_layout(f"Distribution of {col}")
    layout["bargap"] = 0.04
    layout["showlegend"] = False
    layout["xaxis"]["title"] = dict(text=col, font=dict(size=12))
    layout["yaxis"]["title"] = dict(text="Count", font=dict(size=12))
    fig.update_layout(**layout)
    return fig


def _build_box_plots(df: pd.DataFrame, cols: List[str]) -> go.Figure:
    fig = go.Figure()
    for i, col in enumerate(cols[:8]):
        c = _C[i % len(_C)]
        r, g, b = int(c[1:3], 16), int(c[3:5], 16), int(c[5:7], 16)
        fig.add_trace(go.Box(
            y=df[col].dropna(),
            name=col,
            marker_color=c,
            boxmean=True,
            line=dict(width=1.5),
            fillcolor=f"rgba({r},{g},{b},0.2)",
        ))
    layout = _base_layout("Spread and Outliers by Column", height=430)
    layout["showlegend"] = False
    layout["yaxis"]["title"] = dict(text="Value", font=dict(size=12))
    fig.update_layout(**layout)
    return fig


def _build_correlation_heatmap(df: pd.DataFrame, cols: List[str]) -> go.Figure:
    corr = df[cols].corr().round(2)
    n = len(cols)
    height = max(380, min(600, n * 60))
    fig = go.Figure(go.Heatmap(
        z=corr.values,
        x=corr.columns.tolist(),
        y=corr.columns.tolist(),
        colorscale=_DIVERGING,
        zmin=-1, zmax=1,
        text=corr.values,
        texttemplate="%{text:.2f}",
        textfont=dict(size=10, color="#e2e8f0"),
        hoverongaps=False,
        colorbar=dict(thickness=12, len=0.8, tickfont=dict(size=10, color="#94a3b8")),
    ))
    layout = _base_layout("How Columns Relate to Each Other", height=height)
    layout.pop("xaxis", None)
    layout.pop("yaxis", None)
    layout["xaxis"] = dict(tickfont=dict(size=10, color="#94a3b8"))
    layout["yaxis"] = dict(tickfont=dict(size=10, color="#94a3b8"), autorange="reversed")
    fig.update_layout(**layout)
    return fig


def _build_scatter(df: pd.DataFrame, col_a: str, col_b: str,
                   r: float, color_col: Optional[str] = None) -> go.Figure:
    sample = df.sample(min(len(df), 1500), random_state=42) if len(df) > 1500 else df
    if color_col and color_col in df.columns and df[color_col].nunique() <= 8:
        fig = px.scatter(
            sample, x=col_a, y=col_b, color=color_col,
            opacity=0.7, color_discrete_sequence=_C, trendline="ols",
        )
    else:
        fig = px.scatter(
            sample, x=col_a, y=col_b, opacity=0.65,
            color_discrete_sequence=[_C[0]], trendline="ols",
            trendline_color_override=_C[3],
        )
    fig.update_traces(marker=dict(size=5, line=dict(width=0)))
    layout = _base_layout(f"{col_a} vs {col_b}  (relationship: {abs(r):.2f})")
    layout["xaxis"]["title"] = dict(text=col_a, font=dict(size=12))
    layout["yaxis"]["title"] = dict(text=col_b, font=dict(size=12))
    fig.update_layout(**layout)
    return fig


def _build_bar_chart(df: pd.DataFrame, col: str) -> go.Figure:
    vc = df[col].value_counts().head(12)
    labels = [str(x) for x in vc.index.tolist()]
    values = vc.values.tolist()
    bar_colors = [_C[i % len(_C)] for i in range(len(labels))]
    fig = go.Figure(go.Bar(
        x=values, y=labels, orientation="h",
        marker=dict(color=bar_colors, line=dict(width=0)),
        text=[str(v) for v in values],
        textposition="outside",
        textfont=dict(size=11, color="#94a3b8"),
    ))
    layout = _base_layout(f"Most Common Values in {col}",
                          height=max(380, len(labels) * 36 + 80))
    layout["xaxis"]["title"] = dict(text="Count", font=dict(size=12))
    layout["yaxis"]["autorange"] = "reversed"
    layout["showlegend"] = False
    fig.update_layout(**layout)
    return fig


def _build_pie_chart(df: pd.DataFrame, col: str) -> go.Figure:
    vc = df[col].value_counts().head(6)
    fig = go.Figure(go.Pie(
        labels=[str(x) for x in vc.index.tolist()],
        values=vc.values.tolist(),
        hole=0.42,
        marker=dict(colors=_C[:len(vc)], line=dict(color="#0f172a", width=2)),
        textinfo="label+percent",
        textfont=dict(size=12, color="#e2e8f0"),
        insidetextorientation="radial",
    ))
    layout = _base_layout(f"Share of {col}", height=400)
    layout.pop("xaxis", None)
    layout.pop("yaxis", None)
    layout["legend"] = dict(font=dict(size=11, color="#94a3b8"), bgcolor="rgba(0,0,0,0)")
    fig.update_layout(**layout)
    return fig


def _build_time_series(df: pd.DataFrame, date_col: str,
                       numeric_cols: List[str]) -> Optional[go.Figure]:
    try:
        tmp = df.copy()
        tmp[date_col] = pd.to_datetime(tmp[date_col], errors="coerce")
        tmp = tmp.dropna(subset=[date_col]).sort_values(date_col)
        if len(tmp) < 4:
            return None
        cols_to_plot = [c for c in numeric_cols[:3] if c in tmp.columns]
        if not cols_to_plot:
            return None
        fig = go.Figure()
        for i, col in enumerate(cols_to_plot):
            series = tmp.set_index(date_col)[col].dropna()
            if len(series) > 300:
                step = max(1, len(series) // 300)
                series = series.iloc[::step]
            c = _C[i % len(_C)]
            r, g, b = int(c[1:3], 16), int(c[3:5], 16), int(c[5:7], 16)
            fig.add_trace(go.Scatter(
                x=series.index, y=series.values,
                mode="lines", name=col,
                line=dict(color=c, width=2),
                fill="tozeroy" if len(cols_to_plot) == 1 else None,
                fillcolor=f"rgba({r},{g},{b},0.08)" if len(cols_to_plot) == 1 else None,
            ))
        layout = _base_layout("  &  ".join(cols_to_plot) + " Over Time", height=420)
        layout["yaxis"]["title"] = dict(text="Value", font=dict(size=12))
        layout["hovermode"] = "x unified"
        fig.update_layout(**layout)
        return fig
    except Exception as e:
        logger.warning("Time series failed: %s", e)
        return None


def _build_missing_chart(df: pd.DataFrame) -> Optional[go.Figure]:
    missing = df.isna().sum()
    missing = missing[missing > 0].sort_values(ascending=False)
    if missing.empty:
        return None
    pct = (missing / len(df) * 100).round(1)
    colors = [_C[2] if p < 5 else (_C[4] if p < 20 else _C[3]) for p in pct.values]
    fig = go.Figure(go.Bar(
        x=pct.index.tolist(), y=pct.values.tolist(),
        marker=dict(color=colors, line=dict(width=0)),
        text=[f"{v}%" for v in pct.values],
        textposition="outside",
        textfont=dict(size=11, color="#94a3b8"),
    ))
    layout = _base_layout("Missing Data by Column (%)", height=380)
    layout["yaxis"]["title"] = dict(text="% Missing", font=dict(size=12))
    layout["showlegend"] = False
    layout["bargap"] = 0.3
    fig.update_layout(**layout)
    return fig


# ── Main agent ─────────────────────────────────────────────────────────────────

def visualizer_agent(state: AnalysisState) -> AnalysisState:
    state.current_agent = "visualizer"
    logger.info("Visualizer agent started")

    if state.clean_df is None or state.clean_df.empty:
        state.errors.append("Visualizer: No clean data available")
        state.completed_agents.append("visualizer")
        return state

    try:
        df        = state.clean_df
        stats     = state.stats_summary
        col_types = state.column_types
        charts    = {}

        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        cat_cols     = df.select_dtypes(include=["object", "category"]).columns.tolist()
        date_cols    = [c for c, t in col_types.items() if t == "datetime"]
        top_corr     = stats.get("top_correlations", [])

        # 1 — Time series (only when dates exist)
        if date_cols and numeric_cols:
            ts = _build_time_series(df, date_cols[0], numeric_cols)
            if ts:
                charts["time_series"] = ts

        # 2 — Correlation heatmap (only when 2+ numerics exist)
        if len(numeric_cols) >= 2:
            charts["correlation_heatmap"] = _build_correlation_heatmap(df, numeric_cols)

        # 3 — Scatter for strongest pair (only if real relationship exists)
        if top_corr:
            a, b, r = top_corr[0]
            if abs(r) > 0.3:
                color_col = cat_cols[0] if cat_cols else None
                try:
                    charts["scatter_top"] = _build_scatter(df, a, b, r, color_col)
                except Exception as e:
                    logger.warning("Scatter failed: %s", e)

        # 4 — Histograms: highest-variance cols first, max 3
        ranked_num = sorted(
            numeric_cols,
            key=lambda c: df[c].std() / (abs(df[c].mean()) + 1e-9),
            reverse=True,
        )
        for col in ranked_num[:3]:
            charts[f"hist_{col}"] = _build_histogram(df, col)

        # 5 — Box plots (only useful with 2+ numeric cols)
        if len(numeric_cols) >= 2:
            charts["box_plots"] = _build_box_plots(df, numeric_cols)

        # 6 — Category charts
        for col in cat_cols[:2]:
            nuniq = df[col].nunique()
            if nuniq < 2:
                continue
            if 2 <= nuniq <= 6:
                charts[f"pie_{col}"] = _build_pie_chart(df, col)
            else:
                charts[f"bar_{col}"] = _build_bar_chart(df, col)

        # 7 — Missing data chart (only if gaps exist)
        mf = _build_missing_chart(df)
        if mf:
            charts["missing_data"] = mf

        state.charts = charts
        logger.info("Visualizer complete — %d charts generated", len(charts))

    except Exception as e:
        error_msg = f"Visualizer error: {e}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    state.completed_agents.append("visualizer")
    return state