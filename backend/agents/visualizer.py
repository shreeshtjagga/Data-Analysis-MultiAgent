"""Visualizer Agent: generates, scores, and selects the most reliable diverse charts."""
import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from ..core.state import AnalysisState
from ..core.errors import add_pipeline_error

logger = logging.getLogger(__name__)

COLOR_PALETTE     = px.colors.qualitative.Set2
TEMPLATE          = "plotly_white"
MAX_OUTPUT_CHARTS = 6


@dataclass
class ScoredChart:
    key:    str
    fig:    go.Figure
    score:  float
    reason: str = ""


def _str_cols(df: pd.DataFrame) -> list:
    return [
        col for col in df.columns
        if pd.api.types.is_string_dtype(df[col])
        and not pd.api.types.is_bool_dtype(df[col])
        and not pd.api.types.is_numeric_dtype(df[col])
    ]


def _style(fig: go.Figure, height: int = 420) -> go.Figure:
    fig.update_layout(
        template=TEMPLATE,
        height=height,
        font=dict(family="'DM Sans', sans-serif", size=12),
        title_font_size=14,
        title_font_color="#1e293b",
        margin=dict(l=40, r=40, t=55, b=40),
        colorway=COLOR_PALETTE,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(font=dict(size=11)),
    )
    return fig


def _completeness(series: pd.Series) -> float:
    return 1.0 - float(series.isna().mean())


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in _str_cols(df):
        try:
            df[col] = pd.to_datetime(df[col], format="mixed", dayfirst=False)
            continue
        except Exception:
            pass
        try:
            df[col] = pd.to_numeric(df[col], errors="raise")
        except (ValueError, TypeError):
            pass
    return df


def _try_timeseries(
    df: pd.DataFrame, date_cols: list, num_cols: list
) -> Optional[ScoredChart]:
    if not date_cols or not num_cols:
        return None
    dcol, ncol = date_cols[0], num_cols[0]
    comp = min(_completeness(df[dcol]), _completeness(df[ncol]))
    n    = len(df)
    if n < 5 or comp < 0.80:
        return None
    score = 40 + min(n / 10, 30) + comp * 30
    fig   = px.line(
        df.sort_values(dcol), x=dcol, y=ncol,
        title=f"{ncol} Over Time",
        markers=(n <= 60),
    )
    return ScoredChart("timeseries", _style(fig), round(score, 1),
                       f"date={dcol}, val={ncol}, rows={n}, comp={comp:.0%}")


def _try_bar_mean(
    df: pd.DataFrame, cat_cols: list, num_cols: list
) -> Optional[ScoredChart]:
    if not cat_cols or not num_cols:
        return None
    best_cat, best_s = None, -1.0
    for col in cat_cols:
        n_uniq = df[col].nunique()
        if not (2 <= n_uniq <= 15):
            continue
        c     = _completeness(df[col])
        bonus = 1.0 if 4 <= n_uniq <= 8 else 0.6
        s     = c * bonus
        if s > best_s:
            best_cat, best_s = col, s
    if best_cat is None:
        return None
    num_col  = num_cols[0]
    comp     = min(_completeness(df[best_cat]), _completeness(df[num_col]))
    n_groups = df[best_cat].nunique()
    score    = 35 + comp * 40 + min(n_groups * 2, 20)
    grouped  = (
        df.groupby(best_cat)[num_col].mean()
        .reset_index().sort_values(num_col, ascending=False)
    )
    fig = px.bar(
        grouped, x=best_cat, y=num_col,
        title=f"Average {num_col} by {best_cat}",
        color=best_cat, color_discrete_sequence=COLOR_PALETTE,
    )
    fig.update_layout(showlegend=False)
    return ScoredChart("bar_mean", _style(fig), round(score, 1),
                       f"cat={best_cat}({n_groups}), num={num_col}, comp={comp:.0%}")


def _try_donut(df: pd.DataFrame, cat_cols: list) -> Optional[ScoredChart]:
    best_col, best_s = None, -1.0
    for col in cat_cols:
        n    = df[col].nunique()
        comp = _completeness(df[col])
        if not (2 <= n <= 6) or comp < 0.90:
            continue
        top_pct = df[col].value_counts(normalize=True).iloc[0]
        if top_pct > 0.95:
            continue
        s = comp + (1 - top_pct)
        if s > best_s:
            best_col, best_s = col, s
    if best_col is None:
        return None
    n     = df[best_col].nunique()
    comp  = _completeness(df[best_col])
    score = 30 + comp * 40 + (6 - n) * 3
    counts = df[best_col].value_counts().reset_index()
    counts.columns = [best_col, "count"]
    fig = px.pie(
        counts, names=best_col, values="count",
        title=f"Composition of {best_col}",
        hole=0.45, color_discrete_sequence=COLOR_PALETTE,
    )
    fig.update_traces(textposition="outside", textinfo="percent+label")
    return ScoredChart("donut", _style(fig, 420), round(score, 1),
                       f"col={best_col}, unique={n}, comp={comp:.0%}")


def _try_scatter(
    df: pd.DataFrame, num_cols: list, cat_cols: list, stats: dict
) -> Optional[ScoredChart]:
    if len(df) < 10 or len(num_cols) < 2:
        return None
    top_corr = stats.get("strong_correlations", [])
    if top_corr:
        best      = max(top_corr, key=lambda x: abs(x["correlation"]))
        x_col, y_col, r = best["col1"], best["col2"], best["correlation"]
    else:
        x_col, y_col = num_cols[0], num_cols[1]
        r = float(df[[x_col, y_col]].corr().iloc[0, 1])
    if x_col not in df.columns or y_col not in df.columns:
        return None
    comp  = min(_completeness(df[x_col]), _completeness(df[y_col]))
    abs_r = abs(r)
    if abs_r < 0.20 and not top_corr:
        return None
    score = abs_r * 50 + min(len(df) / 20, 20) + comp * 30
    color_arg = cat_cols[0] if cat_cols else None
    fig = px.scatter(
        df, x=x_col, y=y_col, color=color_arg,
        title=f"{x_col} vs {y_col}   (r = {r:.2f})",
        trendline="ols" if len(df) > 5 else None,
        opacity=0.70, color_discrete_sequence=COLOR_PALETTE,
    )
    return ScoredChart("scatter", _style(fig), round(score, 1),
                       f"x={x_col}, y={y_col}, r={r:.2f}, comp={comp:.0%}")


def _try_histogram(
    df: pd.DataFrame, num_cols: list, stats: dict, used: set
) -> Optional[ScoredChart]:
    if not num_cols:
        return None
    outlier_keys = set(stats.get("outliers", {}).keys())
    num_stats    = stats.get("numeric_columns", {})

    def _col_score(col: str) -> float:
        if col in used:
            return -999.0
        s    = _completeness(df[col]) * 30
        s   += 25 if col in outlier_keys else 0
        skew = abs(num_stats.get(col, {}).get("skewness", 0.0))
        s   += min(skew * 5, 20)
        return s

    best = max(num_cols, key=_col_score)
    if _col_score(best) < 0:
        return None

    comp  = _completeness(df[best])
    skew  = abs(num_stats.get(best, {}).get("skewness", 0.0))
    score = comp * 30 + min(skew * 5, 20) + (25 if best in outlier_keys else 0)
    title = f"Distribution of {best}"
    if best in outlier_keys:
        title += "  outliers present"
    fig = px.histogram(
        df, x=best, nbins=30, title=title,
        marginal="box",
        color_discrete_sequence=[COLOR_PALETTE[0]],
    )
    return ScoredChart(f"histogram_{best}", _style(fig), round(score, 1),
                       f"col={best}, comp={comp:.0%}, skew={skew:.2f}")


def _try_boxplot(
    df: pd.DataFrame, num_cols: list, used: set
) -> Optional[ScoredChart]:
    remaining = [c for c in num_cols if c not in used]
    if len(remaining) < 2:
        return None
    cols_to_plot = remaining[:6]
    comp_avg     = float(np.mean([_completeness(df[c]) for c in cols_to_plot]))
    if comp_avg < 0.70:
        return None
    score = comp_avg * 40 + min(len(cols_to_plot) * 5, 25)
    fig   = go.Figure()
    for col in cols_to_plot:
        fig.add_trace(go.Box(y=df[col], name=col, boxmean=True))
    fig.update_layout(title="Numeric Columns — Distribution Overview", showlegend=False)
    return ScoredChart("boxplots", _style(fig), round(score, 1),
                       f"cols={cols_to_plot}, avg_comp={comp_avg:.0%}")


def _try_heatmap(
    df: pd.DataFrame, num_cols: list, stats: dict
) -> Optional[ScoredChart]:
    if len(num_cols) < 3:
        return None
    comp_avg = float(np.mean([_completeness(df[c]) for c in num_cols]))
    if comp_avg < 0.80:
        return None
    n_corr = len(stats.get("strong_correlations", []))
    score  = comp_avg * 40 + min(len(num_cols) * 3, 20) + min(n_corr * 5, 20)
    corr   = df[num_cols].corr().round(2)
    fig    = px.imshow(
        corr, text_auto=True, title="Correlation Heatmap",
        color_continuous_scale="RdBu_r", zmin=-1, zmax=1, aspect="auto",
    )
    return ScoredChart("heatmap", _style(fig, 480), round(score, 1),
                       f"num_cols={len(num_cols)}, avg_comp={comp_avg:.0%}")


def _try_bar_counts(
    df: pd.DataFrame, cat_cols: list, used: set
) -> Optional[ScoredChart]:
    for col in cat_cols:
        if col in used:
            continue
        n    = df[col].nunique()
        comp = _completeness(df[col])
        if not (2 <= n <= 20) or comp < 0.75:
            continue
        score = comp * 35 + min(n * 2, 20)
        vc    = df[col].value_counts().head(15).reset_index()
        vc.columns = [col, "count"]
        fig = px.bar(
            vc, x=col, y="count",
            title=f"Frequency of {col}",
            color=col, color_discrete_sequence=COLOR_PALETTE,
        )
        fig.update_layout(showlegend=False)
        return ScoredChart(f"bar_counts_{col}", _style(fig), round(score, 1),
                           f"col={col}, unique={n}, comp={comp:.0%}")
    return None


def _select_charts(df: pd.DataFrame, stats: dict) -> dict:
    num_cols  = df.select_dtypes(include=np.number).columns.tolist()
    cat_cols  = _str_cols(df)
    date_cols = df.select_dtypes(include="datetime").columns.tolist()

    logger.info(
        "Column inventory — numeric: %d | categorical: %d | datetime: %d",
        len(num_cols), len(cat_cols), len(date_cols),
    )

    candidates: list = []
    used: set        = set()

    def _add(result: Optional[ScoredChart]) -> None:
        if result is not None:
            candidates.append(result)
            logger.debug(
                "Candidate %-22s score=%5.1f  [%s]",
                result.key, result.score, result.reason,
            )

    ts = _try_timeseries(df, date_cols, num_cols)
    _add(ts)
    if ts and date_cols and num_cols:
        used.update([date_cols[0], num_cols[0]])

    sc = _try_scatter(df, num_cols, cat_cols, stats)
    _add(sc)
    if sc:
        top_corr = stats.get("top_correlations", [])
        if top_corr:
            used.update([top_corr[0][0], top_corr[0][1]])

    _add(_try_bar_mean(df, cat_cols, num_cols))
    _add(_try_donut(df, cat_cols))
    _add(_try_heatmap(df, num_cols, stats))
    _add(_try_histogram(df, num_cols, stats, used))
    _add(_try_boxplot(df, num_cols, used))
    _add(_try_bar_counts(df, cat_cols, used))

    if not candidates and len(df.columns) >= 2:
        fig = px.bar(
            df.head(30), x=df.columns[0], y=df.columns[1],
            title=f"{df.columns[1]} by {df.columns[0]}",
        )
        candidates.append(ScoredChart("fallback", _style(fig), 10.0))

    def _base_type(key: str) -> str:
        for prefix in ("histogram_", "bar_counts_"):
            if key.startswith(prefix):
                return prefix.rstrip("_")
        return key

    seen_types: set          = set()
    unique: list[ScoredChart] = []
    for chart in sorted(candidates, key=lambda c: c.score, reverse=True):
        t = _base_type(chart.key)
        if t not in seen_types:
            seen_types.add(t)
            unique.append(chart)

    selected = sorted(unique, key=lambda c: c.score, reverse=True)[:MAX_OUTPUT_CHARTS]

    logger.info(
        "Selected %d/%d charts: %s",
        len(selected), len(candidates),
        [(c.key, c.score) for c in selected],
    )
    return {c.key: c.fig for c in selected}


def run(state: AnalysisState) -> AnalysisState:
    logger.info("Visualizer starting")
    state.current_agent = "visualizer"

    if state.clean_df is None or state.clean_df.empty:
        add_pipeline_error(
            state.errors,
            code="VISUALIZER_NO_DATA",
            message="No clean_df available",
            agent="visualizer",
            error_type="validation",
        )
        return state

    try:
        df             = _coerce_types(state.clean_df)
        state.charts   = _select_charts(df, state.stats_summary)
        logger.info("Visualizer done — %d charts emitted", len(state.charts))
        state.completed_agents.append("visualizer")

    except Exception as exc:
        add_pipeline_error(
            state.errors,
            code="VISUALIZER_FAILED",
            message=str(exc),
            agent="visualizer",
            error_type="agent",
        )
        logger.exception("Visualizer error")

    return state


visualizer_agent = run