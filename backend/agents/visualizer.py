"""Visualizer Agent — data-driven chart selection with heuristic scoring.

Chart selection priority (by heuristic score):
  1. Timeseries line      – temporal trend; highest value when date col present
  2. Multi-line series    – multiple numeric trends on same date axis
  3. Correlation scatter  – strongest numeric pair found by statistician
  4. Bar (mean by cat)    – category vs numeric; sorted, coloured
  5. Correlation heatmap  – ≥3 numeric cols with strong signals
  6. Violin / histogram   – distribution; violin when cat grouping exists
  7. Donut / bar counts   – categorical composition / frequency
  8. Box overview         – spread comparison across remaining numeric cols
"""

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

# ── Design tokens ────────────────────────────────────────────────────────────
COLOR_PALETTE     = px.colors.qualitative.Set2
TEMPLATE          = "plotly_white"
MAX_OUTPUT_CHARTS = 6

# ── Performance thresholds ───────────────────────────────────────────────────
# Max raw rows sent to Plotly per chart type.
# Above these limits we sample/aggregate rather than pass all rows.
_SCATTER_MAX_ROWS   = 2_000   # scatter + violin: heavy WebGL even with canvas
_HIST_MAX_ROWS      = 5_000   # histogram + box: Plotly bins client-side anyway
_TS_MAX_POINTS      = 500     # points per series on time axis
_OLS_MAX_ROWS       = 3_000   # statsmodels OLS trendline cutoff


# ── Helpers ──────────────────────────────────────────────────────────────────

@dataclass
class ScoredChart:
    key:    str
    fig:    go.Figure
    score:  float
    reason: str = ""


def _completeness(series: pd.Series) -> float:
    return 1.0 - float(series.isna().mean())


def _str_cols(df: pd.DataFrame) -> list:
    """Columns that are genuinely categorical (string, non-numeric, non-bool)."""
    return [
        col for col in df.columns
        if pd.api.types.is_string_dtype(df[col])
        and not pd.api.types.is_bool_dtype(df[col])
        and not pd.api.types.is_numeric_dtype(df[col])
    ]


def _sample(df: pd.DataFrame, max_rows: int,
            stratify_col: str | None = None) -> pd.DataFrame:
    """Return at most *max_rows* rows.

    Uses stratified sampling when *stratify_col* is provided so every
    category keeps proportional representation.  Falls back to random
    sampling otherwise.  Always returns the original df unchanged when
    it already fits within the budget.
    """
    if len(df) <= max_rows:
        return df
    if stratify_col and stratify_col in df.columns:
        try:
            return (
                df.groupby(stratify_col, group_keys=False)
                .apply(lambda g: g.sample(
                    min(len(g), max(1, int(max_rows * len(g) / len(df)))),
                    random_state=42,
                ))
                .reset_index(drop=True)
            )
        except Exception:
            pass
    return df.sample(max_rows, random_state=42).reset_index(drop=True)


def _resample_timeseries(df: pd.DataFrame, date_col: str,
                         value_cols: list, max_points: int) -> pd.DataFrame:
    """Aggregate a long timeseries to at most *max_points* per series.

    Chooses the coarsest frequency (D → W → M → Q → Y) that brings
    the row count within budget.  Returns the original df if it already
    fits.
    """
    if len(df) <= max_points:
        return df
    df2 = df[[date_col] + value_cols].dropna(subset=[date_col]).copy()
    df2[date_col] = pd.to_datetime(df2[date_col])
    df2 = df2.set_index(date_col).sort_index()
    for freq in ("D", "W", "ME", "QE", "YE"):
        resampled = df2[value_cols].resample(freq).mean().dropna(how="all").reset_index()
        if len(resampled) <= max_points:
            return resampled
    # absolute fallback: evenly spaced sample
    step = max(1, len(df2) // max_points)
    return df2.iloc[::step].reset_index()


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Try to promote string columns to datetime or numeric."""
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


def _optimal_nbins(n: int) -> int:
    """Sturges-inspired bin count capped for visual clarity."""
    return min(max(int(np.ceil(np.log2(n) + 1)), 10), 60)


# ── Chart builders ────────────────────────────────────────────────────────────

def _try_timeseries(
    df: pd.DataFrame, date_cols: list, num_cols: list
) -> tuple[Optional[ScoredChart], set]:
    """Single or multi-line time series. Returns (chart | None, consumed_cols)."""
    if not date_cols or not num_cols:
        return None, set()

    dcol = date_cols[0]
    comp_d = _completeness(df[dcol])
    if len(df) < 5 or comp_d < 0.80:
        return None, set()

    df_sorted = df.sort_values(dcol)

    # Score each numeric column for timeseries suitability
    valid_nums = [
        c for c in num_cols if _completeness(df[c]) >= 0.70
    ]
    if not valid_nums:
        return None, set()

    # Multi-line: up to 4 numeric series on the same date axis when ≥2 cols qualify
    if len(valid_nums) >= 2:
        series_cols = valid_nums[:4]
        plot_df = _resample_timeseries(df_sorted, dcol, series_cols, _TS_MAX_POINTS)
        df_long = plot_df.melt(id_vars=dcol, var_name="Series", value_name="Value")
        comp_avg = float(np.mean([_completeness(df[c]) for c in series_cols]))
        n = len(df)
        agg_note = f" (agg to {len(plot_df)} pts)" if len(plot_df) < n else ""
        score = 55 + min(n / 10, 30) + comp_avg * 15 + len(series_cols) * 2
        fig = px.line(
            df_long, x=dcol, y="Value", color="Series",
            title=f"Numeric Trends Over Time{agg_note}",
            markers=(len(plot_df) <= 40),
            color_discrete_sequence=COLOR_PALETTE,
        )
        return (
            ScoredChart("timeseries", _style(fig), round(score, 1),
                        f"date={dcol}, series={series_cols}, rows={n}{agg_note}"),
            {dcol} | set(series_cols),
        )

    # Single numeric series
    ncol = valid_nums[0]
    comp = min(comp_d, _completeness(df[ncol]))
    n = len(df)
    plot_df = _resample_timeseries(df_sorted, dcol, [ncol], _TS_MAX_POINTS)
    agg_note = f" (agg to {len(plot_df)} pts)" if len(plot_df) < n else ""
    score = 45 + min(n / 10, 30) + comp * 25
    fig = px.line(
        plot_df, x=dcol, y=ncol,
        title=f"{ncol} Over Time{agg_note}",
        markers=(len(plot_df) <= 60),
        color_discrete_sequence=[COLOR_PALETTE[0]],
    )
    return (
        ScoredChart("timeseries", _style(fig), round(score, 1),
                    f"date={dcol}, val={ncol}, rows={n}, comp={comp:.0%}{agg_note}"),
        {dcol, ncol},
    )


def _try_scatter(
    df: pd.DataFrame, num_cols: list, cat_cols: list, stats: dict
) -> tuple[Optional[ScoredChart], set]:
    """Best-correlated scatter. Returns (chart | None, consumed_cols)."""
    if len(df) < 10 or len(num_cols) < 2:
        return None, set()

    # Prefer the pair with the strongest correlation from the statistician
    top_corr = stats.get("strong_correlations", [])
    if top_corr:
        best = max(top_corr, key=lambda x: abs(x["correlation"]))
        x_col, y_col, r = best["col1"], best["col2"], best["correlation"]
        # Validate columns still in df
        if x_col not in df.columns or y_col not in df.columns:
            top_corr = []
    if not top_corr:
        x_col, y_col = num_cols[0], num_cols[1]
        r = float(df[[x_col, y_col]].dropna().corr().iloc[0, 1])

    if abs(r) < 0.15:          # too weak to be worth showing
        return None, set()

    comp = min(_completeness(df[x_col]), _completeness(df[y_col]))
    score = abs(r) * 55 + min(len(df) / 20, 20) + comp * 25

    # Use a low-cardinality cat column for colour if available
    color_col = next(
        (c for c in cat_cols if 2 <= df[c].nunique() <= 8), None
    )

    # Downsample before rendering — keep stratification by cat col
    plot_df = _sample(df, _SCATTER_MAX_ROWS, stratify_col=color_col)
    sampled = len(plot_df) < len(df)
    title = f"{x_col} vs {y_col}  (r\u2009=\u2009{r:.2f})"
    if sampled:
        title += f"  [{_SCATTER_MAX_ROWS:,} of {len(df):,} rows sampled]"
    # Skip OLS on large sets — it's O(n) but the serialisation overhead is high
    trendline = "ols" if len(plot_df) <= _OLS_MAX_ROWS else None

    try:
        fig = px.scatter(
            plot_df, x=x_col, y=y_col, color=color_col,
            title=title,
            trendline=trendline,
            opacity=0.65,
            color_discrete_sequence=COLOR_PALETTE,
        )
    except ModuleNotFoundError as exc:
        if "statsmodels" in str(exc):
            fig = px.scatter(
                plot_df, x=x_col, y=y_col, color=color_col,
                title=title,
                opacity=0.65,
                color_discrete_sequence=COLOR_PALETTE,
            )
        else:
            raise

    return (
        ScoredChart("scatter", _style(fig), round(score, 1),
                    f"x={x_col}, y={y_col}, r={r:.2f}, comp={comp:.0%}, rows={len(plot_df)}"),
        {x_col, y_col},
    )


def _try_bar_mean(
    df: pd.DataFrame, cat_cols: list, num_cols: list, used: set
) -> Optional[ScoredChart]:
    """Mean of best numeric column grouped by best categorical column."""
    if not cat_cols or not num_cols:
        return None

    # Pick the categorical column with the best cardinality / completeness
    def _cat_score(col: str) -> float:
        n = df[col].nunique()
        if not (2 <= n <= 20):
            return -1.0
        bonus = 1.0 if 4 <= n <= 10 else 0.6
        return _completeness(df[col]) * bonus

    best_cat = max(cat_cols, key=_cat_score)
    if _cat_score(best_cat) < 0:
        return None

    # Prefer a numeric column not already consumed
    num_col = next((c for c in num_cols if c not in used), num_cols[0])
    comp = min(_completeness(df[best_cat]), _completeness(df[num_col]))
    n_groups = df[best_cat].nunique()
    score = 38 + comp * 40 + min(n_groups * 2, 20)

    grouped = (
        df.groupby(best_cat)[num_col].mean()
        .reset_index()
        .sort_values(num_col, ascending=False)
    )
    # Horizontal bars look much better for many categories
    orientation = "h" if n_groups > 8 else "v"
    if orientation == "h":
        fig = px.bar(
            grouped, y=best_cat, x=num_col,
            title=f"Mean {num_col} by {best_cat}",
            orientation="h",
            color=best_cat,
            color_discrete_sequence=COLOR_PALETTE,
        )
    else:
        fig = px.bar(
            grouped, x=best_cat, y=num_col,
            title=f"Mean {num_col} by {best_cat}",
            color=best_cat,
            color_discrete_sequence=COLOR_PALETTE,
        )
    fig.update_layout(showlegend=False)
    return ScoredChart(
        "bar_mean", _style(fig, 440), round(score, 1),
        f"cat={best_cat}({n_groups}), num={num_col}, comp={comp:.0%}",
    )


def _try_heatmap(
    df: pd.DataFrame, num_cols: list, stats: dict, used: set
) -> Optional[ScoredChart]:
    """Correlation heatmap when ≥3 numeric columns exist."""
    eligible = [c for c in num_cols if c not in used or len(num_cols) >= 4]
    if len(eligible) < 3:
        return None
    cols = eligible[:12]          # cap to keep it readable
    comp_avg = float(np.mean([_completeness(df[c]) for c in cols]))
    if comp_avg < 0.75:
        return None

    n_strong = len(stats.get("strong_correlations", []))
    score = comp_avg * 40 + min(len(cols) * 3, 20) + min(n_strong * 4, 20)

    corr = df[cols].corr().round(2)
    fig = px.imshow(
        corr, text_auto=True, title="Correlation Heatmap",
        color_continuous_scale="RdBu_r", zmin=-1, zmax=1, aspect="auto",
    )
    height = max(380, min(len(cols) * 45, 560))
    return ScoredChart(
        "heatmap", _style(fig, height), round(score, 1),
        f"cols={len(cols)}, avg_comp={comp_avg:.0%}, strong={n_strong}",
    )


def _try_violin_or_histogram(
    df: pd.DataFrame, num_cols: list, cat_cols: list,
    stats: dict, used: set
) -> Optional[ScoredChart]:
    """
    Violin when a low-cardinality cat column exists (richer than box).
    Falls back to annotated histogram for the most interesting numeric col.
    """
    num_stats = stats.get("numeric_columns", {})
    outlier_keys = set(stats.get("outliers", {}).keys())

    def _col_score(col: str) -> float:
        if col in used:
            return -999.0
        skew = abs(num_stats.get(col, {}).get("skewness", 0.0))
        return _completeness(df[col]) * 40 + min(skew * 8, 25) + (
            20 if col in outlier_keys else 0
        )

    best_num = max(num_cols, key=_col_score) if num_cols else None
    if best_num is None or _col_score(best_num) < 0:
        return None

    # Prefer violin when we have a good cat grouping
    group_col = next(
        (c for c in cat_cols if 2 <= df[c].nunique() <= 6), None
    )
    comp = _completeness(df[best_num])
    skew = abs(num_stats.get(best_num, {}).get("skewness", 0.0))
    score = comp * 35 + min(skew * 8, 25) + (20 if best_num in outlier_keys else 0)

    if group_col:
        score += 10
        # Stratified sample so each group keeps proportional rows
        plot_df = _sample(df, _SCATTER_MAX_ROWS, stratify_col=group_col)
        sample_note = f"  [{len(plot_df):,} rows]" if len(plot_df) < len(df) else ""
        fig = px.violin(
            plot_df, y=best_num, x=group_col, box=True,
            points="outliers" if len(plot_df) <= 3_000 else False,
            title=f"Distribution of {best_num} by {group_col}{sample_note}",
            color=group_col,
            color_discrete_sequence=COLOR_PALETTE,
        )
        fig.update_layout(showlegend=False)
        return ScoredChart(
            f"violin_{best_num}", _style(fig, 450), round(score, 1),
            f"col={best_num}, group={group_col}, comp={comp:.0%}, rows={len(plot_df)}",
        )

    # Histogram — Plotly bins on the client so more rows ≈ slower JS
    plot_df = _sample(df, _HIST_MAX_ROWS)
    sample_note = f"  [{len(plot_df):,} rows sampled]" if len(plot_df) < len(df) else ""
    nbins = _optimal_nbins(int(plot_df[best_num].notna().sum()))
    title = f"Distribution of {best_num}{sample_note}"
    if best_num in outlier_keys:
        title += "  ⚠ outliers"
    fig = px.histogram(
        plot_df, x=best_num, nbins=nbins, title=title,
        marginal="box",
        color_discrete_sequence=[COLOR_PALETTE[0]],
    )
    return ScoredChart(
        f"histogram_{best_num}", _style(fig, 430), round(score, 1),
        f"col={best_num}, comp={comp:.0%}, skew={skew:.2f}, rows={len(plot_df)}",
    )


def _try_donut(df: pd.DataFrame, cat_cols: list, used: set) -> Optional[ScoredChart]:
    """Donut for low-cardinality categoricals with balanced distribution."""
    best_col, best_s = None, -1.0
    for col in cat_cols:
        if col in used:
            continue
        n = df[col].nunique()
        comp = _completeness(df[col])
        if not (2 <= n <= 7) or comp < 0.85:
            continue
        top_pct = df[col].value_counts(normalize=True).iloc[0]
        if top_pct > 0.93:          # skip near-constant columns
            continue
        s = comp + (1 - top_pct) + (1 / n)
        if s > best_s:
            best_col, best_s = col, s

    if best_col is None:
        return None

    n = df[best_col].nunique()
    comp = _completeness(df[best_col])
    score = 30 + comp * 38 + (7 - n) * 3

    counts = df[best_col].value_counts().reset_index()
    counts.columns = [best_col, "count"]
    fig = px.pie(
        counts, names=best_col, values="count",
        title=f"Composition of {best_col}",
        hole=0.45,
        color_discrete_sequence=COLOR_PALETTE,
    )
    fig.update_traces(textposition="outside", textinfo="percent+label")
    return ScoredChart(
        "donut", _style(fig, 420), round(score, 1),
        f"col={best_col}, unique={n}, comp={comp:.0%}",
    )


def _try_bar_counts(
    df: pd.DataFrame, cat_cols: list, used: set
) -> Optional[ScoredChart]:
    """Frequency bar for the most informative unused categorical column."""
    def _score(col: str) -> float:
        if col in used:
            return -1.0
        n = df[col].nunique()
        if not (2 <= n <= 25):
            return -1.0
        comp = _completeness(df[col])
        if comp < 0.70:
            return -1.0
        return comp * 35 + min(n * 1.5, 20)

    eligible = [c for c in cat_cols if _score(c) >= 0]
    if not eligible:
        return None

    best = max(eligible, key=_score)
    n = df[best].nunique()
    comp = _completeness(df[best])
    score = _score(best)

    vc = df[best].value_counts().head(20).reset_index()
    vc.columns = [best, "count"]
    orientation = "h" if n > 10 else "v"
    if orientation == "h":
        fig = px.bar(
            vc, y=best, x="count",
            title=f"Frequency of {best}",
            orientation="h",
            color=best,
            color_discrete_sequence=COLOR_PALETTE,
        )
    else:
        fig = px.bar(
            vc, x=best, y="count",
            title=f"Frequency of {best}",
            color=best,
            color_discrete_sequence=COLOR_PALETTE,
        )
    fig.update_layout(showlegend=False)
    return ScoredChart(
        f"bar_counts_{best}", _style(fig, 420), round(score, 1),
        f"col={best}, unique={n}, comp={comp:.0%}",
    )


def _try_boxplot(
    df: pd.DataFrame, num_cols: list, used: set
) -> Optional[ScoredChart]:
    """Side-by-side box for ≥2 remaining numeric columns — spread overview."""
    remaining = [c for c in num_cols if c not in used]
    if len(remaining) < 2:
        return None
    cols = remaining[:8]
    comp_avg = float(np.mean([_completeness(df[c]) for c in cols]))
    if comp_avg < 0.65:
        return None
    score = comp_avg * 35 + min(len(cols) * 4, 20)
    # Sample rows — box stats are stable at 5k; no need for all 18k
    plot_df = _sample(df, _HIST_MAX_ROWS)

    sample_note = f"  [{len(plot_df):,} rows sampled]" if len(plot_df) < len(df) else ""
    fig = go.Figure()
    for col in cols:
        fig.add_trace(go.Box(
            y=plot_df[col], name=col, boxmean=True,
            marker_color=COLOR_PALETTE[cols.index(col) % len(COLOR_PALETTE)],
        ))
    fig.update_layout(
        title=f"Numeric Columns — Distribution Overview{sample_note}",
        showlegend=False,
    )
    return ScoredChart(
        "boxplots", _style(fig, 430), round(score, 1),
        f"cols={cols}, avg_comp={comp_avg:.0%}, rows={len(plot_df)}",
    )


# ── Orchestrator ──────────────────────────────────────────────────────────────

def _select_charts(df: pd.DataFrame, stats: dict) -> dict:
    num_cols  = df.select_dtypes(include=np.number).columns.tolist()
    cat_cols  = _str_cols(df)
    date_cols = df.select_dtypes(include="datetime").columns.tolist()

    logger.info(
        "Column inventory — numeric: %d | categorical: %d | datetime: %d",
        len(num_cols), len(cat_cols), len(date_cols),
    )

    candidates: list[ScoredChart] = []
    used: set = set()

    def _add(result: Optional[ScoredChart], consumed: set = frozenset()) -> None:
        if result is not None:
            candidates.append(result)
            used.update(consumed)
            logger.debug(
                "Candidate %-26s score=%5.1f  [%s]",
                result.key, result.score, result.reason,
            )

    # ── Ordered by expected information value ──────────────────────────────
    ts, ts_cols = _try_timeseries(df, date_cols, num_cols)
    _add(ts, ts_cols)

    sc, sc_cols = _try_scatter(df, num_cols, cat_cols, stats)
    _add(sc, sc_cols)

    _add(_try_bar_mean(df, cat_cols, num_cols, used))
    _add(_try_heatmap(df, num_cols, stats, used))
    _add(_try_violin_or_histogram(df, num_cols, cat_cols, stats, used))
    _add(_try_donut(df, cat_cols, used))
    _add(_try_bar_counts(df, cat_cols, used))
    _add(_try_boxplot(df, num_cols, used))

    # ── Absolute fallback: raw bar of first two columns ────────────────────
    if not candidates and len(df.columns) >= 2:
        fig = px.bar(
            df.head(30), x=df.columns[0], y=df.columns[1],
            title=f"{df.columns[1]} by {df.columns[0]}",
        )
        candidates.append(ScoredChart("fallback", _style(fig), 10.0))

    # ── Deduplicate by chart type, then take top MAX_OUTPUT_CHARTS ─────────
    def _base_type(key: str) -> str:
        for prefix in ("histogram_", "bar_counts_", "violin_"):
            if key.startswith(prefix):
                return prefix.rstrip("_")
        return key

    seen_types: set = set()
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


# ── Entry point ───────────────────────────────────────────────────────────────

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
        df           = _coerce_types(state.clean_df)
        state.charts = _select_charts(df, state.stats_summary or {})
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