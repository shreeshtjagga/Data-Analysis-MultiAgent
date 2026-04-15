"""Visualizer Agent — FINAL FIXED & IMPROVED (Dashboard Style)
   • Critical bug fixed: numeric columns (sales, profit, etc.) are NO longer wrongly filtered as IDs
   • Smarter column selection + better scoring
   • New Scatter Matrix chart (perfect for analyst dashboards)
   • Professional centered titles, modern fonts, clean styling
   • Up to 8 high-quality charts
   • Loud confirmation so you know the new version is running
"""

import logging
from dataclasses import dataclass
from typing import Optional
import json
import os

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from ..core.state import AnalysisState
from ..core.errors import add_pipeline_error
from ..core.utils import truncate_stats_for_llm

logger = logging.getLogger(__name__)

# ── Design tokens (professional analyst dashboard) ──────────────────────────
COLOR_PALETTE     = px.colors.qualitative.Set2
TEMPLATE          = "plotly_white"
MAX_OUTPUT_CHARTS = 8

_SCATTER_MAX_ROWS   = 2_000
_HIST_MAX_ROWS      = 5_000
_TS_MAX_POINTS      = 500
_OLS_MAX_ROWS       = 3_000


@dataclass
class ScoredChart:
    key:    str
    fig:    go.Figure
    score:  float
    reason: str = ""


def _completeness(series: pd.Series) -> float:
    return 1.0 - float(series.isna().mean())


def _str_cols(df: pd.DataFrame) -> list:
    """Genuinely categorical string columns."""
    return [
        col for col in df.columns
        if pd.api.types.is_string_dtype(df[col])
        and not pd.api.types.is_bool_dtype(df[col])
        and not pd.api.types.is_numeric_dtype(df[col])
    ]


def _is_likely_id(df: pd.DataFrame, col: str) -> bool:
    """FIXED: Only true ID columns are removed. Numeric columns (sales, profit, etc.) are kept."""
    if col not in df.columns:
        return False
    series = df[col]
    n_rows = len(df)
    n_unique = series.nunique(dropna=True)

    if pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
        return n_unique > 0.95 * n_rows
    elif pd.api.types.is_integer_dtype(series):
        return n_unique > 0.85 * n_rows
    # Float columns are almost never IDs
    return False


def _sample(df: pd.DataFrame, max_rows: int, stratify_col: str | None = None) -> pd.DataFrame:
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


def _resample_timeseries(df: pd.DataFrame, date_col: str, value_cols: list, max_points: int) -> pd.DataFrame:
    if len(df) <= max_points:
        return df
    df2 = df[[date_col] + value_cols].dropna(subset=[date_col]).copy()
    df2[date_col] = pd.to_datetime(df2[date_col])
    df2 = df2.set_index(date_col).sort_index()
    for freq in ("D", "W", "ME", "QE", "YE"):
        resampled = df2[value_cols].resample(freq).mean().dropna(how="all").reset_index()
        if len(resampled) <= max_points:
            return resampled
    step = max(1, len(df2) // max_points)
    return df2.iloc[::step].reset_index()


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


def _style(fig: go.Figure, height: int = 440) -> go.Figure:
    """Dashboard-optimized styling"""
    fig.update_layout(
        template=TEMPLATE,
        height=height,
        font=dict(family="'Inter', 'DM Sans', system-ui, sans-serif", size=13),
        title=dict(font_size=17, x=0.5, xanchor="center", y=0.96, yanchor="top"),
        margin=dict(l=55, r=55, t=90, b=55),
        colorway=COLOR_PALETTE,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(font=dict(size=12), orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(showgrid=True, gridwidth=1, gridcolor="rgba(0,0,0,0.06)"),
        yaxis=dict(showgrid=True, gridwidth=1, gridcolor="rgba(0,0,0,0.06)"),
    )
    return fig


def _optimal_nbins(n: int) -> int:
    return min(max(int(np.ceil(np.log2(n) + 1)), 10), 60)


# ── Chart builders ──────────────────────────────────────────────────────────

def _try_timeseries(df: pd.DataFrame, date_cols: list, num_cols: list) -> tuple[Optional[ScoredChart], set]:
    if not date_cols or not num_cols:
        return None, set()

    def _date_score(dcol):
        comp = _completeness(df[dcol])
        uniq_ratio = df[dcol].nunique() / len(df)
        return comp * 60 + (1 - uniq_ratio) * 20

    dcol = max(date_cols, key=_date_score)
    comp_d = _completeness(df[dcol])
    if len(df) < 5 or comp_d < 0.80:
        return None, set()

    df_sorted = df.sort_values(dcol)
    valid_nums = [c for c in num_cols if _completeness(df[c]) >= 0.70]
    if not valid_nums:
        return None, set()

    if len(valid_nums) >= 2:
        series_cols = valid_nums[:4]
        plot_df = _resample_timeseries(df_sorted, dcol, series_cols, _TS_MAX_POINTS)
        df_long = plot_df.melt(id_vars=dcol, var_name="Series", value_name="Value")
        comp_avg = float(np.mean([_completeness(df[c]) for c in series_cols]))
        n = len(df)
        agg_note = f" (agg to {len(plot_df)} pts)" if len(plot_df) < n else ""
        score = 62 + min(n / 10, 28) + comp_avg * 18 + len(series_cols) * 3
        fig = px.line(df_long, x=dcol, y="Value", color="Series",
                      title=f"Trends Over Time{agg_note}", markers=(len(plot_df) <= 40))
        return ScoredChart("timeseries", _style(fig), round(score, 1),
                           f"best_date={dcol}, series={series_cols}{agg_note}"), {dcol} | set(series_cols)

    ncol = valid_nums[0]
    comp = min(comp_d, _completeness(df[ncol]))
    n = len(df)
    plot_df = _resample_timeseries(df_sorted, dcol, [ncol], _TS_MAX_POINTS)
    agg_note = f" (agg to {len(plot_df)} pts)" if len(plot_df) < n else ""
    score = 52 + min(n / 10, 28) + comp * 28
    fig = px.line(plot_df, x=dcol, y=ncol, title=f"{ncol} Over Time{agg_note}",
                  markers=(len(plot_df) <= 60))
    return ScoredChart("timeseries", _style(fig), round(score, 1),
                       f"date={dcol}, val={ncol}{agg_note}"), {dcol, ncol}


def _try_scatter(df: pd.DataFrame, num_cols: list, cat_cols: list, stats: dict) -> tuple[Optional[ScoredChart], set]:
    if len(df) < 10 or len(num_cols) < 2:
        return None, set()

    top_corr = stats.get("strong_correlations", [])
    if top_corr:
        best = max(top_corr, key=lambda x: abs(x["correlation"]))
        x_col, y_col, r = best["col1"], best["col2"], best["correlation"]
    else:
        x_col, y_col = num_cols[0], num_cols[1]
        r = float(df[[x_col, y_col]].dropna().corr().iloc[0, 1])

    if abs(r) < 0.18:
        return None, set()

    comp = min(_completeness(df[x_col]), _completeness(df[y_col]))
    score = abs(r) * 58 + min(len(df) / 18, 22) + comp * 25

    color_col = next((c for c in cat_cols if 2 <= df[c].nunique() <= 8), None)
    plot_df = _sample(df, _SCATTER_MAX_ROWS, stratify_col=color_col)
    sampled = len(plot_df) < len(df)
    title = f"{x_col} vs {y_col}  (r = {r:.2f})"
    if sampled:
        title += f"  [{_SCATTER_MAX_ROWS:,} of {len(df):,} sampled]"

    trendline = "ols" if len(plot_df) <= _OLS_MAX_ROWS else None
    fig = px.scatter(plot_df, x=x_col, y=y_col, color=color_col,
                     title=title, trendline=trendline, opacity=0.7)
    return ScoredChart("scatter", _style(fig), round(score, 1),
                       f"x={x_col}, y={y_col}, r={r:.2f}"), {x_col, y_col}


def _try_scatter_matrix(df: pd.DataFrame, num_cols: list, used: set, stats: dict) -> Optional[ScoredChart]:
    """New dashboard-friendly multivariate view"""
    eligible = [c for c in num_cols if c not in used]
    if len(eligible) < 3 or len(eligible) > 8:
        return None
    n_strong = len(stats.get("strong_correlations", []))
    score = 48 + min(len(eligible) * 6, 30) + n_strong * 5
    if score < 55:
        return None

    plot_df = _sample(df, 1200)
    dims = eligible[:6]
    fig = px.scatter_matrix(plot_df, dimensions=dims,
                            title="Scatter Matrix — All Numeric Relationships")
    fig.update_traces(diagonal_visible=False, showupperhalf=False)
    return ScoredChart("scatter_matrix", _style(fig, 560), round(score, 1),
                       f"cols={len(dims)}, strong_corrs={n_strong}")


def _try_bar_mean(df: pd.DataFrame, cat_cols: list, num_cols: list, used: set) -> Optional[ScoredChart]:
    if not cat_cols or not num_cols:
        return None

    def _cat_score(col: str) -> float:
        n = df[col].nunique()
        if not (2 <= n <= 20):
            return -1.0
        bonus = 1.0 if 4 <= n <= 10 else 0.6
        return _completeness(df[col]) * bonus

    best_cat = max(cat_cols, key=_cat_score)
    if _cat_score(best_cat) < 0:
        return None

    num_col = next((c for c in num_cols if c not in used), num_cols[0])
    comp = min(_completeness(df[best_cat]), _completeness(df[num_col]))
    n_groups = df[best_cat].nunique()
    score = 42 + comp * 42 + min(n_groups * 2.5, 22)

    grouped = df.groupby(best_cat)[num_col].mean().reset_index().sort_values(num_col, ascending=False)
    orientation = "h" if n_groups > 8 else "v"
    if orientation == "h":
        fig = px.bar(grouped, y=best_cat, x=num_col,
                     title=f"Mean {num_col} by {best_cat}", orientation="h", color=best_cat)
    else:
        fig = px.bar(grouped, x=best_cat, y=num_col,
                     title=f"Mean {num_col} by {best_cat}", color=best_cat)
    fig.update_layout(showlegend=False)
    return ScoredChart("bar_mean", _style(fig, 460), round(score, 1),
                       f"cat={best_cat}({n_groups}), num={num_col}")


def _try_heatmap(df: pd.DataFrame, num_cols: list, stats: dict, used: set) -> Optional[ScoredChart]:
    eligible = [c for c in num_cols if c not in used or len(num_cols) >= 4]
    if len(eligible) < 3:
        return None
    cols = eligible[:12]
    comp_avg = float(np.mean([_completeness(df[c]) for c in cols]))
    if comp_avg < 0.75:
        return None

    n_strong = len(stats.get("strong_correlations", []))
    score = comp_avg * 45 + min(len(cols) * 3.5, 22) + min(n_strong * 5, 22)

    corr = df[cols].corr().round(2)
    fig = px.imshow(corr, text_auto=True, title="Correlation Heatmap",
                    color_continuous_scale="RdBu_r", zmin=-1, zmax=1)
    height = max(380, min(len(cols) * 48, 580))
    return ScoredChart("heatmap", _style(fig, height), round(score, 1),
                       f"cols={len(cols)}, avg_comp={comp_avg:.0%}")


def _try_violin_or_histogram(df: pd.DataFrame, num_cols: list, cat_cols: list,
                             stats: dict, used: set) -> Optional[ScoredChart]:
    num_stats = stats.get("numeric_columns", {})
    outlier_keys = set(stats.get("outliers", {}).keys())

    def _col_score(col: str) -> float:
        if col in used:
            return -999.0
        skew = abs(num_stats.get(col, {}).get("skewness", 0.0))
        return _completeness(df[col]) * 42 + min(skew * 9, 28) + (22 if col in outlier_keys else 0)

    best_num = max(num_cols, key=_col_score) if num_cols else None
    if best_num is None or _col_score(best_num) < 0:
        return None

    group_col = next((c for c in cat_cols if 2 <= df[c].nunique() <= 6), None)
    comp = _completeness(df[best_num])
    skew = abs(num_stats.get(best_num, {}).get("skewness", 0.0))
    score = comp * 38 + min(skew * 9, 28) + (22 if best_num in outlier_keys else 0)

    if group_col:
        score += 12
        plot_df = _sample(df, _SCATTER_MAX_ROWS, stratify_col=group_col)
        fig = px.violin(plot_df, y=best_num, x=group_col, box=True,
                        points="outliers" if len(plot_df) <= 3000 else False,
                        title=f"Distribution of {best_num} by {group_col}",
                        color=group_col)
        fig.update_layout(showlegend=False)
        return ScoredChart(f"violin_{best_num}", _style(fig, 460), round(score, 1),
                           f"col={best_num}, group={group_col}")

    plot_df = _sample(df, _HIST_MAX_ROWS)
    nbins = _optimal_nbins(int(plot_df[best_num].notna().sum()))
    title = f"Distribution of {best_num}"
    if best_num in outlier_keys:
        title += "  ⚠ outliers"
    fig = px.histogram(plot_df, x=best_num, nbins=nbins, title=title, marginal="box")
    return ScoredChart(f"histogram_{best_num}", _style(fig, 440), round(score, 1),
                       f"col={best_num}, skew={skew:.2f}")


def _try_donut(df: pd.DataFrame, cat_cols: list, used: set) -> Optional[ScoredChart]:
    best_col, best_s = None, -1.0
    for col in cat_cols:
        if col in used:
            continue
        n = df[col].nunique()
        comp = _completeness(df[col])
        if not (2 <= n <= 7) or comp < 0.85:
            continue
        top_pct = df[col].value_counts(normalize=True).iloc[0]
        if top_pct > 0.93:
            continue
        s = comp + (1 - top_pct) + (1 / n)
        if s > best_s:
            best_col, best_s = col, s

    if best_col is None:
        return None

    n = df[best_col].nunique()
    comp = _completeness(df[best_col])
    score = 34 + comp * 40 + (7 - n) * 3.5

    counts = df[best_col].value_counts().reset_index()
    counts.columns = [best_col, "count"]
    fig = px.pie(counts, names=best_col, values="count",
                 title=f"Composition of {best_col}", hole=0.45)
    fig.update_traces(textposition="outside", textinfo="percent+label")
    return ScoredChart("donut", _style(fig, 430), round(score, 1),
                       f"col={best_col}, unique={n}")


def _try_bar_counts(df: pd.DataFrame, cat_cols: list, used: set) -> Optional[ScoredChart]:
    def _score(col: str) -> float:
        if col in used:
            return -1.0
        n = df[col].nunique()
        if not (2 <= n <= 60):
            return -1.0
        comp = _completeness(df[col])
        if comp < 0.60:
            return -1.0
        return comp * 38 + min(n * 1.6, 30)

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
        fig = px.bar(vc, y=best, x="count", title=f"Frequency of {best}",
                     orientation="h", color=best)
    else:
        fig = px.bar(vc, x=best, y="count", title=f"Frequency of {best}", color=best)
    fig.update_layout(showlegend=False)
    return ScoredChart(f"bar_counts_{best}", _style(fig, 430), round(score, 1),
                       f"col={best}, unique={n}")


def _try_boxplot(df: pd.DataFrame, num_cols: list, used: set) -> Optional[ScoredChart]:
    remaining = [c for c in num_cols if c not in used]
    if len(remaining) < 2:
        return None
    cols = remaining[:8]
    comp_avg = float(np.mean([_completeness(df[c]) for c in cols]))
    if comp_avg < 0.65:
        return None
    score = comp_avg * 38 + min(len(cols) * 4.5, 22)

    plot_df = _sample(df, _HIST_MAX_ROWS)
    fig = go.Figure()
    for col in cols:
        fig.add_trace(go.Box(y=plot_df[col], name=col, boxmean=True,
                             marker_color=COLOR_PALETTE[cols.index(col) % len(COLOR_PALETTE)]))
    fig.update_layout(title="Numeric Distribution Overview", showlegend=False)
    return ScoredChart("boxplots", _style(fig, 440), round(score, 1),
                       f"cols={len(cols)}, avg_comp={comp_avg:.0%}")


def _llm_select_charts(candidates: list[ScoredChart], stats: dict) -> list[ScoredChart]:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key or len(candidates) <= 2:
        return sorted(candidates, key=lambda c: c.score, reverse=True)[:MAX_OUTPUT_CHARTS]

    candidate_info = []
    for c in candidates:
        candidate_info.append({
            "id": c.key,
            "type": c.key.split('_')[0],
            "details": c.reason,
            "heuristic_score": c.score
        })
        
    try:
        from ..core.utils import truncate_stats_for_llm
        slim_stats = truncate_stats_for_llm(stats, max_numeric_cols=5, max_categorical_cols=5)
    except Exception:
        slim_stats = {}

    prompt = (
        "You are an expert Data Visualizer Agent. "
        "I have a dataset with the following summary statistics:\n"
        f"{json.dumps(slim_stats)}\n\n"
        "And I have generated the following candidate charts based on permutations of the valid data:\n"
        f"{json.dumps(candidate_info)}\n\n"
        f"Select the top {MAX_OUTPUT_CHARTS} charts that tell the most compelling, logical, and insightful data story. "
        "Avoid redundant charts that show the exact same variables unless they convey completely different insights. "
        "Prioritize charts with high impact (e.g., strong correlation scatters, key trend timeseries, clear distribution differences).\n\n"
        "Respond ONLY with a valid JSON format like this:\n"
        '{"selected_ids": ["id_1", "id_2", "..."]}'
    )
    
    try:
        from groq import Groq
        client = Groq(api_key=api_key)
        completion = client.chat.completions.create(
            model=os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"),
            messages=[
                {"role": "system", "content": "You are a chart orchestration LLM. Respond with valid JSON only. No markdown fences."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=200
        )
        raw = (completion.choices[0].message.content or "").strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        result = json.loads(raw)
        
        selected_ids = result.get("selected_ids", [])
        if not selected_ids:
            raise ValueError("No selected IDs returned")
            
        final_charts = []
        for sid in selected_ids:
            for c in candidates:
                if c.key == sid:
                    final_charts.append(c)
                    break
        
        if not final_charts:
             raise ValueError("LLM returned invalid IDs")
             
        return final_charts[:MAX_OUTPUT_CHARTS]
        
    except Exception as exc:
        logger.warning("LLM chart selection failed, falling back to heuristics: %s", exc)
        return sorted(candidates, key=lambda c: c.score, reverse=True)[:MAX_OUTPUT_CHARTS]


# ── Orchestrator ────────────────────────────────────────────────────────────
def _select_charts(df: pd.DataFrame, stats: dict) -> dict:
    num_cols = [col for col in df.select_dtypes(include=np.number).columns if not _is_likely_id(df, col)]
    cat_cols = [col for col in _str_cols(df) if not _is_likely_id(df, col)]
    date_cols = df.select_dtypes(include="datetime").columns.tolist()

    logger.info("Column inventory (cleaned) — numeric: %d | categorical: %d | datetime: %d",
                len(num_cols), len(cat_cols), len(date_cols))

    candidates: list[ScoredChart] = []
    used: set = set()

    def _add(result: Optional[ScoredChart], consumed: set = frozenset()):
        if result is not None:
            candidates.append(result)
            used.update(consumed)

    ts, ts_cols = _try_timeseries(df, date_cols, num_cols)
    _add(ts, ts_cols)

    sc, sc_cols = _try_scatter(df, num_cols, cat_cols, stats)
    _add(sc, sc_cols)

    _add(_try_scatter_matrix(df, num_cols, used, stats))
    _add(_try_bar_mean(df, cat_cols, num_cols, used))
    _add(_try_heatmap(df, num_cols, stats, used))
    _add(_try_violin_or_histogram(df, num_cols, cat_cols, stats, used))
    _add(_try_donut(df, cat_cols, used))
    _add(_try_bar_counts(df, cat_cols, used))
    _add(_try_boxplot(df, num_cols, used))

    if not candidates and len(df.columns) >= 2:
        fig = px.bar(df.head(30), x=df.columns[0], y=df.columns[1],
                     title=f"{df.columns[1]} by {df.columns[0]}")
        candidates.append(ScoredChart("fallback", _style(fig), 12.0))

    def _base_type(key: str) -> str:
        for prefix in ("histogram_", "bar_counts_", "violin_"):
            if key.startswith(prefix):
                return prefix.rstrip("_")
        return key

    # Filter out exact duplicate chart types to ensure variety before handing off
    seen = set()
    unique = []
    for chart in sorted(candidates, key=lambda c: c.score, reverse=True):
        t = _base_type(chart.key)
        if t not in seen:
            seen.add(t)
            unique.append(chart)

    selected = _llm_select_charts(unique, stats)
    
    logger.info("Selected %d improved dashboard charts: %s",
                len(selected), [(c.key, c.score) for c in selected])
    return {c.key: c.fig for c in selected}


# ── Entry point ─────────────────────────────────────────────────────────────
def run(state: AnalysisState) -> AnalysisState:
    logger.info("🚀 ENHANCED VISUALIZER v2.0 (Dashboard mode) LOADED — ID filter FIXED!")
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
        df = _coerce_types(state.clean_df)
        state.charts = _select_charts(df, state.stats_summary or {})
        logger.info("Visualizer done — %d improved dashboard charts generated", len(state.charts))
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