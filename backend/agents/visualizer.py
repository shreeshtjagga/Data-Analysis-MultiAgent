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


def _chart_family(key: str) -> str:
    if key.startswith("histogram_"):
        return "histogram"
    if key.startswith("bar_"):
        return "bar"
    if key.startswith("bar_mean"):
        return "bar"
    if key.startswith("line_"):
        return "line"
    if key.startswith("heatmap"):
        return "heatmap"
    if key.startswith("box_"):
        return "box"
    if key.startswith("scatter"):
        return "scatter"
    if key.startswith("violin_"):
        return "violin"
    if key.startswith("likert_"):
        return "bar"
    return "other"


def _safe_len(values) -> int:
    try:
        if values is None:
            return 0
        return int(np.size(values))
    except Exception:
        return 0


def _unique_count(values) -> int:
    try:
        if values is None:
            return 0
        arr = np.array(values).ravel()
        if arr.size == 0:
            return 0
        return int(pd.Series(arr).dropna().nunique())
    except Exception:
        return 0


def _to_list(values) -> list:
    """Safely convert trace arrays/sequences to a plain list without boolean checks."""
    try:
        if values is None:
            return []
        if isinstance(values, list):
            return values
        if isinstance(values, tuple):
            return list(values)
        if isinstance(values, np.ndarray):
            return values.ravel().tolist()
        # Plotly often uses array-like containers that are list()-compatible.
        return list(values)
    except Exception:
        return []


def _has_chart_signal(key: str, fig: go.Figure) -> bool:
    if not fig or not getattr(fig, "data", None):
        return False

    family = _chart_family(key)
    traces = [t for t in fig.data if t is not None]
    if not traces:
        return False

    if family == "line":
        x_vals = []
        y_vals = []
        for t in traces:
            x_vals.extend(_to_list(getattr(t, "x", None)))
            y_vals.extend(_to_list(getattr(t, "y", None)))
        return (_safe_len(x_vals) >= 12 and _unique_count(x_vals) >= 6 and _unique_count(y_vals) >= 4)

    if family == "histogram":
        vals = _to_list(getattr(traces[0], "x", None))
        if not vals:
            vals = _to_list(getattr(traces[0], "y", None))
        return _safe_len(vals) >= 25 and _unique_count(vals) >= 6

    if family == "bar":
        cats = _to_list(getattr(traces[0], "x", None))
        nums = _to_list(getattr(traces[0], "y", None))
        if _safe_len(cats) == 0:
            cats = _to_list(getattr(traces[0], "y", None))
            nums = _to_list(getattr(traces[0], "x", None))
        return _safe_len(cats) >= 2 and _unique_count(cats) >= 2 and _safe_len(nums) >= 2

    if family == "heatmap":
        z = getattr(traces[0], "z", None)
        try:
            arr = np.array(z)
            return arr.ndim == 2 and arr.shape[0] >= 2 and arr.shape[1] >= 2
        except Exception:
            return False

    if family == "box":
        y_vals = []
        for t in traces:
            y_vals.extend(_to_list(getattr(t, "y", None)))
        return _safe_len(y_vals) >= 20 and _unique_count(y_vals) >= 5

    # Generic fallback for other chart families.
    return any(_safe_len(getattr(t, axis, None)) > 0 for t in traces for axis in ("x", "y", "z", "values"))


def _chart_signal_score(key: str, fig: go.Figure) -> float:
    family = _chart_family(key)
    base = {
        "heatmap": 95.0,
        "line": 88.0,
        "scatter": 86.0,
        "box": 82.0,
        "violin": 80.0,
        "histogram": 76.0,
        "bar": 72.0,
        "other": 60.0,
    }.get(family, 60.0)

    points = 0
    variation = 0
    for t in fig.data:
        points += max(_safe_len(getattr(t, "x", None)), _safe_len(getattr(t, "y", None)), _safe_len(getattr(t, "z", None)))
        variation += max(_unique_count(getattr(t, "x", None)), _unique_count(getattr(t, "y", None)))

    return round(base + min(points / 120.0, 16.0) + min(variation, 14.0), 1)


def _enforce_family_limits(primary: list["ScoredChart"], fallback: list["ScoredChart"]) -> list["ScoredChart"]:
    max_per_family = {
        "heatmap": 1,
        "line": 1,
        "box": 1,
        "histogram": 1,
        "bar": 2,
        "scatter": 1,
        "violin": 1,
        "other": 1,
    }
    selected: list[ScoredChart] = []
    used_per_family: dict[str, int] = {}
    used_keys: set[str] = set()

    for candidate in primary + fallback:
        if candidate.key in used_keys:
            continue
        fam = _chart_family(candidate.key)
        used = used_per_family.get(fam, 0)
        if used >= max_per_family.get(fam, 1):
            continue
        selected.append(candidate)
        used_keys.add(candidate.key)
        used_per_family[fam] = used + 1
        if len(selected) >= MAX_OUTPUT_CHARTS:
            break

    return selected


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


def _clean_other_specify(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse survey free-text variants into 'Other' for all object columns.
    Handles both formats:
      - 'Other (Please Specify):Logistics'  (standard SurveyMonkey)
      - 'Other:SQL'  (compact export format used by this dataset)
    """
    df = df.copy()
    import re
    _other_pat = re.compile(r"^Other[:\s(]", re.IGNORECASE)
    for col in df.select_dtypes(include="object").columns:
        mask = df[col].astype(str).str.contains(_other_pat, na=False)
        collapsed = mask.sum()
        if collapsed > 0:
            df.loc[mask, col] = "Other"
            logger.info(
                "Collapsed %d 'Other:...' entries in '%s' \u2192 'Other'",
                collapsed, col
            )
    return df


def _is_likert(series: pd.Series) -> bool:
    """Detect 0-10 or 1-5 rating/Likert scale columns."""
    if not pd.api.types.is_numeric_dtype(series):
        return False
    col_data = series.dropna()
    if len(col_data) == 0:
        return False
    col_min = col_data.min()
    col_max = col_data.max()
    n_unique = col_data.nunique()
    # A Likert/rating scale: bounded 0-10, integer-like, ≤11 unique values
    return (col_min >= 0 and col_max <= 10 and n_unique <= 11
            and float(col_data.apply(lambda x: x == int(x)).mean()) > 0.95)


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


def _sample(df: pd.DataFrame, max_rows: int, stratify_col: Optional[str] = None) -> pd.DataFrame:
    if len(df) <= max_rows:
        return df
    if stratify_col and stratify_col in df.columns:
        try:
            sampled_idx = []
            for name, group in df.groupby(stratify_col):
                n_sample = min(len(group), max(1, int(max_rows * len(group) / len(df))))
                sampled_idx.extend(group.sample(n_sample, random_state=42).index)
            
            sampled_df = df.loc[sampled_idx]
            if len(sampled_df) > max_rows:
                sampled_df = sampled_df.sample(max_rows, random_state=42)
                
            return sampled_df.reset_index(drop=True)
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
            converted = pd.to_numeric(df[col], errors="raise")
            # Skip label-encoded categoricals: small-range contiguous integers
            # e.g. Browser=0/1/2/3/4, OS=0/1/2, City=0/1/2 are NOT real numerics
            if (pd.api.types.is_integer_dtype(converted)
                    and converted.nunique() <= 15
                    and converted.min() >= 0):
                logger.debug("Skipping coercion of '%s' — looks like label-encoded categorical", col)
                pass  # keep as string
            else:
                df[col] = converted
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
        xaxis=dict(showgrid=True, gridwidth=1, gridcolor="rgba(0,0,0,0.06)",
                   automargin=True, tickformat="~s"),
        yaxis=dict(showgrid=True, gridwidth=1, gridcolor="rgba(0,0,0,0.06)",
                   automargin=True, tickformat="~s"),
    )
    return fig


def _optimal_nbins(n: int) -> int:
    return min(max(int(np.ceil(np.log2(n) + 1)), 10), 60)


def _is_percentage_col(name: str) -> bool:
    lowered = name.lower()
    return "%" in lowered or "percent" in lowered or "percentage" in lowered


def _should_use_sum(name: str, series: pd.Series) -> bool:
    lowered = name.lower()
    keywords = ("population", "total", "count", "volume", "sales", "revenue", "amount", "profit")
    if any(k in lowered for k in keywords):
        return True
    # Fallback: very large, non-negative magnitudes often represent totals
    if series.dropna().min() >= 0 and series.dropna().max() >= 1_000_000:
        return True
    return False


def _is_categorical_numeric(name: str, series: pd.Series) -> bool:
    if not pd.api.types.is_integer_dtype(series):
        return False
    n = len(series)
    if n == 0:
        return False
    nunique = series.nunique(dropna=True)
    if nunique == 0:
        return False
    name_hint = any(k in name.lower() for k in ("city", "country", "state", "region", "referrer", "role", "category"))
    small_cardinality = nunique <= min(20, max(4, int(n * 0.05)))
    bounded_range = (series.dropna().min() >= 0 and series.dropna().max() <= 100)
    return name_hint or (small_cardinality and bounded_range)


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
        # Avoid multi-series plots with wildly different magnitudes
        medians = [abs(df[c].median()) for c in series_cols if df[c].notna().any()]
        if medians and max(medians) / max(min(medians), 1e-6) > 50:
            series_cols = [max(series_cols, key=lambda c: abs(df[c].median()))]
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

    # Only consider correlations between true numeric (non-Likert) columns
    num_col_set = set(num_cols)
    top_corr = [
        c for c in stats.get("strong_correlations", [])
        if c["col1"] in num_col_set and c["col2"] in num_col_set
    ]
    if top_corr:
        best = max(top_corr, key=lambda x: abs(x["correlation"]))
        x_col, y_col, r = best["col1"], best["col2"], best["correlation"]
    else:
        x_col, y_col = num_cols[0], num_cols[1]
        r = float(df[[x_col, y_col]].dropna().corr().iloc[0, 1])

    # Skip derived percentage relationships that are effectively a rescale
    if abs(r) > 0.995 and (_is_percentage_col(x_col) ^ _is_percentage_col(y_col)):
        ratio = (df[y_col] / df[x_col]).replace([np.inf, -np.inf], np.nan).dropna()
        if len(ratio) > 10:
            rel_var = float(ratio.std() / max(abs(ratio.mean()), 1e-6))
            if rel_var < 0.02:
                return None, set()

    # Relax threshold for datasets with few numeric columns (e.g. survey data)
    min_r = 0.10 if len(num_cols) <= 4 else 0.18
    if abs(r) < min_r:
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

    num_col = next((c for c in num_cols if c not in used and not _is_categorical_numeric(c, df[c])), None)
    if num_col is None:
        return None
    comp = min(_completeness(df[best_cat]), _completeness(df[num_col]))
    n_groups = df[best_cat].nunique()
    score = 42 + comp * 42 + min(n_groups * 2.5, 22)

    agg = "sum" if _should_use_sum(num_col, df[num_col]) else "mean"
    grouped = df.groupby(best_cat)[num_col].agg(agg).reset_index().sort_values(num_col, ascending=False)
    orientation = "h" if n_groups > 8 else "v"
    if orientation == "h":
        fig = px.bar(grouped, y=best_cat, x=num_col,
                     title=f"Total {num_col} by {best_cat}" if agg == "sum" else f"Mean {num_col} by {best_cat}",
                     orientation="h", color=best_cat)
    else:
        fig = px.bar(grouped, x=best_cat, y=num_col,
                     title=f"Total {num_col} by {best_cat}" if agg == "sum" else f"Mean {num_col} by {best_cat}",
                     color=best_cat)
        fig.update_layout(xaxis_tickangle=-25, xaxis_automargin=True)
    fig.update_layout(showlegend=False)
    return ScoredChart("bar_mean", _style(fig, 460), round(score, 1),
                       f"cat={best_cat}({n_groups}), num={num_col}, agg={agg}")


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


def _try_likert_bars(df: pd.DataFrame, likert_cols: list) -> Optional[ScoredChart]:
    """Horizontal ranked bar: average score per Likert/rating question. Perfect for surveys."""
    if len(likert_cols) < 2:
        return None
    # Strip column names to short readable labels (take last part after parenthesis/colon)
    def _short_label(col: str) -> str:
        # e.g. 'Q6 - How Happy are you ... (Salary)' → '(Salary)'
        for sep in ["(", "-", ":"]:
            if sep in col:
                parts = col.rsplit(sep, 1)
                candidate = (sep + parts[-1]).strip() if sep != "-" else parts[-1].strip()
                if len(candidate) <= 35:
                    return candidate
        return col[:35]

    means = [
        {"Question": _short_label(col), "Avg Rating": round(df[col].mean(), 2)}
        for col in likert_cols
        if _completeness(df[col]) >= 0.5
    ]
    if len(means) < 2:
        return None
    means_df = pd.DataFrame(means).sort_values("Avg Rating")
    fig = px.bar(
        means_df, x="Avg Rating", y="Question", orientation="h",
        title="Average Satisfaction Ratings (0–10 Scale)",
        color="Avg Rating", color_continuous_scale="RdYlGn",
        range_x=[0, 10], text="Avg Rating"
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(coloraxis_showscale=False, yaxis_title="")
    score = 72.0  # High priority — always valuable in survey datasets
    return ScoredChart("likert_bars", _style(fig, 480), score,
                       f"likert_cols={len(likert_cols)}")


def _try_boxplot(df: pd.DataFrame, num_cols: list, used: set) -> Optional[ScoredChart]:
    remaining = [c for c in num_cols if c not in used]
    if len(remaining) < 2:
        return None
    # Prefer columns with higher variance and cap to reduce label crowding
    variances = {c: float(df[c].var()) for c in remaining}
    cols = [c for c, _ in sorted(variances.items(), key=lambda item: item[1], reverse=True)][:6]
    comp_avg = float(np.mean([_completeness(df[c]) for c in cols]))
    if comp_avg < 0.65:
        return None
    score = comp_avg * 38 + min(len(cols) * 4.5, 22)

    plot_df = _sample(df, _HIST_MAX_ROWS)
    fig = go.Figure()
    for col in cols:
        fig.add_trace(go.Box(y=plot_df[col], name=col, boxmean=True,
                             marker_color=COLOR_PALETTE[cols.index(col) % len(COLOR_PALETTE)]))
    fig.update_layout(title="Numeric Distribution Overview", showlegend=False,
                      xaxis_tickangle=-25, xaxis_automargin=True)
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
def _build_line_if_datetime(df: pd.DataFrame, date_cols: list[str], num_cols: list[str]) -> dict:
    charts: dict[str, go.Figure] = {}
    if not date_cols or not num_cols:
        return charts

    best_date = max(date_cols, key=lambda c: (_completeness(df[c]), df[c].nunique(dropna=True)))
    if _completeness(df[best_date]) < 0.70:
        return charts

    ranked_nums = sorted(
        num_cols,
        key=lambda c: (_completeness(df[c]), float(df[c].var(skipna=True) or 0.0)),
        reverse=True,
    )
    selected_nums = ranked_nums[:4]
    if not selected_nums:
        return charts

    candidate_df = df[[best_date] + selected_nums].dropna(subset=[best_date]).copy()
    if candidate_df.empty or candidate_df[best_date].nunique(dropna=True) < 6:
        return charts

    plot_df = _resample_timeseries(candidate_df.sort_values(best_date), best_date, selected_nums, _TS_MAX_POINTS)
    if plot_df[best_date].nunique(dropna=True) < 6:
        return charts
    long_df = plot_df.melt(id_vars=best_date, var_name="Series", value_name="Value")
    fig = px.line(
        long_df,
        x=best_date,
        y="Value",
        color="Series",
        title=f"Trends Over Time by {best_date}",
        markers=(len(plot_df) <= 80),
    )
    charts[f"line_{best_date}"] = _style(fig, 500)
    return charts


def _build_correlation_heatmap(df: pd.DataFrame, num_cols: list[str]) -> dict:
    charts: dict[str, go.Figure] = {}
    if len(num_cols) < 3:
        return charts

    # Skip unreadable heatmaps when coordinate labels are excessively long.
    label_lengths = [len(str(col)) for col in num_cols]
    max_label_len = max(label_lengths) if label_lengths else 0
    avg_label_len = float(np.mean(label_lengths)) if label_lengths else 0.0
    if max_label_len > 45 or (len(num_cols) >= 7 and avg_label_len > 24):
        logger.info(
            "Skipping correlation heatmap: labels too long (max=%d, avg=%.1f, cols=%d)",
            max_label_len,
            avg_label_len,
            len(num_cols),
        )
        return charts

    sample_df = df[num_cols].sample(min(len(df), 5000), random_state=42) if len(df) > 5000 else df[num_cols]
    corr = sample_df.corr().round(2)
    fig = px.imshow(
        corr,
        text_auto=True,
        title="Correlation Heatmap",
        color_continuous_scale="RdBu_r",
        zmin=-1,
        zmax=1,
    )
    charts["heatmap_correlation"] = _style(fig, max(420, min(620, 220 + 45 * len(num_cols))))
    return charts


def _build_histograms(df: pd.DataFrame, num_cols: list[str]) -> dict:
    charts: dict[str, go.Figure] = {}
    # Keep candidates focused: top-variance numeric columns are usually most informative.
    ranked = sorted(
        num_cols,
        key=lambda c: (_completeness(df[c]), float(df[c].var(skipna=True) or 0.0)),
        reverse=True,
    )[:4]
    for col in ranked:
        non_null = int(df[col].notna().sum())
        if non_null < 5:
            continue
        nbins = _optimal_nbins(non_null)
        plot_df = _sample(df[[col]].dropna(), _HIST_MAX_ROWS)
        fig = px.histogram(plot_df, x=col, nbins=nbins, title=f"Distribution of {col}")
        charts[f"histogram_{col}"] = _style(fig, 430)
    return charts


def _build_categorical_bars(df: pd.DataFrame, cat_cols: list[str]) -> dict:
    charts: dict[str, go.Figure] = {}
    for col in cat_cols:
        nunique = int(df[col].nunique(dropna=True))
        if not (2 <= nunique < 20):
            continue
        vc = df[col].fillna("(Missing)").astype(str).value_counts().reset_index()
        vc.columns = [col, "count"]
        vc = vc.head(20)
        horizontal = nunique > 8
        if horizontal:
            fig = px.bar(vc, y=col, x="count", orientation="h", title=f"Frequency of {col}", color=col)
        else:
            fig = px.bar(vc, x=col, y="count", title=f"Frequency of {col}", color=col)
            fig.update_layout(xaxis_tickangle=-25, xaxis_automargin=True)
        fig.update_layout(showlegend=False)
        charts[f"bar_{col}"] = _style(fig, 430)
    return charts


def _build_boxplot_by_category(df: pd.DataFrame, cat_cols: list[str], num_cols: list[str]) -> dict:
    charts: dict[str, go.Figure] = {}
    if not cat_cols or not num_cols:
        return charts

    eligible_cats = [c for c in cat_cols if 2 <= int(df[c].nunique(dropna=True)) <= 20 and _completeness(df[c]) >= 0.6]
    if not eligible_cats:
        return charts

    cat_col = max(eligible_cats, key=lambda c: (_completeness(df[c]), -df[c].nunique(dropna=True)))
    num_col = max(num_cols, key=lambda c: (_completeness(df[c]), float(df[c].var(skipna=True) or 0.0)))
    plot_df = _sample(df[[cat_col, num_col]].dropna(), _HIST_MAX_ROWS, stratify_col=cat_col)
    if plot_df.empty:
        return charts

    fig = px.box(
        plot_df,
        x=cat_col,
        y=num_col,
        color=cat_col,
        points=False,
        title=f"{num_col} by {cat_col}",
    )
    fig.update_layout(showlegend=False, xaxis_tickangle=-25, xaxis_automargin=True)
    charts[f"box_{num_col}_by_{cat_col}"] = _style(fig, 470)
    return charts


def _select_charts(df: pd.DataFrame, stats: dict) -> dict:
    excluded = {e["column"] for e in stats.get("excluded_columns", [])}

    all_numeric = [
        col
        for col in df.select_dtypes(include=np.number).columns
        if col not in excluded and not _is_likely_id(df, col)
    ]
    cat_num = [col for col in all_numeric if _is_categorical_numeric(col, df[col])]
    num_cols = [col for col in all_numeric if col not in cat_num]

    cat_cols = [col for col in _str_cols(df) if col not in excluded and not _is_likely_id(df, col)]
    cat_cols.extend([col for col in cat_num if col not in cat_cols])

    date_cols = [col for col in df.select_dtypes(include="datetime").columns if col not in excluded]

    logger.info(
        "Rule-based visualizer inventory - numeric: %d | categorical: %d | datetime: %d | excluded: %d",
        len(num_cols), len(cat_cols), len(date_cols), len(excluded),
    )

    charts: dict[str, go.Figure] = {}
    extra_scored_candidates: list[ScoredChart] = []

    # Base profiling charts.
    charts.update(_build_line_if_datetime(df, date_cols, num_cols))
    if len(stats.get("strong_correlations", [])) > 0 or len(num_cols) >= 5:
        charts.update(_build_correlation_heatmap(df, num_cols))
    charts.update(_build_histograms(df, num_cols))
    charts.update(_build_categorical_bars(df, cat_cols))
    charts.update(_build_boxplot_by_category(df, cat_cols, num_cols))

    # Advanced, dataset-specific charts for variety and deeper insight.
    scatter_chart, _ = _try_scatter(df, num_cols, cat_cols, stats)
    if scatter_chart is not None:
        extra_scored_candidates.append(scatter_chart)

    mean_bar_chart = _try_bar_mean(df, cat_cols, num_cols, set())
    if mean_bar_chart is not None:
        extra_scored_candidates.append(mean_bar_chart)

    violin_or_hist_chart = _try_violin_or_histogram(df, num_cols, cat_cols, stats, set())
    if violin_or_hist_chart is not None:
        extra_scored_candidates.append(violin_or_hist_chart)

    likert_chart = _try_likert_bars(df, [c for c in num_cols if _is_likert(df[c])])
    if likert_chart is not None:
        extra_scored_candidates.append(likert_chart)

    if not charts and len(df.columns) >= 2:
        fallback = px.bar(df.head(30), x=df.columns[0], y=df.columns[1], title=f"{df.columns[1]} by {df.columns[0]}")
        charts["fallback"] = _style(fallback, 430)

    candidates: list[ScoredChart] = []
    dropped_no_signal: list[str] = []
    for key, fig in charts.items():
        if not _has_chart_signal(key, fig):
            dropped_no_signal.append(key)
            continue
        candidates.append(
            ScoredChart(
                key=key,
                fig=fig,
                score=_chart_signal_score(key, fig),
                reason=f"family={_chart_family(key)}",
            )
        )

    for extra in extra_scored_candidates:
        if not _has_chart_signal(extra.key, extra.fig):
            dropped_no_signal.append(extra.key)
            continue
        extra.score = max(extra.score, _chart_signal_score(extra.key, extra.fig))
        candidates.append(extra)

    if dropped_no_signal:
        logger.info("Dropped %d low-signal charts: %s", len(dropped_no_signal), dropped_no_signal)

    if not candidates:
        logger.info("No high-signal charts available after filtering")
        return {}

    llm_selected = _llm_select_charts(candidates, stats)
    heur_selected = sorted(candidates, key=lambda c: c.score, reverse=True)

    curated = _enforce_family_limits(llm_selected, heur_selected)
    logger.info(
        "Generated %d candidates, curated to %d professional charts: %s",
        len(candidates),
        len(curated),
        [(c.key, c.score) for c in curated],
    )
    return {c.key: c.fig for c in curated}


# ── Entry point ─────────────────────────────────────────────────────────────
def run(state: AnalysisState) -> AnalysisState:
    logger.info("ENHANCED VISUALIZER v2.0 (rule-based charting) loaded")
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
        df = _clean_other_specify(df)   # collapse survey free-text variants
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