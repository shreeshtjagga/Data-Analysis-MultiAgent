"""
DataPulse Visualizer Agent — Agentic Chart Intelligence v5.0
=============================================================
Full agentic loop with 4-phase parallel pipeline:

  PHASE 1 — PLAN  (LLM Call #2 in overall pipeline, runs in parallel with Phase 2)
    LLM receives the full dataset profile: column names, types,
    min/max/median/skew per numeric col, top values per categorical,
    datetime columns, Likert scales, and strong correlations.
    Returns a prioritised list of chart specs (type + columns + title).
    Runs concurrently with Phase 2 via ThreadPoolExecutor.

  PHASE 2 — HEURISTIC BUILD  (runs concurrently with Phase 1)
    Pure rule-based chart generation — guaranteed fallback for any dataset.
    Each spec is executed by a dedicated smart builder:
    • ranked_bar: sorted top-N horizontal bar
    • grouped_bar: aggregated cat × numeric (sum or mean auto-detected)
    • histogram: auto log-scale for heavy-tailed columns
    • scatter: OLS trendline, color by best categorical
    • heatmap: full correlation matrix
    • box/violin: no strip plots (points=False enforced)
    • donut/pie: low-cardinality categoricals only
    • likert_bar: average rating per question
    • line: time-series with auto-resampling
    Runs concurrently with Phase 1 — ready the instant LLM plan arrives.

  PHASE 3 — EXECUTE + MERGE
    LLM-planned charts are built; merged with heuristic charts.
    LLM charts lead; heuristic fills gaps not covered.

  PHASE 4 — EVALUATE & REFINE  (LLM Call, agentic feedback loop)
    Runs only when the merged pool exceeds MAX_OUTPUT_CHARTS (worth filtering).
    LLM receives a structured summary of the top built charts:
    • chart type, title, columns plotted, data sample
    • data signal quality score
    Judges each chart: KEEP / REPLACE / DROP.
    Replacement charts are built immediately (no extra LLM call).
    This creates a real feedback cycle — the agent sees its own
    output and self-corrects before returning results.
    max_tokens is capped at 500 to keep latency minimal.

  PHASE 5 — SELECT
    Final deduplication, family limits, score-ranked selection
    of top MAX_OUTPUT_CHARTS charts.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from ..core.state import AnalysisState
from ..core.errors import add_pipeline_error
from ..core.utils import truncate_stats_for_llm

logger = logging.getLogger(__name__)

# ── Design tokens ────────────────────────────────────────────────────────────
COLOR_PALETTE     = px.colors.qualitative.Bold
TEMPLATE          = "plotly_white"
MAX_OUTPUT_CHARTS = 8

_SCATTER_MAX_ROWS = 3_000
_HIST_MAX_ROWS    = 8_000
_TS_MAX_POINTS    = 600
_RANKED_BAR_TOP_N = 25


# ── Helpers ──────────────────────────────────────────────────────────────────

def _completeness(s: pd.Series) -> float:
    return 1.0 - float(s.isna().mean())


def _is_heavy_tailed(s: pd.Series) -> bool:
    """Power-law / log-normal indicator: max is 100× the median."""
    clean = s.dropna()
    if clean.empty or (med := float(clean.median())) <= 0:
        return False
    return float(clean.max()) / med > 100


def _is_likert(s: pd.Series) -> bool:
    if not pd.api.types.is_numeric_dtype(s):
        return False
    clean = s.dropna()
    return (
        len(clean) > 0
        and float(clean.min()) >= 0
        and float(clean.max()) <= 10
        and clean.nunique() <= 11
        and float(clean.apply(lambda x: x == int(x)).mean()) > 0.95
    )


def _is_likely_id(df: pd.DataFrame, col: str) -> bool:
    s = df[col]
    n = len(df)
    nu = s.nunique(dropna=True)
    if pd.api.types.is_string_dtype(s) or pd.api.types.is_object_dtype(s):
        return nu > 0.90 * n
    if pd.api.types.is_integer_dtype(s):
        return nu > 0.85 * n
    return False


def _should_sum(name: str, s: pd.Series) -> bool:
    kw = ("population", "total", "count", "volume", "sales", "revenue",
          "amount", "profit", "export", "import", "gdp", "production")
    if any(k in name.lower() for k in kw):
        return True
    clean = s.dropna()
    return clean.min() >= 0 and clean.max() >= 1_000_000


def _sample(df: pd.DataFrame, max_rows: int,
            stratify_col: Optional[str] = None) -> pd.DataFrame:
    if len(df) <= max_rows:
        return df
    if stratify_col and stratify_col in df.columns:
        try:
            parts = []
            for _, grp in df.groupby(stratify_col, observed=True):
                n = max(1, int(max_rows * len(grp) / len(df)))
                parts.append(grp.sample(min(n, len(grp)), random_state=42))
            out = pd.concat(parts)
            if len(out) > max_rows:
                out = out.sample(max_rows, random_state=42)
            return out.reset_index(drop=True)
        except Exception:
            pass
    return df.sample(max_rows, random_state=42).reset_index(drop=True)


def _resample_ts(df: pd.DataFrame, date_col: str,
                 val_cols: list[str], max_pts: int) -> pd.DataFrame:
    if len(df) <= max_pts:
        return df
    df2 = df[[date_col] + val_cols].dropna(subset=[date_col]).copy()
    df2[date_col] = pd.to_datetime(df2[date_col])
    df2 = df2.set_index(date_col).sort_index()
    for freq in ("D", "W", "ME", "QE", "YE"):
        r = df2[val_cols].resample(freq).mean().dropna(how="all").reset_index()
        if len(r) <= max_pts:
            return r
    step = max(1, len(df2) // max_pts)
    return df2.iloc[::step].reset_index()


def _optimal_nbins(n: int) -> int:
    return min(max(int(np.ceil(np.log2(n) + 1)), 8), 70)


def _style(fig: go.Figure, height: int = 450) -> go.Figure:
    fig.update_layout(
        template=TEMPLATE,
        height=height,
        font=dict(family="'Inter', 'DM Sans', system-ui, sans-serif", size=13),
        title=dict(font_size=16, x=0.5, xanchor="center", y=0.96, yanchor="top"),
        margin=dict(l=60, r=60, t=85, b=60),
        colorway=COLOR_PALETTE,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(
            font=dict(size=11), orientation="h",
            yanchor="bottom", y=1.02, xanchor="right", x=1,
        ),
        xaxis=dict(
            showgrid=True, gridwidth=1, gridcolor="rgba(0,0,0,0.06)",
            automargin=True,
        ),
        yaxis=dict(
            showgrid=True, gridwidth=1, gridcolor="rgba(0,0,0,0.06)",
            automargin=True,
        ),
    )
    return fig


@dataclass
class Chart:
    key:   str
    fig:   go.Figure
    score: float = 0.0
    cols:  set[str] = field(default_factory=set)  # columns used


# ── Column classification ────────────────────────────────────────────────────

def _classify(df: pd.DataFrame, excluded: set[str]) -> dict:
    """Return column lists: num, cat, date, likert, identifier."""
    num, cat, date_cols, likert = [], [], [], []

    for col in df.columns:
        if col in excluded:
            continue
        s = df[col]
        if _is_likely_id(df, col):
            continue
        if pd.api.types.is_datetime64_any_dtype(s):
            date_cols.append(col)
        elif pd.api.types.is_numeric_dtype(s):
            if _is_likert(s):
                likert.append(col)
            else:
                num.append(col)
        elif pd.api.types.is_string_dtype(s) or pd.api.types.is_object_dtype(s):
            cat.append(col)

    return {"num": num, "cat": cat, "date": date_cols, "likert": likert}


# ═══════════════════════════════════════════════════════════════════════════
# LLM CHART PLANNER
# ═══════════════════════════════════════════════════════════════════════════

_VALID_CHART_TYPES = {
    "ranked_bar", "grouped_bar", "histogram", "scatter", "line",
    "heatmap", "box", "violin", "donut", "pie", "likert_bar",
    "stacked_bar", "area",
}


def _llm_plan_charts(df: pd.DataFrame, cols: dict, stats: dict) -> Optional[list[dict]]:
    """
    Ask the LLM: given this dataset profile, plan the top charts.
    Returns list of dicts like:
      {"chart_type": "ranked_bar", "x": "Country", "y": "Population",
       "color": null, "title": "Top 25 Countries by Population", "priority": 1}
    """
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return None

    # Build a compact data profile for the LLM
    num_profiles = {}
    for c in cols["num"][:12]:
        s = df[c].dropna()
        if s.empty:
            continue
        num_profiles[c] = {
            "min": round(float(s.min()), 3),
            "max": round(float(s.max()), 3),
            "mean": round(float(s.mean()), 3),
            "median": round(float(s.median()), 3),
            "skew": round(float(s.skew()), 2),
            "unique": int(s.nunique()),
            "heavy_tailed": _is_heavy_tailed(s),
        }

    cat_profiles = {}
    for c in cols["cat"][:10]:
        s = df[c].dropna()
        cat_profiles[c] = {
            "unique": int(s.nunique()),
            "top3": s.value_counts().head(3).to_dict(),
        }

    profile = {
        "rows": len(df),
        "dataset_label": stats.get("dataset_profile", {}).get("label", "Unknown"),
        "dataset_domain": stats.get("dataset_profile", {}).get("domain", "general"),
        "numeric_columns": num_profiles,
        "categorical_columns": cat_profiles,
        "datetime_columns": cols["date"][:5],
        "likert_columns": cols["likert"][:8],
        "strong_correlations": stats.get("strong_correlations", [])[:8],
    }

    prompt = f"""You are an expert data visualization planner.
Given this dataset profile, plan exactly the {MAX_OUTPUT_CHARTS} most insightful charts.
Each chart must use REAL column names from the profile.

Dataset profile:
<profile>{json.dumps(profile, ensure_ascii=True)}</profile>

Rules:
1. For a numeric column that is heavy_tailed=true, use "histogram" with log_scale=true.
2. For top-N entity rankings (e.g. country, city, product by a metric), use "ranked_bar".
3. For numeric vs categorical (≤15 cats), use "grouped_bar" (sum for totals, mean for rates).
4. For two correlated numeric columns, use "scatter".
5. For a datetime + numeric, use "line".
6. For correlation overview (≥3 numeric cols), use "heatmap".
7. For categorical with 2-7 values, use "donut".
8. For likert/rating columns (multiple), use "likert_bar".
9. For distribution + outlier check, use "box".
10. Never repeat the same (x, y) pair. Avoid redundant charts.
11. Prioritize charts that give REAL business/domain insight, not just counts.

Respond ONLY with valid JSON — a list of up to {MAX_OUTPUT_CHARTS} objects:
[
  {{
    "chart_type": "<one of: ranked_bar|grouped_bar|histogram|scatter|line|heatmap|box|violin|donut|likert_bar|stacked_bar>",
    "x": "<column name or null>",
    "y": "<column name or null>",
    "color": "<column name or null>",
    "log_scale": false,
    "title": "<human readable chart title>",
    "agg": "<sum|mean|count>",
    "priority": <1 to {MAX_OUTPUT_CHARTS}>
  }},
  ...
]"""

    try:
        from groq import Groq
        client = Groq(api_key=api_key)
        # Use a smarter model for chart planning (set GROQ_PLANNER_MODEL in .env)
        planner_model = os.getenv("GROQ_PLANNER_MODEL",
                                   os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"))
        completion = client.chat.completions.create(
            model=planner_model,
            messages=[
                {"role": "system", "content": "You are a chart planner. Respond with valid JSON array only. No markdown fences."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=1200,
        )
        raw = (completion.choices[0].message.content or "").strip()
        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = re.sub(r"^```[a-z]*\n?", "", raw).rstrip("`").strip()
        plan = json.loads(raw)
        if not isinstance(plan, list) or len(plan) == 0:
            raise ValueError("LLM returned empty or non-list plan")
        # Validate chart types
        valid = [
            p for p in plan
            if isinstance(p, dict) and p.get("chart_type") in _VALID_CHART_TYPES
        ]
        logger.info("LLM chart plan: %d valid charts", len(valid))
        return sorted(valid, key=lambda p: p.get("priority", 99))
    except Exception as exc:
        logger.warning("LLM chart planning failed, using heuristics: %s", exc)
        return None


# ═══════════════════════════════════════════════════════════════════════════
# SMART CHART BUILDERS
# ═══════════════════════════════════════════════════════════════════════════

def _build_ranked_bar(df: pd.DataFrame, x_col: str, y_col: str,
                      title: str, agg: str = "auto",
                      top_n: int = _RANKED_BAR_TOP_N,
                      color_col: Optional[str] = None) -> Optional[Chart]:
    """Horizontal sorted bar — perfect for entity × metric (country, product, etc.)."""
    if x_col not in df.columns or y_col not in df.columns:
        return None
    if not pd.api.types.is_numeric_dtype(df[y_col]):
        return None
    # x_col must NOT be the same numeric column as y_col
    if x_col == y_col:
        return None
    comp = min(_completeness(df[x_col]), _completeness(df[y_col]))
    if comp < 0.40:
        return None

    if agg == "auto":
        agg = "sum" if _should_sum(y_col, df[y_col]) else "mean"

    try:
        grouped = (
            df.groupby(x_col, observed=True)[y_col]
            .agg(agg)
            .reset_index()
            .dropna(subset=[y_col])
            .sort_values(y_col, ascending=False)
            .head(top_n)
        )
    except Exception:
        return None
    if grouped.empty or grouped[y_col].isna().all():
        return None

    agg_label = "Total" if agg == "sum" else "Average"
    chart_title = title or f"Top {len(grouped)} {x_col} by {agg_label} {y_col}"
    fig = px.bar(
        grouped, x=y_col, y=x_col, orientation="h",
        title=chart_title,
        color=color_col if color_col and color_col in grouped.columns else y_col,
        color_continuous_scale="Blues",
        text=y_col,
    )
    fig.update_traces(texttemplate="%{text:.2s}", textposition="outside")
    fig.update_layout(
        showlegend=False,
        coloraxis_showscale=False,
        yaxis=dict(autorange="reversed"),
    )
    score = 80 + comp * 20
    return Chart(key=f"ranked_bar_{x_col}_{y_col}", fig=_style(fig, 480),
                 score=score, cols={x_col, y_col})


def _build_grouped_bar(df: pd.DataFrame, cat_col: str, num_col: str,
                       title: str, agg: str = "auto",
                       color_col: Optional[str] = None) -> Optional[Chart]:
    """Grouped/aggregated bar: category × numeric metric."""
    if cat_col not in df.columns or num_col not in df.columns:
        return None
    if cat_col == num_col:
        return None
    if not pd.api.types.is_numeric_dtype(df[num_col]):
        return None
    n_cats = df[cat_col].nunique(dropna=True)
    if not (2 <= n_cats <= 40):
        return None
    comp = min(_completeness(df[cat_col]), _completeness(df[num_col]))
    if comp < 0.40:
        return None

    if agg == "auto":
        agg = "sum" if _should_sum(num_col, df[num_col]) else "mean"

    color_col_use = color_col if color_col and color_col in df.columns else cat_col
    try:
        grouped = (
            df.groupby(cat_col, observed=True)[num_col]
            .agg(agg)
            .reset_index()
            .dropna(subset=[num_col])
            .sort_values(num_col, ascending=False)
        )
    except Exception:
        return None
    if grouped.empty:
        return None

    agg_label = "Total" if agg == "sum" else "Average"
    chart_title = title or f"{agg_label} {num_col} by {cat_col}"
    horizontal = n_cats > 10
    if horizontal:
        fig = px.bar(grouped, x=num_col, y=cat_col, orientation="h",
                     title=chart_title, color=color_col_use)
        fig.update_layout(yaxis=dict(autorange="reversed"))
    else:
        fig = px.bar(grouped, x=cat_col, y=num_col,
                     title=chart_title, color=color_col_use)
        fig.update_layout(xaxis_tickangle=-30, xaxis_automargin=True)

    fig.update_layout(showlegend=False)
    score = 72 + comp * 18
    return Chart(key=f"grouped_bar_{cat_col}_{num_col}", fig=_style(fig, 460),
                 score=score, cols={cat_col, num_col})


def _build_histogram(df: pd.DataFrame, num_col: str,
                     log_scale: bool = False, title: str = "") -> Optional[Chart]:
    """Smart histogram with auto log-scale for heavy-tailed data."""
    if num_col not in df.columns:
        return None
    if not pd.api.types.is_numeric_dtype(df[num_col]):
        return None
    clean = df[num_col].dropna()
    if len(clean) < 5:
        return None

    auto_log = log_scale or _is_heavy_tailed(df[num_col])
    plot_df = _sample(df[[num_col]].dropna(), _HIST_MAX_ROWS)
    nbins = _optimal_nbins(len(plot_df))
    log_note = " (log scale)" if auto_log else ""
    chart_title = title or f"Distribution of {num_col}{log_note}"
    fig = px.histogram(plot_df, x=num_col, nbins=nbins,
                       title=chart_title, log_x=auto_log, marginal="box")
    comp = _completeness(df[num_col])
    skew = abs(float(clean.skew()))
    score = 60 + comp * 18 + min(skew * 5, 15) + (8 if auto_log else 0)
    return Chart(key=f"histogram_{num_col}", fig=_style(fig, 440),
                 score=score, cols={num_col})


def _build_scatter(df: pd.DataFrame, x_col: str, y_col: str,
                   color_col: Optional[str] = None,
                   title: str = "") -> Optional[Chart]:
    """Scatter with OLS trendline."""
    if x_col not in df.columns or y_col not in df.columns:
        return None
    if x_col == y_col:
        return None  # identical columns — useless scatter
    if not (pd.api.types.is_numeric_dtype(df[x_col]) and
            pd.api.types.is_numeric_dtype(df[y_col])):
        return None
    pair = df[[x_col, y_col]].dropna()
    if len(pair) < 10:
        return None

    try:
        r_val = pair.corr().iloc[0, 1]
        r = float(r_val) if pd.notna(r_val) else 0.0
    except Exception:
        r = 0.0
    comp = min(_completeness(df[x_col]), _completeness(df[y_col]))
    color_use = (color_col if color_col and color_col in df.columns
                 and df[color_col].nunique() <= 10 else None)
    plot_df = _sample(df, _SCATTER_MAX_ROWS, stratify_col=color_use)
    sampled_note = f"  [{_SCATTER_MAX_ROWS:,} sampled]" if len(df) > _SCATTER_MAX_ROWS else ""
    chart_title = title or f"{x_col} vs {y_col}  (r={r:.2f}){sampled_note}"
    trendline = "ols" if len(plot_df) <= 3000 else None
    fig = px.scatter(plot_df, x=x_col, y=y_col, color=color_use,
                     title=chart_title, trendline=trendline, opacity=0.65)
    score = abs(r) * 55 + min(len(pair) / 20, 20) + comp * 20
    return Chart(key=f"scatter_{x_col}_{y_col}", fig=_style(fig, 460),
                 score=score, cols={x_col, y_col})


def _build_line(df: pd.DataFrame, date_col: str, num_cols: list[str],
                title: str = "") -> Optional[Chart]:
    """Time-series line chart."""
    if date_col not in df.columns or not num_cols:
        return None
    # Exclude date_col from value columns if accidentally included
    num_cols = [c for c in num_cols if c != date_col]
    valid_nums = [c for c in num_cols
                  if c in df.columns and pd.api.types.is_numeric_dtype(df[c])
                  and _completeness(df[c]) >= 0.60]
    if not valid_nums:
        return None

    comp_d = _completeness(df[date_col])
    if comp_d < 0.70 or df[date_col].nunique() < 5:
        return None

    df_s = df.sort_values(date_col)
    # Check magnitude consistency
    if len(valid_nums) > 1:
        meds = [abs(df[c].median()) for c in valid_nums if df[c].notna().any()]
        if meds and max(meds) / max(min(meds), 1e-6) > 50:
            valid_nums = [max(valid_nums, key=lambda c: abs(df[c].median()))]
    cols_use = valid_nums[:4]
    plot_df = _resample_ts(df_s, date_col, cols_use, _TS_MAX_POINTS)
    if plot_df[date_col].nunique() < 4:
        return None

    long_df = plot_df.melt(id_vars=date_col, var_name="Series", value_name="Value")
    chart_title = title or f"Trends Over Time"
    fig = px.line(long_df, x=date_col, y="Value", color="Series",
                  title=chart_title, markers=(len(plot_df) <= 60))
    n = len(df)
    score = 75 + min(n / 10, 20) + comp_d * 15
    return Chart(key=f"line_{date_col}", fig=_style(fig, 480),
                 score=score, cols={date_col} | set(cols_use))


def _build_heatmap(df: pd.DataFrame, num_cols: list[str],
                   title: str = "") -> Optional[Chart]:
    """Correlation heatmap."""
    eligible = [c for c in num_cols if _completeness(df[c]) >= 0.60]
    if len(eligible) < 3:
        return None
    # Skip if labels are too long
    if max(len(c) for c in eligible) > 45:
        return None
    cols = eligible[:12]
    sample = df[cols].sample(min(len(df), 5000), random_state=42) if len(df) > 5000 else df[cols]
    corr = sample.corr().round(2)
    # Drop all-NaN rows/cols (constant columns produce NaN correlation)
    corr = corr.dropna(axis=0, how="all").dropna(axis=1, how="all")
    if corr.empty or corr.shape[0] < 2:
        return None
    chart_title = title or "Correlation Heatmap"
    fig = px.imshow(corr, text_auto=True, title=chart_title,
                    color_continuous_scale="RdBu_r", zmin=-1, zmax=1)
    height = max(380, min(600, 200 + 48 * len(cols)))
    score = 85.0
    return Chart(key="heatmap_correlation", fig=_style(fig, height),
                 score=score, cols=set(cols))


def _build_box(df: pd.DataFrame, cat_col: str, num_col: str,
               title: str = "") -> Optional[Chart]:
    """Box plot: distribution of numeric by category."""
    if cat_col not in df.columns or num_col not in df.columns:
        return None
    if not pd.api.types.is_numeric_dtype(df[num_col]):
        return None
    n_cats = df[cat_col].nunique(dropna=True)
    if not (2 <= n_cats <= 20):
        return None
    comp = min(_completeness(df[cat_col]), _completeness(df[num_col]))
    if comp < 0.40:
        return None

    plot_df = _sample(df[[cat_col, num_col]].dropna(), _HIST_MAX_ROWS, stratify_col=cat_col)
    chart_title = title or f"{num_col} Distribution by {cat_col}"
    fig = px.box(plot_df, x=cat_col, y=num_col, color=cat_col,
                 title=chart_title, points=False,  # NO strip plots
                 notched=(len(plot_df) >= 100))
    fig.update_layout(showlegend=False, xaxis_tickangle=-25, xaxis_automargin=True)
    score = 65 + comp * 18
    return Chart(key=f"box_{num_col}_by_{cat_col}", fig=_style(fig, 470),
                 score=score, cols={cat_col, num_col})


def _build_violin(df: pd.DataFrame, cat_col: str, num_col: str,
                  title: str = "") -> Optional[Chart]:
    """Violin: best for 2-6 categories."""
    if cat_col not in df.columns or num_col not in df.columns:
        return None
    if not pd.api.types.is_numeric_dtype(df[num_col]):
        return None
    n_cats = df[cat_col].nunique(dropna=True)
    if not (2 <= n_cats <= 6):
        return None
    comp = min(_completeness(df[cat_col]), _completeness(df[num_col]))
    if comp < 0.40:
        return None

    plot_df = _sample(df[[cat_col, num_col]].dropna(), _SCATTER_MAX_ROWS, stratify_col=cat_col)
    chart_title = title or f"{num_col} by {cat_col}"
    fig = px.violin(plot_df, x=cat_col, y=num_col, color=cat_col,
                    box=True, points=False, title=chart_title)
    fig.update_layout(showlegend=False)
    score = 68 + comp * 18
    return Chart(key=f"violin_{num_col}_by_{cat_col}", fig=_style(fig, 460),
                 score=score, cols={cat_col, num_col})


def _build_donut(df: pd.DataFrame, cat_col: str, title: str = "") -> Optional[Chart]:
    """Donut pie for categorical columns with 2-8 values."""
    if cat_col not in df.columns:
        return None
    n = df[cat_col].nunique(dropna=True)
    if not (2 <= n <= 8):
        return None
    comp = _completeness(df[cat_col])
    if comp < 0.60:
        return None
    top_pct = df[cat_col].value_counts(normalize=True).iloc[0]
    if top_pct > 0.95:
        return None

    counts = df[cat_col].value_counts().reset_index()
    counts.columns = [cat_col, "count"]
    # Always auto-generate title from the column name, not from any LLM-supplied
    # title — the LLM tends to pass a category *value* (e.g. "Iris-setosa") as
    # the title instead of the column name, which is misleading.
    chart_title = f"Composition of {cat_col}"
    fig = px.pie(counts, names=cat_col, values="count",
                 title=chart_title, hole=0.42)
    fig.update_traces(textposition="outside", textinfo="percent+label")
    score = 62 + comp * 18
    return Chart(key=f"donut_{cat_col}", fig=_style(fig, 440),
                 score=score, cols={cat_col})


def _build_likert_bar(df: pd.DataFrame, likert_cols: list[str],
                      title: str = "") -> Optional[Chart]:
    """Horizontal average-score bar for Likert / rating columns."""
    valid = [c for c in likert_cols if _completeness(df[c]) >= 0.40]
    if len(valid) < 2:
        return None

    def _short(col: str) -> str:
        for sep in ["(", "-", ":"]:
            if sep in col:
                parts = col.rsplit(sep, 1)
                cand = (sep + parts[-1]).strip() if sep != "-" else parts[-1].strip()
                if len(cand) <= 40:
                    return cand
        return col[:40]

    rows = [{"Question": _short(c), "Avg Rating": round(float(df[c].mean()), 2)}
            for c in valid]
    means_df = pd.DataFrame(rows).sort_values("Avg Rating")
    chart_title = title or "Average Satisfaction / Rating Scores"
    fig = px.bar(means_df, x="Avg Rating", y="Question", orientation="h",
                 title=chart_title, color="Avg Rating",
                 color_continuous_scale="RdYlGn", range_x=[0, 10],
                 text="Avg Rating")
    fig.update_traces(textposition="outside")
    fig.update_layout(coloraxis_showscale=False, yaxis_title="")
    score = 75.0
    return Chart(key="likert_bars", fig=_style(fig, 480),
                 score=score, cols=set(valid))


def _build_stacked_bar(df: pd.DataFrame, x_col: str, cat_col: str,
                       num_col: Optional[str] = None,
                       title: str = "") -> Optional[Chart]:
    """
    Stacked bar: x_col (few cats) × cat_col (few cats).
    If num_col is given → stacked by sum of num_col; else by count.
    """
    if x_col not in df.columns or cat_col not in df.columns:
        return None
    nx = df[x_col].nunique(dropna=True)
    nc = df[cat_col].nunique(dropna=True)
    if not (2 <= nx <= 15 and 2 <= nc <= 8):
        return None

    if num_col and num_col in df.columns and pd.api.types.is_numeric_dtype(df[num_col]):
        agg = "sum" if _should_sum(num_col, df[num_col]) else "mean"
        pivot = df.groupby([x_col, cat_col], observed=True)[num_col].agg(agg).reset_index()
        y_label = num_col
    else:
        pivot = df.groupby([x_col, cat_col], observed=True).size().reset_index(name="count")
        y_label = "count"

    chart_title = title or f"{y_label} by {x_col} and {cat_col}"
    fig = px.bar(pivot, x=x_col, y=y_label, color=cat_col,
                 title=chart_title, barmode="stack")
    fig.update_layout(xaxis_tickangle=-25, xaxis_automargin=True)
    score = 65.0
    return Chart(key=f"stacked_{x_col}_{cat_col}", fig=_style(fig, 460),
                 score=score, cols={x_col, cat_col} | ({num_col} if num_col else set()))


def _build_freq_bar(df: pd.DataFrame, cat_col: str,
                    title: str = "") -> Optional[Chart]:
    """Simple frequency/count bar chart for a single categorical column."""
    if cat_col not in df.columns:
        return None
    n = df[cat_col].nunique(dropna=True)
    if not (2 <= n <= 50):
        return None
    comp = _completeness(df[cat_col])
    if comp < 0.50:
        return None

    vc = df[cat_col].value_counts().head(25).reset_index()
    vc.columns = [cat_col, "count"]
    chart_title = title or f"Frequency of {cat_col}"
    horizontal = n > 10
    if horizontal:
        fig = px.bar(vc, y=cat_col, x="count", orientation="h",
                     title=chart_title, color=cat_col)
        fig.update_layout(yaxis=dict(autorange="reversed"))
    else:
        fig = px.bar(vc, x=cat_col, y="count", title=chart_title, color=cat_col)
        fig.update_layout(xaxis_tickangle=-25, xaxis_automargin=True)
    fig.update_layout(showlegend=False)
    score = 55 + comp * 15
    return Chart(key=f"freq_bar_{cat_col}", fig=_style(fig, 440),
                 score=score, cols={cat_col})


# ═══════════════════════════════════════════════════════════════════════════
# LLM PLAN → CHART EXECUTION
# ═══════════════════════════════════════════════════════════════════════════

def _execute_plan(df: pd.DataFrame, plan: list[dict], cols: dict) -> list[Chart]:
    """Turn LLM chart plan into actual Chart objects."""
    charts: list[Chart] = []
    used_keys: set[str] = set()

    for item in plan:
        ct   = item.get("chart_type", "")
        x    = item.get("x")
        y    = item.get("y")
        col  = item.get("color")
        ttl  = item.get("title", "")
        agg  = item.get("agg", "auto")
        log_ = bool(item.get("log_scale", False))

        # Validate columns exist
        def _ok(c):
            return c is None or c in df.columns

        if not (_ok(x) and _ok(y) and _ok(col)):
            logger.debug("LLM plan item skipped (missing cols): %s", item)
            continue

        chart: Optional[Chart] = None

        if ct == "ranked_bar" and x and y:
            # x=entity col (cat or low-card num), y=metric
            chart = _build_ranked_bar(df, x, y, ttl, agg=agg, color_col=col)
            if chart is None and y and x:
                # try swapped
                chart = _build_ranked_bar(df, y, x, ttl, agg=agg, color_col=col)

        elif ct == "grouped_bar" and x and y:
            chart = _build_grouped_bar(df, x, y, ttl, agg=agg, color_col=col)
            if chart is None:
                chart = _build_grouped_bar(df, y, x, ttl, agg=agg, color_col=col)

        elif ct == "histogram" and x:
            chart = _build_histogram(df, x, log_scale=log_, title=ttl)
            if chart is None and y:
                chart = _build_histogram(df, y, log_scale=log_, title=ttl)

        elif ct == "scatter" and x and y:
            # Ensure both are numeric; if LLM mixes cat+num, swap them
            x_num = x in df.columns and pd.api.types.is_numeric_dtype(df[x])
            y_num = y in df.columns and pd.api.types.is_numeric_dtype(df[y])
            if x_num and y_num:
                chart = _build_scatter(df, x, y, color_col=col, title=ttl)
            elif not x_num and y_num:
                # x is categorical — find best numeric fallback for x
                x_fallback = next((c for c in cols["num"] if c != y), None)
                if x_fallback:
                    chart = _build_scatter(df, x_fallback, y, color_col=x if x in df.columns else col, title=ttl)
            elif x_num and not y_num:
                y_fallback = next((c for c in cols["num"] if c != x), None)
                if y_fallback:
                    chart = _build_scatter(df, x, y_fallback, color_col=y if y in df.columns else col, title=ttl)

        elif ct == "line" and x:
            val_cols = [y] if y else cols["num"]
            chart = _build_line(df, x, val_cols, title=ttl)

        elif ct == "heatmap":
            chart = _build_heatmap(df, cols["num"], title=ttl)

        elif ct in ("box", "violin") and x and y:
            # Determine which is cat vs num
            x_is_num = pd.api.types.is_numeric_dtype(df[x]) if x in df.columns else False
            y_is_num = pd.api.types.is_numeric_dtype(df[y]) if y in df.columns else False
            if x_is_num and not y_is_num:
                cc, nc = y, x
            else:
                cc, nc = x, y
            n_cats = df[cc].nunique(dropna=True) if cc in df.columns else 0
            if ct == "violin" or (n_cats <= 6):
                chart = _build_violin(df, cc, nc, title=ttl)
            if chart is None:
                chart = _build_box(df, cc, nc, title=ttl)

        elif ct in ("donut", "pie") and x:
            # Validate that x is an actual column — LLM sometimes passes a
            # category *value* (e.g. "Iris-setosa") instead of a column name.
            # If x is not a column, try to find the first eligible cat column.
            if x not in df.columns:
                x_fallback = next(
                    (c for c in cols["cat"] if 2 <= df[c].nunique(dropna=True) <= 8),
                    None,
                )
                if x_fallback:
                    logger.debug(
                        "LLM donut x=%r is not a column — falling back to %r", x, x_fallback
                    )
                    x = x_fallback
                else:
                    logger.debug("LLM donut x=%r is not a column and no fallback found", x)
                    continue
            chart = _build_donut(df, x)

        elif ct == "likert_bar":
            chart = _build_likert_bar(df, cols["likert"], title=ttl)

        elif ct == "stacked_bar" and x and y:
            chart = _build_stacked_bar(df, x, y, num_col=None, title=ttl)

        if chart is None:
            logger.debug("LLM chart '%s' (%s, %s) could not be built", ct, x, y)
            continue

        if chart.key in used_keys:
            continue

        # Check the chart has real signal
        if not _chart_has_signal(chart):
            logger.debug("Dropping low-signal chart: %s", chart.key)
            continue

        used_keys.add(chart.key)
        charts.append(chart)

    return charts


# ═══════════════════════════════════════════════════════════════════════════
# SIGNAL VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════

def _chart_has_signal(chart: Chart) -> bool:
    """Confirm the figure contains real data (relaxed thresholds)."""
    fig = chart.fig
    if not fig or not getattr(fig, "data", None):
        return False
    traces = [t for t in fig.data if t is not None]
    if not traces:
        return False
    for t in traces:
        for attr in ("x", "y", "z", "values", "r"):
            v = getattr(t, attr, None)
            if v is not None:
                try:
                    if len(v) >= 2:
                        return True
                except Exception:
                    pass
    return False


# ═══════════════════════════════════════════════════════════════════════════
# HEURISTIC FALLBACK ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════

def _heuristic_plan(df: pd.DataFrame, cols: dict, stats: dict) -> list[Chart]:
    """
    Pure rule-based chart generation — no LLM needed.
    Produces sensible charts for ANY data shape.
    Tracks column usage to avoid redundancy.
    """
    charts: list[Chart] = []
    used_pairs: set[frozenset] = set()
    used_singles: set[str] = set()

    def _add(c: Optional[Chart]) -> bool:
        if c is None or not _chart_has_signal(c):
            return False
        pair = frozenset(c.cols)
        if pair in used_pairs:
            return False
        used_pairs.add(pair)
        charts.append(c)
        return True

    num  = cols["num"]
    cat  = cols["cat"]
    date = cols["date"]
    likert = cols["likert"]
    strong_corrs = stats.get("strong_correlations", [])

    # 1. Time-series (highest priority when date exists)
    if date and num:
        _add(_build_line(df, date[0], num))

    # 2. Correlation heatmap (when 3+ numeric cols)
    if len(num) >= 3:
        _add(_build_heatmap(df, num))

    # 3. Likert ratings bar
    if len(likert) >= 2:
        _add(_build_likert_bar(df, likert))

    # 4. Scatter for best correlated pair
    if len(num) >= 2:
        if strong_corrs:
            best = max(strong_corrs, key=lambda x: abs(x["correlation"]))
            c1, c2 = best["col1"], best["col2"]
            if c1 in num and c2 in num:
                color_candidate = next((c for c in cat if df[c].nunique() <= 8), None)
                _add(_build_scatter(df, c1, c2, color_col=color_candidate))
        else:
            _add(_build_scatter(df, num[0], num[1]))

    # 5. For each categorical × best numeric: ranked_bar or grouped_bar
    for cat_col in cat[:4]:
        n_cats = df[cat_col].nunique(dropna=True)
        if n_cats < 2:
            continue
        best_num = max(num, key=lambda c: _completeness(df[c])) if num else None
        if best_num is None:
            continue
        if n_cats > 15:
            c = _build_ranked_bar(df, cat_col, best_num, "")
        else:
            c = _build_grouped_bar(df, cat_col, best_num, "")
        if _add(c):
            used_singles.add(cat_col)
            break  # one cat×num bar is enough initially

    # 6. Second cat group if more cats exist
    remaining_cats = [c for c in cat if c not in used_singles]
    for cat_col in remaining_cats[:2]:
        n_cats = df[cat_col].nunique(dropna=True)
        if 2 <= n_cats <= 8:
            _add(_build_donut(df, cat_col))
        elif n_cats > 8:
            _add(_build_freq_bar(df, cat_col))

    # 7. Histograms for top numeric columns (with auto-log)
    num_by_var = sorted(num, key=lambda c: float(df[c].var(skipna=True) or 0), reverse=True)
    for ncol in num_by_var[:3]:
        if frozenset({ncol}) not in used_pairs:
            _add(_build_histogram(df, ncol))

    # 8. Box plot for best cat × second numeric
    if cat and len(num) >= 2:
        best_cat = max(cat, key=lambda c: _completeness(df[c]))
        n_cats = df[best_cat].nunique(dropna=True)
        if 2 <= n_cats <= 6:
            _add(_build_violin(df, best_cat, num[1]))
        elif 2 <= n_cats <= 15:
            _add(_build_box(df, best_cat, num[1]))

    # 9. Stacked bar if two low-cardinality categoricals exist
    small_cats = [c for c in cat if 2 <= df[c].nunique(dropna=True) <= 8]
    if len(small_cats) >= 2:
        _add(_build_stacked_bar(df, small_cats[0], small_cats[1]))

    # 10. Frequency bar for any remaining cat columns
    for cat_col in cat:
        if len(charts) >= MAX_OUTPUT_CHARTS:
            break
        _add(_build_freq_bar(df, cat_col))

    return charts


# ═══════════════════════════════════════════════════════════════════════════
# DEDUP & FINAL SELECTION
# ═══════════════════════════════════════════════════════════════════════════

def _deduplicate_and_select(charts: list[Chart]) -> dict[str, go.Figure]:
    """
    Remove exact duplicate keys, enforce family limits,
    sort by score, return top MAX_OUTPUT_CHARTS.
    """
    family_limits = {
        "heatmap": 1, "line": 2, "scatter": 1,
        "histogram": 2, "box": 1, "violin": 1,
        "donut": 2, "bar": 3, "likert": 1,
        "stacked": 1, "ranked": 2, "grouped": 2,
        "freq": 2,
    }

    def _family(key: str) -> str:
        for f in family_limits:
            if key.startswith(f):
                return f
        return "other"

    seen_keys: set[str] = set()
    seen_pairs: set[frozenset] = set()
    family_count: dict[str, int] = {}
    selected: list[Chart] = []

    for c in sorted(charts, key=lambda x: x.score, reverse=True):
        if c.key in seen_keys:
            continue
        pair = frozenset(c.cols)
        if pair in seen_pairs and len(pair) > 1:
            continue
        fam = _family(c.key)
        if family_count.get(fam, 0) >= family_limits.get(fam, 2):
            continue

        seen_keys.add(c.key)
        seen_pairs.add(pair)
        family_count[fam] = family_count.get(fam, 0) + 1
        selected.append(c)

        if len(selected) >= MAX_OUTPUT_CHARTS:
            break

    logger.info(
        "Final chart selection: %d charts — %s",
        len(selected),
        [(c.key, round(c.score, 1)) for c in selected],
    )
    return {c.key: c.fig for c in selected}


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════

def _coerce_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Try to parse string columns that look like dates."""
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        sample = df[col].dropna().head(30).astype(str)
        try:
            pd.to_datetime(sample, format="mixed")
            df[col] = pd.to_datetime(df[col], format="mixed", errors="coerce")
        except Exception:
            pass
    return df


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 3 — AGENTIC EVALUATE & REFINE
# ═══════════════════════════════════════════════════════════════════════════

def _chart_summary_for_llm(chart: Chart, df: pd.DataFrame) -> dict:
    """Build a compact human-readable summary of a built chart for LLM evaluation."""
    fig = chart.fig
    summary = {
        "key": chart.key,
        "score": round(chart.score, 1),
        "columns_used": list(chart.cols),
    }
    try:
        layout = fig.layout
        summary["title"] = (
            layout.title.text if layout.title and layout.title.text else chart.key
        )
        trace_previews = []
        for t in fig.data[:2]:  # summarise first 2 traces
            t_info = {"type": t.type}
            # IMPORTANT: check each attribute separately — don't let loop variable
            # shadow; we need the value of whichever attr is non-None.
            for attr in ("x", "y", "values", "labels"):
                val = getattr(t, attr, None)
                if val is not None:
                    try:
                        lst = [str(v) for v in list(val)[:6]]
                        t_info[attr] = lst
                    except Exception:
                        pass
            trace_previews.append(t_info)
        summary["data_preview"] = trace_previews
    except Exception:
        pass
    return summary


def _llm_evaluate_charts(
    charts: list[Chart],
    df: pd.DataFrame,
    cols: dict,
    stats: dict,
) -> list[Chart]:
    """
    AGENTIC PHASE 3: LLM reviews all built charts and decides:
      KEEP   — chart is meaningful and insightful
      REPLACE — chart is poor; LLM provides a replacement spec
      DROP   — chart is useless; remove it

    Returns the final refined list of Chart objects.
    """
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key or len(charts) == 0:
        return charts

    planner_model = os.getenv(
        "GROQ_PLANNER_MODEL",
        os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"),  # fast model for planning
    )

    chart_summaries = [_chart_summary_for_llm(c, df) for c in charts]

    prompt = f"""You are an expert Data Visualization Quality Evaluator.
You built the following charts for a dataset. Review each one and decide if it is high quality.

Dataset domain: {stats.get('dataset_profile', {}).get('label', 'Unknown')}
Numeric columns: {cols['num'][:8]}
Categorical columns: {cols['cat'][:6]}

Built charts summary:
<charts>{json.dumps(chart_summaries, ensure_ascii=True)}</charts>

For EACH chart, respond with one of:
  KEEP   — if it provides clear, meaningful insight
  REPLACE — if it's the wrong chart type for the data (provide a better spec)
  DROP   — if it shows no useful information

Rules for REPLACE:
- Only replace if you can specify a clearly BETTER alternative using existing columns
- A repeated scatter showing the same columns as another chart → DROP
- A histogram with only 1-2 bars visible → REPLACE with ranked_bar
- A box plot with only 1 category → DROP
- An empty or near-empty chart → DROP

Respond ONLY with valid JSON:
{{
  "evaluations": [
    {{"key": "<chart_key>", "decision": "KEEP|REPLACE|DROP",
      "reason": "<one line>",
      "replacement": {{"chart_type": "ranked_bar", "x": "<col>", "y": "<col>",
                      "title": "<title>", "agg": "sum", "log_scale": false}}
      }},
    ...
  ]
}}
"replacement" is ONLY required when decision is REPLACE. Omit it otherwise."""

    try:
        from groq import Groq
        client = Groq(api_key=api_key)
        completion = client.chat.completions.create(
            model=planner_model,
            messages=[
                {"role": "system",
                 "content": "You are a chart quality evaluator. Respond with valid JSON only. No markdown."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=500,
        )
        raw = (completion.choices[0].message.content or "").strip()
        if raw.startswith("```"):
            raw = re.sub(r"^```[a-z]*\n?", "", raw).rstrip("`").strip()
        result = json.loads(raw)
        evaluations = result.get("evaluations", [])
        if not evaluations:
            raise ValueError("Empty evaluations")

        logger.info("[AGENTIC] LLM evaluation: %d decisions received", len(evaluations))

        # Index charts by key for O(1) lookup
        chart_by_key = {c.key: c for c in charts}
        final_charts: list[Chart] = []

        for ev in evaluations:
            key      = ev.get("key", "")
            decision = ev.get("decision", "KEEP").upper()
            reason   = ev.get("reason", "")
            original = chart_by_key.get(key)

            if original is None:
                continue

            if decision == "KEEP":
                logger.info("[AGENTIC] KEEP  '%s' — %s", key, reason)
                final_charts.append(original)

            elif decision == "DROP":
                logger.info("[AGENTIC] DROP  '%s' — %s", key, reason)
                # Dropped — don't add to final list

            elif decision == "REPLACE":
                replacement_spec = ev.get("replacement")
                if replacement_spec and isinstance(replacement_spec, dict):
                    logger.info("[AGENTIC] REPLACE '%s' → %s — %s", key, replacement_spec, reason)
                    new_charts = _execute_plan(df, [replacement_spec], cols)
                    if new_charts and _chart_has_signal(new_charts[0]):
                        logger.info("[AGENTIC] Replacement built successfully: %s", new_charts[0].key)
                        final_charts.append(new_charts[0])
                    else:
                        logger.info("[AGENTIC] Replacement failed — keeping original '%s'", key)
                        final_charts.append(original)
                else:
                    logger.info("[AGENTIC] REPLACE '%s' had no valid spec — keeping original", key)
                    final_charts.append(original)

        # Add any charts that weren't covered by the LLM evaluation (safety net)
        evaluated_keys = {ev.get("key") for ev in evaluations}
        for c in charts:
            if c.key not in evaluated_keys:
                final_charts.append(c)

        logger.info("[AGENTIC] After evaluation: %d charts (was %d)", len(final_charts), len(charts))
        return final_charts

    except Exception as exc:
        logger.warning("[AGENTIC] LLM evaluation failed, keeping all built charts: %s", exc)
        return charts


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ORCHESTRATOR — AGENTIC LOOP
# ═══════════════════════════════════════════════════════════════════════════

def _select_charts(df: pd.DataFrame, stats: dict) -> dict[str, go.Figure]:
    excluded = {e["column"] for e in stats.get("excluded_columns", [])}
    df = _coerce_dates(df)
    cols = _classify(df, excluded)

    logger.info(
        "[VIZ] Column inventory — numeric: %d | cat: %d | date: %d | likert: %d",
        len(cols["num"]), len(cols["cat"]), len(cols["date"]), len(cols["likert"]),
    )

    # ── PHASE 1 + PHASE 2: Run LLM planning and heuristic build concurrently ──
    # Both are read-only on df/cols/stats — safe to parallelise.
    logger.info("[VIZ] Phase 1+2: LLM chart planning AND heuristic build running concurrently...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        plan_future      = pool.submit(_llm_plan_charts, df, cols, stats)
        heuristic_future = pool.submit(_heuristic_plan,  df, cols, stats)

        plan             = plan_future.result()      # None if LLM unavailable
        heuristic_charts = heuristic_future.result()

    # ── PHASE 3: Execute LLM plan specs → Chart objects, then merge ──────
    llm_charts: list[Chart] = []
    if plan:
        llm_charts = _execute_plan(df, plan, cols)
        logger.info("[VIZ] Phase 1 done — %d LLM-planned charts built", len(llm_charts))
    else:
        logger.info("[VIZ] Phase 1 skipped (no API key or LLM failed)")

    logger.info("[VIZ] Phase 2 done — %d heuristic charts built", len(heuristic_charts))

    # Merge: LLM charts lead, heuristic fills anything not covered
    llm_keys   = {c.key for c in llm_charts}
    all_charts = llm_charts + [c for c in heuristic_charts if c.key not in llm_keys]

    # ── SAFETY NET: if nothing was built at all, force at least one chart ──
    if not all_charts:
        logger.warning("[VIZ] No charts generated — bare fallback")
        for col in df.columns:
            fb = _build_freq_bar(df, col) or _build_histogram(df, col)
            if fb and _chart_has_signal(fb):
                all_charts = [fb]
                break

    # ── PHASE 4: Agentic Evaluate & Refine (LLM, re-enabled v5.0) ─────────
    # Only runs when we have more charts than we will output — worth filtering.
    # Capped at 500 tokens → minimal marginal latency (recovered by parallelism).
    if len(all_charts) > MAX_OUTPUT_CHARTS:
        logger.info(
            "[VIZ] Phase 4: Agentic evaluate & refine — reviewing %d charts...",
            len(all_charts),
        )
        all_charts = _llm_evaluate_charts(all_charts, df, cols, stats)
    else:
        logger.info(
            "[VIZ] Phase 4: Skipped (only %d charts — no filtering needed)",
            len(all_charts),
        )

    # ── PHASE 5: Final dedup + score-ranked selection ──────────────────────
    result = _deduplicate_and_select(all_charts)

    # Last resort: if dedup somehow returned empty, use heuristic charts as-is
    if not result and heuristic_charts:
        logger.warning("[VIZ] dedup returned empty — using top heuristic chart")
        result = {heuristic_charts[0].key: heuristic_charts[0].fig}

    return result


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def run(state: AnalysisState) -> AnalysisState:
    logger.info("Agentic Visualizer v5.0 (Parallel Plan+Heuristic → Merge → Evaluate → Select) starting")
    state.current_agent = "visualizer"

    if state.clean_df is None or state.clean_df.empty:
        add_pipeline_error(
            state.errors,
            code="VISUALIZER_NO_DATA",
            message="No clean_df available for visualizer",
            agent="visualizer",
            error_type="validation",
        )
        return state

    try:
        state.charts = _select_charts(state.clean_df, state.stats_summary or {})
        logger.info("[AGENTIC] Visualizer done — %d final charts", len(state.charts))
        state.completed_agents.append("visualizer")
    except Exception as exc:
        add_pipeline_error(
            state.errors,
            code="VISUALIZER_FAILED",
            message=str(exc),
            agent="visualizer",
            error_type="agent",
        )
        logger.exception("Agentic Visualizer v4.0 error")

    return state


visualizer_agent = run