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
from ..core.llm_client import get_groq_client
logger = logging.getLogger(__name__)
COLOR_PALETTE = px.colors.qualitative.Bold
TEMPLATE = 'plotly_white'
MAX_OUTPUT_CHARTS = 12
_SCATTER_MAX_ROWS = 3000
_HIST_MAX_ROWS = 8000
_TS_MAX_POINTS = 600
_RANKED_BAR_TOP_N = 15


def _completeness(s: pd.Series) -> float:
    return 1.0 - float(s.isna().mean())

def _is_heavy_tailed(s: pd.Series) -> bool:
    clean = s.dropna()
    if clean.empty or (med := float(clean.median())) <= 0:
        return False
    return float(clean.max()) / med > 100

def _is_low_variance_categorical(s: pd.Series) -> bool:
    clean = s.dropna()
    if clean.empty:
        return True
    vc = clean.value_counts(normalize=True)
    if vc.empty:
        return True
    return float(vc.iloc[0]) > 0.95

def _is_likert(s: pd.Series) -> bool:
    if not pd.api.types.is_numeric_dtype(s):
        return False
    clean = s.dropna()
    return len(clean) > 0 and float(clean.min()) >= 0 and (float(clean.max()) <= 10) and (clean.nunique() <= 11) and (float(clean.apply(lambda x: x == int(x)).mean()) > 0.95)

def _is_high_cardinality_id(df: pd.DataFrame, col: str) -> bool:
    s = df[col]
    n = len(df)
    if n < 5:
        return False
    nu = s.nunique(dropna=True)
    if nu == 0:
        return False
    col_lower = col.lower()
    id_kw = ('id', 'uuid', 'guid', 'key', 'index', '_id', 'pk', 'email', 'url', 'phone', 'hash')
    is_id_named = any((k in col_lower for k in id_kw))
    # Float (non-integer) numeric columns are continuous values, not IDs
    if pd.api.types.is_float_dtype(s) and not is_id_named:
        return False
    if is_id_named:
        return nu > 0.95 * n
    return nu > 0.995 * n

def _should_sum(name: str, s: pd.Series) -> bool:
    kw = ('population', 'total', 'count', 'volume', 'sales', 'revenue', 'amount', 'profit', 'export', 'import', 'gdp', 'production')
    if any((k in name.lower() for k in kw)):
        return True
    clean = s.dropna()
    return clean.min() >= 0 and clean.max() >= 1000000

def _sample(df: pd.DataFrame, max_rows: int, stratify_col: Optional[str]=None) -> pd.DataFrame:
    if len(df) <= max_rows:
        return df
    if stratify_col and stratify_col in df.columns:
        try:
            parts = []
            for (_, grp) in df.groupby(stratify_col, observed=True):
                n = max(1, int(max_rows * len(grp) / len(df)))
                parts.append(grp.sample(min(n, len(grp)), random_state=42))
            out = pd.concat(parts)
            if len(out) > max_rows:
                out = out.sample(max_rows, random_state=42)
            return out.reset_index(drop=True)
        except Exception:
            pass
    return df.sample(max_rows, random_state=42).reset_index(drop=True)

def _resample_ts(df: pd.DataFrame, date_col: str, val_cols: list[str], max_pts: int) -> pd.DataFrame:
    if len(df) <= max_pts:
        return df
    df2 = df[[date_col] + val_cols].dropna(subset=[date_col]).copy()
    df2[date_col] = pd.to_datetime(df2[date_col], errors='coerce')
    df2 = df2.dropna(subset=[date_col])
    if df2.empty:
        return df2
    df2 = df2.set_index(date_col).sort_index()
    for freq in ('s', 'min', 'h', 'D', 'W', 'M', 'Q', 'A', 'ME', 'QE', 'YE'):
        try:
            r = df2[val_cols].resample(freq).mean().dropna(how='all').reset_index()
        except Exception:
            continue
        if 10 <= len(r) <= max_pts:
            return r
    step = max(1, len(df2) // max_pts)
    return df2.iloc[::step].reset_index()

def _optimal_nbins(n: int) -> int:
    if n >= 5000:
        return 18
    if n >= 1000:
        return 22
    return min(max(int(np.ceil(np.sqrt(max(n, 1)))), 8), 30)

def _histogram_bins(clean: pd.Series) -> tuple[np.ndarray, np.ndarray]:
    values = clean.astype(float).to_numpy()
    if len(values) < 5:
        return np.array([]), np.array([])
    q1, q3 = np.percentile(values, [25, 75])
    iqr = q3 - q1
    if iqr > 0:
        width = 2 * iqr / (len(values) ** (1 / 3))
        fd_bins = int(np.ceil((values.max() - values.min()) / width)) if width > 0 else _optimal_nbins(len(values))
        nbins = min(max(fd_bins, 8), _optimal_nbins(len(values)))
    else:
        nbins = _optimal_nbins(len(values))
    counts, edges = np.histogram(values, bins=nbins)
    return counts, edges

def _is_uninformative_dense_distribution(clean: pd.Series) -> bool:
    if len(clean) < 2000:
        return False
    counts, _ = _histogram_bins(clean)
    if len(counts) < 8 or counts.sum() == 0:
        return False
    nonzero = counts[counts > 0]
    if len(nonzero) < len(counts) * 0.85:
        return False
    mean_count = float(nonzero.mean())
    if mean_count <= 0:
        return False
    coefficient_of_variation = float(nonzero.std() / mean_count)
    return coefficient_of_variation < 0.2

def _distribution_interest(df: pd.DataFrame, col: str) -> float:
    clean = df[col].dropna()
    if clean.empty or _is_uninformative_dense_distribution(clean):
        return -1.0
    counts, _ = _histogram_bins(clean)
    if len(counts) == 0 or counts.sum() == 0:
        return -1.0
    nonzero = counts[counts > 0]
    cv = float(nonzero.std() / max(float(nonzero.mean()), 1e-9)) if len(nonzero) else 0.0
    try:
        skew = abs(float(clean.skew()))
    except Exception:
        skew = 0.0
    return skew + cv + (0.6 if _is_heavy_tailed(clean) else 0.0)

def _style(fig: go.Figure, height: int=450) -> go.Figure:
    fig.update_layout(template=TEMPLATE, height=height, font=dict(family="'Inter', 'DM Sans', system-ui, sans-serif", size=13), title=dict(font_size=16, x=0.5, xanchor='center', y=0.96, yanchor='top'), margin=dict(l=60, r=60, t=85, b=60), colorway=COLOR_PALETTE, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', legend=dict(font=dict(size=11), orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1), xaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.06)', automargin=True), yaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.06)', automargin=True))
    return fig

@dataclass
class Chart:
    key: str
    fig: go.Figure
    score: float = 0.0
    cols: set[str] = field(default_factory=set)
_DATE_NAME_PATTERNS = frozenset({'date', 'dob', 'birth', 'born', 'created', 'updated', 'timestamp', 'year', 'month', 'day', 'time', 'datetime', 'period', 'since'})
_PERSONAL_DATE_KEYWORDS = ('dob', 'dateofbirth', 'birthdate', 'borndate', 'birthyear', 'birthday', 'yob')

def _is_date_named(col: str) -> bool:
    col_norm = col.lower().replace('_', '').replace('-', '').replace(' ', '')
    return any((p in col_norm for p in _DATE_NAME_PATTERNS))

def _is_personal_date_column(col: str) -> bool:
    col_norm = col.lower().replace('_', '').replace('-', '').replace(' ', '')
    return any((kw in col_norm for kw in _PERSONAL_DATE_KEYWORDS))

def _is_group_dimension(df: pd.DataFrame, col: str, max_categories: int=150) -> bool:
    if col not in df.columns:
        return False
    s = df[col]
    n_unique = s.nunique(dropna=True)
    if n_unique < 2 or n_unique > max_categories:
        return False
    if pd.api.types.is_datetime64_any_dtype(s):
        return False
    if _is_low_variance_categorical(s):
        return False
    if pd.api.types.is_numeric_dtype(s):
        col_lower = col.lower()
        id_like = _is_high_cardinality_id(df, col) or any((k in col_lower for k in ('id', 'key', 'code'))) or col_lower.endswith('id')
        return bool(pd.api.types.is_bool_dtype(s) or _is_likert(s) or id_like or n_unique <= 25)
    return True

_ID_NAME_KEYWORDS = ('id', 'uuid', 'guid', 'key', 'index', '_id', 'pk', 'code', 'num', 'no', 'zip', 'postal')

def _is_id_named(col: str) -> bool:
    col_lower = col.lower().strip()
    if col_lower.endswith('id') or col_lower.endswith('_id'):
        return True
    # word-boundary match so we don't false-positive on things like "paid" or "kid_count"
    tokens = re.split('[_\\-\\s]+', col_lower)
    return any((t in _ID_NAME_KEYWORDS for t in tokens if t))

def _is_continuous_numeric(df: pd.DataFrame, col: str) -> bool:
    if col not in df.columns:
        return False
    s = df[col]
    if not pd.api.types.is_numeric_dtype(s):
        return False
    if pd.api.types.is_bool_dtype(s) or _is_likert(s) or _is_high_cardinality_id(df, col) or _is_date_named(col):
        return False
    # Any column that *reads* like an identifier/code (country_id, zip_code, ...) is
    # categorical regardless of how many distinct values it happens to have — a
    # histogram/scatter/box built on it is meaningless even when it isn't "high cardinality".
    if _is_id_named(col):
        return False
    clean = s.dropna()
    if len(clean) < 5 or clean.nunique(dropna=True) <= 1:
        return False
    min_unique = min(10, max(3, int(len(clean) * 0.02)))
    return clean.nunique(dropna=True) >= min_unique

def _is_low_cardinality_category(df: pd.DataFrame, col: Optional[str], max_categories: int=8) -> bool:
    return bool(col and _is_group_dimension(df, col, max_categories=max_categories))

def _passes_pre_render_audit(chart: Optional[Chart]) -> bool:
    if chart is None or not _chart_has_signal(chart):
        return False
    fig = chart.fig
    traces = list(getattr(fig, 'data', []) or [])
    if chart.key.startswith('histogram_'):
        return bool(traces) and all(getattr(t, 'type', None) in ('histogram', 'bar') for t in traces)
    if chart.key.startswith(('ranked_bar_', 'grouped_bar_', 'stacked_', 'freq_bar_')):
        return bool(traces) and all(getattr(t, 'type', None) == 'bar' for t in traces)
    return True

def _classify(df: pd.DataFrame, excluded: set[str]) -> dict:
    (num, cat, date_cols, likert, ids) = ([], [], [], [], [])
    if len(excluded) > 0.5 * len(df.columns):
        logger.warning('[VIZ] Architect excluded %d/%d columns. Ignoring exclusions to find charts.', len(excluded), len(df.columns))
        excluded = set()
    for col in df.columns:
        if col in excluded:
            continue
        s = df[col]
        if _is_high_cardinality_id(df, col):
            ids.append(col)
            continue
        if pd.api.types.is_datetime64_any_dtype(s):
            date_cols.append(col)
        elif pd.api.types.is_numeric_dtype(s):
            col_lower = col.lower()
            id_kw = ('id', 'uuid', 'guid', 'key', 'index', '_id', 'pk')
            is_id_named = any((k in col_lower for k in id_kw)) or col_lower.endswith('id')
            if is_id_named:
                cat.append(col)
            elif _is_date_named(col):
                date_cols.append(col)
            elif _is_likert(s):
                likert.append(col)
            else:
                num.append(col)
        elif pd.api.types.is_string_dtype(s) or pd.api.types.is_object_dtype(s):
            cat.append(col)
    logger.info('[VIZ] Final inventory — Num: %d | Cat: %d | Date: %d | IDs: %d', len(num), len(cat), len(date_cols), len(ids))
    return {'num': num, 'cat': cat, 'date': date_cols, 'likert': likert, 'ids': ids}
_VALID_CHART_TYPES = {'ranked_bar', 'grouped_bar', 'histogram', 'scatter', 'line', 'heatmap', 'box', 'violin', 'donut', 'pie', 'likert_bar', 'stacked_bar', 'area'}

def _llm_plan_charts(df: pd.DataFrame, cols: dict, stats: dict) -> Optional[list[dict]]:
    api_key = os.getenv('GROQ_API_KEY')
    if not api_key:
        return None
    num_profiles = {}
    for c in cols['num'][:12]:
        s = df[c].dropna()
        if s.empty:
            continue
        num_profiles[c] = {'min': round(float(s.min()), 3), 'max': round(float(s.max()), 3), 'mean': round(float(s.mean()), 3), 'median': round(float(s.median()), 3), 'skew': round(float(s.skew()), 2), 'unique': int(s.nunique()), 'heavy_tailed': _is_heavy_tailed(s)}
    cat_profiles = {}
    for c in cols['cat'][:10]:
        s = df[c].dropna()
        cat_profiles[c] = {'unique': int(s.nunique()), 'top3': s.value_counts().head(3).to_dict()}
    profile = {'rows': len(df), 'dataset_label': stats.get('dataset_profile', {}).get('label', 'Unknown'), 'dataset_domain': stats.get('dataset_profile', {}).get('domain', 'general'), 'numeric_columns': num_profiles, 'categorical_columns': cat_profiles, 'datetime_columns': cols['date'][:5], 'likert_columns': cols['likert'][:8], 'strong_correlations': stats.get('strong_correlations', [])[:8]}
    prompt = (
        f'You are the Lead Data Visualization Architect for an advanced automated analytics pipeline.\n'
        f'Given this dataset profile, plan exactly the {MAX_OUTPUT_CHARTS} most insightful charts.\n'
        f'Each chart must use REAL column names from the profile.\n\n'
        f'Dataset profile:\n<profile>{json.dumps(profile, ensure_ascii=True)}</profile>\n\n'
        f'RULES (MANDATORY — violating any rule is a critical failure):\n\n'

        # ── 1. Category & Metric Mapping ──
        f'1. CATEGORY & METRIC MAPPING (Ranked Bar, Grouped Bar, Stacked Bar):\n'
        f'   - Always verify that categorical text dimensions (e.g. gender, batting_style) map strictly to '
        f'group dimensions (x, color, or split legends).\n'
        f'   - NEVER use non-numeric text values as numeric aggregation metrics. '
        f'Ensure x and y pairs are structurally sound before execution.\n'
        f'   - For top-N entity rankings (e.g. country, city, product by a metric), use "ranked_bar".\n'
        f'   - For numeric vs categorical (<=15 cats), use "grouped_bar" (sum for totals, mean for rates).\n\n'

        # ── 2. True Frequency Scaling ──
        f'2. TRUE FREQUENCY SCALING (Histograms):\n'
        f'   - For skewed or heavy-tailed continuous variables (heavy_tailed=true), scale the frequency '
        f'accumulation vertically using log_scale=true (this applies log_y, NOT log_x).\n'
        f'   - NEVER apply horizontal logarithmic compression (log_x=True) to identification keys, '
        f'sparse integers, or zero-bounded counters — it corrupts the visual plot structure.\n'
        f'   - Ensure histograms render clean vertical frequency bars. '
        f'Never approve or inject overlapping multi-scatter artifacts, box-plot marginals, or rug plots.\n\n'

        # ── 3. Time-Series Continuity ──
        f'3. TIME-SERIES CONTINUITY (Line Charts):\n'
        f'   - Ensure date columns are mapped exclusively to the chronological X-axis. '
        f'Never use a datetime or timestamp series as a vertical Y-axis value.\n'
        f'   - Reject time-series plots that contain personal identifying dates '
        f'(like date-of-birth or exact birthdays) which create meaningless sparse spikes.\n'
        f'   - For a datetime + numeric, use "line".\n\n'

        # ── 4. Correlation & Distribution Integrity ──
        f'4. CORRELATION & DISTRIBUTION INTEGRITY (Scatter, Heatmap, Box, Violin):\n'
        f'   - Scatter Plots: Only pair true continuous numeric variables. '
        f'Use the color parameter strictly for low-cardinality categorical series (<=8 distinct values).\n'
        f'   - Heatmaps: Reject correlation metrics if the variance across targeted numeric features is zero. '
        f'Use "heatmap" for correlation overview when >=3 numeric cols exist.\n'
        f'   - Box/Violin Plots: Ensure the splitting category has between 2 and 15 distinct values '
        f'to prevent unreadable, overcrowded distributions.\n\n'

        # ── 5. Pre-Render Self-Audit ──
        f'5. PRE-RENDER SELF-AUDIT:\n'
        f'   - Before submitting each chart spec, ask: "Are the labels scientifically accurate to the '
        f'underlying data? Is the aspect ratio and bar spacing clean? Does this chart reveal a true domain '
        f'insight, or is it a technical glitch?"\n\n'

        # ── Additional standing rules ──
        f'6. For categorical with 2-7 values, use "donut".\n'
        f'7. For likert/rating columns (multiple), use "likert_bar".\n'
        f'8. Never repeat the same (x, y) pair. Avoid redundant charts.\n'
        f'9. TITLES: Use generic attribute names (e.g. "Sales by Region") NOT values. Use only REAL column names.\n'
        f'10. Prioritize charts that give REAL business/domain insight, not just counts.\n'
        f'11. NO ZERO VARIANCE: Never plan charts on columns with zero variance.\n'
        f'12. NEVER use columns whose top category represents >95% of all values as grouping or color axes.\n\n'

        f'Respond ONLY with valid JSON — a list of up to {MAX_OUTPUT_CHARTS} objects:\n'
        f'[\n'
        f'  {{\n'
        f'    "chart_type": "<one of: ranked_bar|grouped_bar|histogram|scatter|line|heatmap|box|violin|donut|likert_bar|stacked_bar>",\n'
        f'    "x": "<column name or null>",\n'
        f'    "y": "<column name or null>",\n'
        f'    "color": "<column name or null>",\n'
        f'    "log_scale": false,\n'
        f'    "title": "<human readable chart title>",\n'
        f'    "agg": "<sum|mean|count>",\n'
        f'    "priority": <1 to {MAX_OUTPUT_CHARTS}>\n'
        f'  }},\n'
        f'  ...\n'
        f']'
    )
    try:
        client = get_groq_client()
        if not client:
            return None
        planner_model = os.getenv('GROQ_PLANNER_MODEL', os.getenv('GROQ_MODEL', 'llama-3.3-70b-versatile'))
        completion = client.chat.completions.create(model=planner_model, messages=[{'role': 'system', 'content': 'You are a chart planner. Respond with valid JSON array only. No markdown fences.'}, {'role': 'user', 'content': prompt}], temperature=0.1, max_tokens=1200)
        raw = (completion.choices[0].message.content or '').strip()
        if raw.startswith('```'):
            raw = re.sub('^```[a-z]*\\n?', '', raw).rstrip('`').strip()
        plan = json.loads(raw)
        if not isinstance(plan, list) or len(plan) == 0:
            raise ValueError('LLM returned empty or non-list plan')
        valid = [p for p in plan if isinstance(p, dict) and p.get('chart_type') in _VALID_CHART_TYPES]
        logger.info('LLM chart plan: %d valid charts', len(valid))
        return sorted(valid, key=lambda p: p.get('priority', 99))
    except Exception as exc:
        logger.warning('LLM chart planning failed, using heuristics: %s', exc)
        return None

def _build_ranked_bar(df: pd.DataFrame, x_col: str, y_col: str, title: str, agg: str='auto', top_n: int=_RANKED_BAR_TOP_N, color_col: Optional[str]=None, stats: dict=None) -> Optional[Chart]:
    if x_col not in df.columns or y_col not in df.columns:
        return None
    if not _is_group_dimension(df, x_col, max_categories=500):
        return None
    if not pd.api.types.is_numeric_dtype(df[y_col]):
        return None
    if x_col == y_col:
        return None
    if df[x_col].nunique(dropna=True) <= 1 or df[y_col].nunique(dropna=True) <= 1:
        return None
    if _is_low_variance_categorical(df[x_col]):
        return None
    comp = min(_completeness(df[x_col]), _completeness(df[y_col]))
    if comp < 0.4:
        return None
    if agg == 'auto':
        try:
            agg = 'sum' if _should_sum(y_col, df[y_col]) else 'mean'
        except Exception as agg_exc:
            logger.debug("Auto agg resolution failed for '%s': %s — defaulting to mean", y_col, agg_exc)
    if agg not in ('sum', 'mean', 'count', 'min', 'max'):
        agg = 'mean'
    try:
        grouped = df.groupby(x_col, observed=True)[y_col].agg(agg).reset_index().dropna(subset=[y_col]).sort_values(y_col, ascending=False).head(top_n)
    except Exception:
        return None
    if grouped.empty or grouped[y_col].isna().all():
        return None
    agg_label = 'Total' if agg == 'sum' else 'Average'
    chart_title = title or f'Top {len(grouped)} {x_col} by {agg_label} {y_col}'
    colors = ['#00d4a8','#4d9fff','#f5a623','#a78bfa','#ff4d6a','#00bcd4','#ff9800','#8bc34a']
    fig = px.bar(grouped, x=y_col, y=x_col, orientation='h', title=chart_title, color=x_col, color_discrete_sequence=colors, text=y_col)
    fig.update_traces(texttemplate='%{text:.2s}', textposition='outside', marker_line_width=0)
    pct_cols = (stats or {}).get('percentage_columns', []) if isinstance(stats, dict) else []
    if y_col in pct_cols:
        fig.update_layout(xaxis_tickformat='.1%')
    fig.update_layout(showlegend=False, coloraxis_showscale=False, yaxis=dict(autorange='reversed'))
    score = 80 + comp * 20
    return Chart(key=f'ranked_bar_{x_col}_{y_col}', fig=_style(fig, 480), score=score, cols={x_col, y_col})

def _build_grouped_bar(df: pd.DataFrame, cat_col: str, num_col: str, title: str, agg: str='auto', color_col: Optional[str]=None) -> Optional[Chart]:
    if cat_col not in df.columns or num_col not in df.columns:
        return None
    if cat_col == num_col:
        return None
    if not _is_group_dimension(df, cat_col, max_categories=100):
        return None
    if not pd.api.types.is_numeric_dtype(df[num_col]):
        return None
    if df[cat_col].nunique(dropna=True) <= 1 or df[num_col].nunique(dropna=True) <= 1:
        return None
    if _is_low_variance_categorical(df[cat_col]):
        return None
    n_cats = df[cat_col].nunique(dropna=True)
    if not 2 <= n_cats <= 40:
        return None
    comp = min(_completeness(df[cat_col]), _completeness(df[num_col]))
    if comp < 0.4:
        return None
    if agg == 'auto':
        agg = 'sum' if _should_sum(num_col, df[num_col]) else 'mean'
    try:
        grouped = df.groupby(cat_col, observed=True)[num_col].agg(agg).reset_index().dropna(subset=[num_col]).sort_values(num_col, ascending=False).head(20)
    except Exception:
        return None
    if grouped.empty:
        return None
    agg_label = 'Total' if agg == 'sum' else 'Average'
    chart_title = title or f'{agg_label} {num_col} by {cat_col}'
    horizontal = n_cats > 10
    colors = ['#00d4a8','#4d9fff','#f5a623','#a78bfa','#ff4d6a','#00bcd4','#ff9800','#8bc34a']
    if horizontal:
        fig = px.bar(grouped, x=num_col, y=cat_col, orientation='h', title=chart_title, color=cat_col, color_discrete_sequence=colors)
        fig.update_layout(yaxis=dict(autorange='reversed'))
    else:
        fig = px.bar(grouped, x=cat_col, y=num_col, title=chart_title, color=cat_col, color_discrete_sequence=colors)
        fig.update_layout(xaxis_tickangle=-30, xaxis_automargin=True)
    fig.update_traces(marker_line_width=0)
    fig.update_layout(showlegend=False)
    score = 72 + comp * 18
    return Chart(key=f'grouped_bar_{cat_col}_{num_col}', fig=_style(fig, 460), score=score, cols={cat_col, num_col})

def _build_histogram(df: pd.DataFrame, num_col: str, log_scale: bool=False, title: str='') -> Optional[Chart]:
    if num_col not in df.columns:
        return None
    if not _is_continuous_numeric(df, num_col):
        return None
    clean = df[num_col].dropna()
    if len(clean) < 5 or clean.nunique(dropna=True) <= 1:
        return None
    if _is_uninformative_dense_distribution(clean):
        logger.debug("Skipping dense uniform distribution for '%s'", num_col)
        return None
    auto_log = log_scale or _is_heavy_tailed(df[num_col])
    plot_df = _sample(df[[num_col]].dropna(), _HIST_MAX_ROWS)[num_col].dropna()
    counts, edges = _histogram_bins(plot_df)
    if len(counts) < 3 or counts.sum() == 0:
        return None
    widths = np.diff(edges)
    centers = edges[:-1] + widths / 2
    percents = counts / counts.sum() * 100
    ranges = np.array([f'{edges[i]:,.2f} - {edges[i + 1]:,.2f}' for i in range(len(counts))])
    log_note = ' (log y scale)' if auto_log else ''
    chart_title = title or f'Distribution of {num_col}{log_note}'
    fig = go.Figure(data=[
        go.Bar(
            x=centers,
            y=percents,
            width=widths * 0.86,
            customdata=ranges,
            marker=dict(color='#6366f1', line=dict(color='rgba(255,255,255,0.45)', width=0.7)),
            hovertemplate='Range: %{customdata}<br>Records: %{y:.2f}%<extra></extra>',
        )
    ])
    fig.update_layout(title=chart_title, bargap=0.08, xaxis_title=num_col, yaxis_title='Records (%)')
    if auto_log:
        fig.update_yaxes(type='log', title='Log Scale')
    comp = _completeness(df[num_col])
    skew = abs(float(clean.skew()))
    score = 52 + comp * 16 + min(skew * 8, 18) + (8 if auto_log else 0)
    return Chart(key=f'histogram_{num_col}', fig=_style(fig, 440), score=score, cols={num_col})


def _build_scatter(df: pd.DataFrame, x_col: str, y_col: str, color_col: Optional[str]=None, title: str='') -> Optional[Chart]:
    if x_col not in df.columns or y_col not in df.columns:
        return None
    if x_col == y_col:
        return None
    if not (_is_continuous_numeric(df, x_col) and _is_continuous_numeric(df, y_col)):
        return None
    if df[x_col].nunique(dropna=True) <= 1 or df[y_col].nunique(dropna=True) <= 1:
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
    color_use = color_col if _is_low_cardinality_category(df, color_col, max_categories=8) else None
    plot_df = _sample(df, _SCATTER_MAX_ROWS, stratify_col=color_use)
    sampled_note = f'  [{_SCATTER_MAX_ROWS:,} sampled]' if len(df) > _SCATTER_MAX_ROWS else ''
    chart_title = title or f'{x_col} vs {y_col}  (r={r:.2f}){sampled_note}'
    trendline = 'ols' if len(plot_df) <= 3000 else None
    fig = px.scatter(plot_df, x=x_col, y=y_col, color=color_use, title=chart_title, trendline=trendline, opacity=0.65)
    score = abs(r) * 55 + min(len(pair) / 20, 20) + comp * 20
    return Chart(key=f'scatter_{x_col}_{y_col}', fig=_style(fig, 460), score=score, cols={x_col, y_col})

def _build_line(df: pd.DataFrame, date_col: str, num_cols: list[str], title: str='') -> Optional[Chart]:
    if date_col not in df.columns or not num_cols:
        return None
    if _is_personal_date_column(date_col):
        return None
    if not (pd.api.types.is_datetime64_any_dtype(df[date_col]) or _is_date_named(date_col)):
        return None
    if df[date_col].nunique(dropna=True) <= 1:
        return None
    num_cols = [c for c in num_cols if c != date_col]
    valid_nums = [c for c in num_cols if c in df.columns and _is_continuous_numeric(df, c) and (_completeness(df[c]) >= 0.6) and df[c].nunique(dropna=True) > 1]
    if not valid_nums:
        return None
    comp_d = _completeness(df[date_col])
    if comp_d < 0.7 or df[date_col].nunique() < 5:
        return None
    df_s = df.sort_values(date_col)
    if len(valid_nums) > 1:
        meds = [abs(df[c].median()) for c in valid_nums if df[c].notna().any()]
        if meds and max(meds) / max(min(meds), 1e-06) > 50:
            valid_nums = [max(valid_nums, key=lambda c: abs(df[c].median()))]
    cols_use = valid_nums[:4]
    plot_df = _resample_ts(df_s, date_col, cols_use, _TS_MAX_POINTS)
    try:
        unique_points = int(plot_df[date_col].nunique())
    except Exception as unique_exc:
        logger.debug("date_col.nunique() failed for '%s': %s", date_col, unique_exc)
        unique_points = -1
    if unique_points < 4:
        return None
    long_df = plot_df.melt(id_vars=date_col, var_name='Series', value_name='Value')
    chart_title = title or f'Trends Over Time'
    fig = px.line(long_df, x=date_col, y='Value', color='Series', title=chart_title, markers=len(plot_df) <= 60)
    n = len(df)
    score = 75 + min(n / 10, 20) + comp_d * 15
    return Chart(key=f'line_{date_col}', fig=_style(fig, 480), score=score, cols={date_col} | set(cols_use))

def _build_heatmap(df: pd.DataFrame, num_cols: list[str], title: str='') -> Optional[Chart]:
    eligible = [c for c in num_cols if _is_continuous_numeric(df, c) and _completeness(df[c]) >= 0.6 and df[c].nunique(dropna=True) > 1 and float(df[c].var(skipna=True) or 0) > 0]
    if len(eligible) < 3:
        return None
    if max((len(c) for c in eligible)) > 45:
        return None
    cols = eligible[:12]
    sample = df[cols].sample(min(len(df), 5000), random_state=42) if len(df) > 5000 else df[cols]
    corr = sample.corr().round(2)
    corr = corr.dropna(axis=0, how='all').dropna(axis=1, how='all')
    mask = (corr.abs() < 0.9999).any(axis=1)
    corr = corr.loc[mask, mask]
    if corr.empty or corr.shape[0] < 2:
        return None
    chart_title = title or 'Correlation Heatmap'
    fig = px.imshow(corr, text_auto=True, title=chart_title, zmin=-1, zmax=1)
    fig.update_layout(
        paper_bgcolor='#161c28',
        plot_bgcolor='#161c28',
        font_color='#e8edf5',
    )
    fig.update_traces(
        colorscale=[
            [0, '#0d3b6e'],
            [0.5, '#1e2d45'],
            [1, '#00d4a8']
        ]
    )
    height = max(380, min(600, 200 + 48 * len(cols)))
    score = 85.0
    return Chart(key='heatmap_correlation', fig=_style(fig, height), score=score, cols=set(cols))

def _build_box(df: pd.DataFrame, cat_col: str, num_col: str, title: str='') -> Optional[Chart]:
    if cat_col not in df.columns or num_col not in df.columns:
        return None
    if not _is_group_dimension(df, cat_col, max_categories=15):
        return None
    if not _is_continuous_numeric(df, num_col):
        return None
    if df[cat_col].nunique(dropna=True) <= 1 or df[num_col].nunique(dropna=True) <= 1:
        return None
    if _is_low_variance_categorical(df[cat_col]):
        return None
    n_cats = df[cat_col].nunique(dropna=True)
    if not 2 <= n_cats <= 15:
        return None
    comp = min(_completeness(df[cat_col]), _completeness(df[num_col]))
    if comp < 0.4:
        return None
    plot_df = _sample(df[[cat_col, num_col]].dropna(), _HIST_MAX_ROWS, stratify_col=cat_col)
    chart_title = title or f'{num_col} Distribution by {cat_col}'
    fig = px.box(plot_df, x=cat_col, y=num_col, color=cat_col, title=chart_title, points=False, notched=len(plot_df) >= 100)
    fig.update_layout(showlegend=False, xaxis_tickangle=-25, xaxis_automargin=True)
    score = 65 + comp * 18
    return Chart(key=f'box_{num_col}_by_{cat_col}', fig=_style(fig, 470), score=score, cols={cat_col, num_col})

def _build_violin(df: pd.DataFrame, cat_col: str, num_col: str, title: str='') -> Optional[Chart]:
    if cat_col not in df.columns or num_col not in df.columns:
        return None
    if not _is_group_dimension(df, cat_col, max_categories=15):
        return None
    if not _is_continuous_numeric(df, num_col):
        return None
    if df[cat_col].nunique(dropna=True) <= 1 or df[num_col].nunique(dropna=True) <= 1:
        return None
    if _is_low_variance_categorical(df[cat_col]):
        return None
    n_cats = df[cat_col].nunique(dropna=True)
    if not 2 <= n_cats <= 15:
        return None
    comp = min(_completeness(df[cat_col]), _completeness(df[num_col]))
    if comp < 0.4:
        return None
    plot_df = _sample(df[[cat_col, num_col]].dropna(), _SCATTER_MAX_ROWS, stratify_col=cat_col)
    chart_title = title or f'{num_col} by {cat_col}'
    fig = px.violin(plot_df, x=cat_col, y=num_col, color=cat_col, box=True, points=False, title=chart_title)
    fig.update_layout(showlegend=False)
    score = 68 + comp * 18
    return Chart(key=f'violin_{num_col}_by_{cat_col}', fig=_style(fig, 460), score=score, cols={cat_col, num_col})

def _build_donut(df: pd.DataFrame, cat_col: str, title: str='') -> Optional[Chart]:
    if cat_col not in df.columns:
        return None
    if not _is_group_dimension(df, cat_col, max_categories=20):
        return None
    if df[cat_col].nunique(dropna=True) <= 1:
        return None
    n = df[cat_col].nunique(dropna=True)
    if not 2 <= n <= 20:
        return None
    comp = _completeness(df[cat_col])
    if comp < 0.6:
        return None
    top_pct = df[cat_col].value_counts(normalize=True).iloc[0]
    if top_pct > 0.95:
        return None
    counts = df[cat_col].value_counts().reset_index()
    counts.columns = [cat_col, 'count']
    chart_title = f'Composition of {cat_col}'
    fig = px.pie(counts, names=cat_col, values='count', title=chart_title, hole=0.42)
    fig.update_traces(textposition='outside', textinfo='percent+label', marker_line_width=0)
    score = 62 + comp * 18
    return Chart(key=f'donut_{cat_col}', fig=_style(fig, 440), score=score, cols={cat_col})

def _build_likert_bar(df: pd.DataFrame, likert_cols: list[str], title: str='') -> Optional[Chart]:
    valid = [c for c in likert_cols if _completeness(df[c]) >= 0.4 and df[c].nunique(dropna=True) > 1]
    if len(valid) < 2:
        return None

    def _short(col: str) -> str:
        for sep in ['(', '-', ':']:
            if sep in col:
                parts = col.rsplit(sep, 1)
                cand = (sep + parts[-1]).strip() if sep != '-' else parts[-1].strip()
                if len(cand) <= 40:
                    return cand
        return col[:40]
    rows = [{'Question': _short(c), 'Avg Rating': round(float(df[c].mean()), 2)} for c in valid]
    means_df = pd.DataFrame(rows).sort_values('Avg Rating')
    chart_title = title or 'Average Satisfaction / Rating Scores'
    fig = px.bar(means_df, x='Avg Rating', y='Question', orientation='h', title=chart_title, color='Avg Rating', color_continuous_scale='RdYlGn', range_x=[0, 10], text='Avg Rating')
    fig.update_traces(textposition='outside')
    fig.update_layout(coloraxis_showscale=False, yaxis_title='')
    score = 75.0
    return Chart(key='likert_bars', fig=_style(fig, 480), score=score, cols=set(valid))

def _build_stacked_bar(df: pd.DataFrame, x_col: str, cat_col: str, num_col: Optional[str]=None, title: str='') -> Optional[Chart]:
    if x_col not in df.columns or cat_col not in df.columns:
        return None
    if not _is_group_dimension(df, x_col, max_categories=30) or not _is_group_dimension(df, cat_col, max_categories=15):
        return None
    if df[x_col].nunique(dropna=True) <= 1 or df[cat_col].nunique(dropna=True) <= 1:
        return None
    if num_col and num_col in df.columns and df[num_col].nunique(dropna=True) <= 1:
        return None
    if _is_low_variance_categorical(df[x_col]) or _is_low_variance_categorical(df[cat_col]):
        return None
    nx = df[x_col].nunique(dropna=True)
    nc = df[cat_col].nunique(dropna=True)
    if not (2 <= nx <= 30 and 2 <= nc <= 15):
        return None
    if num_col and num_col in df.columns and pd.api.types.is_numeric_dtype(df[num_col]):
        agg = 'sum' if _should_sum(num_col, df[num_col]) else 'mean'
        pivot = df.groupby([x_col, cat_col], observed=True)[num_col].agg(agg).reset_index()
        y_label = num_col
    else:
        pivot = df.groupby([x_col, cat_col], observed=True).size().reset_index(name='count')
        y_label = 'count'
    chart_title = title or f'{y_label} by {x_col} and {cat_col}'
    fig = px.bar(pivot, x=x_col, y=y_label, color=cat_col, title=chart_title, barmode='stack')
    fig.update_layout(xaxis_tickangle=-25, xaxis_automargin=True)
    score = 65.0
    return Chart(key=f'stacked_{x_col}_{cat_col}', fig=_style(fig, 460), score=score, cols={x_col, cat_col} | ({num_col} if num_col else set()))

def _build_freq_bar(df: pd.DataFrame, cat_col: str, title: str='') -> Optional[Chart]:
    if cat_col not in df.columns:
        return None
    if not _is_group_dimension(df, cat_col, max_categories=150):
        return None
    if df[cat_col].nunique(dropna=True) <= 1:
        return None
    if _is_low_variance_categorical(df[cat_col]):
        return None
    n = df[cat_col].nunique(dropna=True)
    if not 2 <= n <= 150:
        return None
    comp = _completeness(df[cat_col])
    if comp < 0.5:
        return None
    vc = df[cat_col].value_counts().head(25).reset_index()
    vc.columns = [cat_col, 'count']
    total = int(df[cat_col].notna().sum())
    vc['percent'] = vc['count'] / max(total, 1) * 100
    chart_title = title or f'Top {len(vc)} {cat_col} Values'
    horizontal = n > 10
    use_percent = total > 5000 or int(vc['count'].max()) > 2000
    if horizontal:
        if use_percent:
            fig = px.bar(vc, y=cat_col, x='percent', orientation='h', title=chart_title, color=cat_col)
            fig.update_layout(xaxis_tickformat='.1f')
        else:
            fig = px.bar(vc, y=cat_col, x='count', orientation='h', title=chart_title, color=cat_col)
        fig.update_layout(yaxis=dict(autorange='reversed'))
    else:
        if use_percent:
            fig = px.bar(vc, x=cat_col, y='percent', title=chart_title, color=cat_col)
            fig.update_layout(yaxis_tickformat='.1f')
        else:
            fig = px.bar(vc, x=cat_col, y='count', title=chart_title, color=cat_col)
        fig.update_layout(xaxis_tickangle=-25, xaxis_automargin=True)
    fig.update_traces(marker_line_width=0)
    fig.update_layout(showlegend=False)
    score = 55 + comp * 15
    return Chart(key=f'freq_bar_{cat_col}', fig=_style(fig, 440), score=score, cols={cat_col})

def _build_freq_bar_loose(df: pd.DataFrame, cat_col: str, title: str='') -> Optional[Chart]:
    if cat_col not in df.columns:
        return None
    if not _is_group_dimension(df, cat_col, max_categories=500):
        return None
    if df[cat_col].nunique(dropna=True) <= 1:
        return None
    if _is_low_variance_categorical(df[cat_col]):
        return None
    n = df[cat_col].nunique(dropna=True)
    if not 2 <= n <= 500:
        return None
    comp = _completeness(df[cat_col])
    if comp < 0.2:
        return None
    vc = df[cat_col].value_counts().head(25).reset_index()
    vc.columns = [cat_col, 'count']
    total = int(df[cat_col].notna().sum())
    vc['percent'] = vc['count'] / max(total, 1) * 100
    chart_title = title or f'Top {len(vc)} {cat_col} Values'
    horizontal = n > 10
    use_percent = total > 5000 or int(vc['count'].max()) > 2000
    if horizontal:
        if use_percent:
            fig = px.bar(vc, y=cat_col, x='percent', orientation='h', title=chart_title, color=cat_col)
            fig.update_layout(xaxis_tickformat='.1f')
        else:
            fig = px.bar(vc, y=cat_col, x='count', orientation='h', title=chart_title, color=cat_col)
        fig.update_layout(yaxis=dict(autorange='reversed'))
    else:
        if use_percent:
            fig = px.bar(vc, x=cat_col, y='percent', title=chart_title, color=cat_col)
            fig.update_layout(yaxis_tickformat='.1f')
        else:
            fig = px.bar(vc, x=cat_col, y='count', title=chart_title, color=cat_col)
        fig.update_layout(xaxis_tickangle=-25, xaxis_automargin=True)
    fig.update_traces(marker_line_width=0)
    fig.update_layout(showlegend=False)
    score = 30 + comp * 30
    return Chart(key=f'freq_bar_loose_{cat_col}', fig=_style(fig, 440), score=score, cols={cat_col})

def _execute_plan(df: pd.DataFrame, plan: list[dict], cols: dict, stats: dict) -> list[Chart]:
    charts: list[Chart] = []
    used_keys: set[str] = set()
    col_names = stats.get('columns') or []
    col_types = {}
    for c in stats.get('numeric_columns', {}).keys():
        col_types[c] = 'numeric'
    for c in stats.get('categorical_columns', {}).keys():
        col_types[c] = 'categorical'
    for c in col_names if isinstance(col_names, list) else []:
        if c not in col_types:
            col_types[c] = 'unknown'
    for item in plan:
        ct = item.get('chart_type', '')
        x = item.get('x')
        y = item.get('y')
        ttl = item.get('title')
        agg = item.get('agg', 'auto')
        col = item.get('color')
        log_ = bool(item.get('log_scale', False))

        def _ok(c):
            return c is None or c in df.columns
        if not (_ok(x) and _ok(y) and _ok(col)):
            logger.debug('LLM plan item skipped (missing cols): %s', item)
            continue
        chart: Optional[Chart] = None
        if ct == 'ranked_bar' and x and y:
            chart = _build_ranked_bar(df, x, y, ttl, agg=agg, color_col=col)
            if chart is None and y and x:
                chart = _build_ranked_bar(df, y, x, None, agg=agg, color_col=col)
        elif ct == 'grouped_bar' and x and y:
            chart = _build_grouped_bar(df, x, y, ttl, agg=agg, color_col=col)
            if chart is None:
                chart = _build_grouped_bar(df, y, x, None, agg=agg, color_col=col)
        elif ct == 'histogram' and (x or y):
            hist_col = x or y
            chart = _build_histogram(df, hist_col, log_scale=log_, title=ttl)
        elif ct == 'scatter' and x and y:
            x_num = x in df.columns and pd.api.types.is_numeric_dtype(df[x])
            y_num = y in df.columns and pd.api.types.is_numeric_dtype(df[y])
            if x_num and y_num:
                chart = _build_scatter(df, x, y, color_col=col, title=ttl)
        elif ct == 'box' and x and y:
            chart = _build_box(df, x, y, title=ttl)
            if chart is None:
                chart = _build_box(df, y, x, title=None)
        elif ct == 'violin' and x and y:
            chart = _build_violin(df, x, y, title=ttl)
            if chart is None:
                chart = _build_violin(df, y, x, title=None)
        elif ct == 'line' and x:
            val_cols = [y] if y else cols['num']
            chart = _build_line(df, x, val_cols, title=ttl)
        elif ct == 'heatmap':
            chart = _build_heatmap(df, cols['num'], title=ttl)
        elif ct in ('donut', 'pie') and x:
            if x not in df.columns:
                x_fallback = next((c for c in cols['cat'] if 2 <= df[c].nunique(dropna=True) <= 20), None)
                if x_fallback:
                    logger.debug('LLM donut x=%r is not a column — falling back to %r', x, x_fallback)
                    x = x_fallback
                else:
                    logger.debug('LLM donut x=%r is not a column and no fallback found', x)
                    continue
            chart = _build_donut(df, x)
        elif ct == 'likert_bar':
            chart = _build_likert_bar(df, cols['likert'], title=ttl)
        elif ct == 'stacked_bar' and x and y:
            chart = _build_stacked_bar(df, x, y, num_col=None, title=ttl)
        if chart is None:
            logger.debug("LLM chart '%s' (%s, %s) could not be built", ct, x, y)
            continue
        if chart.key in used_keys:
            continue
        if not _passes_pre_render_audit(chart):
            logger.debug('Dropping low-signal chart: %s', chart.key)
            continue
        used_keys.add(chart.key)
        charts.append(chart)
    return charts

def _chart_has_signal(chart: Chart) -> bool:
    fig = chart.fig
    if not fig or not getattr(fig, 'data', None):
        return False
    traces = [t for t in fig.data if t is not None]
    if not traces:
        return False
    for t in traces:
        for attr in ('x', 'y', 'z', 'values', 'r'):
            v = getattr(t, attr, None)
            if v is not None:
                try:
                    if len(v) >= 2:
                        return True
                except Exception:
                    pass
    return False

def _heuristic_plan(df: pd.DataFrame, cols: dict, stats: dict) -> list[Chart]:
    charts: list[Chart] = []
    used_pairs: set[frozenset] = set()
    used_singles: set[str] = set()
    domain = (stats.get('dataset_profile') or {}).get('domain', 'general')
    prefer_timeseries = domain in ('finance', 'economics', 'sales', 'logistics', 'technology')
    prefer_distribution = domain in ('healthcare', 'research', 'survey', 'education')
    prefer_ranking = domain in ('sports', 'retail', 'marketing')

    def _add(c: Optional[Chart]) -> bool:
        if c is None or not _passes_pre_render_audit(c):
            return False
        pair = frozenset(c.cols)
        if pair in used_pairs:
            return False
        used_pairs.add(pair)
        charts.append(c)
        return True
    num = cols['num']
    cat = cols['cat']
    date = cols['date']
    likert = cols['likert']
    ids = cols['ids']
    strong_corrs = stats.get('strong_correlations', [])
    if len(cat) < 2 and ids:
        low_card_ids = [c for c in ids if 2 <= df[c].nunique(dropna=True) <= 500]
        cat.extend(low_card_ids[:5])
        if low_card_ids:
            logger.info('[VIZ] Promoted %d low-cardinality IDs to categorical for chart generation', len(low_card_ids[:5]))
    plottable_dates = [d for d in date if not _is_personal_date_column(d)]
    if plottable_dates and num:
        _add(_build_line(df, plottable_dates[0], num))
    if len(num) >= 3 and (not prefer_ranking):
        _add(_build_heatmap(df, num))
    if len(likert) >= 2:
        _add(_build_likert_bar(df, likert))
    if prefer_ranking and cat and num:
        best_num = max(num, key=lambda c: _completeness(df[c])) if num else None
        if best_num:
            for cat_col in cat[:3]:
                if _add(_build_ranked_bar(df, cat_col, best_num, '', stats=stats)):
                    break
    if len(num) >= 2:
        if strong_corrs:
            best = max(strong_corrs, key=lambda x: abs(x['correlation']))
            (c1, c2) = (best['col1'], best['col2'])
            if c1 in num and c2 in num:
                color_candidate = next((c for c in cat if _is_low_cardinality_category(df, c, max_categories=8)), None)
                _add(_build_scatter(df, c1, c2, color_col=color_candidate))
        else:
            _add(_build_scatter(df, num[0], num[1]))
    for cat_col in cat[:4]:
        n_cats = df[cat_col].nunique(dropna=True)
        if n_cats < 2:
            continue
        best_num = max(num, key=lambda c: _completeness(df[c])) if num else None
        if best_num is None:
            continue
        if n_cats > 15:
            c = _build_ranked_bar(df, cat_col, best_num, '', stats=stats)
        else:
            c = _build_grouped_bar(df, cat_col, best_num, '')
        if _add(c):
            used_singles.add(cat_col)
            break
    remaining_cats = [c for c in cat if c not in used_singles]
    for cat_col in remaining_cats[:2]:
        n_cats = df[cat_col].nunique(dropna=True)
        if 2 <= n_cats <= 8:
            _add(_build_donut(df, cat_col))
        elif n_cats > 8:
            _add(_build_freq_bar(df, cat_col))
    num_by_var = sorted(num, key=lambda c: float(df[c].var(skipna=True) or 0), reverse=True)
    for ncol in num_by_var[:3]:
        if frozenset({ncol}) not in used_pairs:
            _add(_build_histogram(df, ncol))
    if cat and len(num) >= 2:
        best_cat = max(cat, key=lambda c: _completeness(df[c]))
        n_cats = df[best_cat].nunique(dropna=True)
        if 2 <= n_cats <= 6:
            _add(_build_violin(df, best_cat, num[1]))
        elif 2 <= n_cats <= 15:
            _add(_build_box(df, best_cat, num[1]))
    small_cats = [c for c in cat if 2 <= df[c].nunique(dropna=True) <= 15]
    if len(small_cats) >= 2:
        _add(_build_stacked_bar(df, small_cats[0], small_cats[1]))
    if not charts:
        logger.warning('[VIZ] Heuristic rules produced zero charts — triggering emergency fallback')
        all_possible_categorical = cat + ids
        viable_cols = [c for c in all_possible_categorical if 2 <= df[c].nunique(dropna=True) <= 500]
        if viable_cols:
            logger.info('[VIZ] Emergency fallback using %d viable categorical columns', len(viable_cols))
            for cname in viable_cols[:10]:
                c = _build_freq_bar(df, cname)
                if c is None:
                    c = _build_freq_bar_loose(df, cname)
                _add(c)
        if not charts and num:
            logger.info('[VIZ] Emergency fallback: no categorical found, trying numeric histograms')
            for cname in num[:5]:
                _add(_build_histogram(df, cname))
    return charts

def _apply_analytical_bonus(charts: list[Chart]) -> list[Chart]:
    HIGH_VALUE = ('scatter_', 'heatmap_', 'line_', 'box_', 'violin_')
    MED_VALUE = ('ranked_bar_', 'grouped_bar_', 'histogram_', 'stacked_')
    for c in charts:
        if any((c.key.startswith(h) for h in HIGH_VALUE)):
            c.score += 15
        elif any((c.key.startswith(m) for m in MED_VALUE)):
            c.score += 5
    return charts

def _deduplicate_and_select(charts: list[Chart]) -> dict[str, go.Figure]:
    family_limits = {'heatmap': 2, 'line': 4, 'scatter': 4, 'histogram': 4, 'box': 3, 'violin': 3, 'donut': 4, 'bar': 5, 'likert': 3, 'stacked': 4, 'ranked': 4, 'grouped': 4, 'freq': 4}

    def _family(key: str) -> str:
        for f in family_limits:
            if key.startswith(f):
                return f
        return 'other'
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
        if family_count.get(fam, 0) >= family_limits.get(fam, 5):
            continue
        seen_keys.add(c.key)
        seen_pairs.add(pair)
        family_count[fam] = family_count.get(fam, 0) + 1
        selected.append(c)
        if len(selected) >= MAX_OUTPUT_CHARTS:
            break
    logger.info('Final chart selection: %d charts — %s', len(selected), [(c.key, round(c.score, 1)) for c in selected])
    return {c.key: c.fig for c in selected}

def _coerce_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes(include='object').columns:
        sample = df[col].dropna().head(30).astype(str)
        try:
            pd.to_datetime(sample, format='mixed')
            df[col] = pd.to_datetime(df[col], format='mixed', errors='coerce')
        except Exception:
            pass
    return df

def _chart_summary_for_llm(chart: Chart, df: pd.DataFrame) -> dict:
    fig = chart.fig
    summary = {'key': chart.key, 'score': round(chart.score, 1), 'columns_used': list(chart.cols)}
    try:
        layout = fig.layout
        title_obj = layout.title
        summary['title'] = (title_obj.text if hasattr(title_obj, 'text') else str(title_obj)) if title_obj else chart.key
        trace_previews = []
        for t in fig.data[:2]:
            t_info = {'type': t.type}
            for attr in ('x', 'y', 'values', 'labels'):
                val = getattr(t, attr, None)
                if val is not None:
                    try:
                        lst = [str(v) for v in list(val)[:6]]
                        t_info[attr] = lst
                    except Exception:
                        pass
            trace_previews.append(t_info)
        summary['data_preview'] = trace_previews
    except Exception:
        pass
    return summary

def _llm_evaluate_charts(charts: list[Chart], df: pd.DataFrame, cols: dict, stats: dict) -> list[Chart]:
    api_key = os.getenv('GROQ_API_KEY')
    if not api_key or len(charts) == 0:
        return charts
    planner_model = os.getenv('GROQ_PLANNER_MODEL', 'llama-3.3-70b-versatile')
    chart_summaries = [_chart_summary_for_llm(c, df) for c in charts]
    prompt = (
        f'You are the Lead Data Visualization Quality Evaluator for an advanced analytics pipeline.\n'
        f'You built the following charts for a dataset. Review each one and decide if it is high quality.\n\n'
        f'Dataset domain: {stats.get("dataset_profile", {}).get("label", "Unknown")}\n'
        f'Numeric columns: {cols["num"][:8]}\n'
        f'Categorical columns: {cols["cat"][:6]}\n\n'
        f'Built charts summary:\n<charts>{json.dumps(chart_summaries, ensure_ascii=True)}</charts>\n\n'
        f'For EACH chart, respond with one of:\n'
        f'  KEEP    — if it provides clear, meaningful insight\n'
        f'  REPLACE — if it is the wrong chart type for the data (provide a better spec)\n'
        f'  DROP    — if it shows no useful information\n\n'

        f'EVALUATION RULES (MANDATORY):\n\n'

        f'1. CATEGORY & METRIC MAPPING:\n'
        f'   - Verify categorical text dimensions (e.g. gender, batting_style) map strictly to '
        f'group dimensions (x, color, or legends). NEVER use non-numeric text as a numeric metric.\n'
        f'   - If x and y are structurally swapped (text as metric, number as category), DROP or REPLACE.\n\n'

        f'2. TRUE FREQUENCY SCALING:\n'
        f'   - Histograms must render clean vertical frequency bars.\n'
        f'   - If log_x was applied to ID keys, sparse integers, or zero-bounded counters → REPLACE '
        f'(use log_y instead or switch to ranked_bar).\n'
        f'   - If histogram overlays box-plot marginals, scatter points, or rug plots → REPLACE/DROP.\n'
        f'   - A histogram with only 1-2 visible bars → REPLACE with ranked_bar.\n\n'

        f'3. TIME-SERIES CONTINUITY:\n'
        f'   - Date columns must be on the X-axis only. If a datetime is on Y-axis → DROP.\n'
        f'   - Personal identifying dates (dob, birth_date) as raw axis → DROP.\n\n'

        f'4. CORRELATION & DISTRIBUTION INTEGRITY:\n'
        f'   - Scatter with non-continuous or text variables → DROP/REPLACE.\n'
        f'   - Scatter color must be low-cardinality categorical (<=8 values), not numeric → REPLACE.\n'
        f'   - Box/Violin with only 1 category or >15 categories → DROP.\n'
        f'   - Heatmap on zero-variance numeric features → DROP.\n\n'

        f'5. PRE-RENDER SELF-AUDIT:\n'
        f'   - For each chart ask: "Are the labels scientifically accurate? Does this chart reveal a '
        f'true domain insight, or is it a technical glitch?"\n\n'

        f'6. ADDITIONAL:\n'
        f'   - A repeated scatter showing the same columns as another chart → DROP.\n'
        f'   - An empty or near-empty chart → DROP.\n'
        f'   - A chart plotting a column where all values are identical (zero variance) → DROP.\n'
        f'   - A chart grouping by a column where >95% of values are the same category → DROP.\n\n'

        f'Respond ONLY with valid JSON:\n'
        f'{{\n'
        f'  "evaluations": [\n'
        f'    {{"key": "<chart_key>", "decision": "KEEP|REPLACE|DROP",\n'
        f'      "reason": "<one line>",\n'
        f'      "replacement": {{"chart_type": "ranked_bar", "x": "<col>", "y": "<col>",\n'
        f'                      "title": "<title>", "agg": "sum", "log_scale": false}}\n'
        f'      }},\n'
        f'    ...\n'
        f'  ]\n'
        f'}}\n'
        f'"replacement" is ONLY required when decision is REPLACE. Omit it otherwise.'
    )
    try:
        client = get_groq_client()
        if not client:
            return charts
        completion = client.chat.completions.create(model=planner_model, messages=[{'role': 'system', 'content': 'You are a chart quality evaluator. Respond with valid JSON only. No markdown.'}, {'role': 'user', 'content': prompt}], temperature=0.1, max_tokens=500)
        raw = (completion.choices[0].message.content or '').strip()
        if raw.startswith('```'):
            raw = re.sub('^```[a-z]*\\n?', '', raw).rstrip('`').strip()
        result = json.loads(raw)
        evaluations = result.get('evaluations', [])
        if not evaluations:
            raise ValueError('Empty evaluations')
        logger.info('[AGENTIC] LLM evaluation: %d decisions received', len(evaluations))
        chart_by_key = {c.key: c for c in charts}
        final_charts: list[Chart] = []
        for ev in evaluations:
            key = ev.get('key', '')
            decision = ev.get('decision', 'KEEP').upper()
            reason = ev.get('reason', '')
            original = chart_by_key.get(key)
            if original is None:
                continue
            if decision == 'KEEP':
                logger.info("[AGENTIC] KEEP  '%s' — %s", key, reason)
                final_charts.append(original)
            elif decision == 'DROP':
                logger.info("[AGENTIC] DROP  '%s' — %s", key, reason)
            elif decision == 'REPLACE':
                replacement_spec = ev.get('replacement')
                if replacement_spec and isinstance(replacement_spec, dict):
                    logger.info("[AGENTIC] REPLACE '%s' → %s — %s", key, replacement_spec, reason)
                    new_charts = _execute_plan(df, [replacement_spec], cols, stats)
                    if new_charts and _passes_pre_render_audit(new_charts[0]):
                        logger.info('[AGENTIC] Replacement built successfully: %s', new_charts[0].key)
                        final_charts.append(new_charts[0])
                    else:
                        logger.info("[AGENTIC] Replacement failed — keeping original '%s'", key)
                        final_charts.append(original)
                else:
                    logger.info("[AGENTIC] REPLACE '%s' had no valid spec — keeping original", key)
                    final_charts.append(original)
        evaluated_keys = {ev.get('key') for ev in evaluations}
        for c in charts:
            if c.key not in evaluated_keys:
                final_charts.append(c)
        logger.info('[AGENTIC] After evaluation: %d charts (was %d)', len(final_charts), len(charts))
        return final_charts
    except Exception as exc:
        logger.warning('[AGENTIC] LLM evaluation failed, keeping all built charts: %s', exc)
        return charts

def _cols_from_architect(df: pd.DataFrame, col_types: dict, stats: dict) -> dict:
    excluded = {e['column'] for e in stats.get('excluded_columns', [])}
    (num, cat, date_cols, likert, ids) = ([], [], [], [], [])
    for (col, ctype) in col_types.items():
        if col not in df.columns or col in excluded:
            continue
        if _is_high_cardinality_id(df, col):
            ids.append(col)
            continue
        if ctype == 'datetime':
            date_cols.append(col)
        elif ctype == 'numeric':
            col_lower = col.lower()
            id_kw = ('id', 'uuid', 'guid', 'key', 'index', '_id', 'pk')
            is_id_named = any((k in col_lower for k in id_kw)) or col_lower.endswith('id')
            if is_id_named:
                cat.append(col)
            elif _is_likert(df[col]):
                likert.append(col)
            else:
                num.append(col)
        elif ctype in ('categorical', 'boolean'):
            cat.append(col)
    return {'num': num, 'cat': cat, 'date': date_cols, 'likert': likert, 'ids': ids}

def _select_charts(df: pd.DataFrame, stats: dict, column_types: dict=None) -> dict[str, go.Figure]:
    df = _coerce_dates(df)
    if column_types:
        cols = _cols_from_architect(df, column_types, stats)
    else:
        excluded = {e['column'] for e in stats.get('excluded_columns', [])}
        cols = _classify(df, excluded)
    logger.info('[VIZ] Column inventory — numeric: %d | cat: %d | date: %d | likert: %d', len(cols['num']), len(cols['cat']), len(cols['date']), len(cols['likert']))
    logger.info('[VIZ] Phase 1+2: LLM chart planning AND heuristic build running concurrently...')
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        plan_future = pool.submit(_llm_plan_charts, df, cols, stats)
        heuristic_future = pool.submit(_heuristic_plan, df, cols, stats)
        plan = plan_future.result()
        heuristic_charts = heuristic_future.result()
    llm_charts: list[Chart] = []
    if plan:
        llm_charts = _execute_plan(df, plan, cols, stats)
        logger.info('[VIZ] Phase 1 done — %d LLM-planned charts built', len(llm_charts))
    else:
        logger.info('[VIZ] Phase 1 skipped (no API key or LLM failed)')
    logger.info('[VIZ] Phase 2 done — %d heuristic charts built', len(heuristic_charts))
    llm_keys = {c.key for c in llm_charts}
    all_charts = llm_charts + [c for c in heuristic_charts if c.key not in llm_keys]
    if not all_charts:
        logger.warning('[VIZ] No charts generated — bare fallback')
        for col in df.columns:
            fb = _build_freq_bar(df, col) or _build_histogram(df, col)
            if fb is None:
                fb = _build_freq_bar_loose(df, col)
            if fb and _passes_pre_render_audit(fb):
                all_charts = [fb]
                break
    if len(all_charts) > MAX_OUTPUT_CHARTS + 2:
        logger.info('[VIZ] Phase 4: Agentic evaluate & refine — reviewing %d charts...', len(all_charts))
        all_charts = _llm_evaluate_charts(all_charts, df, cols, stats)
    else:
        logger.info('[VIZ] Phase 4: Skipped (only %d charts — no filtering needed)', len(all_charts))
    all_charts = _apply_analytical_bonus(all_charts)
    result = _deduplicate_and_select(all_charts)
    if not result and heuristic_charts:
        logger.warning('[VIZ] dedup returned empty — using top heuristic chart')
        result = {heuristic_charts[0].key: heuristic_charts[0].fig}
    return result

def run(state: AnalysisState) -> AnalysisState:
    logger.info('Agentic Visualizer v5.0 (Parallel Plan+Heuristic → Merge → Evaluate → Select) starting')
    state.current_agent = 'visualizer'
    if state.clean_df is None or state.clean_df.empty:
        add_pipeline_error(state.errors, code='VISUALIZER_NO_DATA', message='No clean_df available for visualizer', agent='visualizer', error_type='validation')
        return state
    try:
        state.charts = _select_charts(state.clean_df, state.stats_summary or {}, column_types=state.column_types or {})
        logger.info('[AGENTIC] Visualizer done — %d final charts', len(state.charts))
        state.completed_agents.append('visualizer')
    except Exception as exc:
        add_pipeline_error(state.errors, code='VISUALIZER_FAILED', message=str(exc), agent='visualizer', error_type='agent')
        logger.exception('Agentic Visualizer v5.0 error')
    return state
visualizer_agent = run