from __future__ import annotations
import json
import logging
import os
import re
from typing import Optional
import pandas as pd
from .visualizer import _build_box, _build_donut, _build_freq_bar, _build_grouped_bar, _build_heatmap, _build_histogram, _build_line, _build_ranked_bar, _build_scatter, _build_stacked_bar, _build_violin, _chart_has_signal
logger = logging.getLogger(__name__)
SUPPORTED_CHART_TYPES = frozenset({'scatter', 'histogram', 'ranked_bar', 'grouped_bar', 'bar', 'box', 'violin', 'donut', 'pie', 'line', 'heatmap', 'freq_bar', 'stacked_bar'})
_TYPE_DISPLAY = {'scatter': 'Scatter plot', 'histogram': 'Histogram', 'ranked_bar': 'Ranked bar chart', 'grouped_bar': 'Grouped bar chart', 'bar': 'Bar chart', 'box': 'Box plot', 'violin': 'Violin plot', 'donut': 'Donut chart', 'pie': 'Pie chart', 'line': 'Line chart', 'heatmap': 'Heatmap', 'freq_bar': 'Frequency bar chart', 'stacked_bar': 'Stacked bar chart'}
_KEYWORD_TO_CHART_TYPES = {'pie': ['donut', 'pie'], 'donut': ['donut', 'pie'], 'scatter': ['scatter'], 'histogram': ['histogram'], 'distribution': ['histogram', 'box', 'violin'], 'bar': ['ranked_bar', 'grouped_bar', 'freq_bar', 'bar'], 'ranked': ['ranked_bar'], 'grouped': ['grouped_bar'], 'heatmap': ['heatmap'], 'correlation': ['heatmap', 'scatter'], 'box': ['box'], 'violin': ['violin'], 'line': ['line'], 'trend': ['line'], 'time': ['line'], 'frequency': ['freq_bar'], 'stacked': ['stacked_bar']}

def _records_to_df(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    for col in df.columns:
        try:
            converted = pd.to_numeric(df[col], errors='coerce')
            if converted.notna().sum() / max(len(df), 1) >= 0.6:
                df[col] = converted
        except Exception:
            pass
    for col in df.select_dtypes(include=['object']).columns:
        try:
            parsed = pd.to_datetime(df[col], format='mixed', errors='coerce')
        except (ValueError, TypeError):
            try:
                parsed = pd.to_datetime(df[col], infer_datetime_format=True, errors='coerce')
            except Exception:
                continue
        try:
            if parsed.notna().sum() / max(len(df), 1) >= 0.7:
                df[col] = parsed
        except Exception:
            pass
    return df

def _resolve_column(col: Optional[str], actual_cols: set[str]) -> Optional[str]:
    if not col:
        return None
    if col in actual_cols:
        return col
    norm = lambda s: s.lower().replace('_', '').replace(' ', '').replace('-', '')
    normed = norm(col)
    for c in actual_cols:
        if norm(c) == normed:
            return c
    for c in actual_cols:
        nc = norm(c)
        if normed in nc or nc in normed:
            return c
    return None

def _predict_chart_key(chart_type: str, x: Optional[str], y: Optional[str]) -> str:
    ct = chart_type.lower()
    if ct == 'scatter':
        return f'scatter_{x}_{y}'
    if ct == 'histogram':
        return f'histogram_{x or y}'
    if ct in ('ranked_bar', 'bar'):
        return f'ranked_bar_{x}_{y}'
    if ct == 'grouped_bar':
        return f'grouped_bar_{x}_{y}'
    if ct == 'box':
        return f'box_{y}_by_{x}'
    if ct == 'violin':
        return f'violin_{y}_by_{x}'
    if ct in ('donut', 'pie'):
        return f'donut_{x or y}'
    if ct == 'freq_bar':
        return f'freq_bar_{x or y}'
    if ct == 'line':
        return f'line_{x}'
    if ct == 'heatmap':
        return 'heatmap_correlation'
    if ct == 'stacked_bar':
        return f'stacked_{x}_{y}'
    return f'{ct}_{x}_{y}'

def _is_duplicate(candidate_key: str, existing_keys: list[str]) -> bool:
    _PREFIXES = ['scatter_', 'ranked_bar_', 'grouped_bar_', 'histogram_', 'box_', 'violin_', 'donut_', 'freq_bar_', 'line_', 'heatmap_', 'stacked_', 'freq_bar_loose_', 'gen_']

    def _parse(k: str) -> tuple[str, str]:
        k = k.lower()
        for prefix in _PREFIXES:
            if k.startswith(prefix):
                return (prefix.rstrip('_'), k[len(prefix):])
        return ('', k)
    (ctype_c, cols_c) = _parse(candidate_key)
    for ek in existing_keys:
        if candidate_key.lower() == ek.lower():
            return True
        (ctype_e, cols_e) = _parse(ek)
        if not ctype_c or ctype_c != ctype_e:
            continue
        if cols_c == cols_e:
            return True
        parts_c = {p for p in cols_c.split('_') if len(p) > 2}
        parts_e = {p for p in cols_e.split('_') if len(p) > 2}
        if parts_c and parts_e and (parts_c == parts_e):
            return True
    return False

def _detect_requested_chart_types(user_request: str) -> list[str]:
    q = user_request.lower()
    requested = []
    for (keyword, types) in _KEYWORD_TO_CHART_TYPES.items():
        if keyword in q:
            for t in types:
                if t not in requested:
                    requested.append(t)
    return requested

def _detect_mentioned_columns(user_request: str, df_columns: list[str]) -> list[str]:
    q_lower = user_request.lower()
    mentioned = []
    for col in df_columns:
        col_norm = col.lower().replace('_', ' ').replace('-', ' ')
        if col_norm in q_lower or col.lower() in q_lower:
            mentioned.append(col)
    return mentioned

def _build_all_candidates(df: pd.DataFrame, existing_chart_keys: list[str]) -> list[dict]:
    numeric = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    categorical = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c]) and (not pd.api.types.is_datetime64_any_dtype(df[c]))]
    datetime_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
    candidates = []

    def _add(chart_type, x, y, color=None, title='', agg='auto', log_scale=False):
        key = _predict_chart_key(chart_type, x, y)
        if not _is_duplicate(key, existing_chart_keys):
            candidates.append({'chart_type': chart_type, 'x': x, 'y': y, 'color': color, 'title': title, 'agg': agg, 'log_scale': log_scale, '_key': key})
    for (i, c1) in enumerate(numeric):
        for c2 in numeric[i + 1:]:
            _add('scatter', c1, c2, title=f'{c1} vs {c2}')
    for cat in categorical:
        for num in numeric:
            _add('ranked_bar', cat, num, title=f'Top {cat} by {num}')
    for cat in categorical:
        nu = df[cat].nunique(dropna=True)
        if 2 <= nu <= 20:
            for num in numeric:
                _add('grouped_bar', cat, num, title=f'Average {num} by {cat}')
    for c in numeric:
        _add('histogram', c, None, title=f'Distribution of {c}')
    for cat in categorical:
        nu = df[cat].nunique(dropna=True)
        if 2 <= nu <= 8:
            _add('donut', cat, None, title=f'Composition of {cat}')
    for cat in categorical:
        nu = df[cat].nunique(dropna=True)
        if 2 <= nu <= 15:
            for num in numeric:
                _add('box', cat, num, title=f'{num} distribution by {cat}')
    for cat in categorical:
        nu = df[cat].nunique(dropna=True)
        if 2 <= nu <= 6:
            for num in numeric:
                _add('violin', cat, num, title=f'{num} by {cat}')
    for cat in categorical:
        _add('freq_bar', cat, None, title=f'Frequency of {cat}')
    if len(numeric) >= 3:
        _add('heatmap', None, None, title='Correlation Heatmap')
    for dt in datetime_cols:
        for num in numeric[:3]:
            _add('line', dt, num, title=f'{num} over time')
    small_cats = [c for c in categorical if 2 <= df[c].nunique(dropna=True) <= 8]
    if len(small_cats) >= 2:
        _add('stacked_bar', small_cats[0], None, color=small_cats[1], title=f'{small_cats[0]} vs {small_cats[1]}')
    return candidates

def suggest_novel_chart(df_records: list[dict], existing_chart_keys: list[str], user_request: str='', stats_summary: dict=None) -> dict:
    df = _records_to_df(df_records)
    if df.empty:
        return {'cannot_plot': True, 'reason': 'No dataset preview available. Please re-upload your file.'}
    numeric = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    categorical = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c]) and (not pd.api.types.is_datetime64_any_dtype(df[c]))]
    requested_types = _detect_requested_chart_types(user_request)
    mentioned_cols = _detect_mentioned_columns(user_request, list(df.columns))
    logger.info('suggest_novel_chart: requested_types=%s, mentioned_cols=%s, existing=%d', requested_types, mentioned_cols, len(existing_chart_keys))
    all_candidates = _build_all_candidates(df, existing_chart_keys)
    if not all_candidates:
        n_num = len(numeric)
        n_cat = len(categorical)
        return {'cannot_plot': True, 'reason': f'All meaningful chart combinations for this dataset have already been shown. The dataset has {n_num} numeric and {n_cat} categorical column(s) — every useful visualization has been generated.'}
    if requested_types:
        for rtype in requested_types:
            type_candidates = [c for c in all_candidates if c['chart_type'] in (rtype, 'donut' if rtype == 'pie' else rtype, 'pie' if rtype == 'donut' else rtype)]
            if mentioned_cols and type_candidates:
                col_matched = [c for c in type_candidates if c.get('x') in mentioned_cols or c.get('y') in mentioned_cols]
                if col_matched:
                    type_candidates = col_matched
            if type_candidates:
                spec = type_candidates[0]
                spec_clean = {k: v for (k, v) in spec.items() if k != '_key'}
                return {'cannot_plot': False, 'spec': spec_clean, 'reasoning': f'Showing a {_TYPE_DISPLAY.get(rtype, rtype)} as requested.'}
        type_names = [_TYPE_DISPLAY.get(t, t) for t in requested_types]
        type_str = ' or '.join(type_names)
        impossible_reasons = _explain_why_type_impossible(requested_types, df, numeric, categorical, existing_chart_keys)
        return {'cannot_plot': True, 'reason': impossible_reasons or f'All {type_str} combinations are already displayed on the dashboard.'}
    if mentioned_cols:
        col_matched = [c for c in all_candidates if c.get('x') in mentioned_cols or c.get('y') in mentioned_cols]
        if col_matched:
            spec = col_matched[0]
            spec_clean = {k: v for (k, v) in spec.items() if k != '_key'}
            x = spec.get('x') or ''
            y = spec.get('y') or ''
            ct = spec['chart_type']
            col_phrase = f'{y} by {x}' if x and y else x or y or 'the dataset'
            return {'cannot_plot': False, 'spec': spec_clean, 'reasoning': f'Showing {col_phrase} as a {_TYPE_DISPLAY.get(ct, ct)}.'}
    spec = all_candidates[0]
    spec_clean = {k: v for (k, v) in spec.items() if k != '_key'}
    ct = spec['chart_type']
    x = spec.get('x') or ''
    y = spec.get('y') or ''
    col_phrase = f'{y} by {x}' if x and y else x or y or 'the dataset'
    return {'cannot_plot': False, 'spec': spec_clean, 'reasoning': f"Here is a {_TYPE_DISPLAY.get(ct, ct)} of {col_phrase} that hasn't been shown yet."}

def _explain_why_type_impossible(requested_types: list[str], df: pd.DataFrame, numeric: list[str], categorical: list[str], existing_keys: list[str]) -> Optional[str]:
    for rtype in requested_types:
        if rtype in ('donut', 'pie'):
            low_card = [c for c in categorical if 2 <= df[c].nunique(dropna=True) <= 8]
            if not low_card:
                return f"A pie/donut chart requires a categorical column with 2-8 unique values. The categorical columns in this dataset have too many unique values ({', '.join((f'{c} ({df[c].nunique()})' for c in categorical[:3]))})."
        elif rtype == 'scatter':
            if len(numeric) < 2:
                return f"A scatter plot requires at least 2 numeric columns. This dataset only has {len(numeric)}: {', '.join(numeric)}."
        elif rtype == 'heatmap':
            if len(numeric) < 3:
                return f"A heatmap requires at least 3 numeric columns. This dataset only has {len(numeric)}: {', '.join(numeric)}."
        elif rtype == 'line':
            datetime_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
            if not datetime_cols:
                return 'A line chart requires a date/time column, but none was detected in this dataset.'
        elif rtype in ('box', 'violin'):
            if not categorical:
                return f'A {rtype} plot requires a categorical column to group by, but none exists in this dataset.'
        elif rtype in ('ranked_bar', 'grouped_bar', 'bar'):
            if not categorical or not numeric:
                return f'A bar chart requires both a categorical and a numeric column.'
    return None

def generate_on_demand_chart(spec: dict, df_records: list[dict], existing_chart_keys: Optional[list[str]]=None) -> dict:
    existing_chart_keys = existing_chart_keys or []
    chart_type = (spec.get('chart_type') or '').lower().strip()
    x = spec.get('x') or spec.get('x_col')
    y = spec.get('y') or spec.get('y_col')
    color = spec.get('color') or spec.get('color_col')
    title = spec.get('title') or ''
    agg = spec.get('agg') or 'auto'
    log_scale = bool(spec.get('log_scale', False))
    if chart_type not in SUPPORTED_CHART_TYPES:
        return _err('gen_error', f"'{chart_type}' is not a supported chart type. Supported: {', '.join(sorted(SUPPORTED_CHART_TYPES))}.")
    df = _records_to_df(df_records)
    if df.empty:
        return _err('gen_error', 'No data preview available. Please re-upload your dataset.')
    actual_cols = set(df.columns.tolist())
    x = _resolve_column(x, actual_cols)
    y = _resolve_column(y, actual_cols)
    color = _resolve_column(color, actual_cols)
    candidate_key = _predict_chart_key(chart_type, x, y)
    if existing_chart_keys and _is_duplicate(candidate_key, existing_chart_keys):
        logger.info('Duplicate chart detected: %s', candidate_key)
        return {'id': 'duplicate', 'fig': None, 'error': None, 'is_duplicate': True}
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    cat_cols = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]
    chart = None
    if chart_type == 'scatter':
        if not x or not y:
            return _err('gen_scatter', 'Scatter plot requires both an X and Y numeric column.')
        chart = _build_scatter(df, x, y, color_col=color, title=title)
    elif chart_type == 'histogram':
        col = x or y
        if not col:
            return _err('gen_histogram', 'Histogram requires a numeric column.')
        chart = _build_histogram(df, col, log_scale=log_scale, title=title)
    elif chart_type in ('ranked_bar', 'bar'):
        if x and y:
            chart = _build_ranked_bar(df, x, y, title, agg=agg, color_col=color)
            if chart is None:
                chart = _build_ranked_bar(df, y, x, title, agg=agg, color_col=color)
        elif x:
            chart = _build_freq_bar(df, x, title=title)
        elif y:
            chart = _build_freq_bar(df, y, title=title)
        else:
            return _err('gen_bar', 'Bar chart requires at least one column.')
    elif chart_type == 'grouped_bar':
        if not x or not y:
            return _err('gen_grouped_bar', 'Grouped bar requires x (category) and y (numeric) columns.')
        chart = _build_grouped_bar(df, x, y, title, agg=agg, color_col=color)
        if chart is None:
            chart = _build_grouped_bar(df, y, x, None, agg=agg, color_col=color)
    elif chart_type == 'box':
        if not x or not y:
            return _err('gen_box', 'Box plot requires x (category) and y (numeric).')
        chart = _build_box(df, x, y, title=title)
        if chart is None:
            chart = _build_box(df, y, x, title=None)
    elif chart_type == 'violin':
        if not x or not y:
            return _err('gen_violin', 'Violin plot requires x (category, 2–6 groups) and y (numeric).')
        chart = _build_violin(df, x, y, title=title)
        if chart is None:
            chart = _build_violin(df, y, x, title=None)
    elif chart_type in ('donut', 'pie'):
        col = x or y
        if not col:
            return _err('gen_donut', 'Donut/pie chart requires a categorical column.')
        chart = _build_donut(df, col, title=title)
    elif chart_type == 'line':
        if not x:
            return _err('gen_line', 'Line chart requires a datetime/sequential X column.')
        value_cols = [c for c in ([y] if y else []) if c in df.columns] or num_cols[:4]
        chart = _build_line(df, x, value_cols, title=title)
    elif chart_type == 'heatmap':
        chart = _build_heatmap(df, num_cols, title=title or 'Correlation Heatmap')
        if chart is None:
            return _err('gen_heatmap', f"Heatmap needs 3+ numeric columns. This dataset has {len(num_cols)}: {', '.join(num_cols)}.")
    elif chart_type == 'stacked_bar':
        if not x or not color:
            return _err('gen_stacked', 'Stacked bar requires x column and color (grouping) column.')
        chart = _build_stacked_bar(df, x, color, num_col=y, title=title)
    elif chart_type == 'freq_bar':
        col = x or y
        if not col:
            return _err('gen_freq_bar', 'Frequency bar requires a categorical column.')
        chart = _build_freq_bar(df, col, title=title)
    if chart is None or not _chart_has_signal(chart):
        logger.warning('Primary chart build failed (type=%s x=%s y=%s), trying fallbacks', chart_type, x, y)
        fallback_chart = None
        if len(num_cols) >= 3:
            fallback_chart = _build_heatmap(df, num_cols, title='Correlation Heatmap')
        if fallback_chart is None and num_cols:
            fallback_chart = _build_histogram(df, num_cols[0], title=f'Distribution of {num_cols[0]}')
        if fallback_chart is None and cat_cols:
            fallback_chart = _build_freq_bar(df, cat_cols[0], title=f'Frequency of {cat_cols[0]}')
        if fallback_chart is None and cat_cols and num_cols:
            fallback_chart = _build_ranked_bar(df, cat_cols[0], num_cols[0], title=f'Top {cat_cols[0]} by {num_cols[0]}')
        if fallback_chart is not None and _chart_has_signal(fallback_chart):
            chart = fallback_chart
            logger.info('Fallback chart selected: key=%s', chart.key)
    if chart is None or not _chart_has_signal(chart):
        n_num = len(num_cols)
        n_cat = len(cat_cols)
        type_label = _TYPE_DISPLAY.get(chart_type, chart_type)
        col_info = ' and '.join((f"'{c}' ({str(df[c].dtype)})" for c in [x, y] if c and c in df.columns)) or 'the provided columns'
        return _err(f'gen_{chart_type}', f'Could not build a {type_label} with {col_info}. Dataset has {n_num} numeric and {n_cat} categorical columns. Check that the column types match the chart requirements.')
    try:
        fig_dict = json.loads(chart.fig.to_json())
    except Exception as exc:
        logger.error("Serialization failed for '%s': %s", chart.key, exc)
        return _err(chart.key, 'Chart built but failed to serialize.')
    logger.info('Generated on-demand chart: key=%s type=%s', chart.key, chart_type)
    return {'id': chart.key, 'fig': fig_dict, 'error': None, 'is_duplicate': False}

def _err(chart_id: str, message: str) -> dict:
    return {'id': chart_id, 'fig': None, 'error': message, 'is_duplicate': False}