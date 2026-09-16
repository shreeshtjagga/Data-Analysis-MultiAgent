
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

                parsed = pd.to_datetime(df[col], errors='coerce')

            except Exception:

                continue

        try:

            if parsed.notna().sum() / max(len(df), 1) >= 0.7:

                df[col] = parsed

        except Exception:

            pass

    return df

def _normalize_col_tokens(col_name: str) -> list[str]:

    s = re.sub(r'([a-z0-9])([A-Z])', r'\1 \2', col_name)

    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1 \2', s)

    s = s.lower().replace('_', ' ').replace('-', ' ')

    return [w for w in s.split() if w]

def _resolve_column(col: Optional[str], actual_cols: set[str]) -> Optional[str]:

    if not col:

        return None

    if col in actual_cols:

        return col

    norm = lambda s: re.sub(r'[^a-z0-9]', '', s.lower())

    normed = norm(col)

    for c in actual_cols:

        if norm(c) == normed:

            return c

    col_words = ' '.join(_normalize_col_tokens(col))

    for c in actual_cols:

        c_words = ' '.join(_normalize_col_tokens(c))

        if col_words == c_words or col_words in c_words or c_words in col_words:

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

    q_lower = user_request.lower().replace('_', ' ').replace('-', ' ')

    q_tokens = set(re.sub(r'[^a-z0-9\s]', ' ', q_lower).split())

    q_collapsed = re.sub(r'[^a-z0-9]', '', q_lower)

    mentioned = []

    for col in df_columns:

        norm_words = _normalize_col_tokens(col)

        norm_phrase = ' '.join(norm_words)

        col_collapsed = re.sub(r'[^a-z0-9]', '', col.lower())

        if norm_phrase and norm_phrase in q_lower:

            mentioned.append(col)

        elif col_collapsed and col_collapsed in q_collapsed:

            mentioned.append(col)

        elif norm_words and set(norm_words).issubset(q_tokens):

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

def _generate_grounded_reasoning(spec: dict, df: pd.DataFrame, is_requested: bool) -> str:

    ct = spec.get('chart_type')

    x = spec.get('x')

    y = spec.get('y')

    type_display = _TYPE_DISPLAY.get(ct, ct)

    lines = []

    try:

        if ct in ('ranked_bar', 'bar', 'grouped_bar', 'box', 'violin') and x and y:

            if pd.api.types.is_numeric_dtype(df[y]) and not pd.api.types.is_numeric_dtype(df[x]):

                grouped = df.groupby(x)[y].mean().sort_values(ascending=False)

                if not grouped.empty:

                    top_cat = grouped.index[0]

                    top_val = grouped.iloc[0]

                    bot_cat = grouped.index[-1]

                    bot_val = grouped.iloc[-1]

                    lines.append(f"**{top_cat}** leads with the highest average {y} at **{top_val:,.2f}**, while **{bot_cat}** has the lowest at **{bot_val:,.2f}**.")

                    overall_avg = df[y].mean()

                    lines.append(f"The overall average {y} across all categories is **{overall_avg:,.2f}**.")

                    spread_pct = ((top_val - bot_val) / max(bot_val, 1)) * 100

                    if spread_pct > 10:

                        lines.append(f"There is a **{spread_pct:.0f}%** performance gap between the top and bottom category — a key area for strategic focus.")

        elif ct == 'scatter' and x and y:

            corr = df[x].corr(df[y]) if pd.api.types.is_numeric_dtype(df.get(x, pd.Series())) and pd.api.types.is_numeric_dtype(df.get(y, pd.Series())) else float('nan')

            if pd.notna(corr):

                direction = "positive" if corr > 0 else "negative"

                strength = "strong" if abs(corr) > 0.5 else "moderate" if abs(corr) > 0.3 else "weak"

                lines.append(f"There is a **{strength} {direction} correlation** (r = {corr:.2f}) between **{x}** and **{y}**.")

                if abs(corr) > 0.5:

                    lines.append(f"Higher {x} values are generally associated with {'higher' if corr > 0 else 'lower'} {y} values.")

        elif ct == 'histogram' and (x or y):

            col = x or y

            if pd.api.types.is_numeric_dtype(df[col]):

                mean_val = df[col].mean()

                min_val = df[col].min()

                max_val = df[col].max()

                lines.append(f"**{col}** ranges from **{min_val:,.2f}** to **{max_val:,.2f}**, with an average of **{mean_val:,.2f}**.")

                skew = df[col].skew()

                skew_desc = "right-skewed (most values are lower, with some high outliers)" if skew > 0.5 else "left-skewed (most values are higher, with some low outliers)" if skew < -0.5 else "roughly symmetrically distributed"

                lines.append(f"The distribution is **{skew_desc}**.")

        elif ct in ('donut', 'pie', 'freq_bar') and (x or y):

            col = x or y

            counts = df[col].value_counts()

            if not counts.empty:

                top_cat = counts.index[0]

                pct = (counts.iloc[0] / len(df)) * 100

                lines.append(f"**'{top_cat}'** is the most common value in **{col}**, representing **{pct:.1f}%** of all records.")

                if len(counts) >= 2:

                    second_cat = counts.index[1]

                    second_pct = (counts.iloc[1] / len(df)) * 100

                    lines.append(f"The second largest segment is **'{second_cat}'** at **{second_pct:.1f}%**.")

                n_cats = df[col].nunique()

                lines.append(f"In total, **{n_cats}** unique categories are present in **{col}**.")

        elif ct == 'line' and x and y:

            if pd.api.types.is_numeric_dtype(df.get(y, pd.Series())):

                first_val = df[y].dropna().iloc[0] if not df[y].dropna().empty else None

                last_val = df[y].dropna().iloc[-1] if not df[y].dropna().empty else None

                if first_val is not None and last_val is not None:

                    change = last_val - first_val

                    trend = "upward" if change > 0 else "downward"

                    lines.append(f"**{y}** shows an overall **{trend} trend**, moving from **{first_val:,.2f}** to **{last_val:,.2f}** over the recorded time period.")

        elif ct == 'stacked_bar' and x:

            lines.append(f"This stacked bar chart shows how **{x}** is distributed across different segments.")

    except Exception as e:

        logger.debug("Failed to generate grounded reasoning: %s", e)

    col_phrase = f"**{y}** by **{x}**" if x and y else f"**{x or y}**" if (x or y) else "the dataset"

    intro = f"Here is a {type_display} of {col_phrase}."

    all_lines = [intro] + lines if lines else [intro]

    return " ".join(all_lines)

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

                reasoning = _generate_grounded_reasoning(spec, df, is_requested=True)

                return {'cannot_plot': False, 'spec': spec_clean, 'reasoning': reasoning}

        type_names = [_TYPE_DISPLAY.get(t, t) for t in requested_types]

        type_str = ' or '.join(type_names)

        impossible_reasons = _explain_why_type_impossible(requested_types, df, numeric, categorical, existing_chart_keys)

        return {'cannot_plot': True, 'reason': impossible_reasons or f'All {type_str} combinations are already displayed on the dashboard.'}

    if mentioned_cols:

        col_matched = [c for c in all_candidates if c.get('x') in mentioned_cols or c.get('y') in mentioned_cols]

        if col_matched:

            spec = col_matched[0]

            spec_clean = {k: v for (k, v) in spec.items() if k != '_key'}

            reasoning = _generate_grounded_reasoning(spec, df, is_requested=True)

            return {'cannot_plot': False, 'spec': spec_clean, 'reasoning': reasoning}

    _generic_words = {'generate', 'create', 'make', 'build', 'draw', 'show', 'give', 'need', 'want', 'can', 'you', 'i', 'chart', 'plot', 'graph', 'a', 'me', 'new', 'another', 'different', 'some', 'any', 'one', 'please', 'of', 'for'}

    meaningful_words = [w for w in user_request.lower().split() if w not in _generic_words]

    if len(meaningful_words) > 1 and not requested_types and not mentioned_cols:

        return {'cannot_plot': True, 'reason': f"I couldn't find columns matching '{' '.join(meaningful_words)}' in this dataset. Please use exact column names from the data."}

    spec = all_candidates[0]

    spec_clean = {k: v for (k, v) in spec.items() if k != '_key'}

    reasoning = _generate_grounded_reasoning(spec, df, is_requested=False)

    return {'cannot_plot': False, 'spec': spec_clean, 'reasoning': reasoning}

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

            cat_c = x if not pd.api.types.is_numeric_dtype(df[x]) else y

            num_c = y if pd.api.types.is_numeric_dtype(df[y]) else x

            chart = _build_ranked_bar(df, cat_c, num_c, title=title, agg=agg, color_col=color)

        elif x:

            chart = _build_freq_bar(df, x, title=title)

        elif y:

            chart = _build_freq_bar(df, y, title=title)

        else:

            return _err('gen_bar', 'Bar chart requires at least one column.')

    elif chart_type == 'grouped_bar':

        if not x or not y:

            return _err('gen_grouped_bar', 'Grouped bar requires x (category) and y (numeric) columns.')

        cat_c = x if not pd.api.types.is_numeric_dtype(df[x]) else y

        num_c = y if pd.api.types.is_numeric_dtype(df[y]) else x

        chart = _build_grouped_bar(df, cat_c, num_c, title=title, agg=agg, color_col=color)

    elif chart_type == 'box':

        if not x or not y:

            return _err('gen_box', 'Box plot requires x (category) and y (numeric).')

        cat_c = x if not pd.api.types.is_numeric_dtype(df[x]) else y

        num_c = y if pd.api.types.is_numeric_dtype(df[y]) else x

        chart = _build_box(df, cat_c, num_c, title=title)

    elif chart_type == 'violin':

        if not x or not y:

            return _err('gen_violin', 'Violin plot requires x (category, 2–6 groups) and y (numeric).')

        cat_c = x if not pd.api.types.is_numeric_dtype(df[x]) else y

        num_c = y if pd.api.types.is_numeric_dtype(df[y]) else x

        chart = _build_violin(df, cat_c, num_c, title=title)

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

        # 1. If user provided a categorical column, try frequency bar
        if x and x in cat_cols:
            fallback_chart = _build_freq_bar(df, x, title=f'Distribution of {x}')
        elif y and y in cat_cols:
            fallback_chart = _build_freq_bar(df, y, title=f'Distribution of {y}')

        # 2. If both categorical and numeric columns exist, build ranked bar
        if fallback_chart is None and cat_cols and num_cols:
            c_col = x if x in cat_cols else cat_cols[0]
            n_col = y if y in num_cols else num_cols[0]
            fallback_chart = _build_ranked_bar(df, c_col, n_col, title=f'{n_col} by {c_col}')

        # 3. Frequency bar on top category
        if fallback_chart is None and cat_cols:
            fallback_chart = _build_freq_bar(df, cat_cols[0], title=f'Frequency of {cat_cols[0]}')

        # 4. Histogram on numeric column
        if fallback_chart is None and num_cols:
            n_col = x if x in num_cols else (y if y in num_cols else num_cols[0])
            fallback_chart = _build_histogram(df, n_col, title=f'Distribution of {n_col}')

        # 5. Heatmap only if 3+ numeric columns and no other chart worked
        if fallback_chart is None and len(num_cols) >= 3:
            fallback_chart = _build_heatmap(df, num_cols, title='Correlation Heatmap')

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

async def plan_and_generate_custom_chart(

    user_request: str,

    df_records: list[dict],

    existing_chart_keys: Optional[list[str]] = None,

    stats_summary: Optional[dict] = None

) -> dict:

    

    existing_chart_keys = existing_chart_keys or []

    df = _records_to_df(df_records)

    if df.empty:

        return {

            'cannot_plot': True,

            'reason': 'No dataset available to generate plots. Please upload a dataset first.',

            'chart_result': None,

            'explanation': ''

        }

    actual_cols = list(df.columns)

    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

    datetime_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]

    categorical_cols = [c for c in df.columns if c not in numeric_cols and c not in datetime_cols]

    col_meta = {}

    for c in actual_cols:

        if c in numeric_cols:

            col_meta[c] = {'type': 'numeric', 'min': float(df[c].min()) if df[c].notna().any() else 0, 'max': float(df[c].max()) if df[c].notna().any() else 0}

        elif c in datetime_cols:

            col_meta[c] = {'type': 'datetime'}

        else:

            col_meta[c] = {'type': 'categorical', 'nunique': int(df[c].nunique()), 'top_values': list(df[c].value_counts().head(4).index.astype(str))}

    llm_plan = None

    try:

        from ..core.llm_client import call_groq_with_fallback

        prompt = (

            f"You are an expert AI data visualization planner.\n"

            f"User Request: \"{user_request}\"\n\n"

            f"Dataset Columns and Meta:\n{json.dumps(col_meta, indent=2)}\n\n"

            f"Rules:\n"

            f"1. Identify the requested columns (x, y, color) and any row filter conditions.\n"

            f"2. If the user mentions column names that do NOT exist in the dataset, set can_generate=false and explain which columns are available.\n"

            f"3. If the user requested an incompatible chart type (e.g. scatter on 2 text columns, or pie chart on 100+ categories), set can_generate=false, explain why, and suggest a valid alternative.\n"

            f"4. If no chart type was explicitly requested, pick the BEST chart type according to the data types:\n"

            f"   - Categorical + Numeric -> ranked_bar, grouped_bar, or box\n"

            f"   - Datetime + Numeric -> line\n"

            f"   - Numeric + Numeric -> scatter\n"

            f"   - 1 Numeric -> histogram\n"

            f"   - 1 Categorical (2-8 values) -> donut\n"

            f"   - 1 Categorical (>8 values) -> freq_bar or ranked_bar\n"

            f"   - 2 Categoricals -> stacked_bar\n"

            f"   - 3+ Numerics -> heatmap\n"

            f"5. Output JSON ONLY in this format:\n"

            f"{{\n"

            f"  \"can_generate\": true,\n"

            f"  \"reason\": \"\",\n"

            f"  \"chart_type\": \"scatter|ranked_bar|grouped_bar|line|histogram|box|violin|donut|heatmap|stacked_bar|freq_bar\",\n"

            f"  \"x\": \"exact_column_name\",\n"

            f"  \"y\": \"exact_column_name or null\",\n"

            f"  \"color\": \"exact_column_name or null\",\n"

            f"  \"filter_col\": \"column_to_filter or null\",\n"

            f"  \"filter_op\": \"eq|gt|lt|contains or null\",\n"

            f"  \"filter_val\": \"value or null\",\n"

            f"  \"explanation\": \"Brief friendly sentence explaining the chart\"\n"

            f"}}"

        )

        raw_llm = await call_groq_with_fallback(

            messages=[{'role': 'user', 'content': prompt}],

            primary_model=os.getenv('GROQ_MODEL', 'qwen/qwen3.8-27b'),

            temperature=0,

            max_tokens=600

        )

        if '{' in raw_llm and '}' in raw_llm:

            json_str = raw_llm[raw_llm.find('{'):raw_llm.rfind('}')+1]

            llm_plan = json.loads(json_str)

            logger.info("LLM custom chart plan: %s", llm_plan)

    except Exception as llm_err:

        logger.warning("LLM chart planning unavailable/failed: %s (using deterministic rule planner)", llm_err)

    if llm_plan and isinstance(llm_plan, dict):

        if not llm_plan.get('can_generate', True):

            reason = llm_plan.get('reason') or f"Could not generate plot for '{user_request}'. Available columns: {', '.join(actual_cols)}."

            return {'cannot_plot': True, 'reason': reason, 'chart_result': None, 'explanation': ''}

        ct = llm_plan.get('chart_type')

        x_raw = llm_plan.get('x')

        y_raw = llm_plan.get('y')

        color_raw = llm_plan.get('color')

        actual_set = set(actual_cols)

        x_resolved = _resolve_column(x_raw, actual_set) if x_raw else None

        y_resolved = _resolve_column(y_raw, actual_set) if y_raw else None

        color_resolved = _resolve_column(color_raw, actual_set) if color_raw else None

        target_df = df.copy()

        filter_col = _resolve_column(llm_plan.get('filter_col'), actual_set)

        filter_op = llm_plan.get('filter_op')

        filter_val = llm_plan.get('filter_val')

        if filter_col and filter_val is not None:

            try:

                if filter_op == 'eq':

                    target_df = target_df[target_df[filter_col].astype(str).str.lower() == str(filter_val).lower()]

                elif filter_op == 'gt':

                    target_df = target_df[pd.to_numeric(target_df[filter_col], errors='coerce') > float(filter_val)]

                elif filter_op == 'lt':

                    target_df = target_df[pd.to_numeric(target_df[filter_col], errors='coerce') < float(filter_val)]

                elif filter_op == 'contains':

                    target_df = target_df[target_df[filter_col].astype(str).str.lower().str.contains(str(filter_val).lower(), na=False)]

            except Exception as fe:

                logger.warning("Filter application error: %s", fe)

        if target_df.empty:

            return {

                'cannot_plot': True,

                'reason': f"No data rows matched the filter condition ({filter_col} {filter_op or '='} '{filter_val}').",

                'chart_result': None,

                'explanation': ''

            }

        spec = {

            'chart_type': ct,

            'x': x_resolved,

            'y': y_resolved,

            'color': color_resolved,

            'title': f"{ct.replace('_', ' ').title()}: {y_resolved or ''} {'by ' + str(x_resolved) if x_resolved else ''}".strip()

        }

        res = generate_on_demand_chart(spec, target_df.to_dict('records'), existing_chart_keys)

        if not res.get('error') and not res.get('is_duplicate'):

            expl = llm_plan.get('explanation') or _generate_grounded_reasoning(spec, target_df, is_requested=True)

            return {'cannot_plot': False, 'reason': None, 'chart_result': res, 'explanation': expl}

    q_lower = user_request.lower()

    mentioned = []

    for col in actual_cols:

        col_clean = col.lower().replace('_', ' ')

        if col_clean in q_lower or col.lower() in q_lower:

            mentioned.append(col)

    _IGNORE = {'generate', 'create', 'plot', 'chart', 'graph', 'show', 'make', 'draw', 'display', 'versus', 'against', 'where', 'with', 'from', 'each', 'every', 'data', 'value', 'values', 'column', 'columns', 'and', 'the', 'for', 'you', 'can', 'please'}

    words = [w.strip('?,.:;"\'') for w in q_lower.split() if len(w) > 2 and w not in _IGNORE]

    req_types = _detect_requested_chart_types(user_request)

    if len(mentioned) >= 2:

        col1, col2 = mentioned[0], mentioned[1]

        c1_is_num = col1 in numeric_cols

        c2_is_num = col2 in numeric_cols

        c1_is_dt = col1 in datetime_cols

        c2_is_dt = col2 in datetime_cols

        if 'scatter' in req_types:

            if not (c1_is_num and c2_is_num):

                non_num = col1 if not c1_is_num else col2

                return {

                    'cannot_plot': True,

                    'reason': f"A scatter plot requires numeric values for both axes, but '{non_num}' is categorical (text). Would you like a ranked bar chart or box plot of {col1} vs {col2} instead?",

                    'chart_result': None,

                    'explanation': ''

                }

            spec = {'chart_type': 'scatter', 'x': col1, 'y': col2, 'title': f'{col1} vs {col2}'}

            res = generate_on_demand_chart(spec, df_records, existing_chart_keys)

            expl = _generate_grounded_reasoning(spec, df, is_requested=True)

            return {'cannot_plot': False, 'reason': None, 'chart_result': res, 'explanation': expl}

        if c1_is_dt or c2_is_dt:

            dt_col = col1 if c1_is_dt else col2

            val_col = col2 if c1_is_dt else col1

            spec = {'chart_type': 'line', 'x': dt_col, 'y': val_col, 'title': f'{val_col} over {dt_col}'}

        elif c1_is_num and c2_is_num:

            spec = {'chart_type': 'scatter', 'x': col1, 'y': col2, 'title': f'{col1} vs {col2}'}

        elif (c1_is_num and not c2_is_num) or (c2_is_num and not c1_is_num):

            cat_col = col2 if c1_is_num else col1

            num_col = col1 if c1_is_num else col2

            nu = df[cat_col].nunique()

            ct = 'grouped_bar' if 2 <= nu <= 15 else 'ranked_bar'

            spec = {'chart_type': ct, 'x': cat_col, 'y': num_col, 'title': f'{num_col} by {cat_col}'}

        else:

            spec = {'chart_type': 'stacked_bar', 'x': col1, 'color': col2, 'title': f'{col1} segmented by {col2}'}

        res = generate_on_demand_chart(spec, df_records, existing_chart_keys)

        expl = _generate_grounded_reasoning(spec, df, is_requested=True)

        return {'cannot_plot': False, 'reason': None, 'chart_result': res, 'explanation': expl}

    elif len(mentioned) == 1:

        col = mentioned[0]

        if col in numeric_cols:

            spec = {'chart_type': 'histogram', 'x': col, 'title': f'Distribution of {col}'}

        else:

            nu = df[col].nunique()

            ct = 'donut' if 2 <= nu <= 8 else 'freq_bar'

            spec = {'chart_type': ct, 'x': col, 'title': f'Frequency of {col}'}

        res = generate_on_demand_chart(spec, df_records, existing_chart_keys)

        expl = _generate_grounded_reasoning(spec, df, is_requested=True)

        return {'cannot_plot': False, 'reason': None, 'chart_result': res, 'explanation': expl}

    if words and not mentioned:

        return {

            'cannot_plot': True,

            'reason': f"Could not find columns matching '{' '.join(words[:3])}' in the dataset. Available columns are: {', '.join(actual_cols)}.",

            'chart_result': None,

            'explanation': ''

        }

    novel = suggest_novel_chart(df_records, existing_chart_keys, user_request, stats_summary)

    if novel.get('cannot_plot'):

        return {'cannot_plot': True, 'reason': novel.get('reason', 'Could not generate plot.'), 'chart_result': None, 'explanation': ''}

    res = generate_on_demand_chart(novel['spec'], df_records, existing_chart_keys)

    return {'cannot_plot': False, 'reason': None, 'chart_result': res, 'explanation': novel.get('reasoning', '')}

def _err(chart_id: str, message: str) -> dict:

    return {'id': chart_id, 'fig': None, 'error': message, 'is_duplicate': False}

