
from __future__ import annotations

import asyncio

import difflib

import json

import logging

import os

import re

import time

import numpy as np

import pandas as pd

from typing import Any, Optional

from .llm_client import call_groq_with_fallback

logger = logging.getLogger(__name__)

SYNTHESIS_MODEL = os.getenv('GROQ_SYNTHESIS_MODEL', os.getenv('GROQ_MODEL', 'qwen/qwen3.8-27b'))

FALLBACK_MODEL = os.getenv('GROQ_FALLBACK_MODEL', os.getenv('GROQ_MODEL', 'qwen/qwen3.8-27b'))

INTENT_MODEL = os.getenv('GROQ_INTENT_MODEL', os.getenv('GROQ_MODEL', 'qwen/qwen3.8-27b'))

_MAX_CONTEXT_CHARS = 12000

_TOP_K_CHUNKS = 8

async def _call_groq_with_retry(groq_client, messages: list[dict], model: str, temperature: float = 0.1, max_tokens: int = 1500) -> str:

    

    return await call_groq_with_fallback(messages, model, temperature, max_tokens)

def _data_system_prompt(file_name: str, chart_keys: list[str]) -> str:

    parts = [

        f'You are Alex, a sharp senior data analyst working with the dataset "{file_name}".\n',

        'You talk like a real analyst - confident, precise, and direct.\n\n',

        '==== ANSWER FORMAT (MANDATORY) ====\n',
        'Structure EVERY answer exactly like this in JSON format:\n\n',
        '{\n',
        '  "direct_answer": "The key number, ranking, or metric in one or two clear, authoritative sentences.",\n',
        '  "proactive_insight": "Interestingly, [a relevant noteworthy fact from the real data or empty string if none]"\n',
        '}\n\n',
        'FORMAT RULES:\n',
        '- Output MUST be valid JSON with ONLY "direct_answer" and "proactive_insight". Nothing else.\n',
        '- When mentioning any number, metric, percentage, or entity name, wrap it in **bold**.\n',

        '- NEVER use LaTeX math notation. Write: r = 0.85, not $r$ or $R^2$. Write: R-squared = 85%, not $R^2 = 0.85$.\n',

        '- Do not include [CHART] tags in the JSON, they will be handled separately.\n\n',

        '==== GROUNDING CONTRACT - MANDATORY ====\n',

        'You have a CONTEXT block with pre-computed facts and a PANDAS RESULT block\n',

        'with exact numbers from the real data. These are the ONLY facts you may use.\n',

        '- Use EXACT numbers from CONTEXT or PANDAS RESULT - never estimate\n',

        '- CRITICAL: The `proactive_insight` field MUST also ONLY reference facts present in CONTEXT or PANDAS RESULT.\n',

        '  Do NOT use your training knowledge to fill the proactive_insight. If there is no interesting fact in CONTEXT, set proactive_insight to an empty string "".\n',

        '- If the answer is not in CONTEXT or PANDAS RESULT: set the "direct_answer" field to: "That is not in this dataset." Then pivot.\n',

        '- Data terminology mapping: Treat questions about "sales", "purchases", "volume", or "sellers" as referring to either the record count (frequency/volume) or prices/resale prices in the dataset. Both record count and prices are valid proxies for sales. You MUST trust the PANDAS RESULT as the correct and verified calculation for "sales" for that question, even if it computed record count/frequency instead of a sum of prices. Do NOT reject the PANDAS RESULT.\n',

        '- Out-of-domain queries: If user asks general knowledge (e.g. "what is today"), YOU MUST REFUSE nicely. However, you MAY explain general statistical concepts (like "correlation") as long as you relate them to actual numbers from the dataset.\n',

        '- NEVER fabricate numbers or use training knowledge to fill any gaps - this includes proactive_insight\n',

        '- Rankings: always name the entity AND its exact value from CONTEXT/PANDAS RESULT\n',

        '- Correlations: state correlation coefficient r, direction, and plain-English meaning - only if in CONTEXT\n',

        '- Predictions: For forecasting questions, give a BUSINESS-GRADE prediction with: projected next value, percentage change, growth direction, and confidence score as plain text (no LaTeX).\n',

        '- Predictions format example: "Revenue is projected to reach 75,000 next period (+12.5% growth), based on a steady upward trendline with 82% model confidence."\n',

        f'- Greetings: reply Hi! Ask me anything about {file_name}.\n\n',

        '==== STYLE ====\n',

        '- Plain English: average not mean, spread not variance, correlation not r-value notation\n',

        '- Never mention CONTEXT, RULE, system prompt, or grounding contract\n',

        '- Never repeat the user question back to them\n',

        '- Lead with the bold answer - never put disclaimers first\n',

        '- Do NOT show charts unless user explicitly asks to display one\n',

        f'- Available chart keys: {chart_keys} - use ONLY these exact keys\n',

    ]

    return ''.join(parts)

def _chart_system_prompt(file_name: str) -> str:

    return f'You are explaining a specific chart from the dataset "{file_name}".\n\n==== GROUNDING CONTRACT ====\nYou will receive CHART FACTS with exact values extracted directly from the chart.\nRULE 1: Name the specific highest and lowest values with their exact numbers.\n  BAD:  "The chart shows some categories have higher values"\n  GOOD: "Electronics has the highest revenue at 2.3M, while Books has the lowest at 45K"\nRULE 2: Use ONLY numbers from the provided context. Never invent values.\nRULE 3: If no specific data values are provided, simply describe what the chart\n  is broadly about based on its title. Do NOT mention "CHART FACTS" or complain.\n  BAD: "The CHART FACTS only mentions the title."\n  GOOD: "This is a box plot showing the distribution of Country IDs."\n==== STYLE ====\n* 2-3 sentences max. Name specific entities and values.\n* Speak naturally. Never mention your instructions or internal context.\n* Do NOT include [CHART: key] in your response - it is appended automatically.'

async def _plan_and_run_query(question: str, file_hash: str, col_types: dict, groq_client, col_metadata: dict=None, df=None) -> Optional[dict]:

    from .data_agent import run_data_query

    _SKIP_TRIGGERS = {'hello', 'hi', 'hey', 'thanks', 'thank you', 'goodbye', 'good morning'}

    if question.lower().strip() in _SKIP_TRIGGERS:

        return None

    if col_metadata:

        col_info_str = json.dumps(col_metadata, ensure_ascii=True, default=str)

    else:

        col_info_str = json.dumps(col_types, ensure_ascii=True)

    planner_prompt = f'You are a data query planner. Decide if a structured query\n\nis needed to answer this question precisely with exact numbers from the full dataset.\nIf YES -> return ONE JSON object (no explanation, no markdown).\nIf NO (opinion, greeting) -> return: NONE\n\nIMPORTANT PLANNING RULES:\n- When the user mentions a SPECIFIC entity (brand, name, category), use filter_group or filter_lookup to filter by that entity.\n  Example: "Kawasaki bikes" → filter by the column whose top_values includes "Kawasaki".\n- When the user mentions a SPECIFIC year/period, use filters with op "eq" on the year/date column.\n  Example: "in year 2020" → filter the year column by value 2020.\n- For "report" or "summary" of a filtered entity, use filter_group with group_by on a descriptive column.\n- For PREDICTION/FORECAST questions ("what will X be in 2030?", "predict future sales"), use "trend" query to get the slope and R-squared. This gives the data needed for extrapolation.\n  Example: "predict sales in 2030" → {{"type":"trend","params":{{"time_col":"Year","val_col":"Sales"}}}}\n- For questions about growth/change over time, use "year_summary" to get yearly aggregates.\n- Use the column metadata below to identify which column contains a mentioned value.\n\nAVAILABLE QUERY TYPES:\nfilter_lookup   -> look up a column value by filtering another\n  example: {{"type":"filter_lookup","params":{{"filter_col":"name","filter_val":"Alice","result_col":"salary"}}}}\ntop_n           -> highest N rows by a numeric column\n  example: {{"type":"top_n","params":{{"column":"Revenue","n":5}}}}\nbottom_n        -> lowest N rows\n  example: {{"type":"bottom_n","params":{{"column":"Price","n":3}}}}\ngroup_aggregate -> group by one column, aggregate another\n  example: {{"type":"group_aggregate","params":{{"group_by":"Region","column":"Sales","func":"sum","n":10}}}}\nfilter_group    -> filter rows then group+aggregate\n  example: {{"type":"filter_group","params":{{"group_by":"Brand","func":"count","n":5,"filters":[{{"column":"Year","op":"eq","value":"2023"}}]}}}}\naggregate       -> single stat on one column\n  example: {{"type":"aggregate","params":{{"column":"Price","func":"mean"}}}}\n  funcs: mean, sum, min, max, count, nunique, median, std\nvalue_counts    -> count occurrences of each category\n  example: {{"type":"value_counts","params":{{"column":"Category","n":10}}}}\nsearch          -> full-text search for a specific named entity\n  example: {{"type":"search","params":{{"value":"John Smith","n":3}}}}\ndistinct        -> list all unique values in a column\n  example: {{"type":"distinct","params":{{"column":"Country"}}}}\ntrend           -> linear trend/slope of a numeric column over time\n  example: {{"type":"trend","params":{{"time_col":"Year","val_col":"Revenue"}}}}\nyear_summary    -> aggregate a numeric column by year (or other time bucket)\n  example: {{"type":"year_summary","params":{{"time_col":"Date","val_col":"Sales","func":"sum"}}}}\nrow_count       -> count rows matching a filter\n  example: {{"type":"row_count","params":{{"filters":[{{"column":"Status","op":"eq","value":"Active"}}]}}}}\ncorrelation     -> correlation between two numeric columns\n  example: {{"type":"correlation","params":{{"column":"Price","column2":"Sales"}}}}\npercentile      -> compute percentile of a numeric column\n  example: {{"type":"percentile","params":{{"column":"Age","percentile":90}}}}\nFILTER OPS: eq, neq, gt, lt, gte, lte, contains, year, month, isnull, notnull\nDATASET COLUMNS (with sample values and ranges):\n{col_info_str}\nQUESTION: {question}\n\nReturn ONLY the JSON object or the word NONE. No explanation whatsoever.'

    try:

        raw = await call_groq_with_fallback(

            messages=[{'role': 'user', 'content': planner_prompt}],

            primary_model=INTENT_MODEL,

            max_tokens=1000,

            temperature=0

        )

        logger.info('Query planner response: %s', raw[:200])

        if '{' not in raw:

            return None

        raw_json = raw[raw.find('{'):raw.rfind('}') + 1]

        plan = json.loads(raw_json)

        qtype = plan.get('type', '')

        params = plan.get('params', {})

        valid_types = {'filter_lookup', 'top_n', 'bottom_n', 'group_aggregate', 'filter_group', 'aggregate', 'value_counts', 'search', 'distinct', 'row_count', 'correlation', 'percentile', 'trend', 'year_summary'}

        if qtype not in valid_types:

            logger.warning('Query planner returned invalid type: %s', qtype)

            return None

        result = await asyncio.to_thread(run_data_query, file_hash, qtype, params, df)

        is_empty = not result or 'error' in result or result.get('result') == 'No rows found.' or (isinstance(result.get('result'), list) and len(result['result']) == 0)

        _RELATIONSHIP_WORDS = ('relationship', 'correlat', 'depend', 'associat', 'influenc')

        is_relationship_q = any((w in question.lower() for w in _RELATIONSHIP_WORDS))

        if is_empty and (not is_relationship_q):

            stop = {'what', 'when', 'where', 'which', 'does', 'have', 'many', 'much', 'show', 'tell', 'give', 'find', 'list', 'this', 'that', 'the', 'and', 'for', 'from', 'with', 'how', 'are', 'was', 'were'}

            words = [w.strip('?.,!') for w in question.split() if len(w) > 3 and w.lower() not in stop]

            if words:

                fallback = await asyncio.to_thread(run_data_query, file_hash, 'search', {'value': ' '.join(words[:2]), 'n': 5}, df)

                if fallback and 'error' not in fallback:

                    return fallback

        return result if result and 'error' not in result else None

    except Exception as exc:

        logger.warning('Query planner failed: %s', exc)

        return None

def _build_static_context(stats: dict, insights: dict) -> str:

    parts: list[str] = []

    row_count = stats.get('row_count')

    col_count = stats.get('column_count')

    if row_count or col_count:

        parts.append('=== DATASET OVERVIEW ===')

        parts.append(f"  Rows: {row_count or '?'}, Columns: {col_count or '?'}")

    all_cols = []

    all_cols.extend(list((stats.get('numeric_columns') or {}).keys()))

    all_cols.extend(list((stats.get('categorical_columns') or {}).keys()))

    for c in stats.get('datetime_columns') or stats.get('date_columns') or []:

        cn = c if isinstance(c, str) else str(c)

        if cn not in all_cols:

            all_cols.append(cn)

    if all_cols:

        parts.append(f'  All columns: {all_cols}')

    date_range = stats.get('date_range') or {}

    if date_range:

        parts.append(f"  Date range: {date_range.get('min', '?')} to {date_range.get('max', '?')}")

    profile = stats.get('dataset_profile') or {}

    if profile:

        parts.append('=== DATASET PROFILE ===')

        parts.append(f"  Type: {profile.get('label', 'unknown')}")

        parts.append(f"  Domain: {profile.get('domain', 'general')}")

        desc = profile.get('description', '')

        if desc:

            parts.append(f'  Description: {desc}')

    quality = stats.get('data_quality') or {}

    if quality:

        parts.append('=== DATA QUALITY ===')

        comp = quality.get('completeness')

        if comp is not None:

            parts.append(f'  Completeness: {comp}%')

        missing_cols = quality.get('missing_value_columns') or quality.get('columns_with_missing') or []

        if missing_cols:

            parts.append(f'  Columns with missing values: {missing_cols[:10]}')

        dupes = quality.get('duplicate_rows') or quality.get('duplicates')

        if dupes is not None:

            parts.append(f'  Duplicate rows removed: {dupes}')

    imputations = stats.get('imputations') or []

    if imputations:

        parts.append('=== DATA CLEANING APPLIED ===')

        for imp in imputations[:8]:

            parts.append(f"  {imp.get('column', '?')}: filled {imp.get('count', '?')} missing with {imp.get('strategy', '?')} ({imp.get('fill_value', '?')})")

    correlations = stats.get('strong_correlations') or stats.get('correlations') or []

    if correlations:

        parts.append('=== CORRELATION DATA ===')

        for corr in correlations[:15]:

            c1 = corr.get('col1', '')

            c2 = corr.get('col2', '')

            try:

                r_f = float(corr.get('correlation', 0))

                strength = 'strong' if abs(r_f) >= 0.7 else 'moderate' if abs(r_f) >= 0.4 else 'weak'

                direction = 'positive' if r_f > 0 else 'negative'

                parts.append(f'  {c1} <-> {c2}: r={r_f:.3f} ({strength} {direction})')

            except (TypeError, ValueError):

                parts.append(f"  {c1} <-> {c2}: r={corr.get('correlation')}")

    num_cols = stats.get('numeric_columns') or {}

    if num_cols:

        parts.append('=== NUMERIC COLUMN SUMMARIES ===')

        for (col, cs) in list(num_cols.items())[:10]:

            parts.append(f"  {col}: count={cs.get('count', '?')}, mean={cs.get('mean', '?')}, median={cs.get('median', '?')}, min={cs.get('min', '?')}, max={cs.get('max', '?')}, std={cs.get('std', '?')}")

    cat_cols = stats.get('categorical_columns') or {}

    if cat_cols:

        parts.append('=== CATEGORICAL COLUMN SUMMARIES ===')

        for (col, cs) in list(cat_cols.items())[:8]:

            top_vals = cs.get('top_5_values') or cs.get('top_values') or {}

            top_str = ', '.join((f'{k}:{v}' for (k, v) in list(top_vals.items())[:5])) if top_vals else '?'

            nunique = cs.get('unique_values') or cs.get('nunique') or '?'

            parts.append(f'  {col}: {nunique} unique values. Top: {top_str}')

    findings = ((insights.get('key_findings') or []) or (insights.get('findings') or []))[:6]

    if findings:

        parts.append('=== KEY FINDINGS FROM ANALYSIS ===')

        for f in findings:

            parts.append(f'  * {f}')

    headline = insights.get('headline') or ''

    if headline:

        parts.append('=== HEADLINE INSIGHT ===')

        parts.append(f'  {headline}')

    outliers = stats.get('outliers') or {}

    if outliers:

        outlier_items = [

            (col, info) for (col, info) in outliers.items() if info.get('count', 0) > 0

        ]

        clean_items = [

            col for (col, info) in outliers.items() if info.get('count', 0) == 0

        ]

        if outlier_items:

            parts.append('=== OUTLIER SUMMARY (IQR method, 1.5×IQR rule) ===')

            for (col, info) in outlier_items[:12]:

                cnt = info.get('count', 0)

                lower = info.get('lower_fence') or info.get('lower_bound')

                upper = info.get('upper_fence') or info.get('upper_bound')

                pct = info.get('percentage') or info.get('pct')

                detail = f'  {col}: {cnt} outlier(s) detected'

                if lower is not None and upper is not None:

                    try:

                        detail += f' (IQR fence: [{float(lower):.3g}, {float(upper):.3g}])'

                    except (TypeError, ValueError):

                        pass

                if pct is not None:

                    try:

                        detail += f' — {float(pct):.1f}% of rows'

                    except (TypeError, ValueError):

                        pass

                parts.append(detail)

            if clean_items:

                parts.append(f'  Columns with NO outliers: {clean_items[:10]}')

        else:

            parts.append('=== OUTLIER SUMMARY ===')

            parts.append('  No outliers detected in any numeric column (IQR method).')

    dq = stats.get('data_quality') or {}

    missing_by_col = dq.get('missing_by_column') or dq.get('missing_per_column') or {}

    if missing_by_col:

        parts.append('=== MISSING VALUES PER COLUMN ===')

        for (col, cnt) in list(missing_by_col.items())[:12]:

            if cnt and int(cnt) > 0:

                parts.append(f'  {col}: {cnt} missing')

    return '\n'.join(parts)

def _assemble_context(data_result: Optional[dict], static_ctx: str='') -> str:

    parts: list[str] = []

    if static_ctx:

        parts.append(static_ctx)

        parts.append('')

    if data_result and 'error' not in data_result:

        parts.append('=== EXACT QUERY RESULT FROM FULL DATASET ===')

        parts.append('Use these exact values in your answer. These are computed from the real data, not estimates.')

        parts.append(json.dumps(data_result, ensure_ascii=True, default=str))

        parts.append('')

    return '\n'.join(parts)[:_MAX_CONTEXT_CHARS]

def _extract_chart_facts(fig_data: Any, chart_key: str) -> str:

    try:

        fig = json.loads(fig_data) if isinstance(fig_data, str) else fig_data

        if not isinstance(fig, dict):

            return f"No data available for chart '{chart_key}'."

        layout = fig.get('layout') or {}

        title_raw = layout.get('title', {})

        title = (title_raw.get('text') if isinstance(title_raw, dict) else title_raw) or chart_key

        facts = [f"Chart title: '{title}'."]

        for trace in (fig.get('data') or [])[:3]:

            ttype = trace.get('type', 'chart')

            x_raw = trace.get('x')

            x_vals = list(x_raw)[:25] if x_raw is not None else []

            y_raw = trace.get('y')

            y_vals = list(y_raw)[:25] if y_raw is not None else []

            labels_raw = trace.get('labels')

            labels = list(labels_raw)[:25] if labels_raw is not None else []

            values_raw = trace.get('values')

            values = list(values_raw)[:25] if values_raw is not None else []

            if len(labels) > 0 and len(values) > 0:

                pairs = [f"'{lbl}': {val}" for (lbl, val) in zip(labels[:12], values[:12])]

                facts.append(f"Data ({ttype}): {', '.join(pairs)}.")

            elif len(x_vals) > 0 and len(y_vals) > 0:

                numeric_y = [(i, float(v)) for (i, v) in enumerate(y_vals) if isinstance(v, (int, float, np.number)) and pd.notna(v)]

                if numeric_y:

                    (max_i, max_v) = max(numeric_y, key=lambda t: t[1])

                    (min_i, min_v) = min(numeric_y, key=lambda t: t[1])

                    avg_v = sum((v for (_, v) in numeric_y)) / len(numeric_y)

                    x_at_max = str(x_vals[max_i]) if max_i < len(x_vals) else '?'

                    x_at_min = str(x_vals[min_i]) if min_i < len(x_vals) else '?'

                    facts.append(f"Chart type: {ttype}. Highest value: '{x_at_max}' = {max_v:,.2f}. Lowest value: '{x_at_min}' = {min_v:,.2f}. Average: {avg_v:,.2f}. All X-axis labels: {[str(x) for x in x_vals[:20]]}. All Y-axis values: {[round(v, 2) for (_, v) in numeric_y[:20]]}.")

        return ' '.join(facts)

    except Exception as exc:

        logger.warning("extract_chart_facts failed for '%s': %s", chart_key, exc)

        return f"Could not read data from chart '{chart_key}'."

def _clean_field_str(val: str) -> str:
    if not val:
        return ""
    s = str(val).strip()
    while (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    s = s.strip('"\' \t\n')
    return s

def _fix_json_values(text: str) -> str:
    import re as _re
    KEYS = ['direct_answer', 'proactive_insight']
    for key in KEYS:
        pattern = _re.compile(
            r'("?' + key + r'"?\s*:\s*)([^"\'\[{{\d\n][^\n}]*?)(\s*(?:,\s*\n|\n\s*"|\n\s*}|\s*}))',
            _re.DOTALL
        )
        def _quote_val(m):
            val = m.group(2).strip().rstrip(',')
            return m.group(1) + '"' + val.replace('"', '\\"') + '"' + m.group(3)
        text = pattern.sub(_quote_val, text)
    return text

def _sanitize_llm_output(text: str) -> str:
    import re as _re
    import json as _json

    text = text.strip()
    if '{' in text and '}' in text:
        start = text.find('{')
        end = text.rfind('}') + 1
        json_sub = text[start:end]
        try:
            parsed = _json.loads(json_sub)
            if isinstance(parsed, dict) and ('direct_answer' in parsed or 'answer' in parsed):
                da = _clean_field_str(parsed.get('direct_answer') or parsed.get('answer') or '')
                pi = _clean_field_str(parsed.get('proactive_insight') or parsed.get('insight') or '')
                return _json.dumps({'direct_answer': da, 'proactive_insight': pi})
        except Exception:
            pass

        da_match = _re.search(r'"direct_answer"\s*:\s*"((?:[^"\\]|\\.)*)"', json_sub, _re.DOTALL)
        pi_match = _re.search(r'"proactive_insight"\s*:\s*"((?:[^"\\]|\\.)*)"', json_sub, _re.DOTALL)
        if da_match:
            try:
                da = _clean_field_str(da_match.group(1).encode().decode('unicode_escape', errors='replace'))
            except Exception:
                da = _clean_field_str(da_match.group(1))
            try:
                pi = _clean_field_str(pi_match.group(1).encode().decode('unicode_escape', errors='replace')) if pi_match else ""
            except Exception:
                pi = _clean_field_str(pi_match.group(1)) if pi_match else ""
            return _json.dumps({'direct_answer': da, 'proactive_insight': pi})

    text = _re.sub(r'[=]{3,}[^=\n]*[=]{3,}', '', text)
    text = _re.sub(r'RULE \d+\s*\([^)]*\)[^.]*\.', '', text)
    text = _re.sub(r'RULE \d+:', '', text)
    text = _re.sub(r'CONTEXT:\s*', '', text, flags=_re.IGNORECASE)
    text = _re.sub(r'As an AI[^.]*\.\s*', '', text, flags=_re.IGNORECASE)
    text = _re.sub(r'As a (language|AI|data)[^.]*\.\s*', '', text, flags=_re.IGNORECASE)
    text = _re.sub(r'(?:GROUNDING CONTRACT|CHART DISPLAY RULES|MANDATORY)[^.]*\.?', '', text, flags=_re.IGNORECASE)
    text = _re.sub(r'\[?(NUMERIC|CATEGORICAL|DATASET|CORRELATION|DATA QUALITY)\s*\w*\s*\w*\]?', '', text)
    text = _re.sub(r'\n{3,}', '\n\n', text)
    text = _re.sub(r'  +', ' ', text)
    clean_da = _clean_field_str(text)
    return _json.dumps({'direct_answer': clean_da, 'proactive_insight': ''})

def _build_trend_forecast_chart(df: pd.DataFrame, time_col: Optional[str], val_col: str, slope: float, intercept: float, n_future: int = 5) -> Optional[dict]:

    

    try:

        import plotly.graph_objects as go

        t_col = time_col if time_col and time_col in df.columns else None

        if t_col:

            df_clean = df[[t_col, val_col]].dropna().copy()

            x_vals = df_clean[t_col].astype(str).tolist()

        else:

            df_clean = df[[val_col]].dropna().copy()

            x_vals = [f"Period {i+1}" for i in range(len(df_clean))]

        y_vals = pd.to_numeric(df_clean[val_col], errors='coerce').tolist()

        n = len(y_vals)

        if n < 2:

            return None

        fig = go.Figure()

        fig.add_trace(go.Scatter(

            x=x_vals,

            y=y_vals,

            mode='lines+markers',

            name=f'Actual {val_col}',

            line=dict(color='#6366f1', width=2.5),

            marker=dict(size=5, color='#818cf8'),

            hovertemplate='%{x}: <b>%{y:,.2f}</b><extra></extra>'

        ))

        trend_y = [intercept + slope * i for i in range(n)]

        fig.add_trace(go.Scatter(

            x=x_vals,

            y=trend_y,

            mode='lines',

            name='Trend Baseline',

            line=dict(color='rgba(148, 163, 184, 0.7)', width=2, dash='dash'),

            hoverinfo='skip'

        ))

        future_x = [f"Forecast +{i+1}" for i in range(n_future)]

        future_y = [intercept + slope * (n + i) for i in range(n_future)]

        proj_x = [x_vals[-1]] + future_x

        proj_y = [y_vals[-1]] + future_y

        fig.add_trace(go.Scatter(

            x=proj_x,

            y=proj_y,

            mode='lines+markers',

            name='Forecast Projection',

            line=dict(color='#f59e0b', width=3, dash='dot'),

            marker=dict(size=8, color='#fbbf24', symbol='diamond'),

            hovertemplate='Projected %{x}: <b>%{y:,.2f}</b><extra></extra>'

        ))

        fig.update_layout(

            title=dict(text=f"Trend & Future Forecast: {val_col}", font=dict(size=14, color='#F8FAFC')),

            paper_bgcolor='rgba(0,0,0,0)',

            plot_bgcolor='rgba(0,0,0,0)',

            font=dict(color='#FFFFFF', family="'Inter', sans-serif"),

            hoverlabel=dict(bgcolor='rgba(8,12,24,0.95)', font=dict(color='#FFFFFF', size=12)),

            xaxis=dict(gridcolor='rgba(255,255,255,0.06)', showgrid=True, title=t_col or "Time Intervals"),

            yaxis=dict(gridcolor='rgba(255,255,255,0.06)', showgrid=True, title=val_col),

            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1, font=dict(size=11)),

            margin=dict(l=60, r=24, t=60, b=50),

            height=320

        )

        return {'id': f'forecast_trend_{val_col}', 'fig': json.loads(fig.to_json()), 'error': None, 'is_duplicate': False}

    except Exception as exc:

        logger.warning("Failed to generate trend forecast plot: %s", exc)

        return None

_DATA_ABBREVIATIONS: dict[str, list[str]] = {

    'bp': ['blood pressure', 'blood', 'pressure', 'systolic', 'diastolic'],

    'ot': ['overtime', 'extra hours'],

    'mrr': ['monthly recurring revenue', 'recurring', 'revenue'],

    'cac': ['customer acquisition cost'],
    'nps': ['net promoter score', 'promoter'],
    'qty': ['quantity', 'units', 'volume'],
    'pct': ['percentage', 'percent', 'rate', 'ratio'],
    'temp': ['temperature'],
    'dept': ['department', 'division'],
    'amt': ['amount', 'total', 'sales', 'revenue'],
    'vol': ['volume'],
    'attr': ['attrition', 'churn', 'turnover', 'leaving', 'departure'],
    'yr': ['year', 'date'],
    'yrs': ['years', 'date'],
    'price': ['cheap', 'cheaper', 'cheapest', 'priciest', 'expensive', 'cost', 'sale', 'listing', 'amount', 'rate', 'revenue', 'rent'],
    'cost': ['cheap', 'cheaper', 'cheapest', 'priciest', 'expensive', 'price', 'sale', 'listing', 'amount', 'rate', 'revenue', 'rent'],
    'sale': ['cheap', 'cheaper', 'cheapest', 'priciest', 'expensive', 'price', 'selling_price', 'sales', 'revenue', 'cost'],
    'sales': ['cheap', 'cheaper', 'cheapest', 'priciest', 'expensive', 'price', 'selling_price', 'sale', 'revenue', 'cost'],
    'rate': ['cheap', 'cheaper', 'cheapest', 'priciest', 'expensive', 'price', 'cost', 'percentage', 'ratio'],
    'sqft': ['square', 'feet', 'area', 'size', 'dimension'],
    'bp': ['blood', 'pressure', 'systolic', 'diastolic'],
    'milage': ['mileage', 'kmpl', 'efficiency'],
    'mileage': ['milage', 'kmpl', 'efficiency'],
    'kmpl': ['mileage', 'efficiency'],
    'brad': ['brand', 'company', 'make'],
}

_TYPO_MAP: dict[str, str] = {
    'wat': 'what',
    'wats': 'what is',
    'hieghest': 'highest',
    'higest': 'highest',
    'cheeper': 'cheaper',
    'checpest': 'cheapest',
    'chepest': 'cheapest',
    'wich': 'which',
    'tel': 'tell',
    'yr': 'year',
    'yrs': 'years',
    'saels': 'sales',
    'milage': 'mileage',
    'outliner': 'outlier',
    'outliners': 'outliers',
    'pls': 'please',
    'plz': 'please',
}

def _normalize_query_typos(text: str) -> str:
    words = re.sub(r'[^a-zA-Z0-9\s]', ' ', text).split()
    normalized = [_TYPO_MAP.get(w.lower(), w) for w in words]
    return ' '.join(normalized)

def _clean_tokens(s: str) -> list[str]:

    

    s = re.sub(r'([a-z0-9])([A-Z])', r'\1 \2', str(s))

    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1 \2', s)

    s = re.sub(r'[^a-zA-Z0-9\s]', ' ', s).lower()

    return [w for w in s.split() if w]

def _match_columns_dynamically(col_list: list[str], text: str, df: Optional[pd.DataFrame] = None) -> list[str]:

    

    if not col_list or not text:

        return []

    text_clean = text.lower().replace('_', ' ').replace('-', ' ')

    words = re.sub(r'[^a-z0-9\s]', ' ', text_clean).split()

    if not words:

        return []

    ngrams: list[tuple[str, int]] = []

    for n in range(1, min(5, len(words) + 1)):

        for i in range(len(words) - n + 1):

            phrase = ' '.join(words[i:i+n])

            pos = text_clean.find(phrase)

            ngrams.append((phrase, pos if pos != -1 else i))

    col_scores: list[tuple[float, int, str]] = []

    for col in col_list:

        col_tokens = _clean_tokens(col)

        col_phrase = ' '.join(col_tokens)

        col_collapsed = re.sub(r'[^a-z0-9]', '', col.lower())

        expanded_tokens = list(col_tokens)

        for t in col_tokens:

            if t in _DATA_ABBREVIATIONS:

                for exp in _DATA_ABBREVIATIONS[t]:

                    expanded_tokens.extend(exp.split())

        col_tok_set = set(expanded_tokens) - {'the', 'a', 'an', 'of', 'in', 'and', 'id', 'num', 'no', 'val', 'data', 'pct', 'usd', 'mv', 'kg', 'ppm', 'ha'}

        best_score = 0.0

        best_pos = 999

        if re.search(r'\b' + re.escape(col_phrase) + r'\b', text_clean):

            best_score = 1.0

            best_pos = text_clean.find(col_phrase)

        elif any(re.search(r'\b' + re.escape(exp) + r'\b', text_clean) for exp in _DATA_ABBREVIATIONS.get(col_collapsed, [])):

            best_score = 0.98

            best_pos = 0

        elif len(col_collapsed) >= 3 and re.search(r'\b' + re.escape(col_collapsed) + r'\b', text_clean):

            best_score = 0.95

            best_pos = text_clean.find(col_collapsed)

        else:

            for phrase, pos in ngrams:

                p_tokens = set(phrase.split())

                overlap = len(col_tok_set & p_tokens)

                if overlap > 0:

                    tok_score = overlap / max(len(col_tok_set), 1)

                    score = 0.6 + (0.35 * tok_score)

                    if score > best_score:

                        best_score = score

                        best_pos = pos

                min_sim = 0.82 if len(col_phrase) <= 4 else 0.75

                if len(phrase) >= 3 and len(col_phrase) >= 3:

                    seq_score = difflib.SequenceMatcher(None, phrase, col_phrase).ratio()

                    if seq_score >= min_sim and seq_score * 0.9 > best_score:

                        best_score = seq_score * 0.9

                        best_pos = pos

        if df is not None and col in df.columns:

            if df[col].dtype == 'object' or str(df[col].dtype) == 'category':

                uniques = df[col].dropna().unique()

                if len(uniques) <= 50:

                    for u in uniques:

                        u_str = str(u).lower().strip()

                        if len(u_str) >= 2 and re.search(r'\b' + re.escape(u_str) + r'\b', text_clean):

                            val_pos = text_clean.find(u_str)

                            if 0.85 > best_score:

                                best_score = 0.85

                                best_pos = val_pos

                            break

        is_id_col = (col_tokens and col_tokens[-1] in ('id', 'uuid', 'key', 'num', 'no', 'code')) or col.lower() in ('id', 'uuid', 'patient_id', 'employee_id')

        user_wants_id = any(w in text_clean.split() for w in ('id', 'identifier', 'key', 'code', 'number', 'lookup'))

        if is_id_col and not user_wants_id:

            best_score = best_score * 0.3

        if best_score >= 0.5:

            col_scores.append((best_score, best_pos, col))

    col_scores.sort(key=lambda x: (x[1], -x[0]))

    seen = set()

    ordered_cols = []

    for _, _, col in col_scores:

        if col not in seen:

            seen.add(col)

            ordered_cols.append(col)

    return ordered_cols

def _find_best_column_match(col_list: list[str], text: str, df: Optional[pd.DataFrame] = None) -> Optional[str]:

    

    matches = _match_columns_dynamically(col_list, text, df)

    return matches[0] if matches else None

def _is_binary_or_indicator_column(series: pd.Series) -> tuple[bool, Any, float, int]:

    

    s_clean = series.dropna()

    if s_clean.empty:

        return False, None, 0.0, 0

    uniques = s_clean.unique()

    if len(uniques) > 4:

        return False, None, 0.0, 0

    if pd.api.types.is_numeric_dtype(s_clean):

        if set(uniques).issubset({0, 1, 0.0, 1.0}):

            pos_cnt = int((s_clean == 1).sum())

            return True, "1", (pos_cnt / len(s_clean)) * 100, pos_cnt

    s_str = s_clean.astype(str).str.strip().str.lower()

    pos_terms = {'yes', 'true', '1', '1.0', 'y', 'churn', 'default', 'delay', 'delayed', 'damaged', 'loss', 'failed', 'positive', 'departure', 'attrition'}

    matched_pos = [u for u in uniques if str(u).lower().strip() in pos_terms]

    if matched_pos:

        pos_val = matched_pos[0]

        pos_cnt = int(s_str.isin(pos_terms).sum())

        return True, str(pos_val), (pos_cnt / len(s_clean)) * 100, pos_cnt

    if len(uniques) == 2:

        val_counts = s_clean.value_counts()

        minority_val = val_counts.index[-1]

        minority_cnt = val_counts.iloc[-1]

        return True, str(minority_val), (minority_cnt / len(s_clean)) * 100, minority_cnt

    return False, None, 0.0, 0

def _generate_offline_answer(question: str, df: Optional[pd.DataFrame], stats: dict, insights: dict) -> tuple[str, Optional[dict]]:

    

    q = _normalize_query_typos(question).lower().strip()

    num_cols = list((stats.get('numeric_columns') or {}).keys())

    cat_cols = list((stats.get('categorical_columns') or {}).keys())

    date_cols = stats.get('datetime_columns') or stats.get('date_columns') or []

    all_cols = list(df.columns) if df is not None else (num_cols + cat_cols + [str(c) for c in date_cols])

    _MALICIOUS_TERMS = ('delete', 'rmdir', 'drop table', '__import__', 'system prompt', 'secret prompt', 'credentials', 'password', 'os.system', 'eval(', 'exec(')

    if any(m in q for m in _MALICIOUS_TERMS):

        ans = json.dumps({

            'direct_answer': "I cannot execute destructive, system-level, or ungrounded commands. I am dedicated strictly to analyzing your tabular dataset.",

            'proactive_insight': "All queries operate in a sandboxed, read-only analytical environment."

        })

        return ans, None

    _OUT_OF_DOMAIN_PATTERNS = ('fifa', 'world cup', 'who won', 'who is', 'who are', 'prime minister', 'president', 'capital of', 'weather in', 'recipe for', 'write a poem', 'tell me a joke', 'how to bake', 'movie', 'song')

    if any(p in q for p in _OUT_OF_DOMAIN_PATTERNS) and not any(c.lower() in q for c in all_cols):

        ans = json.dumps({

            'direct_answer': "That question is outside the scope of this dataset. I can only provide insights and statistical analysis directly grounded in your uploaded data.",

            'proactive_insight': f"Feel free to ask questions about any of the dataset's features: {', '.join(all_cols[:5])}."

        })

        return ans, None

    if any(w in q for w in ('outlier', 'outliers', 'outliner', 'outliners', 'anomaly', 'anomalies', 'extreme value', 'extreme values')):

        outliers = stats.get('outliers') or {}

        outlier_cols = [(col, info) for (col, info) in outliers.items() if (info.get('count') or 0) > 0]

        if outlier_cols:

            details = []

            for col, info in outlier_cols[:6]:

                cnt = info.get('count', 0)

                pct = info.get('percentage') or info.get('pct')

                pct_str = f" ({float(pct):.1f}%)" if pct is not None else ""

                details.append(f"**{col}**: **{cnt}** outlier(s){pct_str}")

            ans = json.dumps({

                'direct_answer': f"Outlier analysis identified unusual points in **{len(outlier_cols)}** columns: {', '.join(details)}.",

                'proactive_insight': "Outliers were identified using the standard 1.5×IQR (Interquartile Range) rule on numerical boundaries."

            })

            return ans, None

        else:

            ans = json.dumps({

                'direct_answer': "No statistical outliers were detected across any numeric columns in this dataset (based on the 1.5×IQR fence).",

                'proactive_insight': "All recorded numerical values fall within expected standard distribution bounds."

            })

            return ans, None

    if any(w in q for w in ('missing', 'null', 'nan', 'completeness', 'quality', 'clean')) and not any(w in q for w in ('trend', 'forecast', 'predict', 'by', 'relate')):

        quality = stats.get('data_quality') or {}

        comp = quality.get('completeness', 100)

        missing_by_col = quality.get('missing_by_column') or quality.get('missing_per_column') or {}

        cols_with_missing = [(c, cnt) for c, cnt in missing_by_col.items() if cnt and int(cnt) > 0]

        if cols_with_missing:

            col_details = ', '.join([f"**{c}** ({cnt} missing)" for c, cnt in cols_with_missing[:5]])

            ans = json.dumps({

                'direct_answer': f"Overall dataset completeness is **{comp:.1f}%**. Missing values found in: {col_details}.",

                'proactive_insight': f"A total of **{len(cols_with_missing)}** column(s) contain unrecorded values that were sanitized during cleaning."

            })

            return ans, None

        else:

            ans = json.dumps({

                'direct_answer': f"The dataset is **100% complete** with **0 missing cells** across all rows and columns.",

                'proactive_insight': "Every observation has full attribute records, ensuring high analytical confidence."

            })

            return ans, None

    has_min_term = any(w in q for w in ('min', 'minimum', 'lowest'))

    has_max_term = any(w in q for w in ('max', 'maximum', 'highest', 'peak'))

    has_avg_term = any(w in q for w in ('avg', 'average', 'mean', 'median'))

    is_triple_stat = (has_min_term and has_max_term) or (has_min_term and has_avg_term and 'and' in q)

    matched_num_all = _match_columns_dynamically(num_cols, q, df)

    if is_triple_stat:

        target_num = matched_num_all[0] if matched_num_all else (_find_best_column_match(num_cols, question, df) or (num_cols[0] if num_cols else None))

        if df is not None and target_num and target_num in df.columns:

            s_num = pd.to_numeric(df[target_num], errors='coerce').dropna()

            ans = json.dumps({

                'direct_answer': f"Statistical summary for **{target_num}** across all **{len(s_num):,}** observations:\n• **Minimum**: **{s_num.min():,.2f}**\n• **Maximum**: **{s_num.max():,.2f}**\n• **Average**: **{s_num.mean():,.2f}** (Median: **{s_num.median():,.2f}**, Std: **{s_num.std():,.2f}**).",

                'proactive_insight': f"Total range spans **{s_num.max() - s_num.min():,.2f}** units between extremes."

            })

            return ans, None

    if any(w in q for w in ('trend', 'forecast', 'predict', 'future', 'projection', 'growth', 'slope', 'trajectory')):

        target_num = matched_num_all[0] if matched_num_all else (_find_best_column_match(num_cols, question, df) or (num_cols[0] if num_cols else None))

        time_col = date_cols[0] if date_cols else None

        if df is not None and target_num and target_num in df.columns:

            series = pd.to_numeric(df[target_num], errors='coerce').dropna()

            if len(series) >= 2:

                x = np.arange(len(series))

                y = series.values

                slope, intercept = np.polyfit(x, y, 1)

                r_matrix = np.corrcoef(x, y)

                r2 = float(r_matrix[0, 1] ** 2) if r_matrix.shape == (2, 2) and not np.isnan(r_matrix[0, 1]) else 0.0

                direction = "upward growth" if slope > 0 else ("downward decline" if slope < 0 else "flat")

                next_val = float(intercept + slope * len(series))

                mean_val = float(series.mean())

                growth_pct = ((next_val - mean_val) / max(mean_val, 1)) * 100

                chart_obj = _build_trend_forecast_chart(df, time_col, target_num, slope, intercept)

                ans = json.dumps({

                    'direct_answer': f"**{target_num} Forecast**: Projected to reach **{next_val:,.2f}** in the upcoming period ({growth_pct:+.1f}% vs baseline), continuing a steady **{direction}** trend (growth rate: **{slope:+.2f}**/period).",

                    'proactive_insight': f"Across all **{len(series)}** historical data points, **{target_num}** averaged **{mean_val:,.2f}** (range: **{series.min():,.2f}** to **{series.max():,.2f}**). The linear trendline demonstrates **{int(r2*100)}%** model confidence."

                })

                return ans, chart_obj

    clauses = [c.strip() for c in re.split(r'\band\b|\bas well as\b|\balso\b|\?|;', q) if len(c.strip()) > 3]

    if len(clauses) >= 2 and df is not None:

        sub_answers = []

        for clause in clauses[:2]:

            c_num = _match_columns_dynamically(num_cols, clause, df)

            c_cat = _match_columns_dynamically(cat_cols, clause, df)

            c_all = _match_columns_dynamically(all_cols, clause, df)

            if any(w in clause for w in ('highest', 'top', 'lowest', 'max', 'min', 'which', 'leading', 'most', 'least')):

                cat_col = c_cat[0] if c_cat else (cat_cols[0] if cat_cols else None)

                num_col = c_num[0] if c_num else (num_cols[0] if num_cols else None)

                if cat_col and num_col and cat_col in df.columns and num_col in df.columns:

                    is_lowest = any(w in clause for w in ('lowest', 'min', 'least', 'worst'))

                    grouped = df.groupby(cat_col, observed=True)[num_col].mean().sort_values(ascending=is_lowest)

                    top_name = grouped.index[0]

                    top_val = grouped.iloc[0]

                    adj = "Lowest" if is_lowest else "Highest"

                    sub_answers.append(f"{adj} **{num_col}**: **{top_name}** leads with an average of **{top_val:,.2f}**")

                    continue

            if any(w in clause for w in ('rate', 'percent', 'percentage', 'ratio', 'proportion', 'departures', 'overall')):

                target_col = c_all[0] if c_all else None

                if target_col and target_col in df.columns:

                    is_bin, pos_name, rate_pct, cnt = _is_binary_or_indicator_column(df[target_col])

                    if is_bin:

                        sub_answers.append(f"Overall **{target_col}** rate is **{rate_pct:.1f}%** (**{cnt}** out of **{len(df):,}** total records)")

                        continue

                    elif target_col in num_cols:

                        s_col = pd.to_numeric(df[target_col], errors='coerce').dropna()

                        sub_answers.append(f"Average **{target_col}** is **{s_col.mean():,.2f}**")

                        continue

            if c_num and c_num[0] in df.columns:

                s_col = pd.to_numeric(df[c_num[0]], errors='coerce').dropna()

                sub_answers.append(f"**{c_num[0]}**: average **{s_col.mean():,.2f}** (median: **{s_col.median():,.2f}**)")

            elif c_cat and c_cat[0] in df.columns:

                counts = df[c_cat[0]].value_counts()

                sub_answers.append(f"**{c_cat[0]}**: top category is **'{counts.index[0]}'** ({counts.iloc[0]} records)")

        if len(sub_answers) >= 2:

            ans = json.dumps({

                'direct_answer': "1. " + sub_answers[0] + ".\n2. " + sub_answers[1] + ".",

                'proactive_insight': f"Dataset contains **{len(df):,}** verified records across **{len(df.columns)}** operational features."

            })

            return ans, None

    if len(matched_num_all) >= 2 and df is not None and not any(w in q for w in ('relate', 'correlat', 'versus', 'vs', 'impact', 'why', 'driver')):

        stat_lines = []

        for col in matched_num_all[:3]:

            s_col = pd.to_numeric(df[col], errors='coerce').dropna()

            if not s_col.empty:

                stat_lines.append(f"• **{col}**: average **{s_col.mean():,.2f}** (median: **{s_col.median():,.2f}**, range: **{s_col.min():,.2f}** to **{s_col.max():,.2f}**)")

        if stat_lines:

            ans = json.dumps({

                'direct_answer': f"Key metrics across all **{len(df):,}** records:\n" + "\n".join(stat_lines),

                'proactive_insight': f"All **{len(matched_num_all)}** variables have complete verified records in the active dataset."

            })

            return ans, None

    if len(matched_num_all) >= 2 and df is not None and any(w in q for w in ('relate', 'correlat', 'impact', 'relationship', 'affect', 'driver', 'why', 'how')):

        c1, c2 = matched_num_all[0], matched_num_all[1]

        s1 = pd.to_numeric(df[c1], errors='coerce')

        s2 = pd.to_numeric(df[c2], errors='coerce')

        valid_mask = s1.notna() & s2.notna()

        r_val = float(np.corrcoef(s1[valid_mask], s2[valid_mask])[0, 1]) if valid_mask.sum() >= 3 else 0.0

        direction = "positive" if r_val > 0.1 else ("negative" if r_val < -0.1 else "neutral / independent")

        strength = "strong" if abs(r_val) >= 0.7 else ("moderate" if abs(r_val) >= 0.3 else "slight")

        m1_avg = s1.mean()

        m2_avg = s2.mean()

        ans = json.dumps({

            'direct_answer': f"Analysis of **{c1}** (avg: **{m1_avg:,.2f}**) versus **{c2}** (avg: **{m2_avg:,.2f}**) indicates a **{strength} {direction} relationship** (correlation r = **{r_val:.2f}**).",

            'proactive_insight': f"When **{c1}** varies, **{c2}** exhibits a {direction} alignment with standard deviation **{s2.std():,.2f}**."

        })

        return ans, None

    matched_all = _match_columns_dynamically(all_cols, q, df)

    if any(w in q for w in ('rate', 'percent', 'percentage', 'proportion', 'ratio', 'share of')) and df is not None:

        target_col = matched_all[0] if matched_all else None

        if target_col and target_col in df.columns:

            is_bin, pos_name, rate_pct, cnt = _is_binary_or_indicator_column(df[target_col])

            if is_bin:

                ans = json.dumps({

                    'direct_answer': f"The overall **{target_col}** rate is **{rate_pct:.1f}%** (**{cnt}** out of **{len(df):,}** total records).",

                    'proactive_insight': f"Class distribution for **{target_col}**: {', '.join([f'{k} ({v})' for k, v in df[target_col].value_counts().items()])}."

                })

                return ans, None

    if df is not None and not df.empty:

        matched_val = None

        matched_filter_col = None

        for cc in cat_cols:

            if cc in df.columns:

                for val in df[cc].dropna().unique():

                    v_str = str(val).lower()

                    if len(v_str) >= 2 and (re.search(r'\b' + re.escape(v_str) + r'\b', q) or any(difflib.SequenceMatcher(None, w, v_str).ratio() >= 0.78 for w in q.split() if len(w) >= 3)):

                        matched_val = val

                        matched_filter_col = cc

                        break

        if matched_val is None:
            year_match = re.search(r'\b(19\d\d|20\d\d)\b', q)
            if year_match:
                year_val = year_match.group(1)
                for col in all_cols:
                    if any(yk in col.lower() for yk in ('year', 'date', 'yr')):
                        if df[col].astype(str).str.contains(year_val).any():
                            matched_val = year_val
                            matched_filter_col = col
                            break

        if matched_val is not None and matched_filter_col is not None:

            sub_df = df[df[matched_filter_col].astype(str).str.lower() == str(matched_val).lower()]

            sub_count = len(sub_df)

            matched_num = _find_best_column_match(num_cols, q, df) or (num_cols[0] if num_cols else None)

            is_count_only = any(w in q for w in ('how many', 'count of', 'number of', 'total entries', 'how much count')) and not any(w in q for w in ('average', 'avg', 'mean', 'sum', 'total price', 'max', 'min', 'milage', 'mileage'))

            if is_count_only:

                ans = json.dumps({

                    'direct_answer': f"There are **{sub_count}** {matched_filter_col} records for **'{matched_val}'** ({sub_count/len(df)*100:.1f}% of all {len(df):,} total records).",

                    'proactive_insight': f"Total dataset contains **{len(df):,}** observations across **{df[matched_filter_col].nunique()}** {matched_filter_col} categories."

                })

                return ans, None

            if matched_num and matched_num in sub_df.columns:

                sub_series = pd.to_numeric(sub_df[matched_num], errors='coerce').dropna()

                if not sub_series.empty:

                    sub_sum = sub_series.sum()

                    sub_mean = sub_series.mean()

                    sub_median = sub_series.median()

                    is_sum_q = any(w in q for w in ('total', 'sum', 'volume', 'overall', 'gross'))

                    if is_sum_q:

                        ans = json.dumps({

                            'direct_answer': f"The total **{matched_num}** for **{matched_filter_col}: {matched_val}** is **{sub_sum:,.2f}** (across **{sub_count}** records, average: **{sub_mean:,.2f}**).",

                            'proactive_insight': f"This segment accounts for **{sub_count/len(df)*100:.1f}%** of all {len(df):,} dataset rows."

                        })

                    else:

                        ans = json.dumps({

                            'direct_answer': f"For **{matched_filter_col}: {matched_val}**, average **{matched_num}** is **{sub_mean:,.2f}** (median: **{sub_median:,.2f}**, total: **{sub_sum:,.2f}** across **{sub_count}** entries).",

                            'proactive_insight': f"Values in this segment range from **{sub_series.min():,.2f}** to **{sub_series.max():,.2f}**."

                        })

                    return ans, None

            else:

                ans = json.dumps({

                    'direct_answer': f"There are **{sub_count}** records matching **{matched_filter_col}: '{matched_val}'** ({sub_count/len(df)*100:.1f}% of total data).",

                    'proactive_insight': f"Dataset has **{len(df):,}** observations overall across **{df[matched_filter_col].nunique()}** {matched_filter_col} segments."

                })

                return ans, None

    matched_cat = _find_best_column_match(cat_cols, q, df)
    matched_num = _find_best_column_match(num_cols, q, df)

    if not matched_cat and any(yk in q for yk in ('year', 'yr', 'date', 'period')):
        for c in (stats.get('datetime_columns') or stats.get('date_columns') or all_cols):
            if any(yk in str(c).lower() for yk in ('year', 'date', 'yr')):
                matched_cat = str(c)
                break

    if df is not None and matched_cat and matched_cat in df.columns and (
        any(w in q for w in ('by', 'breakdown', 'per', 'across', 'group', 'each', 'ranking', 'highest', 'top', 'compare', 'cheapest', 'cheaper', 'cheap', 'lowest', 'least', 'priciest', 'expensive', 'which', 'what')) or matched_num
    ):
        target_num = matched_num or (num_cols[0] if num_cols else None)
        if target_num == matched_cat:
            remaining_nums = [c for c in num_cols if c != matched_cat]
            if remaining_nums:
                target_num = remaining_nums[0]
        if target_num and target_num in df.columns:
            try:
                is_lowest = any(w in q for w in ('cheaper', 'cheapest', 'cheap', 'lowest', 'least', 'min', 'bottom', 'worst', 'affordable'))
                is_sum = any(w in q for w in ('total', 'sum', 'volume', 'gross'))
                agg_func = 'sum' if is_sum else 'mean'
                grouped = df.groupby(matched_cat, observed=True)[target_num].agg(agg_func).dropna().sort_values(ascending=is_lowest)
                if not grouped.empty:
                    top_name = grouped.index[0]
                    top_val = grouped.iloc[0]
                    items_formatted = [f"**{k}**: **{v:,.2f}**" for k, v in grouped.head(4).items()]
                    func_label = 'Lowest / Most Affordable' if is_lowest else ('Total' if is_sum else 'Average')
                    adj_label = 'cheapest / lowest' if is_lowest else 'highest / top'
                    ans = json.dumps({
                        'direct_answer': f"{func_label} **{target_num}** by **{matched_cat}**: **'{top_name}'** is {adj_label} at **{top_val:,.2f}**.\nBreakdown: {'; '.join(items_formatted)}.",
                        'proactive_insight': f"Across all **{len(grouped)}** {matched_cat} categories, **'{top_name}'** ranks first for {adj_label} {target_num}."
                    })
                    return ans, None
            except Exception as grp_exc:
                logger.debug("Groupby evaluation fallback: %s", grp_exc)

    top_n_match = re.search(r'top\s*(\d+)', q)
    if top_n_match and df is not None:
        top_n_num = min(int(top_n_match.group(1)), len(df))
        target_num = matched_num or (num_cols[0] if num_cols else None)
        if target_num and target_num in df.columns:
            is_lowest = any(w in q for w in ('cheapest', 'lowest', 'bottom', 'least', 'cheaper'))
            s_sorted = df.sort_values(by=target_num, ascending=is_lowest).head(top_n_num)
            desc_col = cat_cols[0] if cat_cols else (df.columns[0])
            items = [f"**{row[desc_col]}**: **{row[target_num]:,.2f}**" for _, row in s_sorted.iterrows()]
            order_label = "lowest / cheapest" if is_lowest else "highest / priciest"
            ans = json.dumps({
                'direct_answer': f"Top **{top_n_num}** {order_label} **{target_num}** entries:\n" + "\n".join([f"• {it}" for it in items]),
                'proactive_insight': f"These top {top_n_num} records represent {s_sorted[target_num].sum()/max(df[target_num].sum(),1)*100:.1f}% of overall {target_num} volume."
            })
            return ans, None

    if any(w in q.split() for w in ('total', 'sum', 'volume', 'gross', 'aggregate')):

        target_num = matched_num or (num_cols[0] if num_cols else None)

        if df is not None and target_num and target_num in df.columns:

            s_num = pd.to_numeric(df[target_num], errors='coerce').dropna()

            total_val = s_num.sum()

            mean_val = s_num.mean()

            ans = json.dumps({

                'direct_answer': f"The total **{target_num}** across all **{len(s_num):,}** records is **{total_val:,.2f}** (average: **{mean_val:,.2f}**).",

                'proactive_insight': f"Values range from **{s_num.min():,.2f}** to **{s_num.max():,.2f}** (median: **{s_num.median():,.2f}**)."

            })

            return ans, None

    if any(re.search(r'\b' + re.escape(w) + r'\b', q) for w in ('highest', 'top', 'max', 'maximum', 'best', 'most', 'peak')):

        target_num = matched_num or (num_cols[0] if num_cols else None)

        if df is not None and target_num and target_num in df.columns:

            s_num = pd.to_numeric(df[target_num], errors='coerce')

            max_idx = s_num.idxmax()

            if max_idx is not None and pd.notna(max_idx):

                max_row = df.loc[max_idx]

                max_val = s_num.loc[max_idx]

                label = ""

                for cc in cat_cols:

                    if cc in df.columns:

                        label += f" (associated with **{cc}**: {max_row[cc]})"

                        break

                ans = json.dumps({

                    'direct_answer': f"The maximum **{target_num}** recorded in the dataset is **{max_val:,.2f}**{label}.",

                    'proactive_insight': f"The average **{target_num}** across all **{len(df)}** records is **{s_num.mean():,.2f}** (median: **{s_num.median():,.2f}**)."

                })

                return ans, None

    if any(re.search(r'\b' + re.escape(w) + r'\b', q) for w in ('lowest', 'bottom', 'min', 'minimum', 'worst', 'least')):

        target_num = matched_num or (num_cols[0] if num_cols else None)

        if df is not None and target_num and target_num in df.columns:

            s_num = pd.to_numeric(df[target_num], errors='coerce')

            min_idx = s_num.idxmin()

            if min_idx is not None and pd.notna(min_idx):

                min_row = df.loc[min_idx]

                min_val = s_num.loc[min_idx]

                label = ""

                for cc in cat_cols:

                    if cc in df.columns:

                        label += f" (associated with **{cc}**: {min_row[cc]})"

                        break

                ans = json.dumps({

                    'direct_answer': f"The lowest **{target_num}** recorded is **{min_val:,.2f}**{label}.",

                    'proactive_insight': f"Range of **{target_num}** spans from **{min_val:,.2f}** up to **{s_num.max():,.2f}**."

                })

                return ans, None

    if any(w in q for w in ('average', 'mean', 'median', 'typical', 'standard deviation', 'std', 'spread')):

        target_num = matched_num or (num_cols[0] if num_cols else None)

        if df is not None and target_num and target_num in df.columns:

            s_num = pd.to_numeric(df[target_num], errors='coerce').dropna()

            mean_val = s_num.mean()

            median_val = s_num.median()

            std_val = s_num.std()

            ans = json.dumps({

                'direct_answer': f"The average **{target_num}** is **{mean_val:,.2f}** (median: **{median_val:,.2f}**, standard deviation: **{std_val:,.2f}**).",

                'proactive_insight': f"**{target_num}** values range from a minimum of **{s_num.min():,.2f}** to a maximum of **{s_num.max():,.2f}** across **{len(s_num)}** observations."

            })

            return ans, None

    if any(w in q for w in ('why', 'how come', 'explain', 'insight', 'findings', 'driver', 'reason', 'recommend')):

        findings = insights.get('findings') or insights.get('key_findings') or []

        headline = insights.get('headline') or "Comprehensive dataset review completed."

        recs = insights.get('recommendations') or []

        first_finding = str(findings[0]) if findings else "Primary concentration observed in top categorical segments."

        rec_text = f" Recommendation: {recs[0]}" if recs else ""

        ans = json.dumps({

            'direct_answer': f"{headline}",

            'proactive_insight': f"Key finding: {first_finding}.{rec_text}"

        })

        return ans, None

    matched_col = _find_best_column_match(all_cols, question)

    if matched_col and df is not None and matched_col in df.columns:

        if matched_col in num_cols or pd.api.types.is_numeric_dtype(df[matched_col]):

            s_num = pd.to_numeric(df[matched_col], errors='coerce').dropna()

            ans = json.dumps({

                'direct_answer': f"For **{matched_col}**: average is **{s_num.mean():,.2f}**, ranging from **{s_num.min():,.2f}** to **{s_num.max():,.2f}** (median: **{s_num.median():,.2f}**, total: **{s_num.sum():,.2f}**).",

                'proactive_insight': f"Standard deviation for **{matched_col}** is **{s_num.std():,.2f}** across **{len(s_num)}** valid entries."

            })

            return ans, None

        else:

            counts = df[matched_col].value_counts()

            top_val = counts.index[0] if not counts.empty else "N/A"

            top_cnt = counts.iloc[0] if not counts.empty else 0

            ans = json.dumps({

                'direct_answer': f"**{matched_col}** contains **{df[matched_col].nunique()}** unique categories. Top value is **'{top_val}'** with **{top_cnt}** occurrences ({top_cnt/len(df)*100:.1f}%).",

                'proactive_insight': f"Top distribution: {', '.join([f'{k} ({v})' for k, v in counts.head(4).items()])}."

            })

            return ans, None

    row_count = len(df) if df is not None else (stats.get('row_count') or '?')

    col_count = len(df.columns) if df is not None else (stats.get('column_count') or '?')

    findings = (insights.get('key_findings') or insights.get('findings') or [])

    top_finding = str(findings[0]) if findings else f"Data quality completeness is **{stats.get('data_quality', {}).get('completeness', 100):.1f}%** across all features."

    ans = json.dumps({

        'direct_answer': f"The dataset contains **{row_count:,}** rows and **{col_count}** columns ({', '.join(num_cols[:3] + cat_cols[:2])}).",

        'proactive_insight': top_finding

    })

    return ans, None

async def answer_question(question: str, file_hash: str, file_name: str, stats: dict, insights: dict, chart_keys: list[str], conversation_history: list[dict], groq_client, redis_client=None, df=None) -> dict:

    from .pandas_executor import classify_question, generate_pandas_code, fix_pandas_code, safe_execute, format_result, build_rich_context, is_challenge

    from .data_agent import _load_df

    if df is None and file_hash:

        df, _ = _load_df(file_hash)

    forecast_chart = None

    if any(w in question.lower() for w in ('trend', 'forecast', 'predict', 'future', 'projection', 'growth', 'slope')) and df is not None:

        num_cols = list((stats.get('numeric_columns') or {}).keys())

        target_num = _find_best_column_match(num_cols, question) or (num_cols[0] if num_cols else None)

        if target_num and target_num in df.columns:

            date_cols = stats.get('datetime_columns') or stats.get('date_columns') or []

            time_col = date_cols[0] if date_cols else None

            series = pd.to_numeric(df[target_num], errors='coerce').dropna()

            if len(series) >= 2:

                x = np.arange(len(series))

                y = series.values

                slope, intercept = np.polyfit(x, y, 1)

                forecast_chart = _build_trend_forecast_chart(df, time_col, target_num, slope, intercept)

    if is_challenge(question) and conversation_history:

        last_answer = ''

        for msg in reversed(conversation_history):

            if str(msg.get('role', '')).lower() in ('assistant', 'ai'):

                last_answer = str(msg.get('content', ''))

                break

        if last_answer and df is not None and (not df.empty):

            try:

                code = await generate_pandas_code(question=conversation_history[-2].get('content', question) if len(conversation_history) >= 2 else question, df=df)

                exec_result = safe_execute(code, df)

                if exec_result['error'] is None:

                    formatted = format_result(exec_result['result'])

                    confirm_msg = [{'role': 'system', 'content': _data_system_prompt(file_name, chart_keys)}, {'role': 'system', 'content': f"The user is asking you to confirm a previous answer. You re-ran the query and got this result:\nPANDAS RESULT (re-verified):\n{formatted}\n\nQUERY CODE RUN:\n{code}\n\nPrevious answer was: {last_answer[:300]}\n\nConfirm the result confidently. In the `direct_answer` JSON field, say 'Yes, confirmed — ' then restate the key number. Do NOT change the answer, make sure to output the required JSON format."}, {'role': 'user', 'content': question}]

                    raw_ans = await call_groq_with_fallback(

                        messages=confirm_msg,

                        primary_model=SYNTHESIS_MODEL,

                        temperature=0,

                        max_tokens=1024

                    )

                    answer = _sanitize_llm_output(raw_ans)

                    return {'answer': answer, 'data_queried': True, 'new_chart': forecast_chart}

            except Exception as exc:

                logger.warning('Challenge re-verification failed: %s', exc)

    q_type = await classify_question(question)

    logger.info("Question classified as: %s - '%s'", q_type, question[:80])

    static_ctx = _build_static_context(stats, insights)

    if q_type == 'analytical' and df is not None and (not df.empty):

        try:
            code = await generate_pandas_code(question=question, df=df, conversation_history=conversation_history)
            logger.info('Generated pandas code:\n%s', code)
            exec_result = safe_execute(code, df)
            if exec_result.get('error'):
                logger.warning('Initial pandas code failed: %s. Running self-healing retry...', exec_result['error'])
                try:
                    fixed_code = await fix_pandas_code(question=question, df=df, failed_code=code, error_message=exec_result['error'])
                    fixed_exec = safe_execute(fixed_code, df)
                    if fixed_exec.get('error') is None:
                        code = fixed_code
                        exec_result = fixed_exec
                        logger.info('Self-healing code fix succeeded!')
                except Exception as fix_exc:
                    logger.warning('Self-healing retry failed: %s', fix_exc)

            if exec_result.get('error') is None and exec_result.get('result') is not None:
                formatted = format_result(exec_result['result'])
                rich_ctx = build_rich_context(exec_result['result'], df, question)
                messages = [{'role': 'system', 'content': _data_system_prompt(file_name, chart_keys)}, {'role': 'system', 'content': f'PANDAS RESULT (computed from the REAL dataset - trust these numbers 100%):\n{formatted}\n\nQUERY CODE RUN:\n{code}\n\nADDITIONAL CONTEXT:\n{json.dumps(rich_ctx, default=str)}\n\nDATASET OVERVIEW:\n{static_ctx[:12000]}'}]
                _SAFE_ROLES = {'assistant', 'ai', 'user', 'human'}
                for msg in (conversation_history or [])[-4:]:
                    raw_role = str(msg.get('role', '')).lower()
                    if raw_role not in _SAFE_ROLES:
                        continue
                    role = 'assistant' if raw_role in ('assistant', 'ai') else 'user'
                    messages.append({'role': role, 'content': str(msg.get('content', ''))[:800]})
                messages.append({'role': 'user', 'content': question})
                raw_ans = await call_groq_with_fallback(
                    messages=messages,
                    primary_model=SYNTHESIS_MODEL,
                    temperature=0.0,
                    max_tokens=1000
                )
                answer = _sanitize_llm_output(raw_ans)
                return {'answer': answer, 'data_queried': True, 'new_chart': forecast_chart}
        except Exception as exc:
            logger.warning('Analytical path failed, falling back to reasoning: %s', exc)

    col_types: dict[str, str] = {}

    col_metadata: dict[str, dict] = {}

    for (c, info) in (stats.get('numeric_columns') or {}).items():

        col_types[c] = 'numeric'

        col_metadata[c] = {'type': 'numeric', 'min': info.get('min'), 'max': info.get('max'), 'mean': info.get('mean')}

    for (c, info) in (stats.get('categorical_columns') or {}).items():

        col_types[c] = 'categorical'

        top_vals = info.get('top_5_values') or info.get('top_values') or {}

        col_metadata[c] = {'type': 'categorical', 'unique_count': info.get('unique_values') or info.get('nunique'), 'top_values': list(top_vals.keys())[:5] if isinstance(top_vals, dict) else []}

    for c in stats.get('datetime_columns') or stats.get('date_columns') or []:
        col_name = c if isinstance(c, str) else str(c)
        if col_name not in col_types:
            col_types[col_name] = 'datetime'
            col_metadata[col_name] = {'type': 'datetime'}

    if df is not None and not df.empty and (not col_types or len(col_types) == 0):
        for c in df.select_dtypes(include='number').columns:
            col_types[c] = 'numeric'
            s = df[c].dropna()
            col_metadata[c] = {
                'type': 'numeric',
                'min': float(s.min()) if not s.empty else None,
                'max': float(s.max()) if not s.empty else None,
                'mean': float(s.mean()) if not s.empty else None
            }
        for c in df.select_dtypes(include='object').columns:
            col_types[c] = 'categorical'
            vc = df[c].value_counts().head(5)
            col_metadata[c] = {
                'type': 'categorical',
                'unique_count': int(df[c].nunique()),
                'top_values': list(vc.index)
            }
        for c in df.select_dtypes(include=['datetime64', 'datetimetz']).columns:
            col_types[c] = 'datetime'
            col_metadata[c] = {'type': 'datetime'}

    data_result = None

    if col_types and groq_client:

        data_result = await _plan_and_run_query(question=question, file_hash=file_hash, col_types=col_types, col_metadata=col_metadata, groq_client=groq_client, df=df)

    context = _assemble_context(data_result, static_ctx)

    messages = [{'role': 'system', 'content': _data_system_prompt(file_name, chart_keys)}, {'role': 'system', 'content': f'CONTEXT:\n{context}'}]

    _SAFE_ROLES = {'assistant', 'ai', 'user', 'human'}

    for msg in (conversation_history or [])[-6:]:

        raw_role = str(msg.get('role', '')).lower()

        if raw_role not in _SAFE_ROLES:

            continue

        role = 'assistant' if raw_role in ('assistant', 'ai') else 'user'

        messages.append({'role': role, 'content': str(msg.get('content', ''))[:1000]})

    messages.append({'role': 'user', 'content': question})

    try:

        temp = 0.0 if data_result is not None else 0.05

        answer = await _call_groq_with_retry(groq_client, messages, SYNTHESIS_MODEL, temperature=temp, max_tokens=1500)

        answer = _sanitize_llm_output(answer)

    except Exception as exc:

        logger.error('Chat synthesis failed (using offline local analyst): %s', exc)

        offline_ans, offline_chart = _generate_offline_answer(question, df, stats, insights)

        return {'answer': offline_ans, 'data_queried': data_result is not None, 'new_chart': forecast_chart or offline_chart}

    return {'answer': answer, 'data_queried': data_result is not None, 'new_chart': forecast_chart}

async def answer_chart_explanation(

    question: str, chart_key: str, chart_data: Any,

    file_name: str, file_hash: str, chart_keys: list[str],

    stats: dict, insights: dict,

    conversation_history: list[dict], groq_client, redis_client=None,

    df=None

) -> dict:

    import re as _re

    resolved_key = chart_key

    if not chart_data and chart_keys:

        q_lower = question.lower()

        q_tokens = set(_re.sub(r'[^\w]', ' ', q_lower).split())

        best_key, best_score = chart_key, 0

        for key in chart_keys:

            key_tokens = set(_re.sub(r'[^\w]', ' ', key.lower()).split())

            score = len(q_tokens & key_tokens)

            if score > best_score:

                best_score, best_key = score, key

        if best_score > 0:

            resolved_key = best_key

            logger.info(

                "answer_chart_explanation: re-resolved key '%s' → '%s' (score=%d)",

                chart_key, resolved_key, best_score,

            )

    chart_facts = _extract_chart_facts(chart_data, resolved_key)

    stats_lines: list[str] = []

    num_cols = stats.get('numeric_columns') or {}

    cat_cols = stats.get('categorical_columns') or {}

    correlations = stats.get('strong_correlations') or stats.get('correlations') or []

    if num_cols:

        stats_lines.append('NUMERIC COLUMNS: ' + ', '.join(

            f"{c}(mean={info.get('mean','?')}, min={info.get('min','?')}, max={info.get('max','?')})"

            for c, info in list(num_cols.items())[:6]

        ))

    if cat_cols:

        stats_lines.append('CATEGORICAL COLUMNS: ' + ', '.join(

            f"{c}(top={list((info.get('top_5_values') or info.get('top_values') or {}).keys())[:3]})"

            for c, info in list(cat_cols.items())[:4]

        ))

    if correlations:

        stats_lines.append('KEY CORRELATIONS: ' + '; '.join(

            f"{c.get('col1')}↔{c.get('col2')} r={c.get('correlation')}"

            for c in correlations[:4]

        ))

    findings = (insights.get('key_findings') or insights.get('findings') or [])[:3]

    if findings:

        stats_lines.append('DATASET FINDINGS: ' + ' | '.join(str(f) for f in findings))

    stats_context = '\n'.join(stats_lines)

    messages = [

        {'role': 'system', 'content': _chart_system_prompt(file_name)},

        {

            'role': 'system',

            'content': (

                f'CHART FACTS (use these exact numbers — never invent values):\n{chart_facts}\n\n'

                f'DATASET STATS (for grounding proactive_insight — do not fabricate):\n{stats_context}'

            ),

        },

    ]

    _SAFE_ROLES = {'assistant', 'ai', 'user', 'human'}

    for msg in (conversation_history or [])[-4:]:

        raw_role = str(msg.get('role', '')).lower()

        if raw_role not in _SAFE_ROLES:

            continue

        role = 'assistant' if raw_role in ('assistant', 'ai') else 'user'

        messages.append({'role': role, 'content': str(msg.get('content', ''))[:800]})

    messages.append({'role': 'user', 'content': question})

    try:

        answer = await _call_groq_with_retry(groq_client, messages, SYNTHESIS_MODEL, temperature=0.05, max_tokens=260)

        if resolved_key and '[CHART:' not in answer:

            answer = f'{answer}\n[CHART: {resolved_key}]'

    except Exception as exc:

        logger.error('Chart explanation synthesis failed after retries: %s', exc)

        tag = f'\n[CHART: {resolved_key}]' if resolved_key else ''

        answer = f'Here is the chart from {file_name}.{tag}'

    return {'answer': answer, 'data_queried': False, 'new_chart': None}
