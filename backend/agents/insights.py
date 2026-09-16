
import json

import logging

import os

from typing import Optional

import pandas as pd

from ..core.state import AnalysisState

from ..core.errors import add_pipeline_error

from ..core.utils import truncate_stats_for_llm, sanitize_for_json

from ..core.llm_client import get_groq_client, call_groq_with_fallback_sync

logger = logging.getLogger(__name__)

def _is_date_or_id(col: str) -> bool:

    cl = col.lower()

    return any(k in cl for k in ('date', 'time', 'timestamp', 'guid', 'uuid', '_id', 'id_')) or cl in ('id', 'pk', 'key')

def _build_column_narrative(stats: dict) -> str:

    lines = []

    numeric = stats.get('numeric_columns', {})

    for (col, d) in list(numeric.items())[:12]:

        skew_val = d.get('skewness', 0)

        if skew_val is None or pd.isna(skew_val):

            skew_val = 0

        if skew_val > 1:

            skew_desc = 'right-skewed'

        elif skew_val < -1:

            skew_desc = 'left-skewed'

        else:

            skew_desc = 'normal-ish'

        lines.append(f"  {col}: average={d.get('mean', 0):.2f}, range=[{d.get('min', 0):.2f} to {d.get('max', 0):.2f}], std={d.get('std', 0):.2f}, distribution={skew_desc}")

    categorical = stats.get('categorical_columns', {})

    valid_cats = {k: v for k, v in categorical.items() if not _is_date_or_id(k)}

    for (col, d) in list(valid_cats.items())[:8]:

        lines.append(f"  {col}: {d.get('unique_values', 0)} categories, top='{d.get('most_common', '')}'")

    date_cols = stats.get('datetime_columns') or stats.get('date_columns') or []

    if date_cols:

        lines.append(f"  Temporal columns: {', '.join(str(c) for c in date_cols[:3])}")

    return '\n'.join(lines)

def _build_llm_prompt(slim_stats: dict) -> str:

    profile = slim_stats.get('dataset_profile') or {}

    domain = profile.get('domain', 'general')

    label = profile.get('label', 'dataset')

    col_narrative = _build_column_narrative(slim_stats)

    clean_stats = sanitize_for_json(slim_stats)

    payload_json = json.dumps(clean_stats, ensure_ascii=True)

    return '\n'.join([

        f'You are a senior data analyst. Explain findings in plain English that business decision-makers understand.',

        f'Dataset: {label} (domain: {domain})',

        '',

        'Column summary:',

        col_narrative,

        '',

        'Full stats (JSON):',

        f'<analysis_json>{payload_json}</analysis_json>',

        '',

        'STRICT RULES:',

        '- Use clean, professional English. Say "average" not "mean".',

        '- Focus on meaningful patterns, top performing categories, and key metric drivers.',

        '- NEVER report meaningless date singletons (e.g. do NOT say "most common date is X with 0.8% of rows").',

        '- Every finding must include at least one concrete number, percentage, or currency figure.',

        '- "recommendations" MUST contain 3-5 high-impact, actionable business recommendations starting with an action verb.',

        '',

        'Respond with ONLY valid JSON (no markdown, no code fences):',

        '{',

        '  "headline": "One clear executive summary conclusion in a single strong sentence.",',

        '  "data_info": ["3-5 clear sentences describing the dataset scope, row count, key metrics, and domain context. Each sentence is a separate string."],',

        '  "findings": ["5-7 distinct analytical findings covering top segments, distributions, averages, and correlations. Each is a separate string."],',

        '  "recommendations": ["3-5 strategic, actionable recommendations starting with verbs (e.g. Focus, Optimize, Audit, Scale). Each is a separate string."]',

        '}'

    ])

def _llm_insights(stats: dict) -> Optional[dict]:

    api_key = os.getenv('GROQ_API_KEY')

    if not api_key:

        return None

    prompt = _build_llm_prompt(stats)

    domain = (stats.get('dataset_profile') or {}).get('domain', 'general')

    label = (stats.get('dataset_profile') or {}).get('label', 'dataset')

    try:

        raw = call_groq_with_fallback_sync(

            messages=[

                {'role': 'system', 'content': f'You are an executive data analyst for {label} ({domain} domain). Write in clear, compelling English. Always respond with valid JSON only.'},

                {'role': 'user', 'content': prompt}

            ],

            primary_model=os.getenv('GROQ_MODEL', 'qwen/qwen3.8-27b'),

            temperature=0.1,

            max_tokens=1200

        )

        if raw.startswith('```'):

            raw = raw.split('\n', 1)[-1].rsplit('```', 1)[0].strip()

        result = json.loads(raw)

        required = {'headline', 'findings', 'data_info'}

        if not isinstance(result, dict) or not required.issubset(result.keys()):

            logger.warning('LLM insights returned unexpected schema: %s', list(result.keys()) if isinstance(result, dict) else type(result))

            return None

        if not isinstance(result.get('findings'), list) or len(result['findings']) == 0:

            logger.warning('LLM insights returned empty findings list')

            return None

        clean_findings = []

        for f in result.get('findings', []):

            f_str = str(f)

            if '0.8%' in f_str or 'most common value is' in f_str.lower() and ('date' in f_str.lower() or '-' in f_str):

                continue

            clean_findings.append(f_str)

        if clean_findings:

            result['findings'] = clean_findings

        recs = result.get('recommendations')

        if not isinstance(recs, list) or len(recs) < 2:

            rules_recs = _rule_based_insights(stats).get('recommendations', [])

            result['recommendations'] = rules_recs if not recs else recs + rules_recs[:max(0, 3 - len(recs))]

        logger.info('LLM insights generated: %d findings, %d data_info, %d recommendations', len(result.get('findings', [])), len(result.get('data_info', [])), len(result.get('recommendations', [])))

        return result

    except Exception as exc:

        logger.warning('LLM insights failed, falling back to rules: %s', exc)

        return None

def _rule_based_insights(stats: dict) -> dict:

    numeric_cols = stats.get('numeric_columns', {})

    categorical_cols = stats.get('categorical_columns', {})

    outliers = stats.get('outliers', {})

    correlations = stats.get('strong_correlations', [])

    dq = stats.get('data_quality', {})

    profile = stats.get('dataset_profile') or {}

    row_count = stats.get('row_count', 0)

    col_count = stats.get('column_count', 0)

    completeness = dq.get('completeness', 100)

    headline_parts = []

    top_num = list(numeric_cols.keys())[0] if numeric_cols else None

    top_cat = None

    for c, info in categorical_cols.items():

        if not any(d in c.lower() for d in ('date', 'time', 'id', 'guid', 'uuid')):

            top_cat = c

            break

    if not top_cat and categorical_cols:

        top_cat = list(categorical_cols.keys())[0]

    if top_num and top_cat:

        num_info = numeric_cols[top_num]

        cat_info = categorical_cols[top_cat]

        top_val = cat_info.get('most_common', 'Top Segment')

        pct = round(cat_info.get('most_common_count', 0) / max(row_count, 1) * 100, 1)

        mean_val = num_info.get('mean', 0)

        headline = f"Analysis of {row_count:,} records reveals primary concentration in '{top_val}' ({pct}% of {top_cat}), with average {top_num} of {mean_val:,.2f}."

    elif top_num:

        num_info = numeric_cols[top_num]

        headline = f"Dataset encompasses {row_count:,} entries with {top_num} averaging {num_info.get('mean', 0):,.2f} across all observations."

    else:

        headline = f"Comprehensive review of {row_count:,} observations across {col_count} features completed with {completeness:.1f}% data completeness."

    data_info = [

        f"This dataset comprises {row_count:,} verified records across {col_count} distinct attributes.",

        f"Data quality is evaluated at {completeness:.1f}% completeness with {dq.get('missing_cells', 0)} missing values across the matrix.",

    ]

    if numeric_cols:

        num_names = list(numeric_cols.keys())

        data_info.append(f"Numeric metrics tracked ({len(num_names)}): {', '.join(num_names[:5])}{'...' if len(num_names) > 5 else ''}.")

    if categorical_cols:

        cat_names = [c for c in categorical_cols.keys() if not any(d in c.lower() for d in ('date', 'time', 'id'))]

        if cat_names:

            data_info.append(f"Categorical dimensions ({len(cat_names)}): {', '.join(cat_names[:5])}{'...' if len(cat_names) > 5 else ''}.")

    date_range = stats.get('date_range')

    if date_range and date_range.get('min') and date_range.get('max'):

        data_info.append(f"Temporal coverage spans from {date_range.get('min')} through {date_range.get('max')}.")

    findings = []

    metric_cols = {c: info for c, info in numeric_cols.items() if not any(w in c.lower() for w in ('year', 'id', 'code', 'zip', 'index'))}

    display_numerics = metric_cols if metric_cols else numeric_cols

    for col, info in list(display_numerics.items())[:2]:

        m_val = info.get('mean', 0)

        min_v = info.get('min', 0)

        max_v = info.get('max', 0)

        if any(w in col.lower() for w in ('year', 'yr')):

            findings.append(f"{col} spans from {int(min_v)} to {int(max_v)} (median: {int(info.get('median', m_val))}).")

        else:

            findings.append(f"Average {col} is {m_val:,.2f}, ranging from a minimum of {min_v:,.2f} up to a peak of {max_v:,.2f}.")

    for col, info in list(categorical_cols.items())[:3]:

        if any(d in col.lower() for d in ('date', 'time', 'id', 'guid', 'uuid')):

            continue

        if info.get('most_common'):

            pct = round(info.get('most_common_count', 0) / max(row_count, 1) * 100, 1)

            findings.append(f"In '{col}', the leading category is '{info['most_common']}' accounting for {pct}% of total records ({info.get('unique_values', '?')} unique categories).")

    if correlations:

        best = correlations[0]

        c1, c2, r_val = best.get('col1'), best.get('col2'), float(best.get('correlation', 0))

        direction = "positive" if r_val > 0 else "negative"

        strength = "strong" if abs(r_val) >= 0.7 else "moderate" if abs(r_val) >= 0.4 else "slight"

        findings.append(f"Observed {strength} {direction} correlation between {c1} and {c2} (r={r_val:.2f}), showing significant joint variance.")

    if outliers:

        outlier_cols = [c for c, o in outliers.items() if o.get('count', 0) > 0]

        if outlier_cols:

            findings.append(f"Statistical anomalies identified in {len(outlier_cols)} metric(s): {', '.join(outlier_cols[:3])} via IQR fence boundaries.")

    recommendations = []

    if top_cat and top_cat in categorical_cols:

        cat_info = categorical_cols[top_cat]

        top_val = cat_info.get('most_common', 'primary segment')

        recommendations.append(f"Prioritize strategic resource allocation in '{top_val}' ({top_cat}) to maximize efficiency in your highest-volume segment.")

    if correlations:

        best = correlations[0]

        recommendations.append(f"Leverage the relationship between {best.get('col1')} and {best.get('col2')} to forecast performance and optimize KPIs.")

    if outliers:

        outlier_cols = [c for c, o in outliers.items() if o.get('count', 0) > 0]

        if outlier_cols:

            recommendations.append(f"Audit high-variance outlier entries in {outlier_cols[0]} to capture exceptional opportunities or mitigate risk.")

    if len(recommendations) < 3:

        recommendations.append("Establish periodic benchmark tracking against historical medians to sustain growth trends.")

    return {

        'headline': headline,

        'data_info': data_info,

        'findings': findings,

        'recommendations': recommendations

    }

def _computed_insights(stats: dict) -> dict:

    outliers = stats.get('outliers', {})

    correlations = stats.get('strong_correlations', [])

    numeric = stats.get('numeric_columns', {})

    outlier_summary = {}

    for (col, info) in outliers.items():

        outlier_summary[col] = f"{info.get('count', 0)} outliers ({info.get('percentage', 0):.2f}%)"

    correlation_insights = [f"{c['col1']} and {c['col2']} are strongly correlated ({c['correlation']:.3f})" for c in correlations[:5]]

    distribution_insights = []

    for (col, col_stats) in list(numeric.items())[:10]:

        skewness = col_stats.get('skewness', 0)

        if abs(skewness) < 0.5:

            dist_type = 'approximately normal'

        elif skewness > 0:

            dist_type = 'right-skewed'

        else:

            dist_type = 'left-skewed'

        distribution_insights.append(f"'{col}' distribution: {dist_type}")

    return {'outlier_summary': outlier_summary, 'correlation_insights': correlation_insights, 'distribution_insights': distribution_insights}

def insights_agent(state: AnalysisState) -> AnalysisState:

    state.current_agent = 'insights'

    logger.info('Insights agent started')

    try:

        stats = state.stats_summary or {}

        if not stats or not stats.get('row_count'):

            logger.warning('stats_summary is empty or partial — using safe minimal insights.')

            state.insights = {'headline': 'Dataset was loaded but full statistical analysis could not be completed.', 'data_info': ['The dataset was uploaded successfully.'], 'findings': ['Statistical analysis encountered an issue. The dataset may contain unusual formatting. Try re-uploading or checking for special characters.'], 'outlier_summary': {}, 'correlation_insights': [], 'distribution_insights': []}

            state.completed_agents.append('insights')

            return state

        slim_stats = truncate_stats_for_llm(stats)

        llm_result = _llm_insights(slim_stats)

        if llm_result:

            raw_findings = llm_result.get('findings', [])

            if isinstance(raw_findings, list) and len(raw_findings) <= 2:

                split_findings = []

                for item in raw_findings:

                    if not isinstance(item, str):

                        split_findings.append(str(item))

                        continue

                    if len(item) > 120 and '. ' in item:

                        parts = [s.strip() + '.' for s in item.split('. ') if s.strip()]

                        parts = [p.replace('..', '.') for p in parts]

                        split_findings.extend(parts)

                    else:

                        split_findings.append(item)

                raw_findings = split_findings

            insights = {'headline': llm_result.get('headline'), 'data_info': llm_result.get('data_info', []), 'findings': raw_findings, 'recommendations': llm_result.get('recommendations', [])}

        else:

            insights = _rule_based_insights(stats)

        insights.update(_computed_insights(stats))

        state.insights = insights

        logger.info('Insights complete. %d data_info, %d findings (LLM=%s)', len(insights.get('data_info', [])), len(insights.get('findings', [])), llm_result is not None)

    except Exception as e:

        logger.error('Insights error: %s', e)

        add_pipeline_error(state.errors, code='INSIGHTS_FAILED', message=str(e), agent='insights', error_type='agent')

        if not state.insights:

            state.insights = {'headline': '', 'data_info': [], 'findings': ['Insights generation encountered an error.'], 'outlier_summary': {}, 'correlation_insights': [], 'distribution_insights': []}

    state.completed_agents.append('insights')

    return state
