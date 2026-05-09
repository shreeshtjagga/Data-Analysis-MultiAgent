import json
import logging
import os
from typing import Optional
import pandas as pd
from ..core.state import AnalysisState
from ..core.errors import add_pipeline_error
from ..core.utils import truncate_stats_for_llm, sanitize_for_json
from ..core.llm_client import get_groq_client
logger = logging.getLogger(__name__)

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
        lines.append(f"  {col}: mean={d.get('mean', 0):.2f}, range=[{d.get('min', 0):.2f}–{d.get('max', 0):.2f}], std={d.get('std', 0):.2f}, distribution={skew_desc}")
    categorical = stats.get('categorical_columns', {})
    for (col, d) in list(categorical.items())[:8]:
        lines.append(f"  {col}: {d.get('unique_values', 0)} categories, top='{d.get('most_common', '')}'")
    return '\n'.join(lines)

def _build_llm_prompt(slim_stats: dict) -> str:
    profile = slim_stats.get('dataset_profile') or {}
    domain = profile.get('domain', 'general')
    label = profile.get('label', 'dataset')
    col_narrative = _build_column_narrative(slim_stats)
    clean_stats = sanitize_for_json(slim_stats)
    payload_json = json.dumps(clean_stats, ensure_ascii=True)
    return '\n'.join([f'You are a friendly data analyst. Your job is to explain data findings in plain English that anyone can understand.', f'Dataset: {label} (domain: {domain})', '', 'Column summary:', col_narrative, '', 'Full stats (JSON):', f'<analysis_json>{payload_json}</analysis_json>', '', 'STRICT LANGUAGE RULES:', '- Use everyday words only. No jargon.', "- Say 'average' not 'mean'. Say 'most common' not 'modal'. Say 'spread' not 'variance'.", '- Every finding must include at least one specific number, percentage, or count.', '- Keep each sentence short (under 20 words).', '', 'Respond with ONLY valid JSON (no markdown, no explanation):', '{', '  "headline": "One plain-English conclusion from the data in one short sentence.",', '  "data_info": ["3-5 simple sentences about WHAT this dataset contains: its size, columns, and topic. No analysis here, just facts. EACH sentence is a SEPARATE array element."],', '  "findings": ["Put EACH finding in its OWN array element. 5-8 findings total. One sentence per element. Each must include a specific number. WRONG: one long string with all findings. RIGHT: ["Finding 1.", "Finding 2.", "Finding 3."]"],', '  "recommendations": ["ONLY include items here if a finding is genuinely actionable. Each recommendation is a SEPARATE array element. Examples: a dominant group worth focusing on, an outlier to investigate, a data quality issue, or a pattern with business implications. If nothing is clearly actionable, return an empty array []. Start each item with a verb."]', '}'])

def _llm_insights(stats: dict) -> Optional[dict]:
    api_key = os.getenv('GROQ_API_KEY')
    if not api_key:
        return None
    prompt = _build_llm_prompt(stats)
    domain = (stats.get('dataset_profile') or {}).get('domain', 'general')
    label = (stats.get('dataset_profile') or {}).get('label', 'dataset')
    try:
        client = get_groq_client()
        if not client:
            return None
        completion = client.chat.completions.create(model=os.getenv('GROQ_MODEL', 'llama-3.1-8b-instant'), messages=[{'role': 'system', 'content': f'You are a friendly data analyst explaining findings about {label} ({domain} domain). Write in plain, simple English — no jargon. Always respond with valid JSON only. No markdown fences.'}, {'role': 'user', 'content': prompt}], temperature=0.1, max_tokens=1200)
        raw = (completion.choices[0].message.content or '').strip()
        if raw.startswith('```'):
            raw = raw.split('\n', 1)[-1].rsplit('```', 1)[0].strip()
        result = json.loads(raw)
        required = {'headline', 'findings', 'data_info'}
        if not isinstance(result, dict) or not required.issubset(result.keys()):
            logger.warning('LLM insights returned unexpected schema: %s', list(result.keys()) if isinstance(result, dict) else type(result))
            return None
        if 'recommendations' not in result or not isinstance(result.get('recommendations'), list):
            result['recommendations'] = []
        if not isinstance(result.get('findings'), list) or len(result['findings']) == 0:
            logger.warning('LLM insights returned empty findings list')
            return None
        logger.info('LLM insights generated: %d findings, %d data_info', len(result.get('findings', [])), len(result.get('data_info', [])))
        return result
    except Exception as exc:
        logger.warning('LLM insights failed, falling back to rules: %s', exc)
        return None

def _rule_based_insights(stats: dict) -> dict:
    numeric_cols = list(stats.get('numeric_columns', {}).keys())
    categorical_cols = list(stats.get('categorical_columns', {}).keys())
    outliers = stats.get('outliers', {})
    correlations = stats.get('strong_correlations', [])
    dq = stats.get('data_quality', {})
    profile = stats.get('dataset_profile') or {}
    data_info = [f"This dataset contains {stats.get('row_count', 0):,} rows and {stats.get('column_count', 0)} columns."]
    if profile.get('label'):
        data_info.append(f"Dataset type: {profile['label']}." + (f" Domain: {profile['domain']}." if profile.get('domain') else ''))
    if numeric_cols:
        data_info.append(f"Numeric columns ({len(numeric_cols)}): {', '.join(numeric_cols[:6])}{('...' if len(numeric_cols) > 6 else '')}.")
    if categorical_cols:
        data_info.append(f"Categorical columns ({len(categorical_cols)}): {', '.join(categorical_cols[:6])}{('...' if len(categorical_cols) > 6 else '')}.")
    completeness = dq.get('completeness', 100)
    data_info.append(f"Data completeness is {completeness:.1f}% with {dq.get('missing_cells', 0)} missing values.")
    findings = []
    if outliers:
        findings.append(f"Outliers detected in {len(outliers)} column(s): {', '.join(list(outliers.keys())[:4])}.")
    if correlations:
        best = correlations[0]
        findings.append(f"Strongest correlation: {best['col1']} and {best['col2']} (r={best['correlation']:.2f}).")
        if len(correlations) > 1:
            findings.append(f'{len(correlations)} strong variable relationships found overall.')
    cat_stats = stats.get('categorical_columns', {})
    for (col, info) in list(cat_stats.items())[:2]:
        if info.get('most_common'):
            pct = round(info.get('most_common_count', 0) / max(stats.get('row_count', 1), 1) * 100, 1)
            findings.append(f"In '{col}', the most common value is '{info['most_common']}' ({pct}% of rows).")
    if not findings:
        findings.append('No strong patterns detected — the dataset may need more varied data for richer insights.')
    return {'headline': '', 'data_info': data_info, 'findings': findings}

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