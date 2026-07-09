from __future__ import annotations
import asyncio
import json
import logging
import os
from typing import Any, Optional
logger = logging.getLogger(__name__)
SYNTHESIS_MODEL = os.getenv('GROQ_SYNTHESIS_MODEL', 'llama-3.3-70b-versatile')
INTENT_MODEL = os.getenv('GROQ_INTENT_MODEL', 'llama-3.1-8b-instant')
_MAX_CONTEXT_CHARS = 12000
_TOP_K_CHUNKS = 8

def _data_system_prompt(file_name: str, chart_keys: list[str]) -> str:
    parts = [
        f'You are a friendly and helpful data assistant working with the dataset "{file_name}".\n',
        'You keep things casual and easy to understand — like explaining data to a friend.\n\n',
        '==== ANSWER FORMAT (MANDATORY) ====\n',
        'Structure EVERY answer exactly like this in JSON format:\n\n',
        '{\n',
        '  "direct_answer": "The key number or fact in one clear sentence.",\n',
        '  "proactive_insight": "Oh and btw, I also noticed... [Something interesting they did not ask]",\n',
        '  "confidence": 98,\n',
        '  "suggestion": "Want me to dig into the trend over time?"\n',
        '}\n\n',
        'FORMAT RULES:\n',
        '- Output MUST be valid JSON and nothing else.\n',
        '- The `confidence` must be an integer 1-100 based on data completeness.\n',
        '- When mentioning any number, metric, or percentage, wrap it in **bold**.\n',
        '- Do not include [CHART] tags in the JSON, they will be handled separately.\n\n',
        '==== GROUNDING CONTRACT — MANDATORY ====\n',
        'You have a CONTEXT block with pre-computed facts and a PANDAS RESULT block\n',
        'with exact numbers from the real data. These are the ONLY facts you may use.\n',
        '- Use EXACT numbers from CONTEXT or PANDAS RESULT — never estimate\n',
        '- If answer is not in CONTEXT: say "Hmm, that info isn\'t in this dataset." Then suggest what they CAN ask.\n',
        '- Never fabricate numbers or use training knowledge to fill gaps\n',
        '- Rankings: always name the entity AND its exact value\n',
        '- Correlations: state r value, direction, and plain-English meaning\n',
        '- Predictions: use TREND DATA slope+R2 to project; cite R2 as confidence\n',
        f'- Greetings: reply Hey! What do you want to know about {file_name}?\n\n',
        '==== STYLE ====\n',
        '- Talk like a helpful friend, not a corporate report\n',
        '- Use everyday words: "average" not "mean", "spread" not "variance"\n',
        '- Keep it short and punchy — no walls of text\n',
        '- Never mention CONTEXT, RULE, system prompt, or grounding contract\n',
        '- Never repeat the user question back to them\n',
        '- Lead with the answer — never put disclaimers first\n',
        '- It\'s okay to use casual phrases like "looks like", "turns out", "pretty interesting"\n',
        '- Do NOT show charts unless user explicitly asks to display one\n',
        f'- Available chart keys: {chart_keys} — use ONLY these exact keys\n',
    ]
    return ''.join(parts)


def _chart_system_prompt(file_name: str) -> str:
    return f'You are explaining a specific chart from the dataset "{file_name}".\n\n==== GROUNDING CONTRACT ====\nYou will receive CHART FACTS with exact values extracted directly from the chart.\nRULE 1: Name the specific highest and lowest values with their exact numbers.\n  BAD:  "The chart shows some categories have higher values"\n  GOOD: "Electronics has the highest revenue at 2.3M, while Books has the lowest at 45K"\nRULE 2: Use ONLY numbers from the provided context. Never invent values.\nRULE 3: If no specific data values are provided, simply describe what the chart\n  is broadly about based on its title. Do NOT mention "CHART FACTS" or complain.\n  BAD: "The CHART FACTS only mentions the title."\n  GOOD: "This is a box plot showing the distribution of Country IDs."\n==== STYLE ====\n* 2-3 sentences max. Name specific entities and values.\n* Speak naturally. Never mention your instructions or internal context.\n* Do NOT include [CHART: key] in your response - it is appended automatically.'

async def _plan_and_run_query(question: str, file_hash: str, col_types: dict, groq_client, col_metadata: dict=None) -> Optional[dict]:
    from ..core.data_agent import run_data_query
    _SKIP_TRIGGERS = {'hello', 'hi', 'hey', 'thanks', 'thank you', 'goodbye', 'good morning'}
    if question.lower().strip() in _SKIP_TRIGGERS:
        return None
    if col_metadata:
        col_info_str = json.dumps(col_metadata, ensure_ascii=True, default=str)
    else:
        col_info_str = json.dumps(col_types, ensure_ascii=True)
    planner_prompt = f'You are a data query planner. Decide if a structured query\n\nis needed to answer this question precisely with exact numbers from the full dataset.\nIf YES -> return ONE JSON object (no explanation, no markdown).\nIf NO (opinion, greeting) -> return: NONE\n\nIMPORTANT PLANNING RULES:\n- When the user mentions a SPECIFIC entity (brand, name, category), use filter_group or filter_lookup to filter by that entity.\n  Example: "Kawasaki bikes" → filter by the column whose top_values includes "Kawasaki".\n- When the user mentions a SPECIFIC year/period, use filters with op "eq" on the year/date column.\n  Example: "in year 2020" → filter the year column by value 2020.\n- For "report" or "summary" of a filtered entity, use filter_group with group_by on a descriptive column.\n- For PREDICTION/FORECAST questions ("what will X be in 2030?", "predict future sales"), use "trend" query to get the slope and R-squared. This gives the data needed for extrapolation.\n  Example: "predict sales in 2030" → {{"type":"trend","params":{{"time_col":"Year","val_col":"Sales"}}}}\n- For questions about growth/change over time, use "year_summary" to get yearly aggregates.\n- Use the column metadata below to identify which column contains a mentioned value.\n\nAVAILABLE QUERY TYPES:\nfilter_lookup   -> look up a column value by filtering another\n  example: {{"type":"filter_lookup","params":{{"filter_col":"name","filter_val":"Alice","result_col":"salary"}}}}\ntop_n           -> highest N rows by a numeric column\n  example: {{"type":"top_n","params":{{"column":"Revenue","n":5}}}}\nbottom_n        -> lowest N rows\n  example: {{"type":"bottom_n","params":{{"column":"Price","n":3}}}}\ngroup_aggregate -> group by one column, aggregate another\n  example: {{"type":"group_aggregate","params":{{"group_by":"Region","column":"Sales","func":"sum","n":10}}}}\nfilter_group    -> filter rows then group+aggregate\n  example: {{"type":"filter_group","params":{{"group_by":"Brand","func":"count","n":5,"filters":[{{"column":"Year","op":"eq","value":"2023"}}]}}}}\naggregate       -> single stat on one column\n  example: {{"type":"aggregate","params":{{"column":"Price","func":"mean"}}}}\n  funcs: mean, sum, min, max, count, nunique, median, std\nvalue_counts    -> count occurrences of each category\n  example: {{"type":"value_counts","params":{{"column":"Category","n":10}}}}\nsearch          -> full-text search for a specific named entity\n  example: {{"type":"search","params":{{"value":"John Smith","n":3}}}}\ndistinct        -> list all unique values in a column\n  example: {{"type":"distinct","params":{{"column":"Country"}}}}\ntrend           -> linear trend/slope of a numeric column over time\n  example: {{"type":"trend","params":{{"time_col":"Year","val_col":"Revenue"}}}}\nyear_summary    -> aggregate a numeric column by year (or other time bucket)\n  example: {{"type":"year_summary","params":{{"time_col":"Date","val_col":"Sales","func":"sum"}}}}\nrow_count       -> count rows matching a filter\n  example: {{"type":"row_count","params":{{"filters":[{{"column":"Status","op":"eq","value":"Active"}}]}}}}\ncorrelation     -> correlation between two numeric columns\n  example: {{"type":"correlation","params":{{"column":"Price","column2":"Sales"}}}}\npercentile      -> compute percentile of a numeric column\n  example: {{"type":"percentile","params":{{"column":"Age","percentile":90}}}}\nFILTER OPS: eq, neq, gt, lt, gte, lte, contains, year, month, isnull, notnull\nDATASET COLUMNS (with sample values and ranges):\n{col_info_str}\nQUESTION: {question}\n\nReturn ONLY the JSON object or the word NONE. No explanation whatsoever.'
    try:
        resp = await asyncio.to_thread(groq_client.chat.completions.create, model=INTENT_MODEL, messages=[{'role': 'user', 'content': planner_prompt}], max_tokens=250, temperature=0)
        raw = (resp.choices[0].message.content or '').strip()
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
        result = await asyncio.to_thread(run_data_query, file_hash, qtype, params)
        is_empty = not result or 'error' in result or result.get('result') == 'No rows found.' or (isinstance(result.get('result'), list) and len(result['result']) == 0)
        _RELATIONSHIP_WORDS = ('relationship', 'correlat', 'depend', 'associat', 'influenc')
        is_relationship_q = any((w in question.lower() for w in _RELATIONSHIP_WORDS))
        if is_empty and (not is_relationship_q):
            stop = {'what', 'when', 'where', 'which', 'does', 'have', 'many', 'much', 'show', 'tell', 'give', 'find', 'list', 'this', 'that', 'the', 'and', 'for', 'from', 'with', 'how', 'are', 'was', 'were'}
            words = [w.strip('?.,!') for w in question.split() if len(w) > 3 and w.lower() not in stop]
            if words:
                fallback = await asyncio.to_thread(run_data_query, file_hash, 'search', {'value': ' '.join(words[:2]), 'n': 5})
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
        outlier_items = [(col, info.get('count', 0)) for (col, info) in outliers.items() if info.get('count', 0) > 0]
        if outlier_items:
            parts.append('=== OUTLIER SUMMARY ===')
            for (col, cnt) in outlier_items[:8]:
                parts.append(f'  {col}: {cnt} outliers detected')
    return '\n'.join(parts)

def _assemble_context(chunks: list[dict], data_result: Optional[dict], static_ctx: str='') -> str:
    parts: list[str] = []
    if static_ctx:
        parts.append(static_ctx)
        parts.append('')
    if chunks:
        parts.append('=== RELEVANT DATASET FACTS (retrieved for this question) ===')
        for chunk in chunks:
            ctype = chunk.get('chunk_type', 'fact').upper()
            parts.append(f"[{ctype}] {chunk['text']}")
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
            x_vals = list(trace.get('x') or [])[:25]
            y_vals = list(trace.get('y') or [])[:25]
            labels = list(trace.get('labels') or [])[:25]
            values = list(trace.get('values') or [])[:25]
            if labels and values:
                pairs = [f"'{lbl}': {val}" for (lbl, val) in zip(labels[:12], values[:12])]
                facts.append(f"Data ({ttype}): {', '.join(pairs)}.")
            elif x_vals and y_vals:
                numeric_y = [(i, float(v)) for (i, v) in enumerate(y_vals) if isinstance(v, (int, float))]
                if numeric_y:
                    (max_i, max_v) = max(numeric_y, key=lambda t: t[1])
                    (min_i, min_v) = min(numeric_y, key=lambda t: t[1])
                    avg_v = sum((v for (_, v) in numeric_y)) / len(numeric_y)
                    x_at_max = x_vals[max_i] if max_i < len(x_vals) else '?'
                    x_at_min = x_vals[min_i] if min_i < len(x_vals) else '?'
                    facts.append(f"Chart type: {ttype}. Highest value: '{x_at_max}' = {max_v:,.2f}. Lowest value: '{x_at_min}' = {min_v:,.2f}. Average: {avg_v:,.2f}. All X-axis labels: {x_vals[:20]}. All Y-axis values: {[round(v, 2) for (_, v) in numeric_y[:20]]}.")
        return ' '.join(facts)
    except Exception as exc:
        logger.warning("extract_chart_facts failed for '%s': %s", chart_key, exc)
        return f"Could not read data from chart '{chart_key}'."

def _sanitize_llm_output(text: str) -> str:
    import re as _re
    if '{' in text and '}' in text:
        return text.strip()
    text = _re.sub('[=]{3,}[^=\\n]*[=]{3,}', '', text)
    text = _re.sub('RULE \\d+\\s*\\([^)]*\\)[^.]*\\.', '', text)
    text = _re.sub('RULE \\d+:', '', text)
    text = _re.sub('CONTEXT:\\s*', '', text, flags=_re.IGNORECASE)
    text = _re.sub('As an AI[^.]*\\.\\s*', '', text, flags=_re.IGNORECASE)
    text = _re.sub('As a (language|AI|data)[^.]*\\.\\s*', '', text, flags=_re.IGNORECASE)
    text = _re.sub('(?:GROUNDING CONTRACT|CHART DISPLAY RULES|MANDATORY)[^.]*\\.?', '', text, flags=_re.IGNORECASE)
    text = _re.sub('\\[?(NUMERIC|CATEGORICAL|DATASET|CORRELATION|DATA QUALITY)\\s*\\w*\\s*\\w*\\]?', '', text)
    text = _re.sub('\\n{3,}', '\n\n', text)
    text = _re.sub('  +', ' ', text)
    return text.strip()

async def answer_question(question: str, file_hash: str, file_name: str, stats: dict, insights: dict, chart_keys: list[str], conversation_history: list[dict], groq_client, redis_client) -> dict:
    import time as _time
    from .indexer import retrieve_chunks
    from .pandas_executor import classify_question, execute_pandas_with_retry, format_result, build_rich_context, is_challenge
    from ..core.data_agent import _load_df
    if is_challenge(question) and conversation_history:
        last_answer = ''
        for msg in reversed(conversation_history):
            if str(msg.get('role', '')).lower() in ('assistant', 'ai'):
                last_answer = str(msg.get('content', ''))
                break
        if last_answer:
            (df, load_err) = _load_df(file_hash)
            if df is not None and (not df.empty):
                try:
                    q = conversation_history[-2].get('content', question) if len(conversation_history) >= 2 else question
                    exec_result = await execute_pandas_with_retry(question=q, df=df, groq_client=groq_client, max_retries=1)
                    if not exec_result['error']:
                        formatted = format_result(exec_result['result'])
                        confirm_msg = [{'role': 'system', 'content': _data_system_prompt(file_name, chart_keys)}, {'role': 'system', 'content': f"The user is asking you to confirm a previous answer. You re-ran the query and got this result:\nPANDAS RESULT (re-verified):\n{formatted}\n\nPrevious answer was: {last_answer[:300]}\n\nConfirm the result confidently. In the `direct_answer` JSON field, say 'Yes, confirmed — ' then restate the key number. Do NOT change the answer, make sure to output the required JSON format."}, {'role': 'user', 'content': question}]
                        completion = await asyncio.to_thread(groq_client.chat.completions.create, model=SYNTHESIS_MODEL, messages=confirm_msg, temperature=0, max_tokens=300)
                        answer = _sanitize_llm_output((completion.choices[0].message.content or '').strip())
                        new_chart = None
                        if exec_result.get('fig_dict'):
                            new_chart = {'id': f'gen_{int(_time.time())}', 'fig': exec_result['fig_dict']}
                        return {'answer': answer, 'data_queried': True, 'new_chart': new_chart, 'code': code}
                except Exception as exc:
                    logger.warning('Challenge re-verification failed: %s', exc)
    q_type = await classify_question(question, groq_client)
    logger.info("Question classified as: %s — '%s'", q_type, question[:80])
    static_ctx = _build_static_context(stats, insights)
    # --- Code-interpreter path: try for ALL question types ---
    (df, load_err) = _load_df(file_hash)
    if df is not None and (not df.empty):
        try:
            logger.info("Running robust Pandas execution loop (with self-correction)")
            exec_result = await execute_pandas_with_retry(question=question, df=df, groq_client=groq_client, max_retries=3)
            if not exec_result['error']:
                code = exec_result['code']
                formatted = format_result(exec_result['result'])
                rich_ctx = build_rich_context(exec_result['result'], df, question)
                messages = [{'role': 'system', 'content': _data_system_prompt(file_name, chart_keys)}, {'role': 'system', 'content': f'PANDAS RESULT (computed from the REAL dataset — trust these numbers 100%):\n{formatted}\n\nADDITIONAL CONTEXT:\n{json.dumps(rich_ctx, default=str)}\n\nDATASET OVERVIEW:\n{static_ctx[:3000]}'}]
                _SAFE_ROLES = {'assistant', 'ai', 'user', 'human'}
                for msg in (conversation_history or [])[-4:]:
                    raw_role = str(msg.get('role', '')).lower()
                    if raw_role not in _SAFE_ROLES:
                        continue
                    role = 'assistant' if raw_role in ('assistant', 'ai') else 'user'
                    messages.append({'role': role, 'content': str(msg.get('content', ''))[:800]})
                messages.append({'role': 'user', 'content': question})
                completion = await asyncio.to_thread(groq_client.chat.completions.create, model=SYNTHESIS_MODEL, messages=messages, temperature=0.05, max_tokens=500)
                answer = _sanitize_llm_output((completion.choices[0].message.content or '').strip())
                new_chart = None
                if exec_result.get('fig_dict'):
                    new_chart = {'id': f'gen_{int(_time.time())}', 'fig': exec_result['fig_dict']}
                return {'answer': answer, 'data_queried': True, 'new_chart': new_chart, 'code': code}
            else:
                logger.warning('Pandas execution failed, falling back to RAG reasoning: %s', exec_result['error'])
        except Exception as exc:
            logger.warning('Code-interpreter path failed, falling back to RAG reasoning: %s', exc)
    elif load_err:
        logger.warning('Parquet load failed: %s', load_err)
    # --- Fallback: RAG-based reasoning path ---
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
    retrieval_coro = retrieve_chunks(file_hash=file_hash, question=question, k=_TOP_K_CHUNKS, redis_client=redis_client)
    if col_types:
        query_coro = _plan_and_run_query(question=question, file_hash=file_hash, col_types=col_types, col_metadata=col_metadata, groq_client=groq_client)
    else:

        async def _null_query() -> None:
            return None
        query_coro = _null_query()
    (chunks, data_result) = await asyncio.gather(retrieval_coro, query_coro)
    context = _assemble_context(chunks, data_result, static_ctx)
    messages = [{'role': 'system', 'content': _data_system_prompt(file_name, chart_keys)}, {'role': 'system', 'content': f'CONTEXT:\n{context}'}]
    _SAFE_ROLES = {'assistant', 'ai', 'user', 'human'}
    for msg in (conversation_history or [])[-6:]:
        raw_role = str(msg.get('role', '')).lower()
        if raw_role not in _SAFE_ROLES:
            continue
        role = 'assistant' if raw_role in ('assistant', 'ai') else 'user'
        messages.append({'role': role, 'content': str(msg.get('content', ''))[:1000]})
    messages.append({'role': 'user', 'content': question})
    has_chunks = bool(chunks)
    has_query = data_result is not None
    if not has_chunks and (not has_query) and (not static_ctx.strip()):
        messages.append({'role': 'system', 'content': "WARNING: No relevant data was found for this question. You MUST respond in the required JSON format with direct_answer: 'That is not in this dataset.' Then suggest what the user CAN ask about in the `suggestion` field."})
    try:
        temp = 0.05 if data_result is not None else 0.1
        completion = await asyncio.to_thread(groq_client.chat.completions.create, model=SYNTHESIS_MODEL, messages=messages, temperature=temp, max_tokens=700)
        answer = (completion.choices[0].message.content or '').strip()
        answer = _sanitize_llm_output(answer)
    except Exception as exc:
        logger.error('RAG synthesis failed: %s', exc)
        answer = 'I hit a temporary error generating your answer. Please try again in a moment.'
    return {'answer': answer, 'data_queried': data_result is not None, 'new_chart': None, 'code': None}

async def answer_chart_explanation(
    question: str, chart_key: str, chart_data: Any,
    file_name: str, file_hash: str, chart_keys: list[str],
    stats: dict, insights: dict,
    conversation_history: list[dict], groq_client, redis_client,
) -> dict:
    import re as _re
    from .indexer import retrieve_chunks

    # Self-healing key resolution: if chart_data is empty but chart_keys exist,
    # re-score them against the question and pick the best match
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

    # Build compact stats context to ground the LLM (prevent generic hallucinations)
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

    chunks = await retrieve_chunks(file_hash=file_hash, question=question, k=5, redis_client=redis_client)
    extra_context = '\n'.join(c['text'] for c in chunks)[:1500]

    messages = [
        {'role': 'system', 'content': _chart_system_prompt(file_name)},
        {
            'role': 'system',
            'content': (
                f'CHART FACTS (use these exact numbers — never invent values):\n{chart_facts}\n\n'
                f'DATASET STATS (for grounding proactive_insight — do not fabricate):\n{stats_context}\n\n'
                f'ADDITIONAL CONTEXT FROM RAG:\n{extra_context}'
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
        completion = await asyncio.to_thread(
            groq_client.chat.completions.create,
            model=SYNTHESIS_MODEL,
            messages=messages,
            temperature=0.05,
            max_tokens=260,
        )
        answer = (completion.choices[0].message.content or '').strip()
        if resolved_key and '[CHART:' not in answer:
            answer = f'{answer}\n[CHART: {resolved_key}]'
    except Exception as exc:
        logger.error('Chart explanation synthesis failed: %s', exc)
        tag = f'\n[CHART: {resolved_key}]' if resolved_key else ''
        answer = f'Here is the chart from {file_name}.{tag}'

    return {'answer': answer, 'data_queried': False, 'new_chart': None}