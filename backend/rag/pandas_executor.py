from __future__ import annotations
import asyncio
import json
import logging
import re
import traceback
from typing import Any, Optional
import numpy as np
import pandas as pd
logger = logging.getLogger(__name__)
_BANNED_PATTERNS = ['\\bimport\\b', '\\b__\\w+__\\b', '\\bexec\\b', '\\beval\\b', '\\bopen\\b', '\\bos\\b\\.', '\\bsys\\b\\.', '\\bsubprocess\\b', '\\bglobals\\b', '\\blocals\\b', '\\bgetattr\\b', '\\bsetattr\\b', '\\bdelattr\\b', '\\bcompile\\b', '\\b__builtins__\\b', '\\.to_csv\\b', '\\.to_excel\\b', '\\.to_parquet\\b', '\\.to_sql\\b', '\\brequests\\b', '\\burllib\\b']
_BANNED_RE = re.compile('|'.join(_BANNED_PATTERNS), re.IGNORECASE)
_MAX_RESULT_ROWS = 50
_MAX_RESULT_CHARS = 3000

async def classify_question(question: str, groq_client) -> str:
    q = question.lower().strip()
    _ANALYTICAL_SIGNALS = ('how many', 'how much', 'total', 'count of', 'number of', 'average', 'sum of', 'maximum', 'minimum', 'what is the', 'what are the', 'list all', 'show all', 'top ', 'bottom ', 'highest', 'lowest', 'most', 'least', 'sales report', 'report of', 'report for', 'bikes in', 'sold in', 'made in', 'manufactured in', 'built in', 'registered in', 'price of')
    if any((sig in q for sig in _ANALYTICAL_SIGNALS)):
        return 'analytical'
    _REASONING_SIGNALS = ('why', 'explain', 'what does', 'what do you think', 'compare', 'vs', 'versus', 'relationship between', 'correlat', 'suggest', 'recommend', 'insight', 'what should', 'what can we', 'interpret')
    if any((sig in q for sig in _REASONING_SIGNALS)):
        return 'reasoning'
    try:
        prompt = f'Classify this data question into ONE category:\n"analytical" — needs exact numbers: counts, sums, averages, filters, specific values, lookups, reports\n"reasoning" — needs interpretation: trends, comparisons, explanations, suggestions, why questions\n\nQuestion: "{question}"\n\nReply with ONLY one word: analytical or reasoning'
        resp = await asyncio.to_thread(groq_client.chat.completions.create, model='llama-3.1-8b-instant', messages=[{'role': 'user', 'content': prompt}], temperature=0, max_tokens=10)
        result = (resp.choices[0].message.content or '').strip().lower()
        if 'analytical' in result:
            return 'analytical'
        if 'reasoning' in result:
            return 'reasoning'
    except Exception as exc:
        logger.warning('Question classification failed: %s', exc)
    return 'analytical'

def _build_codegen_prompt(question: str, columns: list[str], dtypes: dict[str, str], sample_values: dict[str, list]) -> str:
    return f'You are a pandas expert. Write ONLY executable Python/pandas code.\n\nDATASET INFO:\n- DataFrame is already loaded as `df`\n- Columns: {columns}\n- Dtypes: {json.dumps(dtypes, default=str)}\n- Sample values per column: {json.dumps(sample_values, default=str)}\n\nQUESTION: "{question}"\n\nRULES:\n1. Store the final answer in a variable called `result`\n2. Use EXACT column names from the list above (case-sensitive)\n3. Do NOT import anything — `pd` and `np` are already available\n4. `result` must be a scalar, dict, Series, or small DataFrame\n5. For counts: use .shape[0] or .value_counts() or .groupby().size()\n6. For filters: match dtypes exactly. If a year column is int64, compare with int not string\n7. Always .head(20) on large results to prevent memory issues\n8. If the question asks about a specific entity (brand, model, state), FILTER for it\n9. For "sales report" or "report of X": compute count, average price, top models/states\n10. Never use print() — just assign to `result`\n\nReturn ONLY the code. No markdown fences. No explanation.'

async def generate_pandas_code(question: str, df: pd.DataFrame, groq_client) -> str:
    columns = df.columns.tolist()
    dtypes = {col: str(df[col].dtype) for col in columns}
    sample_values = {}
    for col in columns:
        if df[col].dtype == 'object' or str(df[col].dtype) == 'category':
            top = df[col].value_counts().head(5).index.tolist()
            sample_values[col] = [str(v) for v in top]
        elif pd.api.types.is_numeric_dtype(df[col]):
            sample_values[col] = [f'min={df[col].min()}', f'max={df[col].max()}']
    prompt = _build_codegen_prompt(question, columns, dtypes, sample_values)
    resp = await asyncio.to_thread(groq_client.chat.completions.create, model='llama-3.3-70b-versatile', messages=[{'role': 'system', 'content': 'You are a pandas code generator. Output ONLY valid Python code. No markdown. No explanation. No ```.'}, {'role': 'user', 'content': prompt}], temperature=0, max_tokens=500)
    code = (resp.choices[0].message.content or '').strip()
    if code.startswith('```'):
        code = code.split('\n', 1)[-1].rsplit('```', 1)[0].strip()
    return code

def _validate_code(code: str) -> Optional[str]:
    match = _BANNED_RE.search(code)
    if match:
        return f"Unsafe code detected: '{match.group()}'"
    if len(code) > 2000:
        return 'Generated code is too long (>2000 chars)'
    if 'result' not in code:
        return "Code does not assign to 'result' variable"
    return None

def safe_execute(code: str, df: pd.DataFrame) -> dict:
    error = _validate_code(code)
    if error:
        return {'result': None, 'error': error, 'code': code}
    namespace = {'df': df.copy(), 'pd': pd, 'np': np}
    try:
        exec(code, {'__builtins__': {}}, namespace)
    except Exception as exc:
        tb = traceback.format_exc().split('\n')[-3:]
        return {'result': None, 'error': f"Execution error: {exc}\n{''.join(tb)}", 'code': code}
    result = namespace.get('result')
    if result is None:
        return {'result': None, 'error': "Code ran but 'result' was None", 'code': code}
    return {'result': result, 'error': None, 'code': code}

def format_result(result: Any) -> str:
    if result is None:
        return 'No result'
    if isinstance(result, pd.DataFrame):
        if len(result) > _MAX_RESULT_ROWS:
            result = result.head(_MAX_RESULT_ROWS)
        return result.to_string(index=True, max_rows=_MAX_RESULT_ROWS)
    if isinstance(result, pd.Series):
        if len(result) > _MAX_RESULT_ROWS:
            result = result.head(_MAX_RESULT_ROWS)
        return result.to_string()
    if isinstance(result, dict):
        return json.dumps(result, indent=2, default=str)
    if isinstance(result, (list, tuple)):
        return json.dumps(list(result)[:_MAX_RESULT_ROWS], default=str)
    return str(result)

def build_rich_context(result: Any, df: pd.DataFrame, question: str) -> dict:
    ctx = {'total_records': len(df)}
    if isinstance(result, (int, float, np.integer, np.floating)):
        try:
            pct = float(result) / len(df) * 100
            if 0 < pct <= 100:
                ctx['as_percentage'] = f'{pct:.1f}% of {len(df):,} total records'
        except (ZeroDivisionError, ValueError):
            pass
    q_lower = question.lower()
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in q_lower or col_lower.replace('_', ' ') in q_lower:
            if pd.api.types.is_numeric_dtype(df[col]):
                ctx[f'{col}_stats'] = {'mean': round(float(df[col].mean()), 2), 'min': float(df[col].min()), 'max': float(df[col].max())}
            elif df[col].dtype == 'object':
                ctx[f'{col}_top'] = df[col].value_counts().head(3).to_dict()
    return ctx
_CHALLENGE_PHRASES = ('are you sure', 'confirm', 'double check', 'really', 'verify', 'is that correct', 'is that right', "that doesn't seem", "that can't be", 'check again', 'are you certain')

def is_challenge(question: str) -> bool:
    q = question.lower().strip()
    return any((phrase in q for phrase in _CHALLENGE_PHRASES))