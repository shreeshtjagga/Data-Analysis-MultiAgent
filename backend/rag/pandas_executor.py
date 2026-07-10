from __future__ import annotations
import ast
import asyncio
import json
import logging
import re
import threading
import traceback
from typing import Any, Optional
import numpy as np
import pandas as pd
logger = logging.getLogger(__name__)
from ..core.llm_client import call_groq_with_fallback

_BANNED_PATTERNS = [
    r'\bimport\b', r'\bexec\b', r'\beval\b', r'\bopen\b',
    r'\bos\b\.', r'\bsys\b\.', r'\bsubprocess\b',
    r'\bglobals\b', r'\blocals\b',
    r'\bgetattr\b', r'\bsetattr\b', r'\bdelattr\b', r'\bcompile\b',
    r'\b__builtins__\b',
    r'\.to_csv\b', r'\.to_excel\b', r'\.to_parquet\b', r'\.to_sql\b',
    r'\brequests\b', r'\burllib\b',
    r'\bbreakpoint\b', r'\binput\b', r'\bprint\b',
]
_BANNED_RE = re.compile('|'.join(_BANNED_PATTERNS), re.IGNORECASE)

_ALLOWED_NAME_ROOTS = frozenset({
    'df', 'pd', 'np', 'result', 'True', 'False', 'None',
    'len', 'int', 'float', 'str', 'bool', 'list', 'dict', 'tuple', 'set',
    'round', 'abs', 'min', 'max', 'sum', 'sorted', 'enumerate', 'zip', 'range',
    'isinstance', 'type',
})

_BANNED_AST_NODES = (
    ast.Import, ast.ImportFrom,
)

_MAX_EXEC_TIMEOUT_SECONDS = 10
_MAX_RESULT_ROWS = 50
_MAX_RESULT_CHARS = 3000


def _validate_ast(code: str) -> Optional[str]:
    """Parse code into AST and check for unsafe patterns.
    Returns an error string if unsafe, None if OK."""
    try:
        tree = ast.parse(code, mode='exec')
    except SyntaxError as exc:
        return f'Syntax error in generated code: {exc}'

    for node in ast.walk(tree):
        if isinstance(node, _BANNED_AST_NODES):
            return f'Unsafe AST node: {type(node).__name__}'

        if isinstance(node, ast.Attribute):
            attr = node.attr
            if attr.startswith('__') and attr.endswith('__'):
                return f"Unsafe dunder access: '{attr}'"
            if attr in ('to_csv', 'to_excel', 'to_parquet', 'to_sql', 'to_pickle',
                        'to_hdf', 'to_feather', 'to_clipboard'):
                return f"Blocked I/O method: '{attr}'"

        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id in (
                'exec', 'eval', 'compile', 'open', '__import__',
                'getattr', 'setattr', 'delattr', 'globals', 'locals',
                'breakpoint', 'input', 'exit', 'quit',
            ):
                return f"Blocked function call: '{func.id}'"

        if isinstance(node, ast.Starred):
            pass

    return None


async def classify_question(question: str) -> str:
    q = question.lower().strip()
    _ANALYTICAL_SIGNALS = ('how many', 'how much', 'total', 'count of', 'number of', 'average', 'sum of', 'maximum', 'minimum', 'what is the', 'what are the', 'list all', 'show all', 'top ', 'bottom ', 'highest', 'lowest', 'most', 'least', 'sales report', 'report of', 'report for', 'bikes in', 'sold in', 'made in', 'manufactured in', 'built in', 'registered in', 'price of')
    if any((sig in q for sig in _ANALYTICAL_SIGNALS)):
        return 'analytical'
    _REASONING_SIGNALS = ('why', 'explain', 'what does', 'what do you think', 'compare', 'vs', 'versus', 'relationship between', 'correlat', 'suggest', 'recommend', 'insight', 'what should', 'what can we', 'interpret')
    if any((sig in q for sig in _REASONING_SIGNALS)):
        return 'reasoning'
    try:
        prompt = f'Classify this data question into ONE category:\n"analytical" — needs exact numbers: counts, sums, averages, filters, specific values, lookups, reports\n"reasoning" — needs interpretation: trends, comparisons, explanations, suggestions, why questions\n\nQuestion: "{question}"\n\nReply with ONLY one word: analytical or reasoning'
        result = await call_groq_with_fallback(
            messages=[{'role': 'user', 'content': prompt}],
            primary_model='llama-3.1-8b-instant',
            temperature=0,
            max_tokens=10
        )
        result = result.lower()
        if 'analytical' in result:
            return 'analytical'
        if 'reasoning' in result:
            return 'reasoning'
    except Exception as exc:
        logger.warning('Question classification failed: %s', exc)
    return 'analytical'

def _build_codegen_prompt(question: str, columns: list[str], dtypes: dict[str, str], sample_values: dict[str, list]) -> str:
    return f'You are a pandas expert. Write ONLY executable Python/pandas code.\n\nDATASET INFO:\n- DataFrame is already loaded as `df`\n- Columns: {columns}\n- Dtypes: {json.dumps(dtypes, default=str)}\n- Sample values per column: {json.dumps(sample_values, default=str)}\n\nQUESTION: "{question}"\n\nRULES:\n1. Store the final answer in a variable called `result`\n2. Use EXACT column names from the list above (case-sensitive)\n3. Do NOT import anything — `pd` and `np` are already available\n4. `result` must be a scalar, dict, Series, or small DataFrame\n5. For counts: use .shape[0] or .value_counts() or .groupby().size()\n6. For filters: match dtypes exactly. If a year column is int64, compare with int not string\n7. Always .head(20) on large results to prevent memory issues\n8. If the question asks about a specific entity (brand, model, state), FILTER for it\n9. For "sales report" or "report of X": compute count, average price, top models/states\n10. Never use print() — just assign to `result`\n\nReturn ONLY the code. No markdown fences. No explanation.'

async def generate_pandas_code(question: str, df: pd.DataFrame) -> str:
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
    sys_msg = {'role': 'system', 'content': 'You are a pandas code generator. Output ONLY valid Python code. No markdown. No explanation. No ```.'}
    user_msg = {'role': 'user', 'content': prompt}
    code = await call_groq_with_fallback(
        messages=[sys_msg, user_msg],
        primary_model='llama-3.3-70b-versatile',
        temperature=0,
        max_tokens=500,
    )
    if code.startswith('```'):
        code = code.split('\n', 1)[-1].rsplit('```', 1)[0].strip()
    return code

def _validate_code(code: str) -> Optional[str]:
    """Two-pass validation: regex (fast) then AST (thorough)."""
    match = _BANNED_RE.search(code)
    if match:
        return f"Unsafe code detected: '{match.group()}'"
    if len(code) > 2000:
        return 'Generated code is too long (>2000 chars)'
    if 'result' not in code:
        return "Code does not assign to 'result' variable"
    ast_error = _validate_ast(code)
    if ast_error:
        return ast_error
    return None


def _exec_with_timeout(code: str, namespace: dict, timeout: int = _MAX_EXEC_TIMEOUT_SECONDS) -> Optional[str]:
    """Execute code in a thread with a timeout. Returns error string or None."""
    error_holder = [None]

    def _run():
        try:
            exec(code, {'__builtins__': {}}, namespace)
        except Exception as exc:
            tb = traceback.format_exc().split('\n')[-3:]
            error_holder[0] = f"Execution error: {exc}\n{''.join(tb)}"

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    thread.join(timeout=timeout)

    if thread.is_alive():
        logger.warning('Code execution timed out after %ds', timeout)
        return f'Code execution timed out after {timeout} seconds'

    return error_holder[0]


def safe_execute(code: str, df: pd.DataFrame) -> dict:
    error = _validate_code(code)
    if error:
        return {'result': None, 'error': error, 'code': code}
    namespace = {'df': df.copy(), 'pd': pd, 'np': np}
    exec_error = _exec_with_timeout(code, namespace)
    if exec_error:
        return {'result': None, 'error': exec_error, 'code': code}
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