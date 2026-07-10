from __future__ import annotations
import asyncio
import datetime
import json
import logging
import re
import traceback
from typing import Any, Optional
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
logger = logging.getLogger(__name__)
_BANNED_PATTERNS = [r'\bimport\b', r'\bfrom\b\s+\w+\s+\bimport\b', r'\b__\w+__\b', r'\bexec\b', r'\beval\b', r'\bopen\b', r'\bos\b\.', r'\bsys\b\.', r'\bsubprocess\b', r'\bglobals\b', r'\blocals\b', r'\bgetattr\b', r'\bsetattr\b', r'\bdelattr\b', r'\bcompile\b', r'\b__builtins__\b', r'\.to_csv\b', r'\.to_excel\b', r'\.to_parquet\b', r'\.to_sql\b', r'\brequests\b', r'\burllib\b', r'\bstatsmodels\b', r'\bscipy\b', r'\bsklearn\b']
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

def _build_codegen_prompt(question: str, columns: list, dtypes: dict, sample_values: dict) -> str:
    today_str = datetime.date.today().isoformat()
    return f'You are a pandas and Plotly expert. Write ONLY executable Python code.\n\nDATASET INFO:\n- DataFrame is already loaded as `df`\n- Columns: {columns}\n- Dtypes: {json.dumps(dtypes, default=str)}\n- Sample values per column: {json.dumps(sample_values, default=str)}\n\nCURRENT DATE: {today_str} (Use this to resolve relative dates like "last month", "last year")\n\nQUESTION: "{question}"\n\nRULES:\n1. ALWAYS store a text summary of the answer in a variable called `result`\n2. Use EXACT column names from the list above (case-sensitive)\n3. CRITICAL SECURITY RULE: NEVER use the words "import" or "from" in your code. Using "import" or "from" will fail safety verification. The python "datetime" module is already pre-loaded and available. Use datetime.date, datetime.datetime, or datetime.timedelta directly without any import statements.\n4. `result` must be a scalar, dict, Series, or small DataFrame\n5. For counts: use .shape[0] or .value_counts() or .groupby().size()\n6. For filters: match dtypes exactly. If a year column is int64, compare with int not string\n7. Always .head(20) on large results to prevent memory issues\n8. If the question asks about a specific entity (brand, model, state), FILTER for it\n9. For "sales report" or "report of X": compute count, average price, top models/states\n10. Never use print() — just assign to `result`\n11. CHART GENERATION RULES (CRITICAL): If the question asks to visualize, plot, draw, show, chart, graph, or check distribution/comparison/trend, or if visualizing the data would make the answer significantly clearer and more meaningful (e.g. comparing top categories, distributions, numeric columns, or trends over time), ALWAYS create a Plotly figure and store it in a variable called `fig` using `px` or `go`. Style the figure with a premium dark theme: template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#FFFFFF". Provide a custom descriptive title, meaningful X/Y axis labels, and set values/scales beautifully (e.g. sequential or qualitative colors, descending order for bars, markers=True for lines, and custom hover labels). Do not use hardcoded dimensions, keep it responsive.\n12. DATA INTEGRITY & MAPPING (CRITICAL): Always verify that column names match their true meanings. Never swap categorical axes (e.g., do not put batting style values on an axis labeled gender or vice versa). Verify x, y, and color parameter mappings in px/go calls. Ensure the legend strictly reflects the correct sub-category split.\n13. CLEAN HISTOGRAMS (CRITICAL): When plotting distributions (like country IDs or numerical ranges), ensure standard, visible vertical bars are rendered on a clear linear or log scale. Do NOT overlay floating box-plots or disjointed scatter points/rugs (do NOT set marginal="box", marginal="rug", or marginal="violin") unless explicitly requested. If data is highly skewed and requires a log scale, ensure the y-axis (counts) is adjusted so the data doesn\'t appear flattened or broken at 0.\n14. INSIGHT-DRIVEN VISUALIZATIONS & NO ZERO VARIANCE (CRITICAL): Prioritize charts showing meaningful relationships (correlations, distributions, breakdowns). Avoid generating redundant charts or plotting columns with zero variance (where every value in the column is identical).\n15. Even when creating a chart, ALWAYS set `result` to a short, clear, and comprehensive text summary of the numerical facts and findings shown in the chart.\n16. MONTHLY/YEARLY DATA EXTRACTION:\n    - If the user asks for monthly or yearly stats/data, inspect the available columns for date/time fields.\n    - If a datetime/date column is present, convert it using `pd.to_datetime(df[col], errors=\"coerce\")` and group or filter by year (`.dt.year`) or month (`.dt.month` or `.dt.to_period(\"M\")`).\n    - If no datetime/date column exists but year columns like `Registration Year` or `Year of Manufacture` exist, use them as yearly columns. If the query asks for relative monthly data (like \"last month\") and no month/date columns exist, write code to store a descriptive explanation in `result` (e.g., \"The dataset contains yearly columns like \'Registration Year\' and \'Year of Manufacture\' but does not have month-level or daily date details to extract the last month\'s data. However, the overall stats are...\") so the user is informed.\n17. CORRELATION FACTOR ANALYSIS:\n    - For questions about correlation factors (e.g., \"second highest correlation factor\"), write code to compute the correlation matrix of all numeric columns via `df.select_dtypes(include=[np.number]).corr()`.\n    - To avoid getting symmetric duplicates (since corr(A,B) == corr(B,A)), make sure to only keep distinct correlation pairs where the first column name is lexicographically smaller than the second column name (e.g. `col1 < col2`).\n    - Sort the distinct correlation coefficients in descending order (excluding self-correlations of 1.0) to find the exact N-th highest correlation pair and value.\n    - Store a clear text summary of the correlation pair and its value in `result`.\n\nReturn ONLY the code. No markdown fences. No explanation.'

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

async def execute_pandas_with_retry(question: str, df: pd.DataFrame, groq_client, max_retries: int = 3) -> dict:
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
    messages = [
        {'role': 'system', 'content': 'You are a pandas code generator. Output ONLY valid Python code. No markdown. No explanation. No ```.'},
        {'role': 'user', 'content': prompt}
    ]
    
    last_code = ""
    for attempt in range(max_retries):
        resp = await asyncio.to_thread(
            groq_client.chat.completions.create,
            model='llama-3.3-70b-versatile',
            messages=messages,
            temperature=0.1,
            max_tokens=600
        )
        code = (resp.choices[0].message.content or '').strip()
        if code.startswith('```'):
            code = code.split('\n', 1)[-1].rsplit('```', 1)[0].strip()
        last_code = code
        
        exec_result = safe_execute(code, df)
        
        # Validation checks
        if not exec_result['error']:
            res = exec_result['result']
            # Lightweight sanity check (e.g. empty dataframe or empty series)
            if isinstance(res, (pd.DataFrame, pd.Series)) and len(res) == 0:
                exec_result['error'] = "The code executed successfully, but the resulting DataFrame/Series is empty. Did you filter out all rows?"
            else:
                return exec_result  # Success!
                
        # If error, append to messages for retry
        logger.warning(f"Pandas execution failed on attempt {attempt+1}: {exec_result['error']}")
        messages.append({'role': 'assistant', 'content': code})
        messages.append({
            'role': 'user', 
            'content': f"Execution failed with this error:\n{exec_result['error']}\n\nPlease fix the code and return ONLY the corrected Python code."
        })
        
    return {'result': None, 'error': f"Failed after {max_retries} attempts.", 'code': last_code, 'fig_dict': None}

def _validate_code(code: str) -> Optional[str]:
    match = _BANNED_RE.search(code)
    if match:
        return f"Unsafe code detected: '{match.group()}'"
    if len(code) > 3000:
        return 'Generated code is too long (>3000 chars)'
    if 'result' not in code and 'fig' not in code:
        return "Code does not assign to 'result' or 'fig' variable"
    return None

def safe_execute(code: str, df: pd.DataFrame) -> dict:
    error = _validate_code(code)
    if error:
        return {'result': None, 'error': error, 'code': code, 'fig_dict': None}
    import datetime as _dt
    namespace = {'df': df.copy(), 'pd': pd, 'np': np, 'px': px, 'go': go, 'datetime': _dt, 'len': len, 'str': str, 'int': int, 'float': float, 'bool': bool, 'list': list, 'dict': dict, 'tuple': tuple, 'set': set, 'range': range, 'enumerate': enumerate, 'zip': zip, 'sorted': sorted, 'min': min, 'max': max, 'sum': sum, 'abs': abs, 'round': round, 'type': type, 'isinstance': isinstance, 'print': lambda *a, **k: None}
    try:
        exec(code, {'__builtins__': {}}, namespace)
    except Exception as exc:
        tb = traceback.format_exc().split('\n')[-3:]
        return {'result': None, 'error': f"Execution error: {exc}\n{''.join(tb)}", 'code': code, 'fig_dict': None}
    result = namespace.get('result')
    # Capture plotly figure if present
    fig_dict = None
    fig_obj = namespace.get('fig')
    if fig_obj is not None:
        try:
            import json as _json
            fig_dict = _json.loads(fig_obj.to_json())
        except Exception as fig_exc:
            logger.warning('Failed to serialize plotly fig: %s', fig_exc)
    if result is None and fig_dict is None:
        return {'result': None, 'error': "Code ran but neither 'result' nor 'fig' was produced", 'code': code, 'fig_dict': None}
    if result is None and fig_dict is not None:
        result = 'Chart generated successfully.'
    return {'result': result, 'error': None, 'code': code, 'fig_dict': fig_dict}

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