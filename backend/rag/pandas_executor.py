"""
DataPulse Pandas Code Executor — Sandboxed
==========================================
Generates and executes pandas code from natural language questions.
Used for analytical questions (counts, sums, filters, aggregations)
where exact numbers are needed from the full dataset.

Security:
- Only `pd`, `np`, and `df` are exposed in the execution namespace
- No imports, file I/O, exec/eval, or network access allowed
- Code is validated before execution
- Result size is capped to prevent memory issues
"""

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

# ── Banned patterns for sandboxing ─────────────────────────────────────────────
_BANNED_PATTERNS = [
    r'\bimport\b',
    r'\b__\w+__\b',         # dunder access
    r'\bexec\b',
    r'\beval\b',
    r'\bopen\b',
    r'\bos\b\.',
    r'\bsys\b\.',
    r'\bsubprocess\b',
    r'\bglobals\b',
    r'\blocals\b',
    r'\bgetattr\b',
    r'\bsetattr\b',
    r'\bdelattr\b',
    r'\bcompile\b',
    r'\b__builtins__\b',
    r'\.to_csv\b',
    r'\.to_excel\b',
    r'\.to_parquet\b',
    r'\.to_sql\b',
    r'\brequests\b',
    r'\burllib\b',
]
_BANNED_RE = re.compile('|'.join(_BANNED_PATTERNS), re.IGNORECASE)

_MAX_RESULT_ROWS = 50
_MAX_RESULT_CHARS = 3000


# ── Question Classification ───────────────────────────────────────────────────

async def classify_question(question: str, groq_client) -> str:
    """
    Classify a question as 'analytical' or 'reasoning'.
    
    analytical = needs exact numbers: counts, sums, averages, filters, lookups
    reasoning  = needs interpretation: trends, comparisons, why, what-if, explain
    """
    q = question.lower().strip()
    
    # Fast heuristic shortcuts — avoid LLM call for obvious cases
    _ANALYTICAL_SIGNALS = (
        "how many", "how much", "total", "count of", "number of",
        "average", "sum of", "maximum", "minimum", "what is the",
        "what are the", "list all", "show all", "top ", "bottom ",
        "highest", "lowest", "most", "least", "sales report",
        "report of", "report for", "bikes in", "sold in", "made in",
        "manufactured in", "built in", "registered in", "price of",
    )
    if any(sig in q for sig in _ANALYTICAL_SIGNALS):
        return "analytical"
    
    _REASONING_SIGNALS = (
        "why", "explain", "what does", "what do you think",
        "compare", "vs", "versus", "relationship between",
        "correlat", "suggest", "recommend", "insight",
        "what should", "what can we", "interpret",
    )
    if any(sig in q for sig in _REASONING_SIGNALS):
        return "reasoning"
    
    # Borderline — use LLM to classify
    try:
        prompt = (
            "Classify this data question into ONE category:\n"
            "\"analytical\" — needs exact numbers: counts, sums, averages, "
            "filters, specific values, lookups, reports\n"
            "\"reasoning\" — needs interpretation: trends, comparisons, "
            "explanations, suggestions, why questions\n\n"
            f"Question: \"{question}\"\n\n"
            "Reply with ONLY one word: analytical or reasoning"
        )
        resp = await asyncio.to_thread(
            groq_client.chat.completions.create,
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=10,
        )
        result = (resp.choices[0].message.content or "").strip().lower()
        if "analytical" in result:
            return "analytical"
        if "reasoning" in result:
            return "reasoning"
    except Exception as exc:
        logger.warning("Question classification failed: %s", exc)
    
    # Default to analytical — better to run real code than guess
    return "analytical"


# ── Pandas Code Generation ────────────────────────────────────────────────────

def _build_codegen_prompt(
    question: str,
    columns: list[str],
    dtypes: dict[str, str],
    sample_values: dict[str, list],
) -> str:
    """Build the prompt that asks the LLM to write pandas code."""
    return f"""You are a pandas expert. Write ONLY executable Python/pandas code.

DATASET INFO:
- DataFrame is already loaded as `df`
- Columns: {columns}
- Dtypes: {json.dumps(dtypes, default=str)}
- Sample values per column: {json.dumps(sample_values, default=str)}

QUESTION: "{question}"

RULES:
1. Store the final answer in a variable called `result`
2. Use EXACT column names from the list above (case-sensitive)
3. Do NOT import anything — `pd` and `np` are already available
4. `result` must be a scalar, dict, Series, or small DataFrame
5. For counts: use .shape[0] or .value_counts() or .groupby().size()
6. For filters: match dtypes exactly. If a year column is int64, compare with int not string
7. Always .head(20) on large results to prevent memory issues
8. If the question asks about a specific entity (brand, model, state), FILTER for it
9. For "sales report" or "report of X": compute count, average price, top models/states
10. Never use print() — just assign to `result`

Return ONLY the code. No markdown fences. No explanation."""


async def generate_pandas_code(
    question: str,
    df: pd.DataFrame,
    groq_client,
) -> str:
    """Generate pandas code from a natural language question."""
    columns = df.columns.tolist()
    dtypes = {col: str(df[col].dtype) for col in columns}
    
    # Build sample values for each column (helps LLM match entities)
    sample_values = {}
    for col in columns:
        if df[col].dtype == 'object' or str(df[col].dtype) == 'category':
            top = df[col].value_counts().head(5).index.tolist()
            sample_values[col] = [str(v) for v in top]
        elif pd.api.types.is_numeric_dtype(df[col]):
            sample_values[col] = [
                f"min={df[col].min()}", f"max={df[col].max()}"
            ]
    
    prompt = _build_codegen_prompt(question, columns, dtypes, sample_values)
    
    resp = await asyncio.to_thread(
        groq_client.chat.completions.create,
        model="llama-3.3-70b-versatile",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a pandas code generator. Output ONLY valid "
                    "Python code. No markdown. No explanation. No ```."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0,
        max_tokens=500,
    )
    code = (resp.choices[0].message.content or "").strip()
    
    # Strip markdown fences if LLM added them anyway
    if code.startswith("```"):
        code = code.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    
    return code


# ── Safe Execution ─────────────────────────────────────────────────────────────

def _validate_code(code: str) -> Optional[str]:
    """Return error message if code is unsafe, else None."""
    match = _BANNED_RE.search(code)
    if match:
        return f"Unsafe code detected: '{match.group()}'"
    # Limit code length
    if len(code) > 2000:
        return "Generated code is too long (>2000 chars)"
    # Must assign to `result`
    if "result" not in code:
        return "Code does not assign to 'result' variable"
    return None


def safe_execute(code: str, df: pd.DataFrame) -> dict:
    """
    Execute pandas code in a sandboxed environment.
    Returns: {"result": Any, "error": str|None, "code": str}
    """
    # Validate
    error = _validate_code(code)
    if error:
        return {"result": None, "error": error, "code": code}
    
    # Execute in restricted namespace
    namespace = {"df": df.copy(), "pd": pd, "np": np}
    try:
        exec(code, {"__builtins__": {}}, namespace)
    except Exception as exc:
        tb = traceback.format_exc().split("\n")[-3:]
        return {
            "result": None,
            "error": f"Execution error: {exc}\n{''.join(tb)}",
            "code": code,
        }
    
    result = namespace.get("result")
    if result is None:
        return {"result": None, "error": "Code ran but 'result' was None", "code": code}
    
    return {"result": result, "error": None, "code": code}


# ── Result Formatting ──────────────────────────────────────────────────────────

def format_result(result: Any) -> str:
    """Convert a pandas execution result to a clean string for the LLM."""
    if result is None:
        return "No result"
    
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


def build_rich_context(
    result: Any,
    df: pd.DataFrame,
    question: str,
) -> dict:
    """Build additional context beyond the raw result for richer answers."""
    ctx = {"total_records": len(df)}
    
    # If result is a count, add percentage context
    if isinstance(result, (int, float, np.integer, np.floating)):
        try:
            pct = (float(result) / len(df)) * 100
            if 0 < pct <= 100:
                ctx["as_percentage"] = f"{pct:.1f}% of {len(df):,} total records"
        except (ZeroDivisionError, ValueError):
            pass
    
    # Pull high-level column info
    q_lower = question.lower()
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in q_lower or col_lower.replace("_", " ") in q_lower:
            if pd.api.types.is_numeric_dtype(df[col]):
                ctx[f"{col}_stats"] = {
                    "mean": round(float(df[col].mean()), 2),
                    "min": float(df[col].min()),
                    "max": float(df[col].max()),
                }
            elif df[col].dtype == 'object':
                ctx[f"{col}_top"] = df[col].value_counts().head(3).to_dict()
    
    return ctx


# ── Challenge Detection ───────────────────────────────────────────────────────

_CHALLENGE_PHRASES = (
    "are you sure", "confirm", "double check", "really",
    "verify", "is that correct", "is that right", "that doesn't seem",
    "that can't be", "check again", "are you certain",
)

def is_challenge(question: str) -> bool:
    """Detect if the user is challenging/verifying the previous answer."""
    q = question.lower().strip()
    return any(phrase in q for phrase in _CHALLENGE_PHRASES)
