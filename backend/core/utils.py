import re
import pandas as pd
import numpy as np
import logging
import math
from datetime import date, datetime
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)


def _parse_salary_range(series: pd.Series) -> pd.Series:
    """
    Convert survey salary-range strings like '41k-65k', '0-40k', '125k-150k', '225k+'
    into numeric midpoint floats (USD).
    """
    def _to_mid(val):
        if pd.isna(val):
            return np.nan
        s = str(val).lower().replace(",", "").replace("$", "").strip()
        # Extract numbers with optional 'k' suffix
        nums = re.findall(r"(\d+\.?\d*)k?", s)
        multipliers = re.findall(r"(\d+\.?\d*)(k)", s)
        if multipliers:
            parts = [float(n) * 1000 for n, _ in multipliers]
        elif nums:
            parts = [float(n) for n in nums]
        else:
            return np.nan
        return float(np.mean(parts))
    return series.apply(_to_mid)


def _looks_like_salary_range(series: pd.Series) -> bool:
    """
    Returns True if the majority of non-null values look like salary ranges
    e.g. '41k-65k', '0-40k', '225k+', '125k-150k'.
    """
    sample = series.dropna().head(30).astype(str)
    if len(sample) == 0:
        return False
    pattern = re.compile(r"^\d+k?\s*[-–+]?\s*\d*k?$", re.IGNORECASE)
    matched = sample.str.match(r"^\d+k?\s*[\-\–\+]?\s*\d*k?$", na=False)
    return matched.sum() >= len(sample) * 0.5


# ── Null-like text values that should be treated as NaN ────────────────────
_NULL_STRINGS = frozenset({
    "nan", "none", "null", "na", "n/a", "n\\a", "#n/a", "#na", "#null",
    "nil", "undefined", "unknown", "missing", "-", "--", "---", "",
    "not available", "not applicable", "no data", "no response", "nr",
})

# ── Boolean text mappings ───────────────────────────────────────────────────
_TRUE_STRINGS  = frozenset({"yes", "y", "true", "1", "on", "agree", "positive", "correct", "ok"})
_FALSE_STRINGS = frozenset({"no", "n", "false", "0", "off", "disagree", "negative", "incorrect"})


def _normalize_null_strings(series: pd.Series) -> pd.Series:
    """Replace common null-like text values with actual NaN."""
    lower = series.astype(str).str.strip().str.lower()
    return series.where(~lower.isin(_NULL_STRINGS), other=np.nan)


def _try_parse_currency(series: pd.Series) -> Tuple[Optional[pd.Series], bool]:
    """Try to parse currency strings like '$1,234.56', '€500', '1.2M' → float."""
    sample = series.dropna().head(30).astype(str)
    if len(sample) == 0:
        return None, False
    # Must look like currency in >50% of sample
    currency_pat = re.compile(r"^[\$€£¥₹]?[\d,\.]+[kKmMbB]?$")
    matched = sample.str.strip().str.match(r"^[\$€£¥₹]?[\d,\.]+[kKmMbB]?\s*$", na=False)
    if matched.sum() < len(sample) * 0.5:
        return None, False

    def _to_float(val):
        if pd.isna(val):
            return np.nan
        s = re.sub(r"[\$€£¥₹,\s]", "", str(val).strip())
        multiplier = 1
        if s.endswith(("k", "K")):
            s, multiplier = s[:-1], 1_000
        elif s.endswith(("m", "M")):
            s, multiplier = s[:-1], 1_000_000
        elif s.endswith(("b", "B")):
            s, multiplier = s[:-1], 1_000_000_000
        try:
            return float(s) * multiplier
        except ValueError:
            return np.nan

    parsed = series.apply(_to_float)
    valid_ratio = parsed.notna().sum() / max(len(series.dropna()), 1)
    return parsed, valid_ratio > 0.5


def _try_parse_percentage(series: pd.Series) -> Tuple[Optional[pd.Series], bool]:
    """Try to parse percentage strings like '85%', '3.5 %' → float (0.85, 0.035)."""
    sample = series.dropna().head(30).astype(str).str.strip()
    if len(sample) == 0:
        return None, False
    pct_pat = sample.str.match(r"^-?\d+\.?\d*\s*%$", na=False)
    if pct_pat.sum() < len(sample) * 0.5:
        return None, False

    def _to_pct(val):
        if pd.isna(val):
            return np.nan
        try:
            return float(str(val).replace("%", "").strip()) / 100
        except ValueError:
            return np.nan

    parsed = series.apply(_to_pct)
    valid_ratio = parsed.notna().sum() / max(len(series.dropna()), 1)
    return parsed, valid_ratio > 0.5


def _try_parse_boolean(series: pd.Series) -> Tuple[Optional[pd.Series], bool]:
    """Try to parse boolean-text columns (Yes/No, True/False, Y/N) → 0.0/1.0."""
    sample = series.dropna().head(30).astype(str).str.strip().str.lower()
    if len(sample) == 0:
        return None, False
    known = sample.isin(_TRUE_STRINGS | _FALSE_STRINGS)
    if known.sum() < len(sample) * 0.8:
        return None, False

    def _to_bool(val):
        if pd.isna(val):
            return np.nan
        s = str(val).strip().lower()
        if s in _TRUE_STRINGS:
            return 1.0
        if s in _FALSE_STRINGS:
            return 0.0
        return np.nan

    parsed = series.apply(_to_bool)
    valid_ratio = parsed.notna().sum() / max(len(series.dropna()), 1)
    return parsed, valid_ratio > 0.7


def _try_parse_mixed_numeric(series: pd.Series) -> Tuple[Optional[pd.Series], bool]:
    """
    Handle columns that are mostly numeric but contain text noise
    like 'N/A', 'unknown', '-' mixed in with real numbers.
    """
    sample = series.dropna().head(50).astype(str)
    if len(sample) == 0:
        return None, False
    # Try numeric coercion on the whole column
    converted = pd.to_numeric(series, errors="coerce")
    valid_ratio = converted.notna().sum() / max(len(series), 1)
    # Accept if ≥60% are valid numbers (the rest are text noise → NaN)
    if valid_ratio >= 0.60:
        return converted, True
    return None, False


def clean_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """
    Production-grade dataframe cleaner. Handles:
      - Duplicate rows
      - Column name normalization (strips whitespace)
      - Null-like text normalisation ('N/A', 'none', '--' → NaN)
      - Salary / range strings ('41k-65k' → midpoint float)
      - Currency strings ('$1,234.56', '€500', '1.2M' → float)
      - Percentage strings ('85%' → 0.85)
      - Boolean text ('Yes'/'No', 'True'/'False' → 1.0/0.0)
      - Mixed numeric columns (numbers + noise text → numeric + NaN)
      - Infinity / extreme values → NaN
      - Missing value imputation (median/mode, only for <60% null cols)
    Returns (cleaned_df, imputation_records).
    """
    initial_rows = len(df)
    logs: list = []

    # ── 1. Normalize column names (strip whitespace) ────────────────────────
    df.columns = [str(c).strip() for c in df.columns]

    # ── 2. Drop exact duplicate rows ────────────────────────────────────────
    df = df.drop_duplicates()
    dropped = initial_rows - len(df)
    if dropped > 0:
        logger.info("Dropped %d duplicate rows", dropped)

    # ── 3. Strip leading/trailing whitespace from all string cells ──────────
    for col in df.select_dtypes(include=["object"]).columns:
        try:
            df[col] = df[col].astype(str).str.strip()
        except Exception:
            pass

    # ── 4. Normalize null-like text → NaN ───────────────────────────────────
    for col in list(df.select_dtypes(include=["object"]).columns):
        df[col] = _normalize_null_strings(df[col])

    # ── 5. Replace ±Inf in numeric columns with NaN ─────────────────────────
    num_cols_now = df.select_dtypes(include=[np.number]).columns
    for col in num_cols_now:
        inf_count = np.isinf(df[col]).sum()
        if inf_count > 0:
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            logger.info("Replaced %d ±Inf values in '%s' with NaN", inf_count, col)

    # ── 6. Smart type coercion for object columns ────────────────────────────
    for col in list(df.select_dtypes(include=["object"]).columns):
        col_data = df[col].dropna()
        if len(col_data) == 0:
            continue

        # 6a. Salary ranges ('41k-65k', '0-40k', '225k+')
        if _looks_like_salary_range(df[col]):
            parsed = _parse_salary_range(df[col])
            if parsed.notna().sum() / max(len(df), 1) > 0.5:
                df[col] = parsed
                logger.info("Converted salary range '%s' to numeric midpoints", col)
                continue

        # 6b. Currency strings ('$1,234', '€500', '1.2M')
        parsed_cur, ok = _try_parse_currency(df[col])
        if ok and parsed_cur is not None:
            df[col] = parsed_cur
            logger.info("Converted currency column '%s' to float", col)
            continue

        # 6c. Percentage strings ('85%' → 0.85)
        parsed_pct, ok = _try_parse_percentage(df[col])
        if ok and parsed_pct is not None:
            df[col] = parsed_pct
            logger.info("Converted percentage column '%s' to float ratio", col)
            continue

        # 6d. Boolean text ('Yes'/'No', 'True'/'False')
        parsed_bool, ok = _try_parse_boolean(df[col])
        if ok and parsed_bool is not None:
            df[col] = parsed_bool
            logger.info("Converted boolean-text column '%s' to 0/1 float", col)
            continue

        # 6e. Mixed numeric (mostly numbers + noise text like 'N/A')
        parsed_mix, ok = _try_parse_mixed_numeric(df[col])
        if ok and parsed_mix is not None:
            df[col] = parsed_mix
            logger.info("Coerced mixed-type column '%s' to numeric (noise → NaN)", col)
            continue

    # ── 7. Impute — only columns with <60% missing ──────────────────────────
    _IMPUTE_MAX_NULL_RATIO = 0.60

    for col in df.select_dtypes(include=[np.number]).columns:
        missing_count = int(df[col].isna().sum())
        null_ratio = missing_count / max(len(df), 1)
        if missing_count > 0 and null_ratio < _IMPUTE_MAX_NULL_RATIO:
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            logger.info("Filled %d missing in '%s' with median=%s", missing_count, col, median_val)
            logs.append({
                "column": col, "strategy": "median",
                "fill_value": float(median_val) if pd.notna(median_val) else None,
                "count": missing_count,
            })
        elif missing_count > 0:
            logger.info("Skipping imputation of '%s' (%.0f%% null — too sparse)", col, null_ratio * 100)

    for col in df.select_dtypes(include=["object"]).columns:
        missing_count = int(df[col].isna().sum())
        null_ratio = missing_count / max(len(df), 1)
        if missing_count > 0 and null_ratio < _IMPUTE_MAX_NULL_RATIO:
            mode_val = df[col].mode()
            if not mode_val.empty:
                fill = mode_val.iloc[0]
                df[col] = df[col].fillna(fill)
                logger.info("Filled %d missing in '%s' with mode='%s'", missing_count, col, fill)
                logs.append({
                    "column": col, "strategy": "mode",
                    "fill_value": str(fill), "count": missing_count,
                })
        elif missing_count > 0:
            logger.info("Skipping imputation of '%s' (%.0f%% null — too sparse)", col, null_ratio * 100)

    return df.reset_index(drop=True), logs


def detect_column_types(df: pd.DataFrame) -> dict[str, str]:
    type_map: dict[str, str] = {}
    for col in df.columns:
        if pd.api.types.is_bool_dtype(df[col]):
            type_map[col] = "boolean"
        elif pd.api.types.is_numeric_dtype(df[col]):
            type_map[col] = "numeric"
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            type_map[col] = "datetime"
        else:
            sample = df[col].dropna().head(50)
            if sample.empty:
                type_map[col] = "categorical"
                continue
            # Detect HH:MM or HH:MM:SS time strings — NOT datetimes
            time_pat = sample.astype(str).str.match(r"^\d{1,2}:\d{2}(:\d{2})?$", na=False)
            if time_pat.sum() > len(sample) * 0.7:
                type_map[col] = "duration"
                continue
            try:
                pd.to_datetime(sample, format="mixed")
                type_map[col] = "datetime"
            except (ValueError, TypeError):
                type_map[col] = "categorical"
    return type_map


_SLIM_NUMERIC_KEYS = ("mean", "median", "std", "min", "max", "skewness", "count")


def truncate_stats_for_llm(
    stats: dict,
    max_numeric_cols: int = 10,
    max_correlations: int = 5,
    max_categorical_cols: int = 15,
) -> dict:
    """
    Return a compact copy of *stats* suitable for LLM prompts.
    Keeps the most informative columns and strips heavy sub-structures
    (correlation matrices, full value-count dicts, outlier indices).
    """
    truncated = {
        "row_count": stats.get("row_count"),
        "column_count": stats.get("column_count"),
        "data_quality": stats.get("data_quality"),
        "dataset_profile": stats.get("dataset_profile"),
        "imputations": stats.get("imputations"),
        "excluded_columns": stats.get("excluded_columns"),
    }

    # Numeric — top N by variance (most informative columns first)
    numeric = stats.get("numeric_columns", {})
    items = sorted(
        numeric.items(),
        key=lambda kv: abs(kv[1].get("variance", 0)),
        reverse=True,
    )
    selected = items[:max_numeric_cols]
    truncated["numeric_columns"] = {
        col: {k: v for k, v in data.items() if k in _SLIM_NUMERIC_KEYS}
        for col, data in selected
    }
    if len(numeric) > max_numeric_cols:
        truncated["numeric_columns_note"] = (
            f"Showing top {max_numeric_cols} of {len(numeric)} by variance"
        )

    # Correlations — top N
    truncated["strong_correlations"] = stats.get("strong_correlations", [])[:max_correlations]

    # Categorical — lightweight: unique count + most common only
    categorical = stats.get("categorical_columns", {})
    truncated["categorical_columns"] = {
        col: {
            "unique_values": v.get("unique_values"),
            "most_common": v.get("most_common"),
        }
        for col, v in list(categorical.items())[:max_categorical_cols]
    }

    # Outliers — counts only (drop indices, bounds)
    outliers = stats.get("outliers", {})
    truncated["outlier_counts"] = {
        col: v.get("count", 0) for col, v in outliers.items()
    }

    return truncated


def sanitize_floats(value):
    """Recursively replace NaN/Inf float values with JSON-safe None."""
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(value, dict):
        return {k: sanitize_floats(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_floats(v) for v in value]
    return value


def json_default(obj: Any) -> Any:
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        val = float(obj)
        return val if math.isfinite(val) else None
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, (datetime, date, pd.Timestamp)):
        return obj.isoformat()
    if isinstance(obj, set):
        return list(obj)
    return str(obj)


def sanitize_for_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_for_json(v) for v in value]
    if isinstance(value, tuple):
        return [sanitize_for_json(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (np.floating,)):
        val = float(value)
        return val if math.isfinite(val) else None
    if isinstance(value, np.ndarray):
        return sanitize_for_json(value.tolist())
    return value
