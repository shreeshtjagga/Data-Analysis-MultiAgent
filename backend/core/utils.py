import pandas as pd
import numpy as np
import logging
import math
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)


def clean_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """
    Clean the dataframe: strip strings, drop duplicates, impute missing values.
    Returns (cleaned_df, imputation_records) where each record is a dict with:
      - column: column name
      - strategy: 'median' or 'mode'
      - fill_value: the value used to fill
      - count: number of cells that were imputed
    """
    initial_rows = len(df)
    logs = []

    df = df.drop_duplicates()
    dropped = initial_rows - len(df)
    if dropped > 0:
        logger.info("Dropped %d duplicate rows", dropped)

    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        try:
            df[col] = df[col].astype(str).str.strip()
        except Exception:
            pass

    num_cols = df.select_dtypes(include=[np.number]).columns
    for col in num_cols:
        missing_count = int(df[col].isna().sum())
        if missing_count > 0:
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            logger.info("Filled %d missing values in '%s' with median: %s", missing_count, col, median_val)
            logs.append({
                "column": col,
                "strategy": "median",
                "fill_value": float(median_val) if pd.notna(median_val) else None,
                "count": missing_count,
            })

    cat_cols = df.select_dtypes(include=["object"]).columns
    for col in cat_cols:
        missing_count = int(df[col].isna().sum())
        if missing_count > 0:
            mode_val = df[col].mode()
            if not mode_val.empty:
                fill = mode_val.iloc[0]
                df[col] = df[col].fillna(fill)
                logger.info("Filled %d missing values in '%s' with mode: %s", missing_count, col, fill)
                logs.append({
                    "column": col,
                    "strategy": "mode",
                    "fill_value": str(fill),
                    "count": missing_count,
                })

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
