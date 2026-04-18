

import logging

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

from ..core.state import AnalysisState
from ..core.errors import add_pipeline_error

logger = logging.getLogger(__name__)

_CSV_FORMULA_PREFIXES = ("=", "+", "-", "@")
_MAX_CATEGORY_VALUE_CHARS = 200
_MAX_STRONG_CORRELATIONS = 200


def _sanitize_cell_for_output(value: object) -> str:
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    # Strip leading formula characters (single pass to avoid infinite loop)
    max_strip = len(text)
    stripped = 0
    while stripped < max_strip and text.startswith(_CSV_FORMULA_PREFIXES):
        text = text[1:].lstrip()
        stripped += 1
    if len(text) > _MAX_CATEGORY_VALUE_CHARS:
        text = text[:_MAX_CATEGORY_VALUE_CHARS] + "..."
    return text


def statistician_agent(state: AnalysisState) -> AnalysisState:
    state.current_agent = "statistician"
    logger.info("Statistician agent started")

    try:
        if state.clean_df is None or state.clean_df.empty:
            raise ValueError("No clean data available for statistical analysis")

        # Work on a view — we coerce in place (local only, clean_df is not returned)
        df = state.clean_df


        excluded_names: set[str] = set()
        for entry in (state.stats_summary or {}).get("excluded_columns", []):
            excluded_names.add(entry["column"])


        coerced_columns: list[dict] = []
        for col in df.select_dtypes(include=["object"]).columns:
            if col in excluded_names:
                continue
            converted = pd.to_numeric(df[col], errors="coerce")
            valid = converted.notna().sum()
            total = len(df)
            if total > 0 and valid / total > 0.50:
                df[col] = converted
                coerced_columns.append({
                    "column": col,
                    "valid_ratio": round(valid / total, 3),
                })
                logger.info(
                    "Coerced '%s' from object to numeric (%.0f%% valid)",
                    col,
                    valid / total * 100,
                )

        stats_summary: dict = {}


        stats_summary["row_count"] = int(len(df))
        stats_summary["column_count"] = int(len(df.columns))
        stats_summary["columns"] = list(df.columns)
        stats_summary["dtypes"] = {
            col: str(dtype) for col, dtype in df.dtypes.items()
        }
        stats_summary["memory_usage_mb"] = float(
            df.memory_usage(deep=True).sum() / 1024**2
        )
        if coerced_columns:
            stats_summary["coerced_columns"] = coerced_columns


        missing_data = df.isna().sum()
        total_missing = int(missing_data.sum())   # cache — reused in data_quality
        stats_summary["missing_values"] = {
            col: int(count) for col, count in missing_data.items() if count > 0
        }
        stats_summary["missing_percentage"] = {
            col: float((count / len(df)) * 100)
            for col, count in missing_data.items()
            if count > 0
        }


        numeric_cols = [
            c
            for c in df.select_dtypes(include=[np.number]).columns.tolist()
            if c not in excluded_names
        ]
        numeric_stats: dict = {}
        numeric_errors: list[dict] = []
        outliers_summary: dict = {}

        for col in numeric_cols:
            try:
                col_data = df[col].dropna()
                if len(col_data) == 0:
                    continue
                    
                q1 = float(col_data.quantile(0.25))
                q3 = float(col_data.quantile(0.75))
                iqr = q3 - q1
                
                numeric_stats[col] = {
                    "mean": float(col_data.mean()),
                    "median": float(col_data.median()),
                    "std": float(col_data.std()),
                    "min": float(col_data.min()),
                    "max": float(col_data.max()),
                    "q1": q1,
                    "q3": q3,
                    "iqr": iqr,
                    "skewness": float(
                        scipy_stats.skew(col_data, nan_policy="omit")
                    ),
                    "kurtosis": float(
                        scipy_stats.kurtosis(col_data, nan_policy="omit")
                    ),
                    "variance": float(col_data.var()),
                    "count": int(col_data.count()),
                }
                
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr

                outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
                if len(outliers) > 0:
                    outliers_summary[col] = {
                        "count": int(len(outliers)),
                        "percentage": float((len(outliers) / len(df)) * 100),
                        "lower_bound": float(lower_bound),
                        "upper_bound": float(upper_bound),
                    }
                    
            except Exception as col_err:
                logger.warning("Skipping numeric column '%s': %s", col, col_err)
                numeric_errors.append({"column": col, "error": str(col_err)})

        stats_summary["numeric_columns"] = numeric_stats
        if numeric_errors:
            stats_summary["numeric_column_errors"] = numeric_errors


        categorical_cols = [
            c
            for c in df.select_dtypes(include=["object"]).columns.tolist()
            if c not in excluded_names
        ]
        categorical_stats: dict = {}
        categorical_errors: list[dict] = []

        for col in categorical_cols:
            try:
                value_counts = df[col].value_counts()
                top_5_values: dict[str, int] = {}
                for raw_value, count in value_counts.head(5).items():
                    safe_key = _sanitize_cell_for_output(raw_value)
                    top_5_values[safe_key] = top_5_values.get(safe_key, 0) + int(count)

                categorical_stats[col] = {
                    "unique_values": int(df[col].nunique()),
                    "most_common": (
                        _sanitize_cell_for_output(value_counts.index[0]) if len(value_counts) > 0 else None
                    ),
                    "most_common_count": (
                        int(value_counts.iloc[0]) if len(value_counts) > 0 else 0
                    ),
                    "least_common": (
                        _sanitize_cell_for_output(value_counts.index[-1]) if len(value_counts) > 0 else None
                    ),
                    "least_common_count": (
                        int(value_counts.iloc[-1]) if len(value_counts) > 0 else 0
                    ),
                    "diversity_ratio": float(df[col].nunique() / len(df)),
                    "top_5_values": top_5_values,
                }
            except Exception as col_err:
                logger.warning(
                    "Skipping categorical column '%s': %s", col, col_err
                )
                categorical_errors.append({"column": col, "error": str(col_err)})

        stats_summary["categorical_columns"] = categorical_stats
        if categorical_errors:
            stats_summary["categorical_column_errors"] = categorical_errors


        stats_summary["outliers"] = outliers_summary


        if len(numeric_cols) > 1:
            try:
                # Cap at 50 cols — correlation is O(n²) and adds no signal beyond that
                corr_cols = numeric_cols[:50]
                corr_sample = (
                    df[corr_cols].sample(min(len(df), 5000), random_state=42)
                    if len(df) > 5000
                    else df[corr_cols]
                )
                correlation_matrix = corr_sample.corr()
                strong_correlations = []
                for i in range(len(correlation_matrix.columns)):
                    for j in range(i + 1, len(correlation_matrix.columns)):
                        corr_val = correlation_matrix.iloc[i, j]
                        if abs(corr_val) > 0.7 and pd.notna(corr_val):
                            strong_correlations.append(
                                {
                                    "col1": correlation_matrix.columns[i],
                                    "col2": correlation_matrix.columns[j],
                                    "correlation": float(corr_val),
                                }
                            )

                strong_correlations = sorted(
                    strong_correlations,
                    key=lambda item: abs(item["correlation"]),
                    reverse=True,
                )[:_MAX_STRONG_CORRELATIONS]
                stats_summary["strong_correlations"] = strong_correlations
            except Exception as corr_err:
                logger.warning("Correlation analysis failed: %s", corr_err)
                stats_summary["strong_correlations"] = []
        else:
            stats_summary["strong_correlations"] = []


        stats_summary["data_quality"] = {
            "total_cells": int(len(df) * len(df.columns)),
            "missing_cells": total_missing,          # reuse cached value
            "duplicate_rows": int(len(df) - len(df.drop_duplicates())),
            "completeness": float(
                ((len(df) * len(df.columns) - total_missing)
                 / max(len(df) * len(df.columns), 1))
                * 100
            ),
        }


        prev = state.stats_summary or {}
        for key in ("imputations", "excluded_columns", "dataset_profile"):
            if key in prev:
                stats_summary[key] = prev[key]

        state.stats_summary = stats_summary
        logger.info(
            "Statistician complete. %d numeric, %d categorical columns analysed",
            len(numeric_stats),
            len(categorical_stats),
        )
        state.completed_agents.append("statistician")

    except Exception as e:
        logger.error("Statistician error: %s", e)
        add_pipeline_error(
            state.errors,
            code="STATISTICIAN_FAILED",
            message=str(e),
            agent="statistician",
            error_type="agent",
        )

    return state
