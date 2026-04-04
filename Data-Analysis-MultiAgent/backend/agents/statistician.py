import logging
import pandas as pd
import numpy as np
from scipy import stats as scipy_stats
from core.state import AnalysisState

logger = logging.getLogger(__name__)


def statistician_agent(state: AnalysisState) -> AnalysisState:
    """
    Statistician Agent: Calculates comprehensive statistics on the clean dataset.
    Populates state.stats_summary with keys the frontend (app.py) expects.
    """
    state.current_agent = "statistician"
    logger.info("Statistician agent started")

    try:
        if state.clean_df is None or state.clean_df.empty:
            raise ValueError("No clean data available for statistical analysis")

        df = state.clean_df
        stats: dict = {}

        # ── Shape (used by top-level metrics) ────────────────────────────
        stats["shape"] = [int(df.shape[0]), int(df.shape[1])]

        # ── Data types ───────────────────────────────────────────────────
        stats["dtypes"] = {col: str(dtype) for col, dtype in df.dtypes.items()}

        # ── Null / missing values ────────────────────────────────────────
        missing = df.isna().sum()
        stats["nulls"] = {
            col: int(count) for col, count in missing.items() if count > 0
        }

        # ── Descriptive statistics (for the table in Statistics tab) ─────
        describe_df = df.describe(include="all")
        describe: dict = {}
        for col in describe_df.columns:
            col_dict: dict = {}
            for stat_name in describe_df.index:
                val = describe_df.loc[stat_name, col]
                if pd.isna(val):
                    col_dict[stat_name] = None
                elif isinstance(val, (int, np.integer)):
                    col_dict[stat_name] = int(val)
                elif isinstance(val, (float, np.floating)):
                    col_dict[stat_name] = round(float(val), 4)
                else:
                    col_dict[stat_name] = str(val)
            describe[col] = col_dict
        stats["describe"] = describe

        # ── Numeric column deep-dive ─────────────────────────────────────
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        numeric_stats: dict = {}
        for col in numeric_cols:
            col_data = df[col].dropna()
            numeric_stats[col] = {
                "mean": float(col_data.mean()),
                "median": float(col_data.median()),
                "std": float(col_data.std()) if len(col_data) > 1 else 0.0,
                "min": float(col_data.min()),
                "max": float(col_data.max()),
                "skewness": float(scipy_stats.skew(col_data, nan_policy="omit")),
                "kurtosis": float(scipy_stats.kurtosis(col_data, nan_policy="omit")),
            }
        stats["numeric_stats"] = numeric_stats

        # ── Outlier detection (IQR method) ───────────────────────────────
        outliers: dict = {}
        for col in numeric_cols:
            col_data = df[col].dropna()
            if len(col_data) > 0:
                q1 = col_data.quantile(0.25)
                q3 = col_data.quantile(0.75)
                iqr = q3 - q1
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                mask = (df[col] < lower) | (df[col] > upper)
                count = int(mask.sum())
                if count > 0:
                    outliers[col] = {
                        "count": count,
                        "percentage": round(float(count / len(df) * 100), 2),
                        "lower_bound": float(lower),
                        "upper_bound": float(upper),
                    }
        stats["outliers"] = outliers

        # ── Correlation analysis ─────────────────────────────────────────
        top_correlations: list = []
        if len(numeric_cols) > 1:
            corr_matrix = df[numeric_cols].corr()
            for i in range(len(corr_matrix.columns)):
                for j in range(i + 1, len(corr_matrix.columns)):
                    r = corr_matrix.iloc[i, j]
                    if abs(r) > 0.5:
                        top_correlations.append([
                            corr_matrix.columns[i],
                            corr_matrix.columns[j],
                            round(float(r), 4),
                        ])
            top_correlations.sort(key=lambda x: abs(x[2]), reverse=True)
        stats["top_correlations"] = top_correlations

        # ── Category value counts (top 10 per column) ────────────────────
        cat_cols = df.select_dtypes(include=["object"]).columns.tolist()
        category_counts: dict = {}
        for col in cat_cols:
            vc = df[col].value_counts().head(10)
            category_counts[col] = {str(k): int(v) for k, v in vc.items()}
        stats["category_counts"] = category_counts

        # ── Store helper lists for downstream agents ─────────────────────
        stats["columns"] = list(df.columns)
        stats["numeric_columns"] = numeric_cols
        stats["categorical_columns"] = cat_cols

        state.stats_summary = stats
        logger.info(
            "Statistician complete. Generated %d stat groups.", len(stats)
        )

    except Exception as e:
        error_msg = f"Statistician error: {e}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    state.completed_agents.append("statistician")
    return state