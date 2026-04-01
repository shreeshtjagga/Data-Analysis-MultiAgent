import logging
import numpy as np
import pandas as pd
from scipy import stats as scipy_stats
from backend.core.state import AnalysisState

logger = logging.getLogger(__name__)


def statistician_agent(state: AnalysisState) -> AnalysisState:
    """Compute statistical summaries on the cleaned dataset.

    Produces descriptive statistics, correlation matrix, skewness/kurtosis,
    and distribution normality tests for numeric columns.
    """
    state.current_agent = "statistician"
    logger.info("Statistician agent started")

    try:
        df = state.clean_df
        if df is None:
            raise ValueError("No clean_df available — architect must run first")

        summary: dict = {}

        # Basic descriptive statistics
        num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        cat_cols = df.select_dtypes(include=["object"]).columns.tolist()

        if num_cols:
            desc = df[num_cols].describe()
            summary["descriptive"] = {
                col: {stat: float(desc[col][stat]) for stat in desc.index}
                for col in desc.columns
            }

            # Correlation matrix
            corr = df[num_cols].corr()
            summary["correlation"] = {
                col: {other: float(corr[col][other]) for other in corr.columns}
                for col in corr.columns
            }

            # Skewness and kurtosis
            summary["skewness"] = {col: float(df[col].skew()) for col in num_cols}
            summary["kurtosis"] = {col: float(df[col].kurtosis()) for col in num_cols}

            # Normality test (Shapiro-Wilk, limited to 5000 samples)
            normality = {}
            for col in num_cols:
                sample = df[col].dropna()
                if len(sample) >= 8:
                    sample = sample.sample(n=min(len(sample), 5000), random_state=42)
                    stat_val, p_val = scipy_stats.shapiro(sample)
                    normality[col] = {
                        "statistic": float(stat_val),
                        "p_value": float(p_val),
                        "is_normal": p_val > 0.05,
                    }
            summary["normality"] = normality

        # Categorical value counts
        if cat_cols:
            summary["categorical"] = {}
            for col in cat_cols:
                counts = df[col].value_counts()
                summary["categorical"][col] = {
                    "unique_values": int(counts.shape[0]),
                    "top_values": {str(k): int(v) for k, v in counts.head(10).items()},
                }

        summary["row_count"] = int(len(df))
        summary["column_count"] = int(len(df.columns))
        state.stats_summary = summary
        logger.info("Statistical analysis complete: %d numeric, %d categorical columns",
                     len(num_cols), len(cat_cols))

    except Exception as e:
        error_msg = f"Statistician error: {e}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    state.completed_agents.append("statistician")
    return state
