import logging
import pandas as pd
import numpy as np
from scipy import stats as scipy_stats
from core.state import AnalysisState

logger = logging.getLogger(__name__)


def statistician_agent(state: AnalysisState) -> AnalysisState:
    """
    Statistician Agent: Calculates comprehensive statistics on the clean dataset.
    Populates state.stats_summary with detailed statistical analysis.
    """
    state.current_agent = "statistician"
    logger.info("Statistician agent started")

    try:
        if state.clean_df is None or state.clean_df.empty:
            raise ValueError("No clean data available for statistical analysis")

        df = state.clean_df
        stats_summary = {}

        # Basic dataset information
        stats_summary["row_count"] = int(len(df))
        stats_summary["column_count"] = int(len(df.columns))
        stats_summary["columns"] = list(df.columns)
        stats_summary["dtypes"] = {col: str(dtype) for col, dtype in df.dtypes.items()}
        
        # Memory usage
        stats_summary["memory_usage_mb"] = float(df.memory_usage(deep=True).sum() / 1024 ** 2)

        # Missing values analysis
        missing_data = df.isna().sum()
        stats_summary["missing_values"] = {
            col: int(count) for col, count in missing_data.items() if count > 0
        }
        stats_summary["missing_percentage"] = {
            col: float((count / len(df)) * 100) 
            for col, count in missing_data.items() if count > 0
        }

        # Numeric columns analysis
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            numeric_stats = {}
            for col in numeric_cols:
                col_data = df[col].dropna()
                numeric_stats[col] = {
                    "mean": float(col_data.mean()),
                    "median": float(col_data.median()),
                    "std": float(col_data.std()),
                    "min": float(col_data.min()),
                    "max": float(col_data.max()),
                    "q1": float(col_data.quantile(0.25)),
                    "q3": float(col_data.quantile(0.75)),
                    "iqr": float(col_data.quantile(0.75) - col_data.quantile(0.25)),
                    "skewness": float(scipy_stats.skew(col_data, nan_policy='omit')),
                    "kurtosis": float(scipy_stats.kurtosis(col_data, nan_policy='omit')),
                    "variance": float(col_data.var()),
                    "count": int(col_data.count()),
                }
            
            stats_summary["numeric_columns"] = numeric_stats
        else:
            stats_summary["numeric_columns"] = {}

        # Categorical columns analysis
        categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
        if categorical_cols:
            categorical_stats = {}
            for col in categorical_cols:
                value_counts = df[col].value_counts()
                categorical_stats[col] = {
                    "unique_values": int(df[col].nunique()),
                    "most_common": str(value_counts.index[0]) if len(value_counts) > 0 else None,
                    "most_common_count": int(value_counts.iloc[0]) if len(value_counts) > 0 else 0,
                    "least_common": str(value_counts.index[-1]) if len(value_counts) > 0 else None,
                    "least_common_count": int(value_counts.iloc[-1]) if len(value_counts) > 0 else 0,
                    "diversity_ratio": float(df[col].nunique() / len(df)),
                    "top_5_values": value_counts.head(5).to_dict(),
                }
            
            stats_summary["categorical_columns"] = categorical_stats
        else:
            stats_summary["categorical_columns"] = {}

        # Outlier detection using IQR method
        outliers_summary = {}
        for col in numeric_cols:
            col_data = df[col].dropna()
            if len(col_data) > 0:
                Q1 = col_data.quantile(0.25)
                Q3 = col_data.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
                if len(outliers) > 0:
                    outliers_summary[col] = {
                        "count": int(len(outliers)),
                        "percentage": float((len(outliers) / len(df)) * 100),
                        "lower_bound": float(lower_bound),
                        "upper_bound": float(upper_bound),
                        "outlier_indices": outliers.index.tolist()[:10],  # First 10 for preview
                    }
        
        stats_summary["outliers"] = outliers_summary

        # Correlation analysis for numeric columns
        if len(numeric_cols) > 1:
            correlation_matrix = df[numeric_cols].corr()
            # Find strong correlations (>0.7 or <-0.7)
            strong_correlations = []
            for i in range(len(correlation_matrix.columns)):
                for j in range(i + 1, len(correlation_matrix.columns)):
                    corr_val = correlation_matrix.iloc[i, j]
                    if abs(corr_val) > 0.7:
                        strong_correlations.append({
                            "col1": correlation_matrix.columns[i],
                            "col2": correlation_matrix.columns[j],
                            "correlation": float(corr_val)
                        })
            
            stats_summary["correlation_matrix"] = correlation_matrix.to_dict()
            stats_summary["strong_correlations"] = strong_correlations
        else:
            stats_summary["correlation_matrix"] = {}
            stats_summary["strong_correlations"] = []

        # Data quality metrics
        stats_summary["data_quality"] = {
            "total_cells": int(len(df) * len(df.columns)),
            "missing_cells": int(missing_data.sum()),
            "duplicate_rows": int(len(df) - len(df.drop_duplicates())),
            "completeness": float(((len(df) * len(df.columns) - missing_data.sum()) / (len(df) * len(df.columns))) * 100),
        }

        state.stats_summary = stats_summary
        logger.info("Statistician agent complete. Stats summary generated with %d metrics", 
                    len(stats_summary))

    except Exception as e:
        error_msg = f"Statistician error: {e}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    state.completed_agents.append("statistician")
    return state
