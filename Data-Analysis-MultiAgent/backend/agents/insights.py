import logging
import numpy as np
from backend.core.state import AnalysisState

logger = logging.getLogger(__name__)

STRONG_CORR_THRESHOLD = 0.7
HIGH_SKEW_THRESHOLD = 1.0
HIGH_CARDINALITY_THRESHOLD = 50


def insights_agent(state: AnalysisState) -> AnalysisState:
    """Derive actionable insights from statistics and data patterns.

    Analyses correlation strengths, distribution shapes, outliers,
    and categorical patterns to produce human-readable findings.
    """
    state.current_agent = "insights"
    logger.info("Insights agent started")

    try:
        df = state.clean_df
        stats = state.stats_summary
        if df is None or not stats:
            raise ValueError("Missing clean_df or stats_summary — prior agents must run first")

        findings: list[str] = []
        recommendations: list[str] = []

        # Strong correlations
        corr = stats.get("correlation", {})
        seen_pairs: set[tuple[str, str]] = set()
        for col_a, others in corr.items():
            for col_b, val in others.items():
                if col_a == col_b:
                    continue
                pair = tuple(sorted([col_a, col_b]))
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                if abs(val) >= STRONG_CORR_THRESHOLD:
                    direction = "positive" if val > 0 else "negative"
                    findings.append(
                        f"Strong {direction} correlation ({val:.2f}) between '{col_a}' and '{col_b}'."
                    )

        # Skewed distributions
        skewness = stats.get("skewness", {})
        for col, skew_val in skewness.items():
            if abs(skew_val) >= HIGH_SKEW_THRESHOLD:
                direction = "right" if skew_val > 0 else "left"
                findings.append(
                    f"Column '{col}' is heavily {direction}-skewed (skewness={skew_val:.2f}). "
                    f"Consider a log or Box-Cox transform."
                )
                recommendations.append(f"Apply transformation to '{col}' before modeling.")

        # Outlier detection via IQR
        num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        outlier_summary: dict[str, int] = {}
        for col in num_cols:
            q1 = float(df[col].quantile(0.25))
            q3 = float(df[col].quantile(0.75))
            iqr = q3 - q1
            if iqr == 0:
                continue
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            n_outliers = int(((df[col] < lower) | (df[col] > upper)).sum())
            if n_outliers > 0:
                outlier_summary[col] = n_outliers
                pct = n_outliers / len(df) * 100
                findings.append(f"Column '{col}' has {n_outliers} outliers ({pct:.1f}% of rows).")

        # High-cardinality categoricals
        cat_stats = stats.get("categorical", {})
        for col, info in cat_stats.items():
            unique = info.get("unique_values", 0)
            if unique >= HIGH_CARDINALITY_THRESHOLD:
                findings.append(
                    f"Column '{col}' has high cardinality ({unique} unique values). "
                    f"Consider grouping or encoding."
                )
                recommendations.append(f"Reduce cardinality of '{col}' via binning or encoding.")

        # Non-normal distributions
        normality = stats.get("normality", {})
        non_normal = [col for col, info in normality.items() if not info.get("is_normal", True)]
        if non_normal:
            findings.append(
                f"Columns with non-normal distributions: {', '.join(non_normal)}. "
                f"Use non-parametric tests if needed."
            )

        if not findings:
            findings.append("No notable patterns detected in the dataset.")

        state.insights = {
            "findings": findings,
            "recommendations": recommendations,
            "outlier_summary": outlier_summary,
            "total_findings": len(findings),
        }
        logger.info("Generated %d findings, %d recommendations",
                     len(findings), len(recommendations))

    except Exception as e:
        error_msg = f"Insights error: {e}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    state.completed_agents.append("insights")
    return state
