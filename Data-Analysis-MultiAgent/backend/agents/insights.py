import logging
from ..core.state import AnalysisState

logger = logging.getLogger(__name__)


def insights_agent(state: AnalysisState) -> AnalysisState:
    """
    Insights Agent: Generates findings, recommendations, and outlier summaries
    from the statistical analysis produced by the statistician agent.
    """
    state.current_agent = "insights"
    logger.info("Insights agent started")

    try:
        if not state.stats_summary:
            raise ValueError("Missing stats_summary from statistician agent")

        stats = state.stats_summary
        insights = {}

        # Extract key statistics for insights generation
        numeric_cols = list(stats.get("numeric_columns", {}).keys())
        categorical_cols = list(stats.get("categorical_columns", {}).keys())
        outliers = stats.get("outliers", {})
        correlations = stats.get("strong_correlations", [])
        data_quality = stats.get("data_quality", {})

        # Generate findings from statistical analysis
        findings = []

        # Dataset size finding
        findings.append(
            f"Dataset contains {stats.get('row_count', 0)} rows and {stats.get('column_count', 0)} columns"
        )

        # Data quality finding
        completeness = data_quality.get("completeness", 100)
        if completeness < 100:
            findings.append(
                f"Data completeness is {completeness:.2f}% with "
                f"{data_quality.get('missing_cells', 0)} missing values"
            )

        # Numeric columns insights
        if numeric_cols:
            findings.append(
                f"Identified {len(numeric_cols)} numeric columns: {', '.join(numeric_cols[:5])}"
            )
            
            # Find columns with high skewness
            skewed_cols = []
            for col, col_stats in stats.get("numeric_columns", {}).items():
                skewness = col_stats.get("skewness", 0)
                if abs(skewness) > 1:
                    skewed_cols.append(col)
            
            if skewed_cols:
                findings.append(
                    f"Columns with high skewness: {', '.join(skewed_cols)} - "
                    "Consider log transformation or robust statistics"
                )

        # Categorical columns insights
        if categorical_cols:
            findings.append(
                f"Identified {len(categorical_cols)} categorical columns: {', '.join(categorical_cols[:5])}"
            )

        # Outliers insights
        if outliers:
            outlier_cols = list(outliers.keys())
            findings.append(
                f"Detected outliers in {len(outlier_cols)} columns: {', '.join(outlier_cols)}. "
                f"Investigate or consider removing for robust analysis."
            )

        # Correlations insights
        if correlations:
            findings.append(
                f"Found {len(correlations)} strong correlations between variables. "
                f"This may indicate multicollinearity or causal relationships."
            )

        # Duplicates check
        duplicates = data_quality.get("duplicate_rows", 0)
        if duplicates > 0:
            findings.append(
                f"Detected {duplicates} duplicate rows. Consider deduplication."
            )

        insights["findings"] = findings

        # Generate recommendations based on findings and stats
        recommendations = []

        # Data quality recommendations
        if completeness < 95:
            recommendations.append(
                "Investigate missing data patterns and consider imputation strategies"
            )

        if duplicates > 0:
            recommendations.append("Remove or investigate duplicate rows for data integrity")

        # Outlier handling recommendations
        if outliers:
            recommendations.append(
                "Review and decide whether to keep, remove, or transform outliers"
            )

        # Statistical recommendations
        if numeric_cols:
            for col, col_stats in stats.get("numeric_columns", {}).items():
                mean_val = col_stats.get("mean", 0)
                std_val = col_stats.get("std", 0)
                cv = (std_val / mean_val * 100) if mean_val != 0 else 0
                
                if cv > 50:
                    recommendations.append(
                        f"Column '{col}' has high coefficient of variation ({cv:.2f}%) - "
                        "may indicate high variability or need for standardization"
                    )

        # Correlation recommendations
        if correlations:
            recommendations.append(
                "Use regularization techniques (L1/L2) to handle multicollinearity in modeling"
            )

        # Add exploration recommendations
        recommendations.append("Perform exploratory data analysis to understand distributions")
        recommendations.append("Segment data by categorical variables and analyze subgroups")
        recommendations.append("Consider feature engineering based on domain knowledge")

        insights["recommendations"] = recommendations

        # Outlier summary
        outlier_summary = {}
        for col, outlier_info in outliers.items():
            outlier_summary[col] = (
                f"{outlier_info.get('count', 0)} outliers ({outlier_info.get('percentage', 0):.2f}%)"
            )
        
        insights["outlier_summary"] = outlier_summary

        # Additional insights structure
        insights["correlation_insights"] = [
            f"{corr['col1']} and {corr['col2']} are strongly correlated ({corr['correlation']:.3f})"
            for corr in correlations[:5]  # Top 5 correlations
        ]

        # Data distribution insights
        distribution_insights = []
        for col, col_stats in stats.get("numeric_columns", {}).items():
            skewness = col_stats.get("skewness", 0)
            kurtosis = col_stats.get("kurtosis", 0)
            
            if abs(skewness) < 0.5:
                dist_type = "approximately normal"
            elif skewness > 0:
                dist_type = "right-skewed"
            else:
                dist_type = "left-skewed"
            
            distribution_insights.append(f"'{col}' distribution: {dist_type}")
        
        insights["distribution_insights"] = distribution_insights

        state.insights = insights
        logger.info("Insights agent complete. Generated %d findings and %d recommendations",
                    len(findings), len(recommendations))

    except Exception as e:
        error_msg = f"Insights error: {e}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    state.completed_agents.append("insights")
    return state
