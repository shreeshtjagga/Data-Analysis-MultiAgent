import logging
from core.state import AnalysisState

logger = logging.getLogger(__name__)


def summary_agent(state: AnalysisState) -> AnalysisState:
    """
    Summary Agent — lightweight pass.

    The insights_agent already writes executive_summary into state.insights
    in a single combined LLM call. This agent simply validates that it exists
    and logs completion. No additional LLM call is made, saving ~500-800 tokens
    per pipeline run.

    If for any reason executive_summary is missing (e.g. insights agent failed),
    it assembles a fallback summary from raw stats — still no LLM call needed.
    """
    state.current_agent = "summary"
    logger.info("Summary agent started.")

    try:
        if state.insights is None:
            state.insights = {}

        # ── Happy path: insights_agent already wrote this ──────────────────
        if state.insights.get("executive_summary"):
            logger.info("Summary agent: executive_summary already present, skipping LLM call.")
            state.completed_agents.append("summary")
            return state

        # ── Fallback: build a stat-based summary without any LLM call ──────
        stats = state.stats_summary
        if not stats:
            raise ValueError("No stats_summary available for fallback summary.")

        rows    = stats.get("row_count", 0)
        cols    = stats.get("column_count", 0)
        columns = stats.get("columns", [])
        quality = stats.get("data_quality", {})
        numeric = stats.get("numeric_columns", {})
        outliers = stats.get("outliers", {})
        strong_corr = stats.get("strong_correlations", [])
        completeness = quality.get("completeness", 100)

        # Pick the most "interesting" numeric column (highest std/mean ratio)
        highlight_col, highlight_info = None, None
        best_ratio = 0
        for col, info in numeric.items():
            if info["mean"] != 0:
                ratio = abs(info["std"] / info["mean"])
                if ratio > best_ratio:
                    best_ratio = ratio
                    highlight_col = col
                    highlight_info = info

        # Assemble deterministic summary
        parts = [
            f"This dataset contains {rows:,} rows and {cols} columns "
            f"({', '.join(columns[:5])}{'…' if len(columns) > 5 else ''})."
        ]

        if highlight_col and highlight_info:
            parts.append(
                f"{highlight_col} shows high variability with a mean of "
                f"{highlight_info['mean']:.2f} and standard deviation of "
                f"{highlight_info['std']:.2f} (range: {highlight_info['min']:.2f}–{highlight_info['max']:.2f})."
            )

        if strong_corr:
            top = strong_corr[0]
            parts.append(
                f"A strong correlation (r={top['correlation']:.2f}) exists between "
                f"{top['col1']} and {top['col2']}."
            )

        if completeness < 100:
            parts.append(
                f"Data completeness is {completeness:.1f}% — "
                f"missing values should be reviewed before modelling."
            )
        elif outliers:
            top_outlier = max(outliers.items(), key=lambda x: x[1]["percentage"])
            parts.append(
                f"{top_outlier[0]} contains {top_outlier[1]['percentage']:.1f}% outliers "
                f"which may skew aggregate metrics."
            )
        else:
            parts.append("Data quality appears clean with no significant missing values or outliers.")

        state.insights["executive_summary"] = " ".join(parts)
        logger.info("Summary agent: fallback summary assembled from stats (no LLM call).")

    except Exception as e:
        logger.error("Summary error: %s", e)
        state.errors.append(f"Summary error: {e}")

    state.completed_agents.append("summary")
    return state
