import logging
import os
import json
from core.state import AnalysisState

logger = logging.getLogger(__name__)


def _fallback_insights(state: AnalysisState) -> dict:
    """Generate basic insights from stats without LLM."""
    stats = state.stats_summary
    findings: list[str] = []
    anomalies: list[str] = []
    recommendations: list[str] = []

    # Findings from correlations
    for corr in stats.get("top_correlations", [])[:3]:
        a, b, r = corr
        direction = "positive" if r > 0 else "negative"
        findings.append(
            f"Strong {direction} correlation (r={r:.2f}) between {a} and {b}"
        )

    # Findings from data shape
    shape = stats.get("shape", [0, 0])
    findings.append(f"Dataset contains {shape[0]:,} rows and {shape[1]} columns")

    # Anomalies from outliers
    for col, info in stats.get("outliers", {}).items():
        anomalies.append(
            f"{info['count']} outliers detected in {col} "
            f"({info['percentage']}% of data)"
        )

    # Anomalies from missing data
    nulls = stats.get("nulls", {})
    if nulls:
        worst = max(nulls, key=nulls.get)
        anomalies.append(f"Column '{worst}' has {nulls[worst]} missing values")

    if not anomalies:
        anomalies.append("No significant anomalies detected in this dataset")

    # Recommendations
    if nulls:
        recommendations.append("Investigate and address missing data patterns")
    if stats.get("outliers"):
        recommendations.append("Review outlier values for data quality issues")
    if stats.get("top_correlations"):
        recommendations.append(
            "Leverage strong correlations for predictive modeling"
        )
    if not recommendations:
        recommendations.append("Dataset appears clean; proceed with analysis")

    return {
        "key_findings": findings,
        "anomalies": anomalies,
        "recommendations": recommendations,
    }


def insights_agent(state: AnalysisState) -> AnalysisState:
    """
    Insights Agent: Produces structured key_findings, anomalies,
    and recommendations using Groq LLM (with a stats-based fallback).
    """
    state.current_agent = "insights"
    logger.info("Insights agent started")

    if not state.stats_summary:
        state.errors.append("Insights: No statistical summary available")
        state.completed_agents.append("insights")
        return state

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        logger.info("GROQ_API_KEY not set; using fallback insights")
        state.insights = _fallback_insights(state)
        state.completed_agents.append("insights")
        return state

    try:
        from groq import Groq

        client = Groq(api_key=api_key)
        columns = (
            list(state.clean_df.columns)
            if state.clean_df is not None
            else []
        )

        # Truncate stats to fit in prompt context
        stats_text = json.dumps(state.stats_summary, default=str)[:3000]

        prompt = (
            "Analyze this dataset and return ONLY valid JSON "
            "(no markdown, no explanation).\n\n"
            f"Dataset columns: {columns}\n"
            f"Statistics: {stats_text}\n\n"
            "Return JSON with exactly these keys:\n"
            '{\n'
            '  "key_findings": ["finding1", "finding2", ...],\n'
            '  "anomalies": ["anomaly1", ...],\n'
            '  "recommendations": ["rec1", ...]\n'
            '}\n'
            "Provide 3-5 items per list. Be specific and data-driven."
        )

        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )

        text = response.choices[0].message.content.strip()

        # Strip markdown fences if present
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.split("```")[0].strip()

        parsed = json.loads(text)
        state.insights = {
            "key_findings": parsed.get("key_findings", []),
            "anomalies": parsed.get("anomalies", []),
            "recommendations": parsed.get("recommendations", []),
        }
        logger.info("Insights agent complete (LLM)")

    except Exception as e:
        logger.warning("LLM insights failed (%s), using fallback", e)
        state.insights = _fallback_insights(state)

    state.completed_agents.append("insights")
    return state
