"""
Insights Agent
==============
Produces structured key_findings, anomalies, and recommendations.
Uses Groq LLM when available; falls back to rule-based plain-English insights.
"""

import logging
import os
import json
from typing import List
from core.state import AnalysisState

logger = logging.getLogger(__name__)


def _fallback_insights(state: AnalysisState) -> dict:
    """Generate plain-English insights from stats — no LLM needed."""
    stats   = state.stats_summary
    df      = state.clean_df
    findings: List[str] = []
    anomalies: List[str] = []
    recommendations: List[str] = []

    shape = stats.get("shape", [0, 0])
    findings.append(
        f"The dataset has {shape[0]:,} records and {shape[1]} fields — "
        + ("a large enough sample for reliable patterns."
           if shape[0] >= 500 else "a small sample, so patterns may not generalise well.")
    )

    # Correlation findings
    for corr in stats.get("top_correlations", [])[:3]:
        a, b, r = corr
        direction = "tends to go up when" if r > 0 else "tends to go down when"
        strength  = "very strongly" if abs(r) >= 0.8 else "noticeably"
        findings.append(
            f'"{a}" {strength} {direction} "{b}" increases '
            f"(correlation: {r:.2f}). This could be worth investigating further."
        )

    # Category insights
    cat_counts = stats.get("category_counts", {})
    for col, counts in list(cat_counts.items())[:2]:
        if counts:
            top_k = list(counts.keys())[0]
            top_v = list(counts.values())[0]
            total = sum(counts.values())
            pct = round(top_v / total * 100, 1) if total > 0 else 0
            findings.append(
                f'In "{col}", the most common value is "{top_k}" '
                f"which appears in {pct}% of rows."
            )

    if not findings:
        findings.append("Not enough numeric data to identify clear patterns.")

    # Anomalies
    for col, info in stats.get("outliers", {}).items():
        anomalies.append(
            f'"{col}" has {info["count"]} unusually extreme values '
            f'({info["percentage"]}% of rows). These could be errors or rare events.'
        )

    nulls = stats.get("nulls", {})
    if nulls:
        worst_col  = max(nulls, key=lambda c: nulls[c])
        worst_count = nulls[worst_col]
        anomalies.append(
            f'"{worst_col}" is missing {worst_count} values. '
            "Check whether the gaps follow a pattern (e.g. always missing for a certain group)."
        )

    num_stats = stats.get("numeric_stats", {})
    for col, info in num_stats.items():
        if info.get("skewness") and abs(info["skewness"]) > 2:
            direction = "right (lots of high values)" if info["skewness"] > 0 else "left (lots of low values)"
            anomalies.append(
                f'"{col}" is heavily skewed to the {direction}. '
                "The average may not be a good summary — consider the median instead."
            )
            break  # one skewness note is enough

    if not anomalies:
        anomalies.append(
            "No significant data quality issues detected. The dataset looks clean."
        )

    # Recommendations
    if nulls:
        recommendations.append(
            "Investigate why certain fields have missing values — "
            "is it random or does it relate to another column?"
        )
    if stats.get("outliers"):
        recommendations.append(
            "Review the extreme values flagged above. Decide whether to keep, "
            "correct, or remove them before using this data for decisions."
        )
    if stats.get("top_correlations"):
        recommendations.append(
            "The strong relationships found here could be useful for prediction. "
            "Consider building a simple model using the correlated columns."
        )
    if shape[0] < 200:
        recommendations.append(
            "The dataset is quite small. Collecting more data will make any "
            "conclusions more reliable."
        )
    if not recommendations:
        recommendations.append(
            "Data looks solid. You can proceed confidently with analysis or reporting."
        )

    return {
        "key_findings":    findings,
        "anomalies":       anomalies,
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

        client  = Groq(api_key=api_key)
        columns = list(state.clean_df.columns) if state.clean_df is not None else []

        # Truncate stats to fit context
        stats_text = json.dumps(state.stats_summary, default=str)[:3500]

        prompt = (
            "You are a friendly data analyst explaining findings to a non-technical audience.\n"
            "Analyze this dataset and return ONLY valid JSON (no markdown, no extra text).\n\n"
            f"Columns: {columns}\n"
            f"Statistics: {stats_text}\n\n"
            "Return JSON with exactly these keys:\n"
            "{\n"
            '  "key_findings": ["plain English finding 1", ...],\n'
            '  "anomalies": ["plain English anomaly 1", ...],\n'
            '  "recommendations": ["plain English recommendation 1", ...]\n'
            "}\n\n"
            "Rules:\n"
            "- 3–5 items per list\n"
            "- Use plain English, no jargon\n"
            "- Be specific and mention actual column names\n"
            "- Do NOT use bullet characters inside strings"
        )

        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=800,
        )

        text = response.choices[0].message.content.strip()

        # Strip markdown fences if present
        if "```" in text:
            parts = text.split("```")
            text  = parts[1] if len(parts) > 1 else parts[0]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        parsed = json.loads(text)
        state.insights = {
            "key_findings":    parsed.get("key_findings",    []),
            "anomalies":       parsed.get("anomalies",       []),
            "recommendations": parsed.get("recommendations", []),
        }
        logger.info("Insights agent complete (LLM)")

    except Exception as e:
        logger.warning("LLM insights failed (%s); using fallback", e)
        state.insights = _fallback_insights(state)

    state.completed_agents.append("insights")
    return state