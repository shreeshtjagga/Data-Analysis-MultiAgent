"""
Insights Agent (LLM-driven)
────────────────────────────
Reads the stats_summary + dataset_profile and makes one LLM call to produce
specific, narrative findings referencing actual column names and real numbers.

Falls back to lightweight rule-based logic if the LLM call fails.
"""

import json
import logging
import os

from ..core.state import AnalysisState
from ..core.utils import truncate_stats_for_llm

logger = logging.getLogger(__name__)


# ── LLM-powered insights ─────────────────────────────────────────────────────

def _build_llm_prompt(stats: dict) -> str:
    """Build a compact prompt from truncated stats."""
    profile = stats.get("dataset_profile") or {}
    dq = stats.get("data_quality") or {}
    excluded = stats.get("excluded_columns") or []
    imputations = stats.get("imputations") or []

    lines = [
        "You are a senior data analyst. Produce actionable insights for this dataset.",
        "",
        f"Dataset: {profile.get('label', 'unknown')} — {profile.get('description', 'N/A')}",
        f"Domain: {profile.get('domain', 'general')}",
        f"Rows: {stats.get('row_count', '?')}, Columns: {stats.get('column_count', '?')}",
        f"Completeness: {dq.get('completeness', 100):.1f}%, Missing cells: {dq.get('missing_cells', 0)}",
        "",
    ]

    # Numeric columns
    numeric = stats.get("numeric_columns", {})
    if numeric:
        lines.append("Numeric columns (key stats):")
        for col, s in numeric.items():
            lines.append(
                f"  {col}: mean={s.get('mean','?'):.2f}, "
                f"std={s.get('std','?'):.2f}, "
                f"min={s.get('min','?'):.2f}, max={s.get('max','?'):.2f}, "
                f"skew={s.get('skewness','?'):.2f}"
            )
        if stats.get("numeric_columns_note"):
            lines.append(f"  ({stats['numeric_columns_note']})")

    # Correlations
    correlations = stats.get("strong_correlations", [])
    if correlations:
        lines.append("\nStrong correlations:")
        for c in correlations:
            lines.append(f"  {c['col1']} ↔ {c['col2']}: r={c['correlation']:.3f}")

    # Outliers
    outlier_counts = stats.get("outlier_counts", {})
    if outlier_counts:
        lines.append(
            "\nOutliers: "
            + ", ".join(f"{col}({n})" for col, n in outlier_counts.items())
        )

    # Excluded columns
    if excluded:
        lines.append(
            "\nExcluded columns: "
            + ", ".join(f"{e['column']}({e['reason']})" for e in excluded[:5])
        )

    # Imputations
    if imputations:
        lines.append(
            "\nImputations: "
            + ", ".join(
                f"{i['column']}({i['count']} cells, {i['strategy']}={i.get('fill_value')})"
                for i in imputations
            )
        )

    lines.extend([
        "",
        "Respond with ONLY valid JSON (no markdown, no explanation):",
        "{",
        '  "headline": "One executive-summary sentence about this dataset",',
        '  "findings": ["5-8 specific findings referencing column names and numbers"],',
        '  "recommendations": ["3-5 actionable recommendations"],',
        '  "risk_flags": ["any data-quality or analysis concerns"]',
        "}",
    ])

    return "\n".join(lines)


def _llm_insights(stats: dict) -> dict | None:
    """Call Groq LLM for narrative insights. Returns None on failure."""
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return None

    prompt = _build_llm_prompt(stats)

    try:
        from groq import Groq

        client = Groq(api_key=api_key)
        completion = client.chat.completions.create(
            model=os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"),
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a senior data analyst. "
                        "Always respond with valid JSON only. No markdown fences."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            max_tokens=800,
        )
        raw = (completion.choices[0].message.content or "").strip()
        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        result = json.loads(raw)
        logger.info(
            "LLM insights generated: %d findings, %d recommendations",
            len(result.get("findings", [])),
            len(result.get("recommendations", [])),
        )
        return result
    except Exception as exc:
        logger.warning("LLM insights failed, falling back to rules: %s", exc)
        return None


# ── Rule-based fallback ───────────────────────────────────────────────────────

def _rule_based_insights(stats: dict) -> dict:
    """Lightweight template-based insights when the LLM is unavailable."""
    numeric_cols = list(stats.get("numeric_columns", {}).keys())
    categorical_cols = list(stats.get("categorical_columns", {}).keys())
    outliers = stats.get("outliers", {})
    correlations = stats.get("strong_correlations", [])
    dq = stats.get("data_quality", {})

    findings = [
        f"Dataset contains {stats.get('row_count', 0)} rows and "
        f"{stats.get('column_count', 0)} columns",
    ]

    completeness = dq.get("completeness", 100)
    if completeness < 100:
        findings.append(
            f"Data completeness is {completeness:.2f}% with "
            f"{dq.get('missing_cells', 0)} missing values"
        )

    if numeric_cols:
        findings.append(
            f"Identified {len(numeric_cols)} numeric columns: "
            f"{', '.join(numeric_cols[:5])}"
        )

    if categorical_cols:
        findings.append(
            f"Identified {len(categorical_cols)} categorical columns: "
            f"{', '.join(categorical_cols[:5])}"
        )

    if outliers:
        findings.append(
            f"Detected outliers in {len(outliers)} columns: "
            f"{', '.join(outliers.keys())}"
        )

    if correlations:
        findings.append(
            f"Found {len(correlations)} strong correlations between variables"
        )

    recommendations = [
        "Perform exploratory data analysis to understand distributions",
        "Segment data by categorical variables and analyze subgroups",
        "Consider feature engineering based on domain knowledge",
    ]
    if completeness < 95:
        recommendations.insert(0, "Investigate missing data patterns")
    if outliers:
        recommendations.insert(0, "Review and decide on outlier handling")

    return {
        "headline": None,
        "findings": findings,
        "recommendations": recommendations,
        "risk_flags": [],
    }


# ── Computed fields (always derived from stats, not LLM) ─────────────────────

def _computed_insights(stats: dict) -> dict:
    """Fields that are pure data reformatting — no LLM needed."""
    outliers = stats.get("outliers", {})
    correlations = stats.get("strong_correlations", [])
    numeric = stats.get("numeric_columns", {})

    outlier_summary = {}
    for col, info in outliers.items():
        outlier_summary[col] = (
            f"{info.get('count', 0)} outliers ({info.get('percentage', 0):.2f}%)"
        )

    correlation_insights = [
        f"{c['col1']} and {c['col2']} are strongly correlated "
        f"({c['correlation']:.3f})"
        for c in correlations[:5]
    ]

    distribution_insights = []
    for col, col_stats in numeric.items():
        skewness = col_stats.get("skewness", 0)
        if abs(skewness) < 0.5:
            dist_type = "approximately normal"
        elif skewness > 0:
            dist_type = "right-skewed"
        else:
            dist_type = "left-skewed"
        distribution_insights.append(f"'{col}' distribution: {dist_type}")

    return {
        "outlier_summary": outlier_summary,
        "correlation_insights": correlation_insights,
        "distribution_insights": distribution_insights,
    }


# ── Agent entry point ─────────────────────────────────────────────────────────

def insights_agent(state: AnalysisState) -> AnalysisState:
    """
    Insights Agent: produces findings, recommendations, and outlier summaries.
    Uses LLM when available, falls back to rule-based logic.
    """
    state.current_agent = "insights"
    logger.info("Insights agent started")

    try:
        if not state.stats_summary:
            raise ValueError("Missing stats_summary from statistician agent")

        stats = state.stats_summary

        # Truncate stats before sending to LLM (Fix 12)
        slim_stats = truncate_stats_for_llm(stats)

        # Try LLM first, fall back to rules
        llm_result = _llm_insights(slim_stats)
        if llm_result:
            insights = {
                "headline": llm_result.get("headline"),
                "findings": llm_result.get("findings", []),
                "recommendations": llm_result.get("recommendations", []),
                "risk_flags": llm_result.get("risk_flags", []),
            }
        else:
            insights = _rule_based_insights(stats)

        # Always add computed fields from raw stats
        insights.update(_computed_insights(stats))

        state.insights = insights
        logger.info(
            "Insights complete. %d findings, %d recommendations (LLM=%s)",
            len(insights.get("findings", [])),
            len(insights.get("recommendations", [])),
            llm_result is not None,
        )

    except Exception as e:
        error_msg = f"Insights error: {e}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    state.completed_agents.append("insights")
    return state
