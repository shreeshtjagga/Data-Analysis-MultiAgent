import json
import logging
import os
from typing import Optional

from ..core.state import AnalysisState
from ..core.errors import add_pipeline_error
from ..core.utils import truncate_stats_for_llm

logger = logging.getLogger(__name__)


def _build_llm_prompt(stats: dict) -> str:
    payload_json = json.dumps(stats, ensure_ascii=True)
    return "\n".join([
        "You are an Expert Data Analyst. Respond in plain, simple English.",
        "Treat the payload as the truth about a real-world domain.",
        "",
        "Dataset payload (JSON):",
        f"<analysis_json>{payload_json}</analysis_json>",
        "",
        "Respond with ONLY valid JSON (no markdown, no explanation):",
        "{",
        '  "headline": "One powerful conclusion drawn from the data in one sentence.",',
        '  "data_info": ["3-5 factual statements about WHAT this dataset is: its structure, columns, types, size, completeness, and what domain/subject it covers. Plain language, no analysis."],',
        '  "findings": ["5-8 conclusions and patterns DRAWN from the data: correlations, distributions, dominant categories, outlier patterns, trends. Each must be a specific, number-backed observation in plain language."]',
        "}",
    ])




def _llm_insights(stats: dict) -> Optional[dict]:
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


def _rule_based_insights(stats: dict) -> dict:
    numeric_cols = list(stats.get("numeric_columns", {}).keys())
    categorical_cols = list(stats.get("categorical_columns", {}).keys())
    outliers = stats.get("outliers", {})
    correlations = stats.get("strong_correlations", [])
    dq = stats.get("data_quality", {})
    profile = stats.get("dataset_profile", {})

    # data_info: structural facts about what the dataset IS
    data_info = [
        f"This dataset contains {stats.get('row_count', 0):,} rows and {stats.get('column_count', 0)} columns.",
    ]
    if profile.get("label"):
        data_info.append(f"Dataset type: {profile['label']}." + (f" Domain: {profile['domain']}." if profile.get('domain') else ""))
    if numeric_cols:
        data_info.append(f"Numeric columns ({len(numeric_cols)}): {', '.join(numeric_cols[:6])}{'...' if len(numeric_cols) > 6 else ''}.")
    if categorical_cols:
        data_info.append(f"Categorical columns ({len(categorical_cols)}): {', '.join(categorical_cols[:6])}{'...' if len(categorical_cols) > 6 else ''}.")
    completeness = dq.get("completeness", 100)
    data_info.append(f"Data completeness is {completeness:.1f}% with {dq.get('missing_cells', 0)} missing values.")

    # findings: what can be drawn/concluded from the data
    findings = []
    if outliers:
        findings.append(f"Outliers detected in {len(outliers)} column(s): {', '.join(list(outliers.keys())[:4])}.")
    if correlations:
        best = correlations[0]
        findings.append(
            f"Strongest correlation: {best['col1']} and {best['col2']} (r={best['correlation']:.2f})."
        )
        if len(correlations) > 1:
            findings.append(f"{len(correlations)} strong variable relationships found overall.")
    cat_stats = stats.get("categorical_columns", {})
    for col, info in list(cat_stats.items())[:2]:
        if info.get("most_common"):
            pct = round(info.get("most_common_count", 0) / max(stats.get("row_count", 1), 1) * 100, 1)
            findings.append(f"In '{col}', the most common value is '{info['most_common']}' ({pct}% of rows).")
    if not findings:
        findings.append("No strong patterns detected — the dataset may need more varied data for richer insights.")

    return {
        "headline": "",
        "data_info": data_info,
        "findings": findings,
    }



def _computed_insights(stats: dict) -> dict:
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
    for col, col_stats in list(numeric.items())[:10]:
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


def insights_agent(state: AnalysisState) -> AnalysisState:
    state.current_agent = "insights"
    logger.info("Insights agent started")

    try:
        if not state.stats_summary:
            raise ValueError("Missing stats_summary from statistician agent")

        stats = state.stats_summary
        slim_stats = truncate_stats_for_llm(stats)

        llm_result = _llm_insights(slim_stats)
        if llm_result:
            insights = {
                "headline": llm_result.get("headline"),
                "data_info": llm_result.get("data_info", []),
                "findings": llm_result.get("findings", []),
            }
        else:
            insights = _rule_based_insights(stats)

        insights.update(_computed_insights(stats))

        state.insights = insights
        logger.info(
            "Insights complete. %d data_info, %d findings (LLM=%s)",
            len(insights.get("data_info", [])),
            len(insights.get("findings", [])),
            llm_result is not None,
        )

    except Exception as e:
        logger.error("Insights error: %s", e)
        add_pipeline_error(
            state.errors,
            code="INSIGHTS_FAILED",
            message=str(e),
            agent="insights",
            error_type="agent",
        )

    state.completed_agents.append("insights")
    return state

