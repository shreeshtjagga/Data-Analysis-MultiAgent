import json
import logging
import re
from langchain_groq import ChatGroq
from core.state import AnalysisState

logger = logging.getLogger(__name__)


def _compress_stats(stats: dict, max_numeric: int = 5, max_cat: int = 3) -> dict:
    """
    Strips stats_summary down to a tight context packet.
    Avoids dumping the full dict (outlier_indices, full correlation matrix, etc.)
    which can blow 2-3k tokens on its own.
    """
    numeric    = stats.get("numeric_columns", {})
    categorical = stats.get("categorical_columns", {})
    outliers   = stats.get("outliers", {})
    quality    = stats.get("data_quality", {})

    compact_numeric = {
        col: {
            "mean":   round(info["mean"], 2),
            "median": round(info["median"], 2),
            "std":    round(info["std"], 2),
            "min":    round(info["min"], 2),
            "max":    round(info["max"], 2),
            "skew":   round(info["skewness"], 2),
        }
        for col, info in list(numeric.items())[:max_numeric]
    }

    compact_cat = {
        col: {
            "unique":    info["unique_values"],
            "top":       info["most_common"],
            "top_count": info["most_common_count"],
        }
        for col, info in list(categorical.items())[:max_cat]
    }

    compact_outliers = {
        col: {"count": v["count"], "pct": round(v["percentage"], 1)}
        for col, v in outliers.items()
    }

    strong_corr = [
        {"a": c["col1"], "b": c["col2"], "r": round(c["correlation"], 2)}
        for c in stats.get("strong_correlations", [])
    ]

    return {
        "rows":             stats.get("row_count"),
        "cols":             stats.get("column_count"),
        "completeness_pct": round(quality.get("completeness", 100), 1),
        "duplicates":       quality.get("duplicate_rows", 0),
        "missing_cols":     list(stats.get("missing_values", {}).keys()),
        "numeric":          compact_numeric,
        "categorical":      compact_cat,
        "outliers":         compact_outliers,
        "strong_corr":      strong_corr,
    }


def insights_agent(state: AnalysisState) -> AnalysisState:
    """
    Insights Agent — single LLM call, compressed context.
    Produces all insight fields + executive_summary in one shot.
    Eliminates the need for a separate summary agent call.
    """
    state.current_agent = "insights"
    logger.info("Insights Agent started (token-optimised, single call).")

    if not state.stats_summary:
        state.errors.append("Insights Failure: No statistical summary found.")
        state.completed_agents.append("insights")
        return state

    try:
        llm = ChatGroq(model_name="llama-3.3-70b-versatile", temperature=0.3)
        columns = list(state.clean_df.columns) if state.clean_df is not None else []
        ctx = _compress_stats(state.stats_summary)

        prompt = f"""You are a Principal Data Scientist. Analyse this dataset summary and return a single JSON object.

DATASET:
columns: {columns}
{json.dumps(ctx, separators=(',', ':'))}

Return ONLY this JSON (no markdown fences, no extra text):
{{
  "key_findings": [
    "4 strings — each must name a column and include a specific number"
  ],
  "anomalies": [
    "3 strings — flag skew>1, outlier%, dominant categories, or quality issues. Name the column."
  ],
  "recommendations": [
    "3 strings — label P1/P2/P3, start with an imperative verb, name column, explain why"
  ],
  "executive_summary": "2-3 sentence plain prose paragraph. What the dataset contains, the single most important pattern with a number, one next step. No markdown.",
  "strategic_overview": "2 sentences. Most critical business reality. Use **double asterisks** around the key number.",
  "risk_signals": "1 sentence naming the biggest data quality or anomaly concern with specific column names.",
  "priority_action": "1 sentence starting with an imperative verb. The single most important action."
}}"""

        raw = llm.invoke(prompt).content.strip()
        # Strip accidental markdown fences
        raw = re.sub(r"^```(?:json)?", "", raw).strip()
        raw = re.sub(r"```$", "", raw).strip()

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Insights: JSON parse failed, using empty fallback.")
            parsed = {}

        # ── Populate state.insights ────────────────────────────────────────
        state.insights["key_findings"]    = parsed.get("key_findings", [])
        state.insights["anomalies"]       = parsed.get("anomalies", [])
        state.insights["recommendations"] = parsed.get("recommendations", [])
        state.insights["executive_summary"] = parsed.get("executive_summary", "")

        # ── Compose final_report locally — zero extra tokens ───────────────
        overview = parsed.get("strategic_overview", "")
        risk     = parsed.get("risk_signals", "")
        action   = parsed.get("priority_action", "")

        findings_md = "\n".join(f"- {f}" for f in state.insights["key_findings"])
        anomaly_md  = "\n".join(f"- {a}" for a in state.insights["anomalies"])
        recs_md     = "\n".join(f"- {r}" for r in state.insights["recommendations"])

        state.insights["final_report"] = f"""### Strategic Overview
{overview}

### What the Data Reveals
{findings_md}

### Risk Signals
{risk}

{anomaly_md}

### Priority Action
{action}

### Full Recommendations
{recs_md}"""

        logger.info("Insights Agent complete — single LLM call, all fields populated.")

    except Exception as e:
        state.errors.append(f"Insights error: {str(e)}")
        logger.error("Insights error: %s", e)

    state.completed_agents.append("insights")
    return state
