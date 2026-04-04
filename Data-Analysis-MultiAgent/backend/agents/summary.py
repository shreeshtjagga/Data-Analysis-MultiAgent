import logging
import os
import numpy as np
import pandas as pd
from core.state import AnalysisState

logger = logging.getLogger(__name__)


def summary_agent(state: AnalysisState) -> AnalysisState:
    """
    Summary Agent: Computes structured summary data that the frontend expects,
    including health score, numeric highlights, top categories, and date ranges.
    Optionally uses Groq LLM for an executive summary paragraph.
    """
    state.current_agent = "summary"
    logger.info("Summary agent started")

    try:
        df = state.clean_df
        raw_df = state.raw_df
        stats = state.stats_summary
        col_types = state.column_types

        if df is None or df.empty:
            raise ValueError("No clean data available for summary")

        summary: dict = {}

        # ── Basic shape ──────────────────────────────────────────────────
        summary["rows"] = int(df.shape[0])
        summary["cols"] = int(df.shape[1])

        # ── Column type lists ────────────────────────────────────────────
        numeric_cols = [c for c, t in col_types.items() if t == "numeric"]
        cat_cols = [c for c, t in col_types.items() if t == "categorical"]
        date_cols = [c for c, t in col_types.items() if t == "datetime"]
        summary["numeric_cols"] = numeric_cols
        summary["cat_cols"] = cat_cols
        summary["date_cols"] = date_cols

        # ── Missing rate (based on raw data) ─────────────────────────────
        if raw_df is not None and not raw_df.empty:
            raw_total = raw_df.shape[0] * raw_df.shape[1]
            raw_missing = int(raw_df.isna().sum().sum())
        else:
            raw_total = df.shape[0] * df.shape[1]
            raw_missing = int(df.isna().sum().sum())
        summary["missing_rate_pct"] = (
            round(float(raw_missing / raw_total * 100), 1) if raw_total > 0 else 0
        )

        # ── Health score (0-100) ─────────────────────────────────────────
        completeness = (1 - raw_missing / raw_total) * 100 if raw_total > 0 else 100
        dup_rate = (
            (1 - len(df) / len(raw_df)) * 100
            if raw_df is not None and len(raw_df) > 0
            else 0
        )
        outlier_cols = len(stats.get("outliers", {}))
        total_num = max(len(numeric_cols), 1)
        outlier_penalty = min(outlier_cols / total_num * 30, 30)
        health = max(0, min(100, int(completeness - dup_rate * 0.5 - outlier_penalty)))
        summary["health_score"] = health

        # ── Numeric highlights ───────────────────────────────────────────
        highlights = []
        num_stats = stats.get("numeric_stats", {})
        for col in numeric_cols[:5]:
            info = num_stats.get(col, {})
            if info:
                highlights.append({
                    "column": col,
                    "mean": round(info["mean"], 2),
                    "min": round(info["min"], 2),
                    "max": round(info["max"], 2),
                    "std": round(info["std"], 2),
                })
        summary["highlights"] = highlights

        # ── Date range ───────────────────────────────────────────────────
        date_range = None
        for col in date_cols:
            try:
                dt_series = pd.to_datetime(df[col])
                date_range = {
                    "column": col,
                    "from": str(dt_series.min().date()),
                    "to": str(dt_series.max().date()),
                    "span_days": int((dt_series.max() - dt_series.min()).days),
                }
                break
            except Exception:
                continue
        summary["date_range"] = date_range

        # ── Top categories ───────────────────────────────────────────────
        top_categories: dict = {}
        for col in cat_cols[:5]:
            vc = df[col].value_counts()
            if len(vc) > 0:
                top_categories[col] = {
                    "top_value": str(vc.index[0]),
                    "top_pct": round(float(vc.iloc[0] / len(df) * 100), 1),
                    "unique": int(df[col].nunique()),
                }
        summary["top_categories"] = top_categories

        # ── Top correlations (reuse from stats) ──────────────────────────
        summary["top_correlations"] = stats.get("top_correlations", [])

        # ── Optional LLM executive summary ───────────────────────────────
        api_key = os.getenv("GROQ_API_KEY")
        if api_key:
            try:
                from groq import Groq

                client = Groq(api_key=api_key)
                prompt = (
                    "Write a concise 3-4 sentence executive summary of this dataset:\n"
                    f"- {summary['rows']} rows, {summary['cols']} columns\n"
                    f"- Numeric columns: {', '.join(numeric_cols[:5])}\n"
                    f"- Categorical columns: {', '.join(cat_cols[:5])}\n"
                    f"- Data health: {health}/100\n"
                    f"- Missing data: {summary['missing_rate_pct']}%\n"
                    f"- Outlier columns: {outlier_cols}\n"
                    "Write only the summary paragraph."
                )
                response = client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.4,
                )
                summary["executive_summary"] = (
                    response.choices[0].message.content.strip()
                )
            except Exception as e:
                logger.warning("LLM summary failed: %s", e)
                summary["executive_summary"] = ""
        else:
            logger.info("GROQ_API_KEY not set; skipping LLM executive summary")
            summary["executive_summary"] = ""

        state.summary = summary
        logger.info("Summary agent complete. Health score: %d", health)

    except Exception as e:
        error_msg = f"Summary error: {e}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    state.completed_agents.append("summary")
    return state
