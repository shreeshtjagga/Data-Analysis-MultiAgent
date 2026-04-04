"""
Summary Agent
=============
Produces bullet-point plain-English summaries for non-technical users.
No jargon. Every bullet is a single clear sentence anyone can understand.
"""

from typing import Optional, List
import logging
import os
import pandas as pd
from core.state import AnalysisState

logger = logging.getLogger(__name__)


def _health_word(score: int) -> str:
    if score >= 80:
        return "excellent"
    if score >= 60:
        return "good"
    if score >= 40:
        return "fair"
    return "poor"


def _build_bullets(summary: dict, df: pd.DataFrame,
                   num_stats: dict, outliers: dict) -> List[str]:
    """
    Return a flat list of plain-English bullet strings.
    Each string is ONE complete sentence — clear, simple, no stats jargon.
    """
    bullets: List[str] = []
    rows = summary["rows"]
    cols = summary["cols"]
    numeric_cols = summary["numeric_cols"]
    cat_cols     = summary["cat_cols"]
    date_cols    = summary["date_cols"]
    missing_pct  = summary["missing_rate_pct"]
    health       = summary["health_score"]
    top_corr     = summary.get("top_correlations", [])

    # --- Size ---
    if rows < 100:
        size_word = "very small"
    elif rows < 1_000:
        size_word = "small"
    elif rows < 10_000:
        size_word = "medium-sized"
    elif rows < 100_000:
        size_word = "large"
    else:
        size_word = "very large"
    bullets.append(
        f"This is a {size_word} dataset with {rows:,} rows and {cols} columns."
    )

    # --- Column breakdown ---
    parts = []
    if numeric_cols:
        parts.append(f"{len(numeric_cols)} number column(s) ({', '.join(numeric_cols[:4])})")
    if cat_cols:
        parts.append(f"{len(cat_cols)} text/category column(s) ({', '.join(cat_cols[:3])})")
    if date_cols:
        parts.append(f"{len(date_cols)} date column(s) ({', '.join(date_cols[:2])})")
    if parts:
        bullets.append("It contains " + ", ".join(parts) + ".")

    # --- Date range ---
    dr = summary.get("date_range")
    if dr:
        bullets.append(
            f"The date column \"{dr['column']}\" runs from {dr['from']} to {dr['to']}"
            f" — a span of {dr['span_days']:,} days."
        )

    # --- Data quality ---
    bullets.append(
        f"Overall data quality is {_health_word(health)} (score: {health} out of 100)."
    )
    if missing_pct == 0:
        bullets.append("No missing values — all fields are completely filled in.")
    elif missing_pct < 5:
        bullets.append(
            f"Only {missing_pct}% of values are missing, which is very low."
        )
    elif missing_pct < 20:
        bullets.append(
            f"About {missing_pct}% of values are missing — manageable but worth checking."
        )
    else:
        bullets.append(
            f"{missing_pct}% of values are missing — quite a lot, which may affect reliability."
        )

    # --- Outliers ---
    if outliers:
        col_names = list(outliers.keys())[:3]
        counts    = [outliers[c]["count"] for c in col_names]
        pairs     = ", ".join(f"\"{c}\" ({n} values)" for c, n in zip(col_names, counts))
        bullets.append(
            f"Some unusually extreme values were found in: {pairs}."
            " These could be errors or rare real-world events."
        )
    else:
        bullets.append("No unusual extreme values were detected in the data.")

    # --- Numeric highlights ---
    for col in numeric_cols[:3]:
        info = num_stats.get(col)
        if not info:
            continue
        bullets.append(
            f"\"{col}\" ranges from {info['min']:,.1f} to {info['max']:,.1f},"
            f" with a typical value of {info['mean']:,.1f}."
        )

    # --- Category highlights ---
    for col in cat_cols[:2]:
        vc = df[col].value_counts()
        if len(vc) == 0:
            continue
        top_val = str(vc.index[0])
        top_pct = round(vc.iloc[0] / len(df) * 100, 1)
        unique  = df[col].nunique()
        bullets.append(
            f"\"{col}\" has {unique} different values;"
            f" the most frequent is \"{top_val}\" ({top_pct}% of rows)."
        )

    # --- Correlations ---
    for item in top_corr[:2]:
        a, b, r = item
        direction = "rise" if r > 0 else "fall"
        strength  = "strongly" if abs(r) >= 0.8 else "noticeably"
        bullets.append(
            f"When \"{a}\" goes up, \"{b}\" tends to {strength} {direction} too"
            f" (relationship strength: {abs(r):.2f})."
        )
    if not top_corr:
        bullets.append("No strong relationships were found between columns.")

    return bullets


def summary_agent(state: AnalysisState) -> AnalysisState:
    state.current_agent = "summary"
    logger.info("Summary agent started")

    try:
        df       = state.clean_df
        raw_df   = state.raw_df
        stats    = state.stats_summary
        col_types = state.column_types

        if df is None or df.empty:
            raise ValueError("No clean data available for summary")

        summary: dict = {}

        # Shape
        summary["rows"] = int(df.shape[0])
        summary["cols"] = int(df.shape[1])

        # Column type lists
        numeric_cols = [c for c, t in col_types.items() if t == "numeric"]
        cat_cols     = [c for c, t in col_types.items() if t == "categorical"]
        date_cols    = [c for c, t in col_types.items() if t == "datetime"]
        summary["numeric_cols"] = numeric_cols
        summary["cat_cols"]     = cat_cols
        summary["date_cols"]    = date_cols

        # Missing rate
        ref_df      = raw_df if raw_df is not None and not raw_df.empty else df
        raw_total   = ref_df.shape[0] * ref_df.shape[1]
        raw_missing = int(ref_df.isna().sum().sum())
        summary["missing_rate_pct"] = (
            round(raw_missing / raw_total * 100, 1) if raw_total > 0 else 0.0
        )

        # Health score
        completeness    = (1 - raw_missing / raw_total) * 100 if raw_total > 0 else 100
        dup_rate        = (1 - len(df) / len(ref_df)) * 100 if len(ref_df) > 0 else 0
        outlier_cols    = len(stats.get("outliers", {}))
        outlier_penalty = min(outlier_cols / max(len(numeric_cols), 1) * 30, 30)
        health = max(0, min(100, int(completeness - dup_rate * 0.5 - outlier_penalty)))
        summary["health_score"] = health

        # Numeric highlights for UI cards
        num_stats  = stats.get("numeric_stats", {})
        highlights = []
        for col in numeric_cols[:5]:
            info = num_stats.get(col, {})
            if info:
                highlights.append({
                    "column": col,
                    "mean": round(info["mean"], 2),
                    "min":  round(info["min"],  2),
                    "max":  round(info["max"],  2),
                    "std":  round(info["std"],  2),
                })
        summary["highlights"] = highlights

        # Date range
        date_range = None
        for col in date_cols:
            try:
                dt = pd.to_datetime(df[col], errors="coerce").dropna()
                if len(dt) > 0:
                    date_range = {
                        "column":    col,
                        "from":      str(dt.min().date()),
                        "to":        str(dt.max().date()),
                        "span_days": int((dt.max() - dt.min()).days),
                    }
                    break
            except Exception:
                continue
        summary["date_range"] = date_range

        # Top categories
        top_categories: dict = {}
        for col in cat_cols[:5]:
            vc = df[col].value_counts()
            if len(vc) > 0:
                top_categories[col] = {
                    "top_value": str(vc.index[0]),
                    "top_pct":   round(vc.iloc[0] / len(df) * 100, 1),
                    "unique":    int(df[col].nunique()),
                }
        summary["top_categories"] = top_categories
        summary["top_correlations"] = stats.get("top_correlations", [])

        # Build bullet list
        outliers = stats.get("outliers", {})
        bullets  = _build_bullets(summary, df, num_stats, outliers)

        # Try LLM for better bullets (still as a list, not paragraphs)
        api_key = os.getenv("GROQ_API_KEY")
        if api_key:
            try:
                from groq import Groq
                client = Groq(api_key=api_key)
                facts = (
                    f"- {summary['rows']:,} rows, {summary['cols']} columns\n"
                    f"- Number columns: {', '.join(numeric_cols[:6]) or 'none'}\n"
                    f"- Text columns: {', '.join(cat_cols[:6]) or 'none'}\n"
                    f"- Date columns: {', '.join(date_cols[:3]) or 'none'}\n"
                    f"- Health score: {health}/100\n"
                    f"- Missing data: {summary['missing_rate_pct']}%\n"
                    f"- Outlier columns: {list(outliers.keys())[:3]}\n"
                    f"- Numeric stats: "
                    + str({c: {k: round(v, 1) for k, v in info.items()}
                           for c, info in list(num_stats.items())[:4]}) + "\n"
                    f"- Strong relationships: "
                    + str([(a, b, round(r, 2)) for a, b, r in
                           summary['top_correlations'][:3]]) + "\n"
                )
                prompt = (
                    "You are explaining a dataset to someone with no data background.\n"
                    "Write exactly 8 to 10 bullet points. Each bullet is one plain sentence.\n"
                    "Rules:\n"
                    "- No statistics terms (no p-value, variance, std, IQR, etc.)\n"
                    "- Use everyday words\n"
                    "- Mention actual column names in quotes\n"
                    "- Be specific with numbers where helpful\n"
                    "- Start each line with a dash and a space: '- '\n"
                    "- No intro text, no headers, no markdown, just the bullets\n\n"
                    f"Dataset facts:\n{facts}"
                )
                resp = client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.4,
                )
                text = resp.choices[0].message.content.strip()
                llm_bullets = [
                    line.lstrip("- ").strip()
                    for line in text.splitlines()
                    if line.strip().startswith("- ")
                ]
                if len(llm_bullets) >= 4:
                    bullets = llm_bullets
                    logger.info("LLM bullets used (%d items)", len(bullets))
            except Exception as e:
                logger.warning("LLM bullets failed (%s); using built-in", e)

        summary["bullets"] = bullets
        state.summary = summary
        logger.info("Summary agent complete. Health: %d, Bullets: %d", health, len(bullets))

    except Exception as e:
        error_msg = f"Summary error: {e}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    state.completed_agents.append("summary")
    return state