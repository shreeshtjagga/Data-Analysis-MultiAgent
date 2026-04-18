"""
Root-Cause Analysis Agent
==========================
Detects anomalies (Z-score > 3) in numeric columns and runs a sub-analysis
to find which categorical dimensions primarily drive each anomaly.
Uses Groq LLM to narrate findings in plain English.
"""
import json
import logging
import os
from typing import Any, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_Z_THRESHOLD = 2.5       # Z-score threshold for anomaly detection
_MAX_ANOMALIES = 8       # Max anomalies to report
_MAX_DRIVER_CATS = 5     # Max categorical breakdown columns to analyze per anomaly


def _detect_anomalies(df: pd.DataFrame, numeric_cols: list[str]) -> list[dict]:
    """Detect anomalous rows using Z-score method per numeric column."""
    anomalies = []

    for col in numeric_cols:
        series = df[col].dropna()
        if len(series) < 10:
            continue

        mean = series.mean()
        std = series.std()
        if std == 0:
            continue

        z_scores = ((df[col] - mean) / std).abs()
        anomaly_mask = z_scores > _Z_THRESHOLD
        anomaly_rows = df[anomaly_mask].copy()

        if anomaly_rows.empty:
            continue

        # Take worst outlier per column
        anomaly_rows["_z_score"] = z_scores[anomaly_mask]
        worst = anomaly_rows.nlargest(1, "_z_score").iloc[0]

        direction = "spike" if (worst[col] - mean) > 0 else "dip"
        deviation_pct = abs((worst[col] - mean) / mean * 100) if mean != 0 else 0

        anomalies.append({
            "column": col,
            "type": direction,
            "value": float(worst[col]),
            "mean": round(float(mean), 4),
            "std": round(float(std), 4),
            "z_score": round(float(worst["_z_score"]), 2),
            "deviation_pct": round(deviation_pct, 1),
            "row_index": int(worst.name) if hasattr(worst, "name") else -1,
        })

    # Sort by Z-score descending (worst anomalies first)
    anomalies.sort(key=lambda x: x["z_score"], reverse=True)
    return anomalies[:_MAX_ANOMALIES]


def _find_drivers(df: pd.DataFrame, anomaly: dict, categorical_cols: list[str]) -> dict:
    """
    For a detected anomaly, find which categorical column values are most
    associated with extreme values in the anomaly column.
    """
    col = anomaly["column"]
    drivers = {}

    for cat_col in categorical_cols[:_MAX_DRIVER_CATS]:
        try:
            cat_series = df[cat_col].dropna().astype(str)
            num_series = df[col].dropna()

            # Align indices
            combined = pd.concat([cat_series.rename("cat"), num_series.rename("num")], axis=1).dropna()
            if len(combined) < 5:
                continue

            group_means = combined.groupby("cat")["num"].mean().sort_values(ascending=False)
            overall_mean = combined["num"].mean()

            # Find groups that deviate most from the mean
            deviating = {}
            for group_val, group_mean in group_means.items():
                if overall_mean != 0:
                    dev = abs((group_mean - overall_mean) / overall_mean * 100)
                    if dev > 15:  # 15%+ deviation threshold
                        deviating[str(group_val)] = {
                            "mean": round(float(group_mean), 3),
                            "deviation_pct": round(float(dev), 1),
                        }

            if deviating:
                drivers[cat_col] = deviating

        except Exception as e:
            logger.debug("Driver analysis failed for %s / %s: %s", col, cat_col, e)

    return drivers


def _llm_explain_anomaly(anomaly: dict, drivers: dict) -> Optional[str]:
    """Ask Groq LLM to narrate the root cause in plain English."""
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return None

    driver_text = ""
    for cat, groups in list(drivers.items())[:3]:
        top_group = max(groups.items(), key=lambda x: x[1]["deviation_pct"])
        driver_text += f"\n  - In '{cat}', the '{top_group[0]}' segment shows a {top_group[1]['deviation_pct']}% deviation from average."

    prompt = (
        f"A data analyst detected a {anomaly['type']} in column '{anomaly['column']}': "
        f"a value of {anomaly['value']:.2f} vs an average of {anomaly['mean']:.2f} "
        f"(Z-score: {anomaly['z_score']}, deviation: {anomaly['deviation_pct']}% from mean).\n"
        f"Potential drivers:{driver_text if driver_text else ' No clear categorical driver found.'}\n\n"
        "In 1-2 sentences, write a plain-English explanation of this anomaly that a CEO would understand. "
        "Be specific about what the anomaly means and what likely caused it."
    )

    try:
        from groq import Groq
        client = Groq(api_key=api_key)
        completion = client.chat.completions.create(
            model=os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"),
            messages=[
                {"role": "system", "content": "You are a senior data analyst explaining anomalies concisely."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.25,
            max_tokens=150,
        )
        return (completion.choices[0].message.content or "").strip()
    except Exception as e:
        logger.warning("LLM anomaly explanation failed: %s", e)
        return None


def _llm_overall_summary(anomalies_with_causes: list[dict]) -> Optional[str]:
    """Generate an overall root-cause summary across all anomalies."""
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key or not anomalies_with_causes:
        return None

    top = anomalies_with_causes[:3]
    bullet_points = [
        f"• {a['column']}: {a['type']} of {a['value']:.2f} (mean: {a['mean']:.2f}, Z={a['z_score']})"
        for a in top
    ]

    prompt = (
        "As a senior data analyst, write a 2-3 sentence executive summary "
        "explaining the key anomalies found in this dataset and their likely root causes:\n\n"
        + "\n".join(bullet_points)
    )

    try:
        from groq import Groq
        client = Groq(api_key=api_key)
        completion = client.chat.completions.create(
            model=os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"),
            messages=[
                {"role": "system", "content": "You are a senior data analyst. Be concise and insightful."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.25,
            max_tokens=200,
        )
        return (completion.choices[0].message.content or "").strip()
    except Exception as e:
        logger.warning("LLM overall summary failed: %s", e)
        return None


def run_rootcause_analysis(clean_df_preview: list[dict], stats_summary: dict) -> dict:
    """
    Main entry point for root-cause analysis.
    Accepts the clean_df_preview (list of row dicts) and existing stats_summary.
    Returns anomalies with driver breakdowns and LLM explanations.
    """
    try:
        df = pd.DataFrame(clean_df_preview)
        if df.empty:
            return {"error": "No data available for analysis", "anomalies": [], "root_cause_summary": None}

        numeric_cols = list((stats_summary.get("numeric_columns") or {}).keys())
        categorical_cols = list((stats_summary.get("categorical_columns") or {}).keys())

        numeric_cols = [c for c in numeric_cols if c in df.columns]
        categorical_cols = [c for c in categorical_cols if c in df.columns]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        anomalies = _detect_anomalies(df, numeric_cols)

        if not anomalies:
            return {
                "anomalies": [],
                "total_anomalies": 0,
                "root_cause_summary": (
                    "No significant anomalies detected in this dataset. "
                    "All numeric columns appear to be within expected statistical bounds (Z-score < 2.5)."
                ),
            }

        # Enrich each anomaly with driver breakdown and LLM explanation
        enriched = []
        for anomaly in anomalies:
            drivers = _find_drivers(df, anomaly, categorical_cols)
            llm_explanation = _llm_explain_anomaly(anomaly, drivers)

            if not llm_explanation:
                # Rule-based fallback
                direction_word = "spike" if anomaly["type"] == "spike" else "drop"
                llm_explanation = (
                    f"This {direction_word} in '{anomaly['column']}' ({anomaly['deviation_pct']}% above/below average) "
                    f"represents a statistical outlier with a Z-score of {anomaly['z_score']}. "
                    "Investigate the specific rows and linked categorical segments for the root cause."
                )

            enriched.append({
                **anomaly,
                "driver_breakdown": drivers,
                "explanation": llm_explanation,
            })

        overall_summary = _llm_overall_summary(enriched)
        if not overall_summary:
            cols_affected = list({a["column"] for a in enriched})
            overall_summary = (
                f"Detected {len(enriched)} anomalies across {len(cols_affected)} column(s): "
                f"{', '.join(cols_affected[:5])}. "
                "These outliers deviate significantly from expected patterns and warrant further investigation."
            )

        return {
            "anomalies": enriched,
            "total_anomalies": len(enriched),
            "root_cause_summary": overall_summary,
        }

    except Exception as e:
        logger.error("Root-cause analysis failed: %s", e)
        return {"error": str(e), "anomalies": [], "root_cause_summary": None}
