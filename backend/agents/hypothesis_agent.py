"""
Hypothesis Testing Agent
========================
Runs rigorous statistical tests (T-Test, ANOVA, Chi-Square) on the dataset
and produces plain-English verdicts with statistical confidence levels.
"""
import json
import logging
import os
from typing import Any, Optional

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

logger = logging.getLogger(__name__)

_MAX_CHI_CATEGORIES = 20  # Skip chi-square if a column has too many unique values
_MAX_PAIRED_TESTS = 30    # Limit pairwise tests to avoid bloat
_MIN_GROUP_SIZE = 10      # Minimum samples per group for a valid test


def _p_value_label(p: float) -> dict:
    """Return human-readable confidence label for a p-value."""
    if p < 0.001:
        return {"label": "Extremely significant", "confidence": "99.9%", "color": "green"}
    elif p < 0.01:
        return {"label": "Highly significant", "confidence": "99%", "color": "green"}
    elif p < 0.05:
        return {"label": "Significant", "confidence": "95%", "color": "yellow"}
    elif p < 0.1:
        return {"label": "Marginally significant", "confidence": "90%", "color": "orange"}
    else:
        return {"label": "Not significant", "confidence": "<90%", "color": "red"}


def _run_ttests(df: pd.DataFrame, numeric_cols: list[str]) -> list[dict]:
    """Run pairwise T-Tests between all numeric columns (first 30 pairs max)."""
    results = []
    pairs_done = 0

    for i, col_a in enumerate(numeric_cols):
        for col_b in numeric_cols[i + 1:]:
            if pairs_done >= _MAX_PAIRED_TESTS:
                break
            try:
                a = df[col_a].dropna().values
                b = df[col_b].dropna().values
                if len(a) < _MIN_GROUP_SIZE or len(b) < _MIN_GROUP_SIZE:
                    continue

                stat, p = scipy_stats.ttest_ind(a, b, equal_var=False)  # Welch's T-Test
                label_info = _p_value_label(p)

                verdict = (
                    f"With {label_info['confidence']} confidence, '{col_a}' and '{col_b}' "
                    f"have {'statistically different' if p < 0.05 else 'similar'} distributions. "
                    f"This is {'not random chance.' if p < 0.05 else 'within expected random variation.'}"
                )

                results.append({
                    "test_type": "T-Test (Welch)",
                    "col_a": col_a,
                    "col_b": col_b,
                    "statistic": round(float(stat), 4),
                    "p_value": round(float(p), 6),
                    "significant": bool(p < 0.05),
                    "verdict": verdict,
                    **label_info,
                })
                pairs_done += 1
            except Exception as e:
                logger.debug("T-Test failed for %s vs %s: %s", col_a, col_b, e)

    return results


def _run_anova(df: pd.DataFrame, numeric_cols: list[str], categorical_cols: list[str]) -> list[dict]:
    """Run one-way ANOVA: for each categorical column, test each numeric column across its groups."""
    results = []

    for cat_col in categorical_cols[:5]:  # Limit to first 5 categorical cols
        groups_series = df[cat_col].dropna()
        unique_groups = groups_series.unique()

        if len(unique_groups) < 3 or len(unique_groups) > 20:
            continue  # ANOVA needs 3+ groups but not too many

        for num_col in numeric_cols[:10]:
            try:
                grouped_data = [
                    df[df[cat_col] == g][num_col].dropna().values
                    for g in unique_groups
                ]
                # Filter out tiny groups
                grouped_data = [g for g in grouped_data if len(g) >= _MIN_GROUP_SIZE]
                if len(grouped_data) < 3:
                    continue

                stat, p = scipy_stats.f_oneway(*grouped_data)
                label_info = _p_value_label(p)

                verdict = (
                    f"ANOVA shows that '{num_col}' varies {'significantly' if p < 0.05 else 'insignificantly'} "
                    f"across {len(grouped_data)} groups of '{cat_col}' "
                    f"(F={stat:.2f}, p={p:.4f}). "
                    f"{'Group differences are real, not random.' if p < 0.05 else 'No meaningful group difference detected.'}"
                )

                results.append({
                    "test_type": "ANOVA (One-Way)",
                    "col_a": num_col,
                    "col_b": cat_col,
                    "groups_tested": int(len(grouped_data)),
                    "statistic": round(float(stat), 4),
                    "p_value": round(float(p), 6),
                    "significant": bool(p < 0.05),
                    "verdict": verdict,
                    **label_info,
                })
            except Exception as e:
                logger.debug("ANOVA failed for %s / %s: %s", cat_col, num_col, e)

    return results


def _run_chi_square(df: pd.DataFrame, categorical_cols: list[str]) -> list[dict]:
    """Run Chi-Square test between pairs of categorical columns."""
    results = []
    pairs_done = 0

    for i, col_a in enumerate(categorical_cols):
        for col_b in categorical_cols[i + 1:]:
            if pairs_done >= 15:
                break
            try:
                a_vals = df[col_a].dropna().astype(str)
                b_vals = df[col_b].dropna().astype(str)

                if a_vals.nunique() > _MAX_CHI_CATEGORIES or b_vals.nunique() > _MAX_CHI_CATEGORIES:
                    continue

                # Align indices
                combined = pd.concat([a_vals.rename("a"), b_vals.rename("b")], axis=1).dropna()
                if len(combined) < _MIN_GROUP_SIZE:
                    continue

                contingency = pd.crosstab(combined["a"], combined["b"])
                stat, p, dof, _ = scipy_stats.chi2_contingency(contingency)
                label_info = _p_value_label(p)

                verdict = (
                    f"Chi-Square test reveals that '{col_a}' and '{col_b}' are "
                    f"{'statistically associated (not independent)' if p < 0.05 else 'statistically independent'} "
                    f"(χ²={stat:.2f}, df={dof}, p={p:.4f}). "
                    f"{'Their relationship is real and significant.' if p < 0.05 else 'No significant association found.'}"
                )

                results.append({
                    "test_type": "Chi-Square",
                    "col_a": col_a,
                    "col_b": col_b,
                    "degrees_of_freedom": int(dof),
                    "statistic": round(float(stat), 4),
                    "p_value": round(float(p), 6),
                    "significant": bool(p < 0.05),
                    "verdict": verdict,
                    **label_info,
                })
                pairs_done += 1
            except Exception as e:
                logger.debug("Chi-Square failed for %s vs %s: %s", col_a, col_b, e)

    return results


def _llm_narrate(tests: list[dict]) -> Optional[str]:
    """Ask Groq LLM to narrate the top 3 most significant findings."""
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key or not tests:
        return None

    # Take top 5 significant results
    top = sorted([t for t in tests if t.get("significant")], key=lambda x: x["p_value"])[:5]
    if not top:
        top = sorted(tests, key=lambda x: x["p_value"])[:3]

    summaries = [
        f"• {t['test_type']}: {t['col_a']} vs {t['col_b']} — p={t['p_value']}, {t['label']}"
        for t in top
    ]

    prompt = (
        "You are a senior statistician. Based on these hypothesis test results, "
        "write a 3-4 sentence executive summary explaining what the data proves, "
        "using plain English that a CEO can understand. Be specific about which variables matter.\n\n"
        "Results:\n" + "\n".join(summaries)
    )

    try:
        from groq import Groq
        client = Groq(api_key=api_key)
        completion = client.chat.completions.create(
            model=os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"),
            messages=[
                {"role": "system", "content": "You are a senior statistician. Be concise, precise, and professional."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.25,
            max_tokens=250,
        )
        return (completion.choices[0].message.content or "").strip()
    except Exception as e:
        logger.warning("LLM narration for hypothesis failed: %s", e)
        return None


def run_hypothesis_tests(clean_df_preview: list[dict], stats_summary: dict) -> dict:
    """
    Main entry point for hypothesis testing.
    Accepts the clean_df_preview (list of row dicts) and existing stats_summary.
    Returns structured test results + LLM narrative.
    """
    try:
        df = pd.DataFrame(clean_df_preview)
        if df.empty:
            return {"error": "No data available for hypothesis testing", "tests": [], "narrative": None}

        # Identify column types from stats_summary
        numeric_cols = list((stats_summary.get("numeric_columns") or {}).keys())
        categorical_cols = list((stats_summary.get("categorical_columns") or {}).keys())

        # Filter to columns that actually exist in the preview df
        numeric_cols = [c for c in numeric_cols if c in df.columns]
        categorical_cols = [c for c in categorical_cols if c in df.columns]

        # Coerce numerics from object dtype if needed
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        all_tests = []

        if len(numeric_cols) >= 2:
            all_tests.extend(_run_ttests(df, numeric_cols))

        if numeric_cols and categorical_cols:
            all_tests.extend(_run_anova(df, numeric_cols, categorical_cols))

        if len(categorical_cols) >= 2:
            all_tests.extend(_run_chi_square(df, categorical_cols))

        # Sort by p-value (most significant first)
        all_tests.sort(key=lambda x: x.get("p_value", 1.0))

        significant_count = sum(1 for t in all_tests if t.get("significant"))
        narrative = _llm_narrate(all_tests)

        if not narrative:
            if significant_count > 0:
                narrative = (
                    f"Analysis found {significant_count} statistically significant relationships "
                    f"out of {len(all_tests)} tests conducted. "
                    f"The most significant finding involves "
                    f"'{all_tests[0]['col_a']}' and '{all_tests[0]['col_b']}' "
                    f"with a p-value of {all_tests[0]['p_value']:.4f}."
                )
            else:
                narrative = (
                    f"Ran {len(all_tests)} statistical tests across the dataset. "
                    "No strong statistically significant relationships were detected at the 95% confidence level, "
                    "suggesting the variables may be largely independent of one another."
                )

        return {
            "tests": all_tests,
            "total_tests": len(all_tests),
            "significant_count": significant_count,
            "narrative": narrative,
        }

    except Exception as e:
        logger.error("Hypothesis testing failed: %s", e)
        return {"error": str(e), "tests": [], "narrative": None}
