"""
DataPulse On-Demand Plot Generator
===================================
Generates new Plotly charts programmatically during chat sessions,
driven by LLM-emitted [PLOT_REQUEST: json] tags in the /chat endpoint.

Supported chart types:
  scatter, histogram, ranked_bar, grouped_bar, bar,
  box, violin, donut, pie, line, heatmap, freq_bar, stacked_bar
"""

from __future__ import annotations

import json
import logging
from typing import Optional

import pandas as pd

from .visualizer import (
    _build_box,
    _build_donut,
    _build_freq_bar,
    _build_grouped_bar,
    _build_heatmap,
    _build_histogram,
    _build_line,
    _build_ranked_bar,
    _build_scatter,
    _build_stacked_bar,
    _build_violin,
)

logger = logging.getLogger(__name__)

SUPPORTED_CHART_TYPES = frozenset({
    "scatter",
    "histogram",
    "ranked_bar",
    "grouped_bar",
    "bar",
    "box",
    "violin",
    "donut",
    "pie",
    "line",
    "heatmap",
    "freq_bar",
    "stacked_bar",
})

# Friendly display names for error messages
_TYPE_DISPLAY = {
    "scatter":     "Scatter plot",
    "histogram":   "Histogram",
    "ranked_bar":  "Ranked bar chart",
    "grouped_bar": "Grouped bar chart",
    "bar":         "Bar chart",
    "box":         "Box plot",
    "violin":      "Violin plot",
    "donut":       "Donut chart",
    "pie":         "Pie chart",
    "line":        "Line chart",
    "heatmap":     "Heatmap",
    "freq_bar":    "Frequency bar chart",
    "stacked_bar": "Stacked bar chart",
}


# ── DataFrame reconstruction ──────────────────────────────────────────────────

def _records_to_df(records: list[dict]) -> pd.DataFrame:
    """
    Reconstruct a pandas DataFrame from the JSON records list
    (the clean_df preview sent by the frontend, up to 100 rows).
    Numeric-string columns are coerced back to float.
    """
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)

    # Coerce columns that look numeric back to float
    for col in df.columns:
        try:
            converted = pd.to_numeric(df[col], errors="coerce")
            valid_ratio = converted.notna().sum() / max(len(df), 1)
            if valid_ratio >= 0.60:
                df[col] = converted
        except Exception:
            pass

    # Attempt datetime parsing on object columns that look like dates
    for col in df.select_dtypes(include=["object"]).columns:
        try:
            parsed = pd.to_datetime(df[col], format="mixed", errors="coerce")
            valid_ratio = parsed.notna().sum() / max(len(df), 1)
            if valid_ratio >= 0.70:
                df[col] = parsed
        except Exception:
            pass

    return df


# ── Column validation ─────────────────────────────────────────────────────────

def _resolve_column(col: Optional[str], actual_cols: set[str]) -> Optional[str]:
    """
    Return the column name as-is if it exists. If not, try a
    case-insensitive match. Returns None if no match is found.
    """
    if not col:
        return None
    if col in actual_cols:
        return col
    # Case-insensitive fallback
    lower_map = {c.lower(): c for c in actual_cols}
    return lower_map.get(col.lower())


# ── Main entry point ──────────────────────────────────────────────────────────

def generate_on_demand_chart(
    spec: dict,
    df_records: list[dict],
) -> dict:
    """
    Build a Plotly chart from an LLM-emitted plot specification.

    Args:
        spec:       Chart spec dict, e.g.:
                    {"chart_type": "scatter", "x": "Age", "y": "Salary",
                     "color": "Department", "title": "Age vs Salary",
                     "agg": "mean", "log_scale": false}
        df_records: List of row dicts (the clean_df preview, ≤100 rows).

    Returns:
        {
            "id":    str  — unique key for this chart,
            "fig":   dict — Plotly figure JSON (or None on failure),
            "error": str  — human-readable error message (or None on success),
        }
    """
    # ── Parse spec fields ────────────────────────────────────────────────────
    chart_type = (spec.get("chart_type") or "").lower().strip()
    x     = spec.get("x") or spec.get("x_col")
    y     = spec.get("y") or spec.get("y_col")
    color = spec.get("color") or spec.get("color_col")
    title = spec.get("title") or ""
    agg   = spec.get("agg") or "auto"
    log_scale = bool(spec.get("log_scale", False))

    # ── Validate chart type ──────────────────────────────────────────────────
    if chart_type not in SUPPORTED_CHART_TYPES:
        friendly = ", ".join(sorted(SUPPORTED_CHART_TYPES))
        return {
            "id":    "gen_error",
            "fig":   None,
            "error": (
                f"'{chart_type}' is not a supported chart type. "
                f"Supported types: {friendly}."
            ),
        }

    # ── Reconstruct DataFrame ────────────────────────────────────────────────
    df = _records_to_df(df_records)
    if df.empty:
        return {
            "id":    "gen_error",
            "fig":   None,
            "error": "No data preview is available to build this chart. Please re-upload your dataset.",
        }

    actual_cols: set[str] = set(df.columns.tolist())

    # ── Resolve & validate column names ─────────────────────────────────────
    x     = _resolve_column(x,     actual_cols)
    y     = _resolve_column(y,     actual_cols)
    color = _resolve_column(color, actual_cols)

    # Collect numeric columns for chart types that need them automatically
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

    # ── Route to correct builder ─────────────────────────────────────────────
    chart = None

    if chart_type == "scatter":
        if not x or not y:
            return _err("gen_scatter", "Scatter plot requires both an X column and a Y column.")
        chart = _build_scatter(df, x, y, color_col=color, title=title)

    elif chart_type == "histogram":
        col = x or y
        if not col:
            return _err("gen_histogram", "Histogram requires at least one numeric column (x).")
        chart = _build_histogram(df, col, log_scale=log_scale, title=title)

    elif chart_type in ("ranked_bar", "bar"):
        if x and y:
            chart = _build_ranked_bar(df, x, y, title, agg=agg, color_col=color)
            if chart is None:
                chart = _build_ranked_bar(df, y, x, title, agg=agg, color_col=color)
        elif x:
            chart = _build_freq_bar(df, x, title=title)
        elif y:
            chart = _build_freq_bar(df, y, title=title)
        else:
            return _err("gen_bar", "Bar chart requires at least one column (x).")

    elif chart_type == "grouped_bar":
        if not x or not y:
            return _err("gen_grouped_bar", "Grouped bar chart requires x (category) and y (numeric) columns.")
        chart = _build_grouped_bar(df, x, y, title, agg=agg, color_col=color)
        if chart is None:
            # Swap - clear title as it likely matches the original x/y orientation
            chart = _build_grouped_bar(df, y, x, None, agg=agg, color_col=color)

    elif chart_type == "box":
        if not x or not y:
            return _err("gen_box", "Box plot requires x (category) and y (numeric) columns.")
        chart = _build_box(df, x, y, title=title)
        if chart is None:
            # Swap - clear title
            chart = _build_box(df, y, x, title=None)

    elif chart_type == "violin":
        if not x or not y:
            return _err("gen_violin", "Violin plot requires x (category) and y (numeric) columns.")
        chart = _build_violin(df, x, y, title=title)
        if chart is None:
            # Swap - clear title
            chart = _build_violin(df, y, x, title=None)

    elif chart_type in ("donut", "pie"):
        col = x or y
        if not col:
            return _err("gen_donut", "Donut/pie chart requires a categorical column.")
        chart = _build_donut(df, col, title=title)

    elif chart_type == "line":
        if not x:
            return _err("gen_line", "Line chart requires an X column (date/time or sequential).")
        value_cols = [c for c in ([y] if y else num_cols[:4]) if c in df.columns]
        if not value_cols:
            value_cols = num_cols[:4]
        chart = _build_line(df, x, value_cols, title=title)

    elif chart_type == "heatmap":
        cols_to_use = num_cols
        chart = _build_heatmap(df, cols_to_use, title=title or "Correlation Heatmap")
        if chart is None and len(num_cols) < 3:
            return _err(
                "gen_heatmap",
                f"Heatmap requires at least 3 numeric columns. "
                f"This dataset preview has {len(num_cols)} numeric column(s).",
            )

    elif chart_type == "stacked_bar":
        if not x or not color:
            return _err("gen_stacked", "Stacked bar chart requires x column and a color (grouping) column.")
        chart = _build_stacked_bar(df, x, color, num_col=y, title=title)

    elif chart_type == "freq_bar":
        col = x or y
        if not col:
            return _err("gen_freq_bar", "Frequency bar chart requires a categorical column (x).")
        chart = _build_freq_bar(df, col, title=title)

    # ── Handle builder failure ───────────────────────────────────────────────
    if chart is None:
        type_label = _TYPE_DISPLAY.get(chart_type, chart_type)
        col_info = []
        if x:
            dtype = str(df[x].dtype) if x in df.columns else "unknown"
            col_info.append(f"'{x}' ({dtype})")
        if y:
            dtype = str(df[y].dtype) if y in df.columns else "unknown"
            col_info.append(f"'{y}' ({dtype})")
        col_str = " and ".join(col_info) if col_info else "the provided columns"
        return _err(
            f"gen_{chart_type}",
            (
                f"Could not build a {type_label} with {col_str}. "
                "Check that the column types are appropriate — "
                "e.g., scatter/histogram need numeric columns; bar/ranked_bar need one categorical and one numeric column."
            ),
        )

    # ── Serialize Plotly figure ──────────────────────────────────────────────
    try:
        fig_json = chart.fig.to_json()
        fig_dict = json.loads(fig_json)
    except Exception as exc:
        logger.error("Failed to serialize generated chart '%s': %s", chart.key, exc)
        return _err(chart.key, "Chart was built successfully but failed to serialize.")

    logger.info("Generated on-demand chart: key=%s type=%s", chart.key, chart_type)
    return {
        "id":    chart.key,
        "fig":   fig_dict,
        "error": None,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _err(chart_id: str, message: str) -> dict:
    """Return a standardised error result dict."""
    return {"id": chart_id, "fig": None, "error": message}
