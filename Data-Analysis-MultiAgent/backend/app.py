import io
import html
import json
import logging
import os
from typing import Optional
from dotenv import load_dotenv

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

try:
    from groq import Groq
except Exception:
    Groq = None

load_dotenv()

from core.graph import run_pipeline

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CONTEXT BUILDERS  (unchanged from your original)
# ═══════════════════════════════════════════════════════════════════════════════

def _to_plain_list(values, max_items: int = 8) -> list:
    if values is None:
        return []
    try:
        if hasattr(values, "tolist"):
            values = values.tolist()
        if isinstance(values, (str, bytes)):
            return [str(values)[:80]]
        values = list(values)
    except Exception:
        return [str(values)[:80]]
    cleaned = []
    for item in values[:max_items]:
        if isinstance(item, (int, float, str, bool)) or item is None:
            cleaned.append(item)
        else:
            cleaned.append(str(item))
    return cleaned


def _extract_chart_context(charts: dict) -> list[dict]:
    summaries = []
    for chart_key, fig in charts.items():
        title = chart_key
        traces = []
        trace_types = []
        try:
            if getattr(fig, "layout", None) and getattr(fig.layout, "title", None):
                title = fig.layout.title.text or chart_key
            for trace in list(fig.data)[:3]:
                trace_type = str(getattr(trace, "type", "unknown"))
                trace_types.append(trace_type)
                x_vals = _to_plain_list(getattr(trace, "x", None), max_items=10)
                y_vals = _to_plain_list(getattr(trace, "y", None), max_items=10)
                trace_info = {
                    "type": trace_type,
                    "name": str(getattr(trace, "name", ""))[:80],
                    "x_preview": x_vals,
                    "y_preview": y_vals,
                }
                if trace_type == "bar" and x_vals and y_vals:
                    try:
                        numeric_pairs = [(x, float(y)) for x, y in zip(x_vals, y_vals)]
                        if numeric_pairs:
                            top_bar = max(numeric_pairs, key=lambda p: p[1])
                            trace_info["bar_peak"] = {
                                "category": str(top_bar[0]),
                                "value": round(top_bar[1], 4),
                            }
                    except Exception:
                        pass
                traces.append(trace_info)
        except Exception:
            pass
        summaries.append({
            "chart_key": chart_key,
            "title": str(title),
            "trace_types": sorted(set(trace_types)) if trace_types else [],
            "traces": traces,
        })
    return summaries


def _chart_visibility_notes(stats: dict, charts: dict) -> list[dict]:
    shown_keys = set((charts or {}).keys())
    dtypes = stats.get("dtypes", {}) or {}
    row_count = int(stats.get("row_count", 0) or 0)
    numeric_stats = stats.get("numeric_columns", {}) or {}
    categorical_stats = stats.get("categorical_columns", {}) or {}
    numeric_count = len(numeric_stats)
    categorical_count = len(categorical_stats)
    date_cols = [col for col, dtype in dtypes.items() if "datetime" in str(dtype).lower()]

    def _note(chart_type, shown, reason):
        return {"chart_type": chart_type, "shown": shown, "reason": reason}

    notes = []
    timeseries_shown = any(k.startswith("timeseries") for k in shown_keys)
    if timeseries_shown:
        notes.append(_note("timeseries", True, "Shown because datetime and numeric columns were available."))
    elif not date_cols:
        notes.append(_note("timeseries", False, "Not shown because no datetime column was detected."))
    elif numeric_count == 0:
        notes.append(_note("timeseries", False, "Not shown because there are no numeric columns for the y-axis."))
    elif row_count < 5:
        notes.append(_note("timeseries", False, "Not shown because at least 5 rows are needed."))
    else:
        notes.append(_note("timeseries", False, "Eligible but not selected in the top-ranked chart set."))

    bar_mean_shown = "bar_mean" in shown_keys
    valid_bar_group_cols = [
        col for col, meta in categorical_stats.items()
        if 2 <= int(meta.get("unique_values", 0) or 0) <= 15
    ]
    if bar_mean_shown:
        notes.append(_note("bar_mean", True, "Shown because grouping categories and numeric measures were available."))
    elif numeric_count == 0:
        notes.append(_note("bar_mean", False, "Not shown because there are no numeric columns."))
    elif categorical_count == 0:
        notes.append(_note("bar_mean", False, "Not shown because there are no categorical columns."))
    elif not valid_bar_group_cols:
        notes.append(_note("bar_mean", False, "Not shown because no categorical column has 2-15 distinct values."))
    else:
        notes.append(_note("bar_mean", False, "Eligible but not selected in the top-ranked chart set."))

    donut_shown = "donut" in shown_keys
    valid_donut_cols = []
    for col, meta in categorical_stats.items():
        unique_values = int(meta.get("unique_values", 0) or 0)
        most_common_count = int(meta.get("most_common_count", 0) or 0)
        top_pct = (most_common_count / row_count) if row_count > 0 else 1.0
        if 2 <= unique_values <= 6 and top_pct <= 0.95:
            valid_donut_cols.append(col)
    if donut_shown:
        notes.append(_note("donut", True, "Shown because a compact categorical split was available."))
    elif categorical_count == 0:
        notes.append(_note("donut", False, "Not shown because there are no categorical columns."))
    elif not valid_donut_cols:
        notes.append(_note("donut", False, "Not shown because category counts were not suitable for a donut chart."))
    else:
        notes.append(_note("donut", False, "Eligible but not selected in the top-ranked chart set."))

    scatter_shown = "scatter" in shown_keys
    if scatter_shown:
        notes.append(_note("scatter", True, "Shown because at least two numeric columns were available."))
    elif row_count < 10:
        notes.append(_note("scatter", False, "Not shown because at least 10 rows are needed."))
    elif numeric_count < 2:
        notes.append(_note("scatter", False, "Not shown because at least two numeric columns are required."))
    else:
        notes.append(_note("scatter", False, "Eligible but not selected in the top-ranked chart set."))

    heatmap_shown = "heatmap" in shown_keys
    if heatmap_shown:
        notes.append(_note("heatmap", True, "Shown because enough numeric columns existed for correlations."))
    elif numeric_count < 3:
        notes.append(_note("heatmap", False, "Not shown because at least three numeric columns are required."))
    else:
        notes.append(_note("heatmap", False, "Eligible but not selected in the top-ranked chart set."))

    histogram_shown = any(k.startswith("histogram_") for k in shown_keys)
    if histogram_shown:
        notes.append(_note("histogram", True, "Shown because numeric distribution analysis was possible."))
    elif numeric_count == 0:
        notes.append(_note("histogram", False, "Not shown because there are no numeric columns."))
    else:
        notes.append(_note("histogram", False, "Eligible but not selected in the top-ranked chart set."))

    boxplot_shown = "boxplots" in shown_keys
    if boxplot_shown:
        notes.append(_note("boxplots", True, "Shown because multiple numeric columns were available."))
    elif numeric_count < 2:
        notes.append(_note("boxplots", False, "Not shown because at least two numeric columns are required."))
    else:
        notes.append(_note("boxplots", False, "Eligible but not selected in the top-ranked chart set."))

    bar_counts_shown = any(k.startswith("bar_counts_") for k in shown_keys)
    valid_bar_count_cols = [
        col for col, meta in categorical_stats.items()
        if 2 <= int(meta.get("unique_values", 0) or 0) <= 20
    ]
    if bar_counts_shown:
        notes.append(_note("bar_counts", True, "Shown because a categorical frequency chart was available."))
    elif categorical_count == 0:
        notes.append(_note("bar_counts", False, "Not shown because there are no categorical columns."))
    elif not valid_bar_count_cols:
        notes.append(_note("bar_counts", False, "Not shown because categorical cardinality was out of range (2-20)."))
    else:
        notes.append(_note("bar_counts", False, "Eligible but not selected in the top-ranked chart set."))

    return notes


def _build_dataset_context(result: dict, file_name: str) -> dict:
    stats = result.get("stats_summary", {}) or {}
    insights = result.get("insights", {}) or {}
    charts = result.get("charts") or {}

    sample_rows = []
    clean = result.get("clean_df")
    if isinstance(clean, dict):
        try:
            sample_df = pd.DataFrame(clean).head(8)
            sample_rows = sample_df.to_dict(orient="records")
        except Exception:
            sample_rows = []

    return {
        "dataset_name": file_name,
        "stats": {
            "row_count": stats.get("row_count", 0),
            "column_count": stats.get("column_count", 0),
            "columns": (stats.get("columns", []) or [])[:30],
            "numeric_columns": list((stats.get("numeric_columns", {}) or {}).keys())[:20],
            "categorical_columns": list((stats.get("categorical_columns", {}) or {}).keys())[:20],
            "data_quality": stats.get("data_quality", {}),
            "outliers": dict(list((stats.get("outliers", {}) or {}).items())[:10]),
            "strong_correlations": (stats.get("strong_correlations", []) or [])[:15],
        },
        "insights": {
            "findings": (insights.get("findings", []) or [])[:20],
            "recommendations": (insights.get("recommendations", []) or [])[:20],
            "distribution_insights": (insights.get("distribution_insights", []) or [])[:20],
        },
        "chart_summaries": _extract_chart_context(charts),
        "chart_visibility_notes": _chart_visibility_notes(stats, charts),
        "shown_chart_keys": list(charts.keys()),
        "sample_rows": sample_rows,
    }


def _infer_focus_chart(question: str, dataset_context: dict) -> Optional[dict]:
    """Pick the most relevant generated chart for a question, if any."""
    q = (question or "").lower()
    summaries = dataset_context.get("chart_summaries", []) or []
    if not summaries:
        return None

    chart_type_words = {
        "bar": "bar",
        "line": "scatter",
        "scatter": "scatter",
        "heatmap": "heatmap",
        "histogram": "histogram",
        "box": "box",
        "pie": "pie",
        "donut": "pie",
    }

    for keyword, trace_hint in chart_type_words.items():
        if keyword in q:
            for chart in summaries:
                key_title = (chart.get("chart_key", "") + " " + chart.get("title", "")).lower()
                trace_types = [str(t).lower() for t in chart.get("trace_types", [])]
                if keyword in key_title or any(trace_hint in t for t in trace_types):
                    return chart

    # Fallback: if user mentions chart/graph, provide first generated chart context.
    if "chart" in q or "graph" in q:
        return summaries[0]

    return None


def _is_chart_question(question: str) -> bool:
    q = (question or "").lower()
    keywords = [
        "chart", "graph", "plot", "bar", "line", "scatter", "heatmap",
        "histogram", "box", "boxplot", "pie", "donut",
    ]
    return any(k in q for k in keywords)


def _format_chart_response(chart_summary: dict) -> str:
    title = chart_summary.get("title") or chart_summary.get("chart_key", "Chart")
    trace_types = chart_summary.get("trace_types", [])
    trace_list = chart_summary.get("traces", [])

    x_preview = []
    y_preview = []
    bar_peak = None
    if trace_list:
        t0 = trace_list[0]
        x_preview = t0.get("x_preview", [])[:3]
        y_preview = t0.get("y_preview", [])[:3]
        bar_peak = t0.get("bar_peak")

    key_points = [
        f"X-axis: values like {x_preview} (sample)",
        f"Y-axis: values like {y_preview} (sample)",
    ]
    if bar_peak:
        key_points.append(f"Main insight: highest bar is {bar_peak['category']} at {bar_peak['value']}")
    else:
        key_points.append("Main insight: compare the largest and smallest values shown")

    return (
        f"Title: {title}\n\n"
        "Key Points:\n"
        + "\n".join([f"- {p}" for p in key_points])
        + "\n\nConclusion:\n- This chart summarizes the main pattern visible in the generated data."
    )


def _chart_not_shown_reason(question: str, dataset_context: dict) -> str:
    q = (question or "").lower()
    notes = dataset_context.get("chart_visibility_notes", [])
    chart_keyword_map = {
        "bar": "bar_mean",
        "line": "timeseries",
        "time": "timeseries",
        "scatter": "scatter",
        "heatmap": "heatmap",
        "histogram": "histogram",
        "box": "boxplots",
        "pie": "donut",
        "donut": "donut",
    }
    asked_type = next((chart_keyword_map[k] for k in chart_keyword_map if k in q), None)
    if asked_type:
        for note in notes:
            if note.get("chart_type") == asked_type:
                return (
                    f"Title: {asked_type} chart\n\n"
                    "Key Points:\n"
                    f"- Status: {'shown' if note.get('shown') else 'not shown'}\n"
                    f"- Reason: {note.get('reason')}\n\n"
                    "Conclusion:\n- Try adjusting the dataset so this chart becomes eligible."
                )
    return "Title: Chart\n\nKey Points:\n- Reason not found in current session\n\nConclusion:\n- Ask about a specific chart type."


def _fallback_chat_answer(question: str, dataset_context: dict) -> str:
    q = question.lower()
    stats = dataset_context.get("stats", {})
    charts = dataset_context.get("chart_summaries", [])
    chart_notes = dataset_context.get("chart_visibility_notes", [])
    focus_chart = dataset_context.get("focus_chart") or {}

    if focus_chart and ("chart" in q or "graph" in q or "explain" in q):
        title = focus_chart.get("title", focus_chart.get("chart_key", "selected chart"))
        trace_types = focus_chart.get("trace_types", [])
        trace_list = focus_chart.get("traces", [])
        if trace_list:
            t0 = trace_list[0]
            x_preview = t0.get("x_preview", [])[:3]
            y_preview = t0.get("y_preview", [])[:3]
            return (
                f"Selected chart: {title}. Type: {', '.join(trace_types) if trace_types else 'unknown'}. "
                f"Preview points: x={x_preview}, y={y_preview}. "
                "Interpretation is based on the generated chart data already in this session."
            )
        return f"Selected chart: {title}. I can explain it using session chart context."

    if "not show" in q or "not shown" in q or "why" in q:
        chart_keyword_map = {
            "bar": "bar_mean", "line": "timeseries", "time": "timeseries",
            "scatter": "scatter", "heatmap": "heatmap", "histogram": "histogram",
            "box": "boxplots", "pie": "donut", "donut": "donut",
        }
        asked_type = next((chart_keyword_map[k] for k in chart_keyword_map if k in q), None)
        if asked_type:
            for note in chart_notes:
                if note.get("chart_type") == asked_type:
                    return (
                        f"{asked_type} chart status: {'shown' if note.get('shown') else 'not shown'}. "
                        f"Reason: {note.get('reason')}"
                    )

    if "bar" in q:
        bar_chart = next((c for c in charts if "bar" in c.get("trace_types", [])), None)
        if bar_chart:
            lines = [f"Found a bar chart: {bar_chart.get('title', 'Bar chart')}."]
            for trace in bar_chart.get("traces", []):
                peak = trace.get("bar_peak")
                if peak:
                    lines.append(f"Top bar: {peak['category']} with value {peak['value']}.")
                    break
            lines.append("Ask about specific categories or business interpretation.")
            return "\n".join(lines)

    if "outlier" in q:
        outliers = stats.get("outliers", {})
        if not outliers:
            return "No outliers were detected in the current analysis."
        return f"Outliers detected in: {', '.join(list(outliers.keys())[:5])}."

    if "correlation" in q:
        corrs = stats.get("strong_correlations", [])
        if not corrs:
            return "No strong correlations were detected in this dataset."
        top = corrs[0]
        return f"Strong correlation between {top.get('col1')} and {top.get('col2')}: r={top.get('correlation'):.3f}."

    return (
        "AI service unavailable right now. Try asking about outliers, "
        "correlations, a specific column, or chart title."
    )


def _ask_dataset_chatbot(question: str, dataset_context: dict, chat_history: list[dict]) -> str:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key or Groq is None:
        return _fallback_chat_answer(question, dataset_context)

    system_prompt = (
        "You are a data-analysis assistant embedded in an analytics dashboard. "
        "Answer ONLY using the provided dataset context. "
        "The user already sees generated charts in the app, so do not ask for image uploads. "
        "Use chart metadata/context from this session to explain charts. "
        "When explaining a chart: describe its title, what the axes represent, "
        "the key visible pattern, and one practical takeaway — in plain English. "
        "Keep answers concise (3-5 sentences). Use bullet points only when listing multiple items. "
        "If information is missing, say so and suggest what to ask next."
    )

    try:
        client = Groq(api_key=api_key)
        model_name = os.getenv("GROQ_CHAT_MODEL", "llama-3.3-70b-versatile")

        messages = [
            {"role": "system", "content": system_prompt},
            {
                "role": "system",
                "content": "Dataset context:\n" + json.dumps(dataset_context, default=str)[:18000],
            },
        ]
        for msg in chat_history[-8:]:
            role = msg.get("role")
            content = str(msg.get("content", ""))
            if role in ("user", "assistant") and content:
                messages.append({"role": role, "content": content})
        messages.append({"role": "user", "content": question})

        completion = client.chat.completions.create(
            model=model_name,
            messages=messages,
            temperature=0.2,
            max_tokens=700,
        )
        reply = completion.choices[0].message.content
        return (reply or "").strip() or "I could not generate an answer for that question."
    except Exception as exc:
        logger.exception("Dataset chat error")
        return _fallback_chat_answer(question, dataset_context) + f"\n\n(Service error: {exc})"


# ═══════════════════════════════════════════════════════════════════════════════
# FLOATING CHATBOT  — pure HTML/CSS/JS injected once at the bottom of the page
# Uses a hidden Streamlit form for message submission (avoids full page reload
# on every keystroke while still keeping everything server-side).
# ═══════════════════════════════════════════════════════════════════════════════

def _build_chat_messages_html(messages: list[dict]) -> str:
    """Render message history as HTML bubbles."""
    if not messages:
        return (
            '<div class="fc-empty">'
            '<div class="fc-empty-icon">◈</div>'
            '<p>Ask me anything about your dataset — charts, outliers, distributions, correlations…</p>'
            '</div>'
        )
    rows = []
    for msg in messages[-30:]:
        role = msg.get("role", "assistant")
        safe = html.escape(str(msg.get("content", ""))).replace("\n", "<br>")
        label = "You" if role == "user" else "AI"
        rows.append(
            f'<div class="fc-msg fc-{role}">'
            f'<span class="fc-label">{label}</span>'
            f'<div class="fc-text">{safe}</div>'
            f'</div>'
        )
    return "".join(rows)


def _inject_floating_chatbot(messages: list[dict]) -> None:
    """
    Injects the floating chat bubble + panel as a fixed-position HTML overlay.
    The panel renders the chat history statically. Sending a message uses a
    hidden Streamlit form so the backend processes it normally.
    """
    history_html = _build_chat_messages_html(messages)

    # Quick-prompt chips shown when chat is empty
    chips_html = ""
    if not messages:
        chips = [
            "Explain the bar chart",
            "What are the outliers?",
            "Any strong correlations?",
            "Summarise data quality",
            "Which columns are skewed?",
        ]
        chips_html = "".join(
            f'<button class="fc-chip" onclick="fillInput(this)">{c}</button>'
            for c in chips
        )

    overlay_html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Barlow:wght@400;500;600;700&display=swap');

  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

  :root {{
    --bg:      #0b0f1a;
    --surf:    #111827;
    --surf2:   #1a2236;
    --border:  #1e2d47;
    --accent:  #3b82f6;
    --accent2: #6366f1;
    --green:   #10b981;
    --text:    #e2e8f0;
    --muted:   #64748b;
    --radius:  14px;
    --w:       340px;
    --h:       510px;
  }}

  body {{ background: transparent; overflow: hidden; }}

  /* ─── Bubble ─── */
  #fc-bubble {{
    position: fixed;
    left: 22px;
    bottom: 28px;
    width: 50px;
    height: 50px;
    border-radius: 50%;
    background: linear-gradient(145deg, var(--accent) 0%, var(--accent2) 100%);
    box-shadow: 0 0 0 0 rgba(99,102,241,0.5);
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 99999;
    transition: transform .22s cubic-bezier(.34,1.56,.64,1);
    animation: bubble-pulse 3s ease-in-out infinite;
    user-select: none;
  }}
  #fc-bubble:hover {{ transform: scale(1.1); }}
  #fc-bubble.active {{ animation: none; transform: scale(0.95); }}
  @keyframes bubble-pulse {{
    0%, 100% {{ box-shadow: 0 0 0 0 rgba(99,102,241,0.5); }}
    50%       {{ box-shadow: 0 0 0 10px rgba(99,102,241,0); }}
  }}
  #fc-bubble svg {{ pointer-events: none; }}

  /* unread badge */
  #fc-badge {{
    position: absolute;
    top: -3px; right: -3px;
    width: 14px; height: 14px;
    background: var(--green);
    border-radius: 50%;
    border: 2px solid var(--bg);
    display: none;
  }}

  /* ─── Panel ─── */
  #fc-panel {{
    position: fixed;
    left: 22px;
    bottom: 90px;
    width: var(--w);
    height: var(--h);
    background: var(--surf);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    box-shadow: 0 24px 60px rgba(0,0,0,0.6), 0 0 0 1px rgba(99,102,241,0.07);
    display: flex;
    flex-direction: column;
    z-index: 99998;
    overflow: hidden;
    opacity: 0;
    pointer-events: none;
    transform: translateY(14px) scale(0.97);
    transform-origin: bottom left;
    transition: opacity .2s ease, transform .2s cubic-bezier(.34,1.3,.64,1);
  }}
  #fc-panel.open {{
    opacity: 1;
    pointer-events: all;
    transform: translateY(0) scale(1);
  }}

  /* ─── Header ─── */
  .fc-header {{
    display: flex;
    align-items: center;
    gap: 9px;
    padding: 12px 14px 11px;
    background: var(--surf2);
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
  }}
  .fc-header-live {{
    width: 7px; height: 7px;
    border-radius: 50%;
    background: var(--green);
    box-shadow: 0 0 5px var(--green);
    flex-shrink: 0;
    animation: blink 2.5s ease-in-out infinite;
  }}
  @keyframes blink {{
    0%,100% {{ opacity:1; }} 50% {{ opacity:.35; }}
  }}
  .fc-header-title {{
    font-family: 'Barlow', sans-serif;
    font-size: 13px;
    font-weight: 700;
    color: var(--text);
    letter-spacing: .03em;
    flex: 1;
  }}
  .fc-header-sub {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 9.5px;
    color: var(--muted);
    letter-spacing: .04em;
  }}
  .fc-close {{
    width: 22px; height: 22px;
    border-radius: 6px;
    background: transparent;
    border: none;
    cursor: pointer;
    color: var(--muted);
    font-size: 15px;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: background .15s, color .15s;
  }}
  .fc-close:hover {{ background: var(--border); color: var(--text); }}

  /* ─── Messages ─── */
  #fc-messages {{
    flex: 1;
    overflow-y: auto;
    padding: 14px 12px 8px;
    display: flex;
    flex-direction: column;
    gap: 10px;
    scroll-behavior: smooth;
    scrollbar-width: thin;
    scrollbar-color: var(--border) transparent;
  }}
  #fc-messages::-webkit-scrollbar {{ width: 3px; }}
  #fc-messages::-webkit-scrollbar-thumb {{ background: var(--border); border-radius: 3px; }}

  .fc-empty {{
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 10px;
    padding: 20px;
    text-align: center;
  }}
  .fc-empty-icon {{
    font-size: 28px;
    opacity: .35;
    color: var(--accent2);
  }}
  .fc-empty p {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    color: var(--muted);
    line-height: 1.6;
    max-width: 240px;
  }}

  .fc-msg {{
    display: flex;
    flex-direction: column;
    gap: 3px;
    max-width: 90%;
    animation: msg-in .18s ease;
  }}
  @keyframes msg-in {{
    from {{ opacity:0; transform: translateY(5px); }}
    to   {{ opacity:1; transform: translateY(0); }}
  }}
  .fc-msg.fc-user  {{ align-self: flex-end; }}
  .fc-msg.fc-assistant {{ align-self: flex-start; }}

  .fc-label {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 9px;
    letter-spacing: .08em;
    text-transform: uppercase;
    color: var(--muted);
    padding: 0 4px;
  }}
  .fc-text {{
    font-family: 'Barlow', sans-serif;
    font-size: 12.5px;
    line-height: 1.55;
    padding: 8px 11px;
    border-radius: 10px;
  }}
  .fc-user .fc-text {{
    background: linear-gradient(135deg, var(--accent) 0%, var(--accent2) 100%);
    color: #fff;
    border-bottom-right-radius: 3px;
  }}
  .fc-assistant .fc-text {{
    background: var(--surf2);
    color: var(--text);
    border: 1px solid var(--border);
    border-bottom-left-radius: 3px;
  }}

  /* ─── Chips ─── */
  #fc-chips {{
    padding: 0 12px 8px;
    display: flex;
    flex-wrap: wrap;
    gap: 5px;
    flex-shrink: 0;
  }}
  .fc-chip {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10px;
    padding: 4px 9px;
    border: 1px solid var(--border);
    border-radius: 20px;
    color: var(--muted);
    background: transparent;
    cursor: pointer;
    transition: border-color .15s, color .15s, background .15s;
    white-space: nowrap;
  }}
  .fc-chip:hover {{
    border-color: var(--accent);
    color: var(--accent);
    background: rgba(59,130,246,.07);
  }}

  /* ─── Input row ─── */
  .fc-input-row {{
    display: flex;
    align-items: flex-end;
    gap: 7px;
    padding: 9px 12px 13px;
    border-top: 1px solid var(--border);
    background: var(--surf2);
    flex-shrink: 0;
  }}
  #fc-input {{
    flex: 1;
    background: var(--surf);
    border: 1px solid var(--border);
    border-radius: 9px;
    padding: 8px 11px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11.5px;
    color: var(--text);
    resize: none;
    outline: none;
    min-height: 36px;
    max-height: 80px;
    transition: border-color .15s;
    line-height: 1.5;
  }}
  #fc-input::placeholder {{ color: var(--muted); }}
  #fc-input:focus {{ border-color: var(--accent); }}

  #fc-send {{
    width: 34px; height: 34px;
    border-radius: 9px;
    background: linear-gradient(135deg, var(--accent), var(--accent2));
    border: none;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    transition: opacity .15s, transform .15s;
  }}
  #fc-send:hover {{ opacity: .85; transform: scale(1.06); }}

  /* loading dots */
  .fc-typing {{
    display: none;
    align-self: flex-start;
    background: var(--surf2);
    border: 1px solid var(--border);
    border-radius: 10px;
    border-bottom-left-radius: 3px;
    padding: 9px 13px;
  }}
  .fc-typing span {{
    display: inline-block;
    width: 5px; height: 5px;
    border-radius: 50%;
    background: var(--accent);
    margin: 0 2px;
    animation: dot .9s infinite;
  }}
  .fc-typing span:nth-child(2) {{ animation-delay: .15s; }}
  .fc-typing span:nth-child(3) {{ animation-delay: .3s; }}
  @keyframes dot {{
    0%,80%,100% {{ transform: translateY(0); }}
    40%         {{ transform: translateY(-5px); }}
  }}
</style>
</head>
<body>

<!-- ── Bubble ── -->
<div id="fc-bubble" onclick="togglePanel()" title="Ask about your data">
  <div id="fc-badge"></div>
  <svg width="20" height="20" viewBox="0 0 24 24" fill="none">
    <path d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z"
      stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
  </svg>
</div>

<!-- ── Panel ── -->
<div id="fc-panel">

  <!-- Header -->
  <div class="fc-header">
    <div class="fc-header-live"></div>
    <span class="fc-header-title">Dataset Assistant</span>
    <span class="fc-header-sub">GROQ · LLaMA</span>
    <button class="fc-close" onclick="togglePanel()">✕</button>
  </div>

  <!-- Messages -->
  <div id="fc-messages">
    {history_html}
    <div class="fc-typing" id="fc-typing">
      <span></span><span></span><span></span>
    </div>
  </div>

  <!-- Quick chips (only when empty) -->
  <div id="fc-chips">
    {chips_html}
  </div>

  <!-- Input -->
  <div class="fc-input-row">
    <textarea id="fc-input" rows="1" placeholder="Ask about your data…"
      onkeydown="handleKey(event)" oninput="autoGrow(this)"></textarea>
    <button id="fc-send" onclick="sendMessage()" title="Send">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none">
        <path d="M22 2L11 13M22 2L15 22l-4-9-9-4 20-7z"
          stroke="white" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
      </svg>
    </button>
  </div>

</div>

<script>
  // ── Hidden Streamlit communication bridge ──
  // We write the user question into a hidden <input> that Streamlit watches
  // via st.components. We POST via query-param trick.

  function togglePanel() {{
    const panel  = document.getElementById('fc-panel');
    const bubble = document.getElementById('fc-bubble');
    const badge  = document.getElementById('fc-badge');
    const open   = panel.classList.toggle('open');
    bubble.classList.toggle('active', open);
    badge.style.display = 'none';
    if (open) scrollToBottom();
  }}

  function scrollToBottom() {{
    const msgs = document.getElementById('fc-messages');
    msgs.scrollTop = msgs.scrollHeight;
  }}

  function autoGrow(el) {{
    el.style.height = 'auto';
    el.style.height = Math.min(el.scrollHeight, 80) + 'px';
  }}

  function handleKey(e) {{
    if (e.key === 'Enter' && !e.shiftKey) {{
      e.preventDefault();
      sendMessage();
    }}
  }}

  function fillInput(btn) {{
    document.getElementById('fc-input').value = btn.textContent.trim();
    document.getElementById('fc-input').focus();
  }}

  function sendMessage() {{
    const input = document.getElementById('fc-input');
    const text  = input.value.trim();
    if (!text) return;

    // Append user bubble immediately
    appendBubble('user', 'You', text);
    input.value = '';
    input.style.height = 'auto';

    // Hide chips
    document.getElementById('fc-chips').style.display = 'none';

    // Show typing indicator
    document.getElementById('fc-typing').style.display = 'block';
    scrollToBottom();

    // Send to Streamlit parent via postMessage
    window.parent.postMessage({{type: 'fc_message', text: text}}, '*');
  }}

  function appendBubble(role, label, text) {{
    const typing = document.getElementById('fc-typing');
    const msgs   = document.getElementById('fc-messages');

    // Remove empty state if present
    const empty = msgs.querySelector('.fc-empty');
    if (empty) empty.remove();

    const wrapper = document.createElement('div');
    wrapper.className = `fc-msg fc-${{role}}`;
    wrapper.innerHTML =
      `<span class="fc-label">${{label}}</span>` +
      `<div class="fc-text">${{text.replace(/\\n/g,'<br>')}}</div>`;
    msgs.insertBefore(wrapper, typing);
    scrollToBottom();
  }}

  // Receive reply from Streamlit parent
  window.addEventListener('message', function(e) {{
    if (e.data && e.data.type === 'fc_reply') {{
      document.getElementById('fc-typing').style.display = 'none';
      appendBubble('assistant', 'AI', e.data.text);
      scrollToBottom();
    }}
  }});

  // Auto-scroll on load
  window.addEventListener('load', scrollToBottom);
</script>
</body>
</html>
"""

    # Render the overlay in a zero-height container so it doesn't push layout
    components.html(overlay_html, height=0, scrolling=False)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="AI Data Analyst",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL CSS  (your original styles, unchanged)
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&family=Outfit:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'Outfit', sans-serif;
    background-color: #080c14;
    color: #e2e8f0;
}
[data-testid="stAppViewContainer"] .main .block-container {
    padding-top: clamp(2.25rem, 4vh, 3.5rem);
    padding-bottom: 1.5rem;
    max-width: 1400px;
}
[data-testid="stSidebar"] {
    background: #080c14;
    border-right: 1px solid rgba(99, 102, 241, 0.15);
}
[data-testid="stSidebar"] * { color: #e2e8f0 !important; }
.stApp {
    background: #080c14;
    background-image: radial-gradient(ellipse at 20% 10%, rgba(99,102,241,0.08) 0%, transparent 60%),
                      radial-gradient(ellipse at 80% 80%, rgba(16,185,129,0.05) 0%, transparent 60%);
}
div[data-testid="stMetric"] {
    background: rgba(15, 23, 42, 0.8);
    border: 1px solid rgba(99, 102, 241, 0.2);
    border-top: 2px solid #6366f1;
    border-radius: 8px;
    padding: 16px 18px;
    transition: border-color 0.2s, transform 0.2s;
    backdrop-filter: blur(8px);
}
div[data-testid="stMetric"]:hover {
    border-top-color: #a5b4fc;
    transform: translateY(-2px);
    box-shadow: 0 8px 24px rgba(99,102,241,0.15);
}
div[data-testid="stMetric"] label {
    font-size: 0.68rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #64748b !important;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    font-family: 'Syne', sans-serif !important;
    font-size: 1.8rem !important;
    font-weight: 800 !important;
    color: #e2e8f0 !important;
}
.stTabs [data-baseweb="tab-list"] {
    gap: 0;
    border-bottom: 1px solid rgba(99,102,241,0.2);
    background: transparent;
    padding: 0;
}
.stTabs [data-baseweb="tab"] {
    height: 42px;
    padding: 0 22px;
    font-size: 0.82rem;
    font-weight: 500;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: #64748b !important;
    border-bottom: 2px solid transparent;
    border-radius: 0;
    background: transparent;
    transition: all 0.2s;
}
.stTabs [data-baseweb="tab"]:hover { color: #a5b4fc !important; border-bottom-color: rgba(99,102,241,0.4); }
.stTabs [aria-selected="true"] {
    color: #6366f1 !important;
    font-weight: 700 !important;
    border-bottom: 2px solid #6366f1 !important;
    background: transparent !important;
}
[data-testid="stTabsContent"] { padding-top: 1.5rem; }
.stButton > button {
    width: 100%;
    border-radius: 6px;
    background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%);
    color: white !important;
    border: none;
    font-family: 'Syne', sans-serif;
    font-weight: 700;
    font-size: 0.85rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 0.65rem 1.2rem;
    transition: all 0.2s;
    box-shadow: 0 4px 15px rgba(99,102,241,0.3);
}
.stButton > button:hover {
    background: linear-gradient(135deg, #818cf8 0%, #6366f1 100%);
    box-shadow: 0 6px 20px rgba(99,102,241,0.45);
    transform: translateY(-1px);
}
hr { border-color: rgba(99,102,241,0.15) !important; }
[data-testid="stDataFrame"] { border: 1px solid rgba(99,102,241,0.15); border-radius: 8px; overflow: hidden; }
[data-testid="stExpander"] {
    border: 1px solid rgba(99,102,241,0.15) !important;
    border-radius: 8px !important;
    background: rgba(15, 23, 42, 0.5) !important;
}
.page-header {
    display: flex;
    align-items: center;
    gap: 14px;
    margin-top: 0.25rem;
    margin-bottom: 1.5rem;
    padding-bottom: 1rem;
    border-bottom: 1px solid rgba(99,102,241,0.15);
}
.page-header .title {
    font-family: 'Syne', sans-serif;
    font-size: 1.6rem;
    font-weight: 800;
    color: #f1f5f9;
    margin: 0;
}
.section-label {
    font-family: 'Syne', sans-serif;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: #475569;
    margin-bottom: 10px;
    padding-bottom: 6px;
    border-bottom: 1px solid rgba(99,102,241,0.12);
}
.warning-block {
    background: rgba(251,191,36,0.07);
    border: 1px solid rgba(251,191,36,0.2);
    border-left: 3px solid #fbbf24;
    border-radius: 6px;
    padding: 10px 14px;
    font-size: 0.83rem;
    color: #fde68a;
    margin-bottom: 8px;
}
.empty-state-box {
    text-align: center;
    padding: 60px 40px;
    background: rgba(15, 23, 42, 0.4);
    border: 1px dashed rgba(99,102,241,0.2);
    border-radius: 12px;
}
.empty-state-box .icon { font-size: 2.5rem; margin-bottom: 16px; }
.empty-state-box h2 {
    font-family: 'Syne', sans-serif;
    font-size: 1.4rem;
    font-weight: 800;
    color: #e2e8f0;
    margin: 0 0 10px 0;
}
.empty-state-box p { font-size: 0.9rem; color: #475569; max-width: 380px; margin: 0 auto; line-height: 1.7; }
code, .mono { font-family: 'JetBrains Mono', monospace !important; }
[data-testid="stWarning"] {
    background: rgba(251,191,36,0.07) !important;
    border: 1px solid rgba(251,191,36,0.2) !important;
    color: #fde68a !important;
}
[data-testid="stInfo"] {
    background: rgba(56,189,248,0.06) !important;
    border: 1px solid rgba(56,189,248,0.2) !important;
    color: #7dd3fc !important;
}

/* Chat styling */
.chat-user-question {
    font-size: 1.08rem;
    font-weight: 700;
    line-height: 1.55;
    color: #e2e8f0;
    margin: 10px 0 8px 0;
}

.chat-ai-label {
    font-size: 0.9rem;
    font-weight: 700;
    color: #93c5fd;
    margin: 12px 0 4px 0;
}

/* ── make components.html height-0 truly invisible ── */
iframe[height="0"] {
    position: fixed !important;
    left: 0; bottom: 0;
    width: 100vw !important;
    height: 100vh !important;
    pointer-events: none;
    border: none !important;
    z-index: 99990;
    background: transparent !important;
}
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# SESSION STATE
# ═══════════════════════════════════════════════════════════════════════════════
_DEFAULTS = {
    "analysis_result": None,
    "uploaded_file_name": None,
    "file_bytes": None,
    "chat_messages": [],
    "chat_dataset_name": None,
    "force_chat_tab": False,
    "chat_pending_question": None,
    "chat_autoscroll": False,
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div style="padding:20px 4px 12px 4px">
        <p style="font-family:'Syne',sans-serif;font-size:1.3rem;font-weight:800;
                  color:#f1f5f9;letter-spacing:-.02em;margin:0">◈ Data Analyst</p>
        <p style="font-size:.72rem;color:#334155;margin:3px 0 0 0;letter-spacing:.06em">
          POWERED BY LANGGRAPH + GROQ</p>
    </div>
    """, unsafe_allow_html=True)
    st.divider()

    uploaded_file = st.file_uploader(
        "Upload Dataset",
        type=["csv"],
        help="CSV files up to 200 MB.",
    )

    if uploaded_file is not None:
        current_bytes = uploaded_file.getvalue()
        if st.session_state["uploaded_file_name"] != uploaded_file.name:
            st.session_state["file_bytes"] = current_bytes
            st.session_state["uploaded_file_name"] = uploaded_file.name
            st.session_state["analysis_result"] = None
            st.session_state["chat_messages"] = []
            st.session_state["chat_dataset_name"] = uploaded_file.name
        elif st.session_state["file_bytes"] is None:
            st.session_state["file_bytes"] = current_bytes

    if st.session_state["uploaded_file_name"]:
        st.markdown(f"""
        <div style="background:rgba(16,185,129,.08);border:1px solid rgba(16,185,129,.25);
                    border-radius:6px;padding:8px 12px;margin:8px 0;font-size:.78rem;color:#34d399;">
            Loaded: <code style="color:#6ee7b7;font-size:.76rem">
            {st.session_state['uploaded_file_name']}</code>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    has_file = st.session_state["file_bytes"] is not None
    run_clicked = st.button("Run Analysis", type="primary", disabled=not has_file)

# ═══════════════════════════════════════════════════════════════════════════════
# RUN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════
if run_clicked and st.session_state["file_bytes"] is not None:
    with st.spinner("Running analysis pipeline…"):
        try:
            df = pd.read_csv(io.BytesIO(st.session_state["file_bytes"]))
            state = run_pipeline(df)
            result_data = state.model_dump()
            st.session_state["analysis_result"] = result_data
        except Exception as exc:
            st.error(f"Pipeline error: {exc}")
            logger.exception("Pipeline error")

result = st.session_state.get("analysis_result")

# ═══════════════════════════════════════════════════════════════════════════════
# EMPTY STATE
# ═══════════════════════════════════════════════════════════════════════════════
if result is None:
    st.markdown("<br><br>", unsafe_allow_html=True)
    _, mid, _ = st.columns([1, 2.2, 1])
    with mid:
        st.markdown("""
        <div class="empty-state-box">
            <div class="icon">◈</div>
            <h2>AI Data Analyst</h2>
            <p>Upload a CSV in the sidebar, then click
            <strong style="color:#818cf8">Run Analysis</strong> to get visualisations,
            statistical breakdowns, and AI-driven insights.</p>
        </div>
        """, unsafe_allow_html=True)
    st.stop()

# ═══════════════════════════════════════════════════════════════════════════════
# PROCESS PENDING CHAT MESSAGE  (before rendering anything)
# ═══════════════════════════════════════════════════════════════════════════════
file_name = st.session_state.get("uploaded_file_name", "Dataset")

if st.session_state.get("chat_dataset_name") != file_name:
    st.session_state["chat_messages"] = []
    st.session_state["chat_dataset_name"] = file_name

# ═══════════════════════════════════════════════════════════════════════════════
# HEADER
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown(f"""
<div class="page-header">
    <p class="title">Analysis</p>
    <code style="font-size:1rem;color:#6366f1;background:rgba(99,102,241,.1);
                 padding:3px 10px;border-radius:4px">{file_name}</code>
</div>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# WARNINGS
# ═══════════════════════════════════════════════════════════════════════════════
errors = result.get("errors", [])
if errors:
    with st.expander("Processing warnings", expanded=False):
        for err in errors:
            st.markdown(f'<div class="warning-block">{err}</div>', unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TOP METRICS
# ═══════════════════════════════════════════════════════════════════════════════
stats    = result.get("stats_summary", {})
insights = result.get("insights", {})

row_count    = stats.get("row_count", 0)
col_count    = stats.get("column_count", 0)
missing_cells = stats.get("data_quality", {}).get("missing_cells", 0)
completeness  = stats.get("data_quality", {}).get("completeness", 100)
outlier_cols  = len(stats.get("outliers", {}))
strong_corrs  = len(stats.get("strong_correlations", []))

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Rows",           f"{row_count:,}")
c2.metric("Columns",        col_count)
c3.metric("Missing Values", missing_cells)
c4.metric("Outlier Cols",   outlier_cols)
c5.metric("Correlations",   strong_corrs)
c6.metric("Completeness",   f"{completeness:.1f}%")
st.markdown("<br>", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════════════════════════
tab_summary, tab_charts, tab_insights, tab_stats, tab_data, tab_chat = st.tabs([
    "SUMMARY", "CHARTS", "INSIGHTS", "STATISTICS", "DATA PREVIEW", "CHAT WITH AI"
])

# ── TAB 1 · SUMMARY ──────────────────────────────────────────────────────────
with tab_summary:
    if not insights:
        st.warning("No insights available yet.")
    else:
        findings      = insights.get("findings", [])
        recommendations = insights.get("recommendations", [])
        outlier_summary = insights.get("outlier_summary", {})

        st.markdown("### What's in your data?")
        for f in findings:
            st.markdown(f"- {f}")

        st.markdown("### What should you do?")
        for r in recommendations:
            st.markdown(f"- {r}")

        st.markdown("### Data Issues")
        if outlier_summary:
            for col, summary in outlier_summary.items():
                st.markdown(f"- **{col}**: {summary}")
        else:
            st.markdown("- No outliers detected")

# ── TAB 2 · CHARTS ───────────────────────────────────────────────────────────
with tab_charts:
    charts = result.get("charts") or {}
    chart_list = list(charts.values())
    if not chart_list:
        st.info("No visualisations could be generated from this dataset.")
    else:
        for i in range(0, len(chart_list), 2):
            row = st.columns(2)
            row[0].plotly_chart(chart_list[i], use_container_width=True)
            if i + 1 < len(chart_list):
                row[1].plotly_chart(chart_list[i + 1], use_container_width=True)

# ── TAB 3 · INSIGHTS ─────────────────────────────────────────────────────────
with tab_insights:
    insights_data = result.get("insights") or {}
    if not insights_data:
        st.warning("No insights generated yet.")
    else:
        st.markdown("### Numeric Column Deep Dive")
        numeric_cols = stats.get("numeric_columns", {})
        for col, col_stats in numeric_cols.items():
            with st.expander(f"{col}"):
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Mean",     f"{col_stats.get('mean', 0):.2f}")
                c2.metric("Std Dev",  f"{col_stats.get('std', 0):.2f}")
                c3.metric("Skewness", f"{col_stats.get('skewness', 0):.2f}")
                c4.metric("Kurtosis", f"{col_stats.get('kurtosis', 0):.2f}")
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Min", f"{col_stats.get('min', 0):.2f}")
                c2.metric("Max", f"{col_stats.get('max', 0):.2f}")
                c3.metric("Q1",  f"{col_stats.get('q1', 0):.2f}")
                c4.metric("Q3",  f"{col_stats.get('q3', 0):.2f}")

        st.divider()
        st.markdown("### Correlation Analysis")
        strong_corrs_list = stats.get("strong_correlations", [])
        if strong_corrs_list:
            for corr in strong_corrs_list:
                r = corr.get("correlation", 0)
                direction = "Positive" if r > 0 else "Negative"
                st.markdown(
                    f"**{direction}**: **{corr['col1']}** ↔ **{corr['col2']}** "
                    f"— r = `{r:.3f}`"
                )
        else:
            st.info("No strong correlations found.")

        st.divider()
        st.markdown("### Distribution Patterns")
        for col, col_stats in numeric_cols.items():
            skew = col_stats.get("skewness", 0)
            kurt = col_stats.get("kurtosis", 0)
            iqr  = col_stats.get("iqr", 0)
            if abs(skew) < 0.5:      dist_type = "Approximately Normal"
            elif skew > 1:           dist_type = "Heavily Right-Skewed"
            elif skew > 0:           dist_type = "Slightly Right-Skewed"
            elif skew < -1:          dist_type = "Heavily Left-Skewed"
            else:                    dist_type = "Slightly Left-Skewed"
            st.markdown(
                f"- **{col}**: {dist_type} | "
                f"Skew: `{skew:.2f}` | Kurtosis: `{kurt:.2f}` | IQR: `{iqr:.2f}`"
            )

        st.divider()
        st.markdown("### Outlier Deep Dive")
        outliers = stats.get("outliers", {})
        if outliers:
            for col, info in outliers.items():
                st.markdown(
                    f"- **{col}**: `{info.get('count')}` outliers "
                    f"({info.get('percentage', 0):.2f}%) — "
                    f"bounds: `{info.get('lower_bound', 0):.2f}` → `{info.get('upper_bound', 0):.2f}`"
                )
        else:
            st.success("No outliers detected.")

        st.divider()
        st.markdown("### Categorical Column Breakdown")
        categorical_cols = stats.get("categorical_columns", {})
        if categorical_cols:
            for col, col_stats in categorical_cols.items():
                st.markdown(
                    f"- **{col}**: `{col_stats.get('unique_values')}` unique | "
                    f"Top: **{col_stats.get('most_common')}** ({col_stats.get('most_common_count')} times)"
                )
        else:
            st.info("No categorical columns found.")

# ── TAB 4 · STATISTICS ───────────────────────────────────────────────────────
with tab_stats:
    if not stats:
        st.warning("No statistics available yet.")
    else:
        st.markdown("### Data Overview")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Rows",         f"{stats.get('row_count', 0):,}")
        col2.metric("Columns",      stats.get('column_count', 0))
        col3.metric("Memory (MB)",  f"{stats.get('memory_usage_mb', 0):.2f}")
        col4.metric("Completeness", f"{stats.get('data_quality', {}).get('completeness', 100):.1f}%")
        st.divider()

        numeric_cols = stats.get("numeric_columns", {})
        if numeric_cols:
            st.markdown("### Numeric Columns Statistics")
            numeric_stats_list = []
            for col_name, col_stats in numeric_cols.items():
                numeric_stats_list.append({
                    "Column": col_name,
                    "Mean":   f"{col_stats.get('mean', 0):.2f}",
                    "Median": f"{col_stats.get('median', 0):.2f}",
                    "Std Dev":f"{col_stats.get('std', 0):.2f}",
                    "Min":    f"{col_stats.get('min', 0):.2f}",
                    "Max":    f"{col_stats.get('max', 0):.2f}",
                })
            st.dataframe(pd.DataFrame(numeric_stats_list), use_container_width=True)
            st.divider()

        categorical_cols = stats.get("categorical_columns", {})
        if categorical_cols:
            st.markdown("### Categorical Columns")
            for col_name, col_stats in categorical_cols.items():
                st.markdown(f"**{col_name}** — "
                            f"{col_stats.get('unique_values',0)} unique | "
                            f"Top: {col_stats.get('most_common','N/A')} "
                            f"({col_stats.get('most_common_count',0)}×) | "
                            f"Diversity: {col_stats.get('diversity_ratio',0):.3f}")
            st.divider()

        st.markdown("### Data Quality")
        dq = stats.get('data_quality', {})
        col1, col2, col3 = st.columns(3)
        col1.metric("Missing Cells",  dq.get('missing_cells', 0))
        col2.metric("Duplicate Rows", dq.get('duplicate_rows', 0))
        col3.metric("Total Cells",    dq.get('total_cells', 0))

        outliers = stats.get("outliers", {})
        if outliers:
            st.divider()
            st.markdown("### Outliers Detected")
            st.dataframe(pd.DataFrame([{
                "Column": col,
                "Count": info.get('count', 0),
                "Percentage": f"{info.get('percentage', 0):.2f}%",
                "Lower Bound": f"{info.get('lower_bound', 0):.2f}",
                "Upper Bound": f"{info.get('upper_bound', 0):.2f}",
            } for col, info in outliers.items()]), use_container_width=True)

        strong_corrs_list = stats.get("strong_correlations", [])
        if strong_corrs_list:
            st.divider()
            st.markdown("### Strong Correlations")
            st.dataframe(pd.DataFrame([{
                "Column 1": c.get("col1", ""),
                "Column 2": c.get("col2", ""),
                "Correlation": f"{c.get('correlation', 0):.3f}",
            } for c in strong_corrs_list]), use_container_width=True)

# ── TAB 5 · DATA PREVIEW ─────────────────────────────────────────────────────
with tab_data:
    raw   = result.get("raw_df")
    clean = result.get("clean_df")

    if isinstance(raw, dict):
        try:   raw = pd.DataFrame(raw)
        except Exception: raw = None
    if isinstance(clean, dict):
        try:   clean = pd.DataFrame(clean)
        except Exception: clean = None

    col_l, col_r = st.columns(2, gap="large")
    with col_l:
        st.markdown('<p class="section-label">Raw Dataset</p>', unsafe_allow_html=True)
        st.caption("First 100 rows before processing")
        if raw is not None:
            st.dataframe(raw.head(100), use_container_width=True)
            buf = io.BytesIO(); raw.to_csv(buf, index=False)
            st.download_button("↓ Download Raw CSV", buf.getvalue(), "raw_data.csv", "text/csv")
        else:
            st.info("Raw data not available.")

    with col_r:
        st.markdown('<p class="section-label">Cleaned Dataset</p>', unsafe_allow_html=True)
        st.caption("First 100 rows after processing")
        if clean is not None:
            st.dataframe(clean.head(100), use_container_width=True)
            buf = io.BytesIO(); clean.to_csv(buf, index=False)
            st.download_button("↓ Download Cleaned CSV", buf.getvalue(), "cleaned_data.csv", "text/csv")
        else:
            st.info("Cleaned data not available.")

# ── TAB 6 · CHAT WITH AI ────────────────────────────────────────────────────
with tab_chat:
    st.markdown("### Chat With AI")
    st.caption("Ask questions about the dataset, charts, and any analysis output.")

    # Process any pending question first (captured from bottom input on prior rerun).
    pending_question = st.session_state.get("chat_pending_question")
    if pending_question:
        question = pending_question.strip()
        st.session_state["chat_pending_question"] = None
        st.session_state["chat_messages"].append({"role": "user", "content": question})
        dataset_context = _build_dataset_context(result, file_name)

        inferred_chart = _infer_focus_chart(question, dataset_context)
        is_chart_question = _is_chart_question(question)

        if is_chart_question and inferred_chart:
            answer = _format_chart_response(inferred_chart)
            st.session_state["chat_messages"].append({
                "role": "assistant",
                "content": answer,
                "chart_key": inferred_chart.get("chart_key"),
            })
        elif is_chart_question:
            answer = _chart_not_shown_reason(question, dataset_context)
            st.session_state["chat_messages"].append({"role": "assistant", "content": answer})
        else:
            answer = _ask_dataset_chatbot(question, dataset_context, st.session_state["chat_messages"])
            st.session_state["chat_messages"].append({"role": "assistant", "content": answer})

        # Streamlit reruns and often re-selects the first tab; request chat tab restore.
        st.session_state["force_chat_tab"] = True
        st.session_state["chat_autoscroll"] = True

    # Full-width output area. Keep full dialogue so user can scroll up for previous chats.
    if st.session_state["chat_messages"]:
        st.markdown('<div id="chat-dialog-anchor"></div>', unsafe_allow_html=True)
        with st.container(height=520):
            for idx, msg in enumerate(st.session_state["chat_messages"]):
                if msg.get("role") == "user":
                    user_text = html.escape(str(msg.get("content", "")))
                    st.markdown(
                        f'<div class="chat-user-question">You: {user_text}</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    chart_key = msg.get("chart_key")
                    if chart_key:
                        chart_obj = (result.get("charts") or {}).get(chart_key)
                        if chart_obj is not None:
                            st.plotly_chart(
                                chart_obj,
                                use_container_width=True,
                                key=f"chat_dialog_chart_{idx}_{chart_key}",
                            )
                    st.markdown('<div class="chat-ai-label">AI:</div>', unsafe_allow_html=True)
                    st.markdown(f"{msg.get('content', '')}")

            st.markdown('<div id="chat-end-marker"></div>', unsafe_allow_html=True)

        if st.session_state.get("chat_autoscroll"):
            components.html(
                """
                <script>
                  const scrollChatToBottom = () => {
                                        const endMarker = window.parent.document.getElementById('chat-end-marker');
                                        if (!endMarker) return false;
                                        endMarker.scrollIntoView({ behavior: 'auto', block: 'end' });
                                        return true;
                  };
                  if (!scrollChatToBottom()) {
                    let tries = 0;
                    const t = setInterval(() => {
                      tries += 1;
                      if (scrollChatToBottom() || tries > 20) clearInterval(t);
                    }, 120);
                  }
                </script>
                """,
                height=0,
            )
            st.session_state["chat_autoscroll"] = False
    else:
        st.info("Start by asking a question, for example: Explain the bar graph.")

    # User input fixed at bottom of the tab area.
    chat_question = st.chat_input("Ask about dataset or charts...")
    if chat_question and chat_question.strip():
        st.session_state["chat_pending_question"] = chat_question.strip()
        st.session_state["force_chat_tab"] = True
        st.rerun()

# Keep CHAT WITH AI active after submit by re-selecting tab client-side once.
if st.session_state.get("force_chat_tab"):
        components.html(
                """
                <script>
                    const clickChatTab = () => {
                        const tabs = window.parent.document.querySelectorAll('[data-baseweb="tab"]');
                        for (const tab of tabs) {
                            if ((tab.textContent || '').trim().toUpperCase() === 'CHAT WITH AI') {
                                tab.click();
                                return true;
                            }
                        }
                        return false;
                    };
                    if (!clickChatTab()) {
                        let tries = 0;
                        const timer = setInterval(() => {
                            tries += 1;
                            if (clickChatTab() || tries > 20) clearInterval(timer);
                        }, 120);
                    }
                </script>
                """,
                height=0,
        )
        st.session_state["force_chat_tab"] = False
    