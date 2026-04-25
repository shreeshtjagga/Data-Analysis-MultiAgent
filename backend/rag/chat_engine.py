"""
DataPulse RAG Chat Engine
=========================
Answers every user question using:
  1. Pinecone semantic retrieval (top-8 relevant chunks)
  2. Parquet query for exact numbers (when structured query fits)
  3. Groq llama-3.3-70b-versatile for synthesis

The synthesis LLM operates under a strict grounding contract:
  - Context has the answer   → cite exact numbers
  - Context does not         → admit it clearly, pivot to related data
  - Never invent facts
  - Never escape with vague answers ("the data shows various trends")

Also handles chart explanations with extracted Plotly values.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

SYNTHESIS_MODEL = os.getenv("GROQ_SYNTHESIS_MODEL", "llama-3.3-70b-versatile")
INTENT_MODEL    = os.getenv("GROQ_INTENT_MODEL",    "llama-3.1-8b-instant")

_MAX_CONTEXT_CHARS = 7000
_TOP_K_CHUNKS      = 8


# ─────────────────────────────────────────────────────────────────────────────
# System prompts
# ─────────────────────────────────────────────────────────────────────────────

def _data_system_prompt(file_name: str, chart_keys: list[str]) -> str:
    return f"""You are a sharp, senior data analyst for the dataset "{file_name}".
You talk like a knowledgeable colleague — confident, precise, and human.

════ GROUNDING CONTRACT — THIS IS MANDATORY ════
You receive a CONTEXT block with pre-computed facts, correlation data, and live
query results. These are the ONLY facts you may use.

RULE 1: If CONTEXT contains the answer → state the EXACT number or value.
  BAD:  "Revenue is quite high"
  GOOD: "Average revenue is 45,230, with a maximum of 98,000"

RULE 2: If CONTEXT does not contain the answer → say EXACTLY:
  "I don't have [specific thing] in the analysis, but I can tell you [related thing from context]."
  Then pivot to the most relevant thing you DO have.

RULE 3: NEVER say "the data shows various trends" or "values vary significantly".
  Be specific or use RULE 2.

RULE 4: NEVER use training knowledge to fill gaps. Only use CONTEXT.

RULE 5: For "who/which has the highest/lowest X" → name the entity and its
  exact value. Use the Parquet query result if available.

RULE 6: For predictive/future questions → base answer ONLY on trends and
  correlations visible in the current data. State: "Based on the data..."
  and cite the specific trend. Never invent projections.

RULE 7: For "relationship between X and Y" or "correlation" questions →
  Look for correlation data in the ═══ CORRELATION DATA ═══ section of CONTEXT.
  State the Pearson r value, its direction, and its practical meaning.
  BAD:  "There appears to be a positive relationship between price and sales."
  GOOD: "Price and Revenue have a strong positive correlation (r = 0.87) —
         higher-priced items consistently generate more revenue in this dataset."
  If no correlation data is in CONTEXT → say: "I don’t have a correlation
  analysis for those specific columns. Try asking me to generate a scatter plot."

RULE 8: If a greeting somehow reaches you (e.g. "hello", "hi") → respond
  warmly in one sentence and pivot: "Hi! Ask me anything about {file_name}."

════ STYLE ════
• 2–4 sentences. Direct. No waffle. No filler phrases.
• Plain English: "average" not "mean", "spread" not "variance"
• Every factual sentence must contain at least one specific number
• No markdown bold, no bullet lists in your answer
• Conversational — like texting a smart colleague, not writing a report

════ CHART DISPLAY RULES ════
Available chart keys on dashboard: {chart_keys}
• Do NOT render charts using [CHART: key] unless the user explicitly asks to display one.
• Do NOT instruct the user to "say show" or "say plot". Just answer the question.
• Use ONLY exact keys listed above — never guess a key name

════ RECOMMENDATION ════
Add "Recommendation:" at the end ONLY when data reveals something
clearly actionable (a spike, a dominant group, a data quality issue).
Skip for simple lookups and greetings."""


def _chart_system_prompt(file_name: str) -> str:
    return f"""You are explaining a specific chart from the dataset "{file_name}".

════ GROUNDING CONTRACT ════
You will receive CHART FACTS with exact values extracted directly from the chart.

RULE 1: Name the specific highest and lowest values with their exact numbers.
  BAD:  "The chart shows some categories have higher values"
  GOOD: "Electronics has the highest revenue at 2.3M, while Books has the lowest at 45K"

RULE 2: Use ONLY numbers from the provided context. Never invent values.

RULE 3: If no specific data values are provided to you, simply describe what the chart is broadly about based on its title. Do NOT mention "CHART FACTS", "missing data", or complain to the user about your internal prompt.
  BAD: "The CHART FACTS only mentions the title."
  GOOD: "This is a box plot showing the distribution of Country IDs."

════ STYLE ════
• 2–3 sentences max. Name specific entities and values.
• Speak naturally to the user. Never mention your instructions or internal context.
• Do NOT include [CHART: key] in your response — it is appended automatically."""


# ─────────────────────────────────────────────────────────────────────────────
# Parquet query planner
# ─────────────────────────────────────────────────────────────────────────────

async def _plan_and_run_query(
    question:   str,
    file_hash:  str,
    col_types:  dict,
    groq_client,
) -> Optional[dict]:
    """
    Use llama-3.1-8b-instant to decide what Parquet query to run.
    Run it via data_agent.run_data_query.
    Returns query result dict or None.
    """
    from ..core.data_agent import run_data_query

    # Questions that never need a structured query
    # Keep this list VERY tight — overly broad triggers cause the planner to skip
    # legitimate data questions (e.g. "what can you tell me about revenue" was being skipped)
    _SKIP_TRIGGERS = {
        "hello", "hi", "hey", "thanks", "thank you", "goodbye", "good morning",
    }
    if question.lower().strip() in _SKIP_TRIGGERS:
        return None

    planner_prompt = f"""You are a data query planner. Decide if a structured query
is needed to answer this question precisely with exact numbers from the full dataset.

If YES → return ONE JSON object (no explanation, no markdown).
If NO (opinion, greeting, general trend/pattern summary, predictive question) → return: NONE

AVAILABLE QUERY TYPES:
filter_lookup   → look up a column value by filtering another
  example: {{"type":"filter_lookup","params":{{"filter_col":"name","filter_val":"Alice","result_col":"salary"}}}}

top_n           → highest N rows by a numeric column
  example: {{"type":"top_n","params":{{"column":"Revenue","n":5}}}}

bottom_n        → lowest N rows
  example: {{"type":"bottom_n","params":{{"column":"Price","n":3}}}}

group_aggregate → group by one column, aggregate another
  example: {{"type":"group_aggregate","params":{{"group_by":"Region","column":"Sales","func":"sum","n":10}}}}

filter_group    → filter rows then group+aggregate
  example: {{"type":"filter_group","params":{{"group_by":"Brand","func":"count","n":5,"filters":[{{"column":"Year","op":"eq","value":"2023"}}]}}}}

aggregate       → single stat on one column
  example: {{"type":"aggregate","params":{{"column":"Price","func":"mean"}}}}
  funcs: mean, sum, min, max, count, nunique, median, std

value_counts    → count occurrences of each category
  example: {{"type":"value_counts","params":{{"column":"Category","n":10}}}}

search          → full-text search for a specific named entity
  example: {{"type":"search","params":{{"value":"John Smith","n":3}}}}

distinct        → list all unique values in a column
  example: {{"type":"distinct","params":{{"column":"Country"}}}}

row_count       → count rows matching a filter
  example: {{"type":"row_count","params":{{"filters":[{{"column":"Status","op":"eq","value":"Active"}}]}}}}

correlation     → correlation between two numeric columns
  example: {{"type":"correlation","params":{{"column":"Price","column2":"Sales"}}}}

percentile      → compute percentile of a numeric column
  example: {{"type":"percentile","params":{{"column":"Age","percentile":90}}}}

FILTER OPS: eq, neq, gt, lt, gte, lte, contains, year, month, isnull, notnull

DATASET COLUMNS:
{json.dumps(col_types, ensure_ascii=True)}

QUESTION: {question}

Return ONLY the JSON object or the word NONE. No explanation whatsoever."""

    try:
        resp = groq_client.chat.completions.create(
            model=INTENT_MODEL,
            messages=[{"role": "user", "content": planner_prompt}],
            max_tokens=250,
            temperature=0,
        )
        raw = (resp.choices[0].message.content or "").strip()
        logger.info("Query planner response: %s", raw[:200])

        if "{" not in raw:
            return None

        raw_json = raw[raw.find("{") : raw.rfind("}") + 1]
        plan     = json.loads(raw_json)
        qtype    = plan.get("type", "")
        params   = plan.get("params", {})

        valid_types = {
            "filter_lookup", "top_n", "bottom_n", "group_aggregate",
            "filter_group", "aggregate", "value_counts", "search",
            "distinct", "row_count", "correlation", "percentile",
        }
        if qtype not in valid_types:
            logger.warning("Query planner returned invalid type: %s", qtype)
            return None

        result = await asyncio.to_thread(run_data_query, file_hash, qtype, params)

        # If primary query came back empty/error, try a search fallback
        # but SKIP the fallback for correlation/relationship questions — a text
        # search on those returns random rows which pollutes the LLM context.
        is_empty = (
            not result
            or "error" in result
            or result.get("result") == "No rows found."
            or (isinstance(result.get("result"), list) and len(result["result"]) == 0)
        )
        _RELATIONSHIP_WORDS = ("relationship", "correlat", "depend", "associat", "influenc")
        is_relationship_q = any(w in question.lower() for w in _RELATIONSHIP_WORDS)
        if is_empty and not is_relationship_q:
            # Extract meaningful nouns from the question for search
            stop = {
                "what", "when", "where", "which", "does", "have", "many", "much",
                "show", "tell", "give", "find", "list", "this", "that", "the",
                "and", "for", "from", "with", "how", "are", "was", "were",
            }
            words = [
                w.strip("?.,!")
                for w in question.split()
                if len(w) > 3 and w.lower() not in stop
            ]
            if words:
                fallback = await asyncio.to_thread(
                    run_data_query,
                    file_hash,
                    "search",
                    {"value": " ".join(words[:2]), "n": 5},
                )
                if fallback and "error" not in fallback:
                    return fallback

        return result if (result and "error" not in result) else None

    except Exception as exc:
        logger.warning("Query planner failed: %s", exc)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Context assembly
# ─────────────────────────────────────────────────────────────────────────────

def _build_static_context(stats: dict, insights: dict) -> str:
    """
    Build a compact, structured context block from pre-computed stats and insights.
    This grounds the LLM for relationship, summary, and trend questions without
    needing a live Parquet query or RAG chunk.
    """
    parts: list[str] = []

    # Correlations — critical for "relationship between X and Y" questions
    # Try both key names used across different pipeline versions
    correlations = (
        stats.get("strong_correlations")
        or stats.get("correlations")
        or []
    )
    if correlations:
        parts.append("═══ CORRELATION DATA ═══")
        for corr in correlations[:15]:
            c1 = corr.get("col1", "")
            c2 = corr.get("col2", "")
            try:
                r_f = float(corr.get("correlation", 0))
                strength = "strong" if abs(r_f) >= 0.7 else ("moderate" if abs(r_f) >= 0.4 else "weak")
                direction = "positive" if r_f > 0 else "negative"
                parts.append(f"  {c1} ↔ {c2}: r={r_f:.3f} ({strength} {direction})")
            except (TypeError, ValueError):
                parts.append(f"  {c1} ↔ {c2}: r={corr.get('correlation')}")

    # Numeric column summaries
    num_cols = stats.get("numeric_columns") or {}
    if num_cols:
        parts.append("═══ NUMERIC COLUMN SUMMARIES ═══")
        for col, cs in list(num_cols.items())[:10]:
            parts.append(
                f"  {col}: count={cs.get('count','?')}, mean={cs.get('mean','?')}, "
                f"min={cs.get('min','?')}, max={cs.get('max','?')}, std={cs.get('std','?')}"
            )

    # Categorical column summaries
    # Use top_5_values (primary key from stats pipeline) with top_values as fallback
    cat_cols = stats.get("categorical_columns") or {}
    if cat_cols:
        parts.append("═══ CATEGORICAL COLUMN SUMMARIES ═══")
        for col, cs in list(cat_cols.items())[:8]:
            top_vals = cs.get("top_5_values") or cs.get("top_values") or {}
            top_str  = ", ".join(f"{k}:{v}" for k, v in list(top_vals.items())[:5]) if top_vals else "?"
            # unique_values is the key used by stats pipeline; nunique is a fallback
            nunique  = cs.get("unique_values") or cs.get("nunique") or "?"
            parts.append(f"  {col}: {nunique} unique values. Top: {top_str}")

    # Key findings — try both key names used across pipeline versions
    findings = (
        (insights.get("key_findings") or [])
        or (insights.get("findings") or [])
    )[:6]
    if findings:
        parts.append("═══ KEY FINDINGS FROM ANALYSIS ═══")
        for f in findings:
            parts.append(f"  • {f}")

    return "\n".join(parts)


def _assemble_context(
    chunks:      list[dict],
    data_result: Optional[dict],
    static_ctx:  str = "",
) -> str:
    parts: list[str] = []

    if static_ctx:
        parts.append(static_ctx)
        parts.append("")

    if chunks:
        parts.append("═══ RELEVANT DATASET FACTS (retrieved for this question) ═══")
        for chunk in chunks:
            ctype = chunk.get("chunk_type", "fact").upper()
            parts.append(f"[{ctype}] {chunk['text']}")
        parts.append("")

    if data_result and "error" not in data_result:
        parts.append("═══ EXACT QUERY RESULT FROM FULL DATASET ═══")
        parts.append(
            "Use these exact values in your answer. "
            "These are computed from the real data, not estimates."
        )
        parts.append(json.dumps(data_result, ensure_ascii=True, default=str))
        parts.append("")

    return "\n".join(parts)[:_MAX_CONTEXT_CHARS]


# ─────────────────────────────────────────────────────────────────────────────
# Chart fact extractor
# ─────────────────────────────────────────────────────────────────────────────

def _extract_chart_facts(fig_data: Any, chart_key: str) -> str:
    """
    Pull every readable number from a Plotly chart JSON.
    This is what the LLM uses to explain charts without hallucinating.
    """
    try:
        fig = json.loads(fig_data) if isinstance(fig_data, str) else fig_data
        if not isinstance(fig, dict):
            return f"No data available for chart '{chart_key}'."

        layout    = fig.get("layout") or {}
        title_raw = layout.get("title", {})
        title     = (
            title_raw.get("text") if isinstance(title_raw, dict) else title_raw
        ) or chart_key

        facts = [f"Chart title: '{title}'."]

        for trace in (fig.get("data") or [])[:3]:
            ttype  = trace.get("type", "chart")
            x_vals = list(trace.get("x") or [])[:25]
            y_vals = list(trace.get("y") or [])[:25]
            labels = list(trace.get("labels") or [])[:25]
            values = list(trace.get("values") or [])[:25]

            if labels and values:
                pairs = [f"'{lbl}': {val}" for lbl, val in zip(labels[:12], values[:12])]
                facts.append(f"Data ({ttype}): {', '.join(pairs)}.")

            elif x_vals and y_vals:
                numeric_y = [
                    (i, float(v)) for i, v in enumerate(y_vals)
                    if isinstance(v, (int, float))
                ]
                if numeric_y:
                    max_i, max_v = max(numeric_y, key=lambda t: t[1])
                    min_i, min_v = min(numeric_y, key=lambda t: t[1])
                    avg_v = sum(v for _, v in numeric_y) / len(numeric_y)
                    x_at_max = x_vals[max_i] if max_i < len(x_vals) else "?"
                    x_at_min = x_vals[min_i] if min_i < len(x_vals) else "?"
                    facts.append(
                        f"Chart type: {ttype}. "
                        f"Highest value: '{x_at_max}' = {max_v:,.2f}. "
                        f"Lowest value: '{x_at_min}' = {min_v:,.2f}. "
                        f"Average: {avg_v:,.2f}. "
                        f"All X-axis labels: {x_vals[:20]}. "
                        f"All Y-axis values: {[round(v, 2) for _, v in numeric_y[:20]]}."
                    )

        return " ".join(facts)

    except Exception as exc:
        logger.warning("extract_chart_facts failed for '%s': %s", chart_key, exc)
        return f"Could not read data from chart '{chart_key}'."


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

async def answer_question(
    question:             str,
    file_hash:            str,
    file_name:            str,
    stats:                dict,
    insights:             dict,
    chart_keys:           list[str],
    conversation_history: list[dict],
    groq_client,
    redis_client,
) -> dict:
    """
    Answer a data question using RAG retrieval + Parquet query (in parallel).

    Returns: {"answer": str, "data_queried": bool, "new_chart": None}
    """
    from .indexer import retrieve_chunks

    # Build column type map for the query planner
    col_types: dict[str, str] = {}
    for c in (stats.get("numeric_columns") or {}).keys():
        col_types[c] = "numeric"
    for c in (stats.get("categorical_columns") or {}).keys():
        col_types[c] = "categorical"

    # Run Pinecone retrieval and Parquet query planner in parallel.
    # FIX: Skip the query planner entirely when col_types is empty — it means
    # no stats were passed in the context (e.g. first message, context omission).
    # An empty col_types causes the planner to emit invalid column names, wasting
    # a Groq API call and potentially poisoning the LLM context with noise.
    retrieval_coro = retrieve_chunks(
        file_hash=file_hash,
        question=question,
        k=_TOP_K_CHUNKS,
        redis_client=redis_client,
    )
    if col_types:
        query_coro = _plan_and_run_query(
            question=question,
            file_hash=file_hash,
            col_types=col_types,
            groq_client=groq_client,
        )
    else:
        logger.info("Skipping query planner: no column type info in stats context")
        async def _null_query() -> None:
            return None
        query_coro = _null_query()

    chunks, data_result = await asyncio.gather(retrieval_coro, query_coro)

    # Build static context from pre-computed stats/insights
    # This gives the LLM exact correlation values, column summaries, and key
    # findings so relationship and summary questions are grounded, not hallucinated.
    static_ctx = _build_static_context(stats, insights)
    context    = _assemble_context(chunks, data_result, static_ctx)

    # Assemble message list
    messages = [
        {"role": "system", "content": _data_system_prompt(file_name, chart_keys)},
        {"role": "system", "content": f"CONTEXT:\n{context}"},
    ]

    # Append conversation history (last 6 turns, capped per message).
    # FIX: Only allow "user" and "assistant" roles. A "system" role passed in
    # conversation history would inject an extra system prompt into the Groq
    # message list, allowing prompt injection via manipulated history payloads.
    _SAFE_ROLES = {"assistant", "ai", "user", "human"}
    for msg in (conversation_history or [])[-6:]:
        raw_role = str(msg.get("role", "")).lower()
        if raw_role not in _SAFE_ROLES:
            logger.warning("Skipping unsupported history role '%s' to prevent injection", raw_role)
            continue
        role = "assistant" if raw_role in ("assistant", "ai") else "user"
        messages.append({"role": role, "content": str(msg.get("content", ""))[:1000]})

    messages.append({"role": "user", "content": question})

    # Synthesize answer
    try:
        temp = 0.05 if data_result is not None else 0.1
        completion = groq_client.chat.completions.create(
            model=SYNTHESIS_MODEL,
            messages=messages,
            temperature=temp,
            max_tokens=450,  # bumped from 300 — analytical answers need more room
        )
        answer = (completion.choices[0].message.content or "").strip()
    except Exception as exc:
        logger.error("RAG synthesis failed: %s", exc)
        answer = (
            "I hit a temporary error generating your answer. "
            "Please try again in a moment."
        )

    return {
        "answer":       answer,
        "data_queried": data_result is not None,
        "new_chart":    None,
    }


async def answer_chart_explanation(
    question:             str,
    chart_key:            str,
    chart_data:           Any,
    file_name:            str,
    file_hash:            str,
    chart_keys:           list[str],
    conversation_history: list[dict],
    groq_client,
    redis_client,
) -> dict:
    """
    Explain a chart using extracted Plotly values + RAG column-stat context.

    Returns: {"answer": str, "data_queried": False, "new_chart": None}
    """
    from .indexer import retrieve_chunks

    # Extract real numbers from the Plotly chart JSON
    chart_facts = _extract_chart_facts(chart_data, chart_key)

    # Also pull the most relevant column-stat chunks for additional context
    chunks = await retrieve_chunks(
        file_hash=file_hash,
        question=question,
        k=5,
        redis_client=redis_client,
    )
    extra_context = "\n".join(c["text"] for c in chunks)[:2000]

    messages = [
        {"role": "system", "content": _chart_system_prompt(file_name)},
        {
            "role": "system",
            "content": (
                f"CHART FACTS (use these exact numbers — never invent values):\n"
                f"{chart_facts}\n\n"
                f"ADDITIONAL DATASET CONTEXT:\n{extra_context}"
            ),
        },
        {"role": "user", "content": question},
    ]

    try:
        completion = groq_client.chat.completions.create(
            model=SYNTHESIS_MODEL,
            messages=messages,
            temperature=0.05,
            max_tokens=220,
        )
        answer = (completion.choices[0].message.content or "").strip()
        # Always append the chart tag so the frontend renders the chart
        if chart_key and "[CHART:" not in answer:
            answer = f"{answer}\n[CHART: {chart_key}]"
    except Exception as exc:
        logger.error("Chart explanation synthesis failed: %s", exc)
        chart_tag = f"\n[CHART: {chart_key}]" if chart_key else ""
        answer    = f"Here is the chart from {file_name}.{chart_tag}"

    return {"answer": answer, "data_queried": False, "new_chart": None}
