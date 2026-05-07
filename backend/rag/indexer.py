   
from __future__ import annotations

import json
import logging
import math
from typing import Any, Optional

logger = logging.getLogger(__name__)

                                                             
_REDIS_CHUNKS_KEY = "rag:texts:{}"
_REDIS_TTL        = 259200          


                                                                               
                                  
                                                                               

def _f(v: Any, decimals: int = 2) -> str:
                                                                      
    try:
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return "N/A"
        if abs(x) >= 1_000_000_000:
            return f"{x / 1_000_000_000:.2f}B"
        if abs(x) >= 1_000_000:
            return f"{x / 1_000_000:.2f}M"
        if abs(x) >= 10_000:
            return f"{x:,.0f}"
        if abs(x) >= 100:
            return f"{x:,.{max(0, decimals - 1)}f}"
        return f"{x:.{decimals}f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return str(v) if v is not None else "N/A"


def _skew_to_english(skew: Any) -> str:
    try:
        s = float(skew)
        if math.isnan(s):
            return "shape unknown"
        if s > 2.0:
            return (
                "very strongly right-skewed — a small number of very high values "
                "pull the average up"
            )
        if s > 1.0:
            return "right-skewed — most values are low but some very high values exist"
        if s > 0.4:
            return "slightly right-skewed"
        if s < -2.0:
            return (
                "very strongly left-skewed — a small number of very low values "
                "pull the average down"
            )
        if s < -1.0:
            return "left-skewed — most values are high but some very low values exist"
        if s < -0.4:
            return "slightly left-skewed"
        return "roughly symmetric — most values cluster near the average"
    except (TypeError, ValueError):
        return "shape unknown"


                                                                               
                           
                                                                               

def _build_overview(stats: dict, insights: dict) -> dict:
    profile      = stats.get("dataset_profile") or {}
    quality      = stats.get("data_quality") or {}
    row_count    = stats.get("row_count", 0)
    col_count    = stats.get("column_count", 0)
    completeness = quality.get("completeness", 100)
    missing      = quality.get("missing_cells", 0)
    dupes        = quality.get("duplicate_rows", 0)
    label        = profile.get("label", "dataset")
    domain       = profile.get("domain", "general")
    description  = profile.get("description", "")
    headline     = insights.get("headline", "")

    num_cols = list((stats.get("numeric_columns") or {}).keys())
    cat_cols = list((stats.get("categorical_columns") or {}).keys())

    text = (
        f"This dataset is '{label}' in the {domain} domain. "
        f"{description} "
        f"It has {row_count:,} rows and {col_count} columns total. "
        f"Numeric columns ({len(num_cols)}): {', '.join(num_cols[:25])}. "
        f"Categorical columns ({len(cat_cols)}): {', '.join(cat_cols[:15])}. "
        f"Data completeness: {completeness:.1f}%. "
        f"Missing values: {missing:,} cells across the dataset. "
        f"Duplicate rows: {dupes}. "
    )
    if headline:
        text += f"Key finding: {headline}"

    return {
        "chunk_id":   "overview",
        "chunk_type": "overview",
        "column":     "all",
        "text":       text.strip(),
    }


def _build_data_quality(stats: dict) -> dict:
    quality      = stats.get("data_quality") or {}
    missing_pct  = stats.get("missing_percentage") or {}
    missing_vals = stats.get("missing_values") or {}
    completeness = quality.get("completeness", 100)
    missing      = quality.get("missing_cells", 0)
    dupes        = quality.get("duplicate_rows", 0)
    total        = quality.get("total_cells", 0)

    worst = sorted(missing_pct.items(), key=lambda x: x[1], reverse=True)[:6]
    worst_str = "; ".join(
        f"'{col}' has {pct:.1f}% missing ({missing_vals.get(col, '?')} rows)"
        for col, pct in worst
    )

    text = (
        f"Data quality: {completeness:.1f}% complete. "
        f"Total cells: {total:,}. "
        f"Missing cells: {missing:,}. "
        f"Duplicate rows: {dupes}. "
    )
    if worst_str:
        text += f"Columns with missing data: {worst_str}."

    return {
        "chunk_id":   "data_quality",
        "chunk_type": "data_quality",
        "column":     "all",
        "text":       text.strip(),
    }


def _build_column_stat(col: str, d: dict, missing_pct: float, row_count: int) -> dict:
    null_rows = round(missing_pct / 100 * max(row_count, 1))
    text = (
        f"Column '{col}' numeric statistics: "
        f"average = {_f(d.get('mean'))}, "
        f"median = {_f(d.get('median'))}, "
        f"standard deviation = {_f(d.get('std'))}, "
        f"minimum = {_f(d.get('min'))}, "
        f"maximum = {_f(d.get('max'))}, "
        f"25th percentile = {_f(d.get('q1'))}, "
        f"75th percentile = {_f(d.get('q3'))}, "
        f"IQR = {_f(d.get('iqr'))}. "
        f"Non-null count: {d.get('count', 'N/A')}. "
        f"Missing: {null_rows} rows ({missing_pct:.1f}%)."
    )
    return {
        "chunk_id":   f"col_stat_{col}",
        "chunk_type": "column_stat",
        "column":     col,
        "text":       text.strip(),
    }


def _build_distribution(col: str, d: dict) -> dict:
    shape = _skew_to_english(d.get("skewness"))
    text = (
        f"Distribution of '{col}': {shape}. "
        f"Values range from {_f(d.get('min'))} to {_f(d.get('max'))}. "
        f"Average is {_f(d.get('mean'))}, median is {_f(d.get('median'))}. "
        f"The middle 50% of values fall between "
        f"{_f(d.get('q1'))} and {_f(d.get('q3'))}. "
        f"Skewness = {_f(d.get('skewness'), 3)}, "
        f"kurtosis = {_f(d.get('kurtosis'), 3)}."
    )
    return {
        "chunk_id":   f"dist_{col}",
        "chunk_type": "distribution",
        "column":     col,
        "text":       text.strip(),
    }


def _build_categorical(col: str, d: dict, row_count: int) -> dict:
    rc      = max(row_count, 1)
    top5    = d.get("top_5_values") or {}
    top_str = ", ".join(
        f"'{k}' = {v} times ({v / rc * 100:.1f}%)"
        for k, v in list(top5.items())[:5]
    )
    most_common  = d.get("most_common", "N/A")
    most_count   = d.get("most_common_count", 0)
    most_pct     = most_count / rc * 100
    least_common = d.get("least_common", "N/A")
    least_count  = d.get("least_common_count", 0)
    diversity    = d.get("diversity_ratio", 0)

    text = (
        f"Categorical column '{col}': "
        f"{d.get('unique_values', 0)} unique values across {row_count:,} rows "
        f"(diversity ratio = {diversity:.3f}). "
        f"Most common: '{most_common}' appears {most_count:,} times "
        f"= {most_pct:.1f}% of all rows. "
        f"Least common: '{least_common}' appears {least_count} times. "
        f"Top 5 values: {top_str}."
    )
    return {
        "chunk_id":   f"cat_{col}",
        "chunk_type": "categorical_freq",
        "column":     col,
        "text":       text.strip(),
    }


def _build_correlation(c1: str, c2: str, r: float) -> dict:
    r_abs     = abs(r)
    direction = "positive" if r > 0 else "negative"
    if r_abs >= 0.9:
        strength = "very strong"
    elif r_abs >= 0.7:
        strength = "strong"
    elif r_abs >= 0.5:
        strength = "moderate"
    else:
        strength = "weak"

    trend = (
        f"When '{c1}' increases, '{c2}' tends to "
        f"{'increase' if r > 0 else 'decrease'} as well."
    )
    text = (
        f"Correlation between '{c1}' and '{c2}': r = {r:.4f}. "
        f"This is a {strength} {direction} linear relationship. "
        f"{trend}"
    )
    return {
        "chunk_id":   f"corr_{c1}_{c2}",
        "chunk_type": "correlation",
        "column":     f"{c1},{c2}",
        "text":       text.strip(),
    }


def _build_outlier(col: str, d: dict, row_count: int) -> dict:
    count = d.get("count", 0)
    pct   = d.get("percentage", 0)
    lo    = d.get("lower_bound")
    hi    = d.get("upper_bound")
    text = (
        f"Outliers in '{col}': {count} rows ({pct:.2f}% of {row_count:,} total rows) "
        f"have values outside the normal range of {_f(lo)} to {_f(hi)}. "
        f"These extreme values may represent data errors, rare events, or genuinely "
        f"exceptional cases worth investigating."
    )
    return {
        "chunk_id":   f"outlier_{col}",
        "chunk_type": "outlier",
        "column":     col,
        "text":       text.strip(),
    }


def _build_finding(idx: int, finding: str) -> dict:
    return {
        "chunk_id":   f"finding_{idx}",
        "chunk_type": "insight_finding",
        "column":     "all",
        "text":       f"Key finding #{idx + 1}: {finding}",
    }


def _build_recommendation(idx: int, rec: str) -> dict:
    return {
        "chunk_id":   f"rec_{idx}",
        "chunk_type": "recommendation",
        "column":     "all",
        "text":       f"Actionable recommendation: {rec}",
    }


def _build_chart_chunk(key: str, fig_data: Any) -> Optional[dict]:
       
    try:
        fig = json.loads(fig_data) if isinstance(fig_data, str) else fig_data
        if not isinstance(fig, dict):
            return None

        layout    = fig.get("layout") or {}
        title_raw = layout.get("title", {})
        title     = (
            title_raw.get("text") if isinstance(title_raw, dict) else title_raw
        ) or key

        facts    = [f"Chart '{title}' (key={key}):"]
        has_data = False

        for trace in (fig.get("data") or [])[:3]:
            ttype  = trace.get("type", "chart")
            x_vals = list(trace.get("x") or [])[:25]
            y_vals = list(trace.get("y") or [])[:25]
            labels = list(trace.get("labels") or [])[:25]
            values = list(trace.get("values") or [])[:25]

            if labels and values:
                has_data = True
                pairs = [
                    f"'{lbl}': {_f(val)}"
                    for lbl, val in zip(labels[:12], values[:12])
                ]
                facts.append(f"Type={ttype}. Data: {', '.join(pairs)}.")

            elif x_vals and y_vals:
                has_data   = True
                numeric_y  = [
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
                        f"Type={ttype}. "
                        f"Highest: '{x_at_max}' = {_f(max_v)}. "
                        f"Lowest: '{x_at_min}' = {_f(min_v)}. "
                        f"Average: {_f(avg_v)}. "
                        f"All X: {x_vals[:20]}. "
                        f"All Y: {[_f(v) for _, v in numeric_y[:20]]}."
                    )

        if not has_data:
            return None

        return {
            "chunk_id":   f"chart_{key}",
            "chunk_type": "chart_summary",
            "column":     "all",
            "text":       " ".join(facts).strip(),
        }
    except Exception as exc:
        logger.warning("Chart chunk build failed for '%s': %s", key, exc)
        return None


                                                                               
                       
                                                                               

async def build_rag_index(
    file_hash:    str,
    stats:        dict,
    insights:     dict,
    charts:       dict,
    redis_client,
) -> int:
       
    import asyncio
    from .pinecone_client import embed_texts, upsert_chunks, delete_namespace

                                                                            
    raw_chunks: list[dict] = []

    raw_chunks.append(_build_overview(stats, insights))
    raw_chunks.append(_build_data_quality(stats))

    row_count       = max(stats.get("row_count", 1), 1)
    missing_pct_map = stats.get("missing_percentage") or {}

                                                        
    for col, d in (stats.get("numeric_columns") or {}).items():
        pct = missing_pct_map.get(col, 0.0)
        raw_chunks.append(_build_column_stat(col, d, pct, row_count))
        raw_chunks.append(_build_distribution(col, d))

                         
    for col, d in (stats.get("categorical_columns") or {}).items():
        raw_chunks.append(_build_categorical(col, d, row_count))

                                   
    for corr in (stats.get("strong_correlations") or []):
        c1 = corr.get("col1", "")
        c2 = corr.get("col2", "")
        r  = corr.get("correlation", 0.0)
        if c1 and c2 and abs(r) > 0.4:
            raw_chunks.append(_build_correlation(c1, c2, r))

              
    for col, d in (stats.get("outliers") or {}).items():
        if d.get("count", 0) > 0:
            raw_chunks.append(_build_outlier(col, d, row_count))

                                                 
    for i, finding in enumerate(insights.get("findings") or []):
        if finding and isinstance(finding, str):
            raw_chunks.append(_build_finding(i, finding))

                                
    for i, rec in enumerate(insights.get("recommendations") or []):
        if rec and isinstance(rec, str):
            raw_chunks.append(_build_recommendation(i, rec))

                                                    
    for key, fig_data in (charts or {}).items():
        chunk = _build_chart_chunk(key, fig_data)
        if chunk:
            raw_chunks.append(chunk)

    if not raw_chunks:
        logger.warning("RAG indexer: 0 chunks built for %s", file_hash[:8])
        return 0

                                                                            
    texts      = [c["text"] for c in raw_chunks]
    embeddings = await asyncio.to_thread(embed_texts, texts)

    if embeddings is None:
        logger.error("RAG indexer: Pinecone embedding failed for %s", file_hash[:8])
        return 0

    for i, chunk in enumerate(raw_chunks):
        chunk["embedding"] = embeddings[i] if i < len(embeddings) else None

                                                                             
    await asyncio.to_thread(delete_namespace, file_hash)
    count = await asyncio.to_thread(upsert_chunks, file_hash, raw_chunks)

                                                                            
    try:
        texts_backup = [
            {
                "chunk_id":   c["chunk_id"],
                "chunk_type": c["chunk_type"],
                "column":     c["column"],
                "text":       c["text"],
            }
            for c in raw_chunks
        ]
        payload = json.dumps(texts_backup, ensure_ascii=True)
        await redis_client.setex(
            _REDIS_CHUNKS_KEY.format(file_hash),
            _REDIS_TTL,
            payload,
        )
        logger.info(
            "RAG index complete: %d chunks, namespace=%s, redis_backup=ok",
            count,
            file_hash[:8],
        )
    except Exception as exc:
        logger.warning("RAG Redis backup failed (non-fatal): %s", exc)

    return count


async def retrieve_chunks(
    file_hash:    str,
    question:     str,
    k:            int = 8,
    redis_client  = None,
) -> list[dict]:
       
    import asyncio
    from .pinecone_client import embed_query, query_chunks

    if not file_hash:
        return []

                                       
    q_vec = await asyncio.to_thread(embed_query, question)
    if q_vec is not None:
        results = await asyncio.to_thread(query_chunks, file_hash, q_vec, k)
        if results:
            logger.info(
                "RAG retrieved %d chunks via Pinecone (top score: %.3f)",
                len(results),
                results[0].get("score", 0),
            )
            return results

                                    
    logger.warning("RAG: Pinecone unavailable, falling back to Redis keyword search")
    if redis_client is None:
        return []

    try:
        raw = await redis_client.get(_REDIS_CHUNKS_KEY.format(file_hash))
        if not raw:
            return []
        all_chunks = json.loads(raw)
        q_words    = set(question.lower().split())
        scored: list[tuple[int, dict]] = []
        for chunk in all_chunks:
            words   = set(chunk["text"].lower().split())
            overlap = len(q_words & words)
            scored.append((overlap, chunk))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [c for _, c in scored[:k]]
    except Exception as exc:
        logger.error("RAG Redis fallback failed: %s", exc)
        return []
