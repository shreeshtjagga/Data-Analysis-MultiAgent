import logging
import pandas as pd
import os
from typing import Any, Dict

logger = logging.getLogger(__name__)

# Absolute path: backend/storage/data/ relative to this file's location
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))       # .../backend/core/
_BACKEND_DIR = os.path.dirname(_THIS_DIR)                    # .../backend/
STORAGE_DIR = os.path.join(_BACKEND_DIR, "storage", "data")


def _load_df(file_hash: str):
    """Load the full Parquet dataset from disk."""
    path = os.path.join(STORAGE_DIR, f"{file_hash}.parquet")
    if not os.path.exists(path):
        return None, {"error": f"Full dataset not cached yet for this session. Please re-upload the file."}
    return pd.read_parquet(path), None


def _filter_df(df: pd.DataFrame, filters: list) -> pd.DataFrame:
    """
    Apply a list of filter conditions to a DataFrame.
    Each filter is: {"column": "col", "op": "eq|contains|gt|lt|gte|lte|year|month", "value": ...}
    """
    for f in (filters or []):
        col = f.get("column")
        op = f.get("op", "eq")
        val = f.get("value")
        if col not in df.columns:
            logger.warning("Filter column '%s' not found, skipping.", col)
            continue
        try:
            if op == "eq":
                df = df[df[col].astype(str).str.lower() == str(val).lower()]
            elif op == "contains":
                df = df[df[col].astype(str).str.lower().str.contains(str(val).lower(), na=False)]
            elif op == "gt":
                df = df[pd.to_numeric(df[col], errors="coerce") > float(val)]
            elif op == "lt":
                df = df[pd.to_numeric(df[col], errors="coerce") < float(val)]
            elif op == "gte":
                df = df[pd.to_numeric(df[col], errors="coerce") >= float(val)]
            elif op == "lte":
                df = df[pd.to_numeric(df[col], errors="coerce") <= float(val)]
            elif op == "year":
                df = df[pd.to_datetime(df[col], errors="coerce").dt.year == int(val)]
            elif op == "month":
                df = df[pd.to_datetime(df[col], errors="coerce").dt.month == int(val)]
        except Exception as e:
            logger.warning("Filter op '%s' on column '%s' failed: %s", op, col, e)
    return df


def run_data_query(file_hash: str, query_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a targeted analytical query on the full Parquet dataset.

    Supported query types:
    - filter_lookup        : look up a column value by filtering another column ("what is X for Y")
    - value_counts         : count occurrences of each category in a column (with optional filters)
    - filter_aggregate     : filter rows then aggregate a column (sum/mean/count/max/min)
    - group_aggregate      : group by a column, aggregate another
    - filter_group         : filter rows then group-by and aggregate (THE MAIN ONE for "X in year Y")
    - top_n                : return top N rows sorted by a column (with optional filters)
    - lookup               : get value at a specific row index
    - aggregate            : single aggregate on a column (no grouping)
    - row_count            : count rows matching a filter
    """
    df, err = _load_df(file_hash)
    if err:
        return err

    try:
        filters = params.get("filters", [])
        col = params.get("column")
        group_col = params.get("group_by") or params.get("groupby")
        func = params.get("func", "count")
        n = int(params.get("n", 10))

        # ── filter_lookup ("what is country_id for India?") ───────────────
        if query_type == "filter_lookup":
            filter_col = params.get("filter_col")
            filter_val = params.get("filter_val")
            result_col = params.get("result_col")

            # Fuzzy column matching — handle case differences and slight name mismatches
            def _resolve_col(name):
                if not name:
                    return None
                if name in df.columns:
                    return name
                # Case-insensitive exact match
                lower_map = {c.lower().replace("_", "").replace(" ", ""): c for c in df.columns}
                key = name.lower().replace("_", "").replace(" ", "")
                if key in lower_map:
                    return lower_map[key]
                # Partial substring match
                for c in df.columns:
                    if key in c.lower().replace("_", "") or c.lower().replace("_", "") in key:
                        return c
                return None

            resolved_filter = _resolve_col(filter_col)
            resolved_result = _resolve_col(result_col)

            if not resolved_filter:
                return {"error": f"Filter column '{filter_col}' not found. Available: {list(df.columns)}"}
            if not resolved_result:
                return {"error": f"Result column '{result_col}' not found. Available: {list(df.columns)}"}

            mask = df[resolved_filter].astype(str).str.lower().str.contains(
                str(filter_val).lower(), na=False
            )
            filtered = df[mask]
            if filtered.empty:
                return {"result": f"No rows found where '{resolved_filter}' contains '{filter_val}'."}

            unique_vals = filtered[resolved_result].dropna().unique()[:10].tolist()
            return {
                "query": f"{resolved_result} where {resolved_filter} = '{filter_val}'",
                "result": [str(v) for v in unique_vals],
                "match_count": int(mask.sum()),
            }

        # ── value_counts ────────────────────────────────────────────────────
        elif query_type == "value_counts":
            df = _filter_df(df, filters)
            if col not in df.columns:
                return {"error": f"Column '{col}' not found. Available: {list(df.columns)}"}
            result = df[col].value_counts().head(n).to_dict()
            return {"query": f"Value counts of '{col}' (top {n})", "result": result}

        # ── filter_group (MAIN: "which brand sold most in 2021?") ──────────
        elif query_type in ("filter_group", "filter_aggregate"):
            df = _filter_df(df, filters)
            if df.empty:
                return {"result": "No rows matched the filter conditions."}
            if not group_col:
                return {"error": "Missing 'group_by' column for filter_group query."}
            if group_col not in df.columns:
                return {"error": f"Group-by column '{group_col}' not found. Available: {list(df.columns)}"}

            if func == "count":
                result = df.groupby(group_col).size().sort_values(ascending=False).head(n).to_dict()
            elif col and col in df.columns:
                result = df.groupby(group_col)[col].agg(func).sort_values(ascending=False).head(n).to_dict()
            else:
                result = df.groupby(group_col).size().sort_values(ascending=False).head(n).to_dict()

            label = f"Group '{group_col}'"
            if filters:
                cond_strs = [f"{f['column']} {f['op']} {f['value']}" for f in filters]
                label += f" where [{', '.join(cond_strs)}]"
            return {"query": label, "func": func, "result": result}

        # ── group_aggregate (no filter, just group + agg) ──────────────────
        elif query_type == "group_aggregate":
            if not group_col or group_col not in df.columns:
                return {"error": f"group_by column '{group_col}' not found. Available: {list(df.columns)}"}
            if func == "count":
                result = df.groupby(group_col).size().sort_values(ascending=False).head(n).to_dict()
            elif col and col in df.columns:
                result = df.groupby(group_col)[col].agg(func).sort_values(ascending=False).head(n).to_dict()
            else:
                result = df.groupby(group_col).size().sort_values(ascending=False).head(n).to_dict()
            return {"query": f"Group '{group_col}' → {func}", "result": result}

        # ── aggregate (single column, no grouping) ─────────────────────────
        elif query_type == "aggregate":
            df = _filter_df(df, filters)
            if not col or col not in df.columns:
                return {"error": f"Column '{col}' not found. Available: {list(df.columns)}"}
            numeric = pd.to_numeric(df[col], errors="coerce")
            result = getattr(numeric, func)() if hasattr(numeric, func) else numeric.mean()
            return {"query": f"{func}('{col}')", "result": float(result) if pd.notna(result) else "N/A"}

        # ── top_n ──────────────────────────────────────────────────────────
        elif query_type == "top_n":
            df = _filter_df(df, filters)
            if not col or col not in df.columns:
                return {"error": f"Column '{col}' not found. Available: {list(df.columns)}"}
            ascending = params.get("ascending", False)
            result = df.sort_values(col, ascending=ascending).head(n).to_dict(orient="records")
            return {"query": f"Top {n} by '{col}'", "result": result}

        # ── lookup (specific row) ──────────────────────────────────────────
        elif query_type == "lookup":
            idx = params.get("row_index", 0)
            if not (0 <= idx < len(df)):
                return {"error": f"Row index {idx} is out of bounds (dataset has {len(df)} rows)."}
            if col:
                if col not in df.columns:
                    return {"error": f"Column '{col}' not found."}
                return {"query": f"Row {idx}, column '{col}'", "result": str(df.iloc[idx][col])}
            return {"query": f"Row {idx}", "result": df.iloc[idx].to_dict()}

        # ── row_count ──────────────────────────────────────────────────────
        elif query_type == "row_count":
            df = _filter_df(df, filters)
            return {"query": "Row count after filtering", "result": len(df)}

        return {"error": f"Unknown query type: '{query_type}'. Supported: value_counts, filter_group, group_aggregate, aggregate, top_n, lookup, row_count"}

    except Exception as exc:
        logger.error("Data query failed (type=%s): %s", query_type, exc, exc_info=True)
        return {"error": str(exc)}
