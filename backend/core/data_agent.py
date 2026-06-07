import logging
import os
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Optional
from .constants import PARQUET_STORAGE_DIR, ENABLE_DISK_CACHE
logger = logging.getLogger(__name__)

def _load_df(file_hash: str):
    import re as _re
    if not file_hash or not _re.match('^[a-f0-9]{64}$', file_hash):
        return (None, {'error': 'Invalid file hash format.'})
    if not ENABLE_DISK_CACHE:
        return (None, {'error': 'disk_cache_disabled'})
    path = os.path.join(PARQUET_STORAGE_DIR, f'{file_hash}.parquet')
    if not os.path.exists(path):
        return (None, {'error': 'disk_cache_miss'})
    try:
        return (pd.read_parquet(path), None)
    except Exception as exc:
        logger.warning('Failed to read Parquet for hash %s: %s', file_hash[:8], exc)
        return (None, {'error': 'disk_cache_miss'})


def load_df_from_records(records: List[dict]) -> Optional[pd.DataFrame]:
    """Build a DataFrame from JSON records stored in the DB clean_data column.
    Used as a production fallback when the Parquet disk cache is unavailable."""
    if not records:
        return None
    try:
        return pd.DataFrame(records)
    except Exception as exc:
        logger.warning('load_df_from_records failed: %s', exc)
        return None

def _resolve_column(name: str, df: pd.DataFrame):
    if not name:
        return None
    if name in df.columns:
        return name
    norm = lambda s: s.lower().replace('_', '').replace(' ', '').replace('-', '')
    normed_name = norm(name)
    for c in df.columns:
        if norm(c) == normed_name:
            return c
    for c in df.columns:
        nc = norm(c)
        if normed_name in nc or nc in normed_name:
            return c
    name_words = set(name.lower().replace('_', ' ').replace('-', ' ').split())
    (best_col, best_score) = (None, 0)
    for c in df.columns:
        col_words = set(c.lower().replace('_', ' ').replace('-', ' ').split())
        overlap = len(name_words & col_words)
        if overlap > best_score:
            (best_score, best_col) = (overlap, c)
    if best_score > 0:
        return best_col
    return None

def _filter_df(df: pd.DataFrame, filters: list) -> pd.DataFrame:
    for f in filters or []:
        col = f.get('column')
        op = f.get('op', 'eq')
        val = f.get('value')
        resolved = _resolve_column(col, df)
        if not resolved:
            logger.warning("Filter column '%s' not found in %s, skipping.", col, list(df.columns))
            continue
        col = resolved
        try:
            if op == 'eq':
                candidate = df[df[col].astype(str).str.lower() == str(val).lower()]
            elif op == 'neq':
                candidate = df[df[col].astype(str).str.lower() != str(val).lower()]
            elif op == 'contains':
                candidate = df[df[col].astype(str).str.lower().str.contains(str(val).lower(), na=False)]
            elif op == 'gt':
                candidate = df[pd.to_numeric(df[col], errors='coerce') > float(val)]
            elif op == 'lt':
                candidate = df[pd.to_numeric(df[col], errors='coerce') < float(val)]
            elif op == 'gte':
                candidate = df[pd.to_numeric(df[col], errors='coerce') >= float(val)]
            elif op == 'lte':
                candidate = df[pd.to_numeric(df[col], errors='coerce') <= float(val)]
            elif op == 'year':
                candidate = df[pd.to_datetime(df[col], errors='coerce').dt.year == int(val)]
            elif op == 'month':
                candidate = df[pd.to_datetime(df[col], errors='coerce').dt.month == int(val)]
            elif op == 'isnull':
                candidate = df[df[col].isnull()]
            elif op == 'notnull':
                candidate = df[df[col].notnull()]
            else:
                logger.warning("Unknown filter op '%s', skipping.", op)
                continue
            df = candidate
        except Exception as e:
            logger.warning("Filter op '%s' on column '%s' failed: %s — skipping this filter", op, col, e)
    return df

def _format_top_n_result(rows: list, sort_col: str, label_candidates: list, n: int) -> Dict[str, Any]:
    if not rows:
        return {'result': 'No rows found.'}
    label_col = None
    for hint in label_candidates:
        for col in rows[0].keys():
            if hint.lower() in col.lower():
                label_col = col
                break
        if label_col:
            break
    if not label_col:
        for (col, val) in rows[0].items():
            if isinstance(val, str) and col.lower() != sort_col.lower():
                label_col = col
                break
    if not label_col:
        label_col = list(rows[0].keys())[0] if rows[0] else None
    if label_col and sort_col:
        ranked = {str(row.get(label_col, f'Row {i + 1}')): row.get(sort_col) for (i, row) in enumerate(rows[:n])}
        return {'sort_column': sort_col, 'label_column': label_col, 'ranked_results': ranked, 'top_entry': {'entity': str(rows[0].get(label_col, 'Unknown')), 'value': rows[0].get(sort_col)}}
    return {'result': rows[:n]}

def run_data_query(file_hash: str, query_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
    (df, err) = _load_df(file_hash)
    if err:
        return err
    try:
        filters = params.get('filters', [])
        col_raw = params.get('column')
        group_col_raw = params.get('group_by') or params.get('groupby')
        func = params.get('func', 'count')
        n = int(params.get('n', 10))
        col = _resolve_column(col_raw, df) if col_raw else None
        group_col = _resolve_column(group_col_raw, df) if group_col_raw else None
        if query_type == 'filter_lookup':
            filter_col_raw = params.get('filter_col')
            filter_val = params.get('filter_val')
            result_col_raw = params.get('result_col')
            filter_col = _resolve_column(filter_col_raw, df)
            result_col = _resolve_column(result_col_raw, df)
            if not filter_col:
                return {'error': f"Filter column '{filter_col_raw}' not found. Available: {list(df.columns)}"}
            if not result_col:
                return {'error': f"Result column '{result_col_raw}' not found. Available: {list(df.columns)}"}
            mask = df[filter_col].astype(str).str.lower().str.contains(str(filter_val).lower(), na=False)
            filtered = df[mask]
            if filtered.empty:
                return {'result': f"No rows found where '{filter_col}' contains '{filter_val}'."}
            unique_vals = filtered[result_col].dropna().unique()[:10].tolist()
            return {'query': f"{result_col} where {filter_col} = '{filter_val}'", 'result': [str(v) for v in unique_vals], 'match_count': int(mask.sum())}
        elif query_type == 'value_counts':
            df = _filter_df(df, filters)
            if not col:
                return {'error': f"Column '{col_raw}' not found. Available: {list(df.columns)}"}
            result = df[col].value_counts().head(n).to_dict()
            return {'query': f"Value counts of '{col}' (top {n})", 'result': result}
        elif query_type in ('filter_group', 'filter_aggregate'):
            df = _filter_df(df, filters)
            if df.empty:
                return {'result': 'No rows matched the filter conditions.'}
            if not group_col:
                return {'error': f"Missing or invalid 'group_by' column '{group_col_raw}'. Available: {list(df.columns)}"}
            if func == 'count':
                result = df.groupby(group_col).size().sort_values(ascending=False).head(n).to_dict()
            elif col and col in df.columns:
                result = df.groupby(group_col)[col].agg(func).sort_values(ascending=False).head(n).to_dict()
            else:
                result = df.groupby(group_col).size().sort_values(ascending=False).head(n).to_dict()
            label = f"Group '{group_col}'"
            if filters:
                cond_strs = [f"{f['column']} {f['op']} {f['value']}" for f in filters]
                label += f" where [{', '.join(cond_strs)}]"
            return {'query': label, 'func': func, 'result': result}
        elif query_type == 'group_aggregate':
            if not group_col:
                return {'error': f"group_by column '{group_col_raw}' not found. Available: {list(df.columns)}"}
            df = _filter_df(df, filters)
            if func == 'count':
                result = df.groupby(group_col).size().sort_values(ascending=False).head(n).to_dict()
            elif col and col in df.columns:
                result = df.groupby(group_col)[col].agg(func).sort_values(ascending=False).head(n).to_dict()
            else:
                result = df.groupby(group_col).size().sort_values(ascending=False).head(n).to_dict()
            return {'query': f"Group '{group_col}' → {func}('{col}')", 'result': result}
        elif query_type == 'aggregate':
            df = _filter_df(df, filters)
            if not col:
                return {'error': f"Column '{col_raw}' not found. Available: {list(df.columns)}"}
            numeric = pd.to_numeric(df[col], errors='coerce')
            if func in ('mean', 'sum', 'min', 'max', 'median', 'std', 'var', 'count'):
                result = getattr(numeric, func)()
            elif func == 'nunique':
                result = df[col].nunique()
            else:
                result = numeric.mean()
            return {'query': f"{func}('{col}')", 'result': float(result) if pd.notna(result) else 'N/A'}
        elif query_type == 'top_n':
            df = _filter_df(df, filters)
            if not col:
                return {'error': f"Sort column '{col_raw}' not found. Available: {list(df.columns)}"}
            numeric_col = pd.to_numeric(df[col], errors='coerce')
            df = df.copy()
            df['__sort__'] = numeric_col
            sorted_df = df.dropna(subset=['__sort__']).sort_values('__sort__', ascending=False).drop(columns=['__sort__'])
            rows = sorted_df.head(n).to_dict(orient='records')
            label_hints = params.get('label_columns', ['name', 'model', 'title', 'brand', 'product', 'item', 'id'])
            result = _format_top_n_result(rows, col, label_hints, n)
            result['query'] = f"Top {n} rows by '{col}' (highest first)"
            return result
        elif query_type == 'bottom_n':
            df = _filter_df(df, filters)
            if not col:
                return {'error': f"Sort column '{col_raw}' not found. Available: {list(df.columns)}"}
            numeric_col = pd.to_numeric(df[col], errors='coerce')
            df = df.copy()
            df['__sort__'] = numeric_col
            sorted_df = df.dropna(subset=['__sort__']).sort_values('__sort__', ascending=True).drop(columns=['__sort__'])
            rows = sorted_df.head(n).to_dict(orient='records')
            label_hints = params.get('label_columns', ['name', 'model', 'title', 'brand', 'product', 'item', 'id'])
            result = _format_top_n_result(rows, col, label_hints, n)
            result['query'] = f"Bottom {n} rows by '{col}' (lowest first)"
            return result
        elif query_type == 'lookup':
            idx = params.get('row_index', 0)
            if not 0 <= idx < len(df):
                return {'error': f'Row index {idx} is out of bounds (dataset has {len(df)} rows).'}
            if col:
                return {'query': f"Row {idx}, column '{col}'", 'result': str(df.iloc[idx][col])}
            return {'query': f'Row {idx}', 'result': df.iloc[idx].to_dict()}
        elif query_type == 'row_count':
            df = _filter_df(df, filters)
            return {'query': 'Row count after filtering', 'result': len(df)}
        elif query_type == 'distinct':
            df = _filter_df(df, filters)
            if not col:
                return {'error': f"Column '{col_raw}' not found. Available: {list(df.columns)}"}
            unique_vals = df[col].dropna().unique()
            sorted_vals = sorted([str(v) for v in unique_vals])
            return {'query': f"Distinct values of '{col}'", 'count': len(sorted_vals), 'result': sorted_vals[:50], 'note': f'Showing first 50 of {len(sorted_vals)} unique values' if len(sorted_vals) > 50 else None}
        elif query_type == 'search':
            search_val = params.get('value') or params.get('query', '')
            target_col_raw = params.get('search_col')
            target_col = _resolve_column(target_col_raw, df) if target_col_raw else None
            if target_col:
                mask = df[target_col].astype(str).str.lower().str.contains(str(search_val).lower(), na=False)
                result_df = df[mask]
            else:
                str_cols = [c for c in df.columns if df[c].dtype == object]
                if not str_cols:
                    str_cols = list(df.columns)
                mask = pd.Series([False] * len(df), index=df.index)
                for c in str_cols:
                    mask = mask | df[c].astype(str).str.lower().str.contains(str(search_val).lower(), na=False)
                result_df = df[mask]
            rows = result_df.head(n).to_dict(orient='records')
            return {'query': f"Search '{search_val}'" + (f" in column '{target_col}'" if target_col else ' across all text columns'), 'match_count': len(result_df), 'result': rows}
        elif query_type == 'correlation':
            col2_raw = params.get('column2') or params.get('col2')
            col2 = _resolve_column(col2_raw, df) if col2_raw else None
            if not col or not col2:
                return {'error': f"Need two valid columns. Got '{col_raw}' and '{col2_raw}'. Available: {list(df.columns)}"}
            c1 = pd.to_numeric(df[col], errors='coerce')
            c2 = pd.to_numeric(df[col2], errors='coerce')
            corr = c1.corr(c2)
            return {'query': f"Correlation between '{col}' and '{col2}'", 'result': round(float(corr), 4) if pd.notna(corr) else 'N/A', 'interpretation': ('strong positive' if corr > 0.7 else 'moderate positive' if corr > 0.4 else 'weak positive' if corr > 0.1 else 'strong negative' if corr < -0.7 else 'moderate negative' if corr < -0.4 else 'weak negative' if corr < -0.1 else 'negligible') if pd.notna(corr) else 'N/A'}
        elif query_type == 'percentile':
            df = _filter_df(df, filters)
            if not col:
                return {'error': f"Column '{col_raw}' not found. Available: {list(df.columns)}"}
            pct = float(params.get('percentile', 50)) / 100.0
            numeric = pd.to_numeric(df[col], errors='coerce').dropna()
            result = numeric.quantile(pct)
            return {'query': f"{int(pct * 100)}th percentile of '{col}'", 'result': float(result) if pd.notna(result) else 'N/A'}
        elif query_type == 'trend':
            time_col_raw = params.get('time_col')
            val_col_raw = params.get('val_col')
            time_col = _resolve_column(time_col_raw, df)
            val_col = _resolve_column(val_col_raw, df)
            if not time_col or not val_col:
                return {'error': f"Missing or invalid columns: '{time_col_raw}', '{val_col_raw}'"}
            df_trend = df[[time_col, val_col]].dropna()
            if len(df_trend) < 2:
                return {'error': 'Not enough valid data points to compute trend.'}
            if df_trend[time_col].dtype == 'object':
                try:
                    df_trend[time_col] = pd.to_numeric(df_trend[time_col].str.extract('(\\d{4})')[0])
                except:
                    return {'error': f"Could not parse '{time_col}' into numeric time values."}
            df_trend = df_trend.dropna()
            if len(df_trend) < 2:
                return {'error': 'Not enough valid numeric time points.'}
            x = df_trend[time_col].values
            y = df_trend[val_col].values
            try:
                (slope, intercept) = np.polyfit(x, y, 1)
                correlation_matrix = np.corrcoef(x, y)
                r_squared = correlation_matrix[0, 1] ** 2
                direction = 'increasing' if slope > 0 else 'decreasing'
                last_year = float(x.max())
                last_value = float(y[x.argmax()])
                return {'query': f"Linear trend of '{val_col}' over '{time_col}'", 'result': f'Trend is {direction}. Slope: {slope:.4f} per year. R-squared: {r_squared:.4f}. Last data point: {val_col}={last_value:.2f} at {time_col}={last_year:.0f}. Intercept: {intercept:.4f}.', 'slope': float(slope), 'intercept': float(intercept), 'r_squared': float(r_squared), 'last_year': last_year, 'last_value': last_value}
            except Exception as e:
                return {'error': f'Trend calculation failed: {str(e)}'}
        elif query_type == 'year_summary':
            time_col_raw = params.get('time_col')
            val_col_raw = params.get('val_col')
            time_col = _resolve_column(time_col_raw, df)
            val_col = _resolve_column(val_col_raw, df)
            if not time_col or not val_col:
                return {'error': f"Missing or invalid columns: '{time_col_raw}', '{val_col_raw}'"}
            df_time = df[[time_col, val_col]].copy()
            if df_time[time_col].dtype == 'object':
                df_time['year'] = df_time[time_col].astype(str).str.extract('(\\d{4})')[0]
            else:
                df_time['year'] = df_time[time_col]
            df_time['year'] = df_time['year'].astype(str)
            df_time = df_time[df_time['year'].str.match('^\\d{4}$', na=False)]
            if df_time.empty:
                return {'error': 'Could not extract valid years.'}
            result = df_time.groupby('year')[val_col].agg(func).to_dict()
            return {'query': f"Yearly '{func}' of '{val_col}'", 'result': result}
        return {'error': f"Unknown query type: '{query_type}'. Supported: filter_lookup, value_counts, filter_group, filter_aggregate, group_aggregate, aggregate, top_n, bottom_n, lookup, row_count, distinct, search, correlation, percentile"}
    except Exception as exc:
        logger.error('Data query failed (type=%s): %s', query_type, exc, exc_info=True)
        return {'error': str(exc)}