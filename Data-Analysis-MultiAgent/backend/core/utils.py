import pandas as pd
import numpy as np
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def load_csv(file_path: str) -> pd.DataFrame:

    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    if path.suffix.lower() != ".csv":
        raise ValueError(f"Expected a CSV file, got: {path.suffix}")

    df = pd.read_csv(file_path)
    if df.empty:
        raise ValueError("The CSV file is empty")

    logger.info("Loaded %d rows and %d columns from %s", len(df), len(df.columns), file_path)
    return df


def clean_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """
    Clean the dataframe: strip strings, drop duplicates, impute missing values.
    Returns (cleaned_df, imputation_records) where each record is a dict with:
      - column: column name
      - strategy: 'median' or 'mode'
      - fill_value: the value used to fill
      - count: number of cells that were imputed
    """
    initial_rows = len(df)
    logs = []

    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        df[col] = df[col].str.strip()

    df = df.drop_duplicates()
    dropped = initial_rows - len(df)
    if dropped > 0:
        logger.info("Dropped %d duplicate rows", dropped)

    num_cols = df.select_dtypes(include=[np.number]).columns
    for col in num_cols:
        missing_count = int(df[col].isna().sum())
        if missing_count > 0:
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            logger.info("Filled %d missing values in '%s' with median: %s", missing_count, col, median_val)
            logs.append({
                "column": col,
                "strategy": "median",
                "fill_value": float(median_val) if pd.notna(median_val) else None,
                "count": missing_count,
            })

    cat_cols = df.select_dtypes(include=["object"]).columns
    for col in cat_cols:
        missing_count = int(df[col].isna().sum())
        if missing_count > 0:
            mode_val = df[col].mode()
            if not mode_val.empty:
                fill = mode_val.iloc[0]
                df[col] = df[col].fillna(fill)
                logger.info("Filled %d missing values in '%s' with mode: %s", missing_count, col, fill)
                logs.append({
                    "column": col,
                    "strategy": "mode",
                    "fill_value": str(fill),
                    "count": missing_count,
                })

    return df.reset_index(drop=True), logs


def detect_column_types(df: pd.DataFrame) -> dict[str, str]:
    type_map: dict[str, str] = {}
    for col in df.columns:
        if pd.api.types.is_bool_dtype(df[col]):
            type_map[col] = "boolean"
        elif pd.api.types.is_numeric_dtype(df[col]):
            type_map[col] = "numeric"
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            type_map[col] = "datetime"
        else:
            try:
                pd.to_datetime(df[col], format="mixed")
                type_map[col] = "datetime"
            except (ValueError, TypeError):
                type_map[col] = "categorical"
    return type_map


def safe_describe(df: pd.DataFrame) -> dict:
    summary = {}
    summary["shape"] = {"rows": int(df.shape[0]), "columns": int(df.shape[1])}
    summary["columns"] = list(df.columns)
    summary["dtypes"] = {col: str(dtype) for col, dtype in df.dtypes.items()}
    summary["missing_values"] = {
        col: int(count) for col, count in df.isna().sum().items() if count > 0
    }

    num_df = df.select_dtypes(include=[np.number]).describe()
    if num_df.empty:
        summary["numeric_summary"] = {}
    else:
        summary["numeric_summary"] = {
            col: {stat: float(num_df[col][stat]) for stat in num_df.index}
            for col in num_df.columns
        }

    return summary
