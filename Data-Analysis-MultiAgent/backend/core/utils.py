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


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    
    initial_rows = len(df)

    # Strip whitespace from string columns
    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        df[col] = df[col].str.strip()

    # Drop fully duplicate rows
    df = df.drop_duplicates()
    dropped = initial_rows - len(df)
    if dropped > 0:
        logger.info("Dropped %d duplicate rows", dropped)

    # Fill missing numeric values with column median
    num_cols = df.select_dtypes(include=[np.number]).columns
    for col in num_cols:
        if df[col].isna().any():
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            logger.info("Filled missing values in '%s' with median: %s", col, median_val)

    # Fill missing categorical values with mode
    cat_cols = df.select_dtypes(include=["object"]).columns
    for col in cat_cols:
        if df[col].isna().any():
            mode_val = df[col].mode()
            if not mode_val.empty:
                df[col] = df[col].fillna(mode_val.iloc[0])
                logger.info("Filled missing values in '%s' with mode: %s", col, mode_val.iloc[0])

    return df.reset_index(drop=True)


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
            # Try parsing as datetime
            try:
                pd.to_datetime(df[col], infer_datetime_format=True)
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

    num_df = df.describe()
    summary["numeric_summary"] = {
        col: {stat: float(num_df[col][stat]) for stat in num_df.index}
        for col in num_df.columns
    }

    return summary
