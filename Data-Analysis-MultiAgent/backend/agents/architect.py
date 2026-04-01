import logging
import pandas as pd
from backend.core.state import AnalysisState
from backend.core.utils import load_csv, clean_dataframe, detect_column_types

logger = logging.getLogger(__name__)


def architect_agent(state: AnalysisState) -> AnalysisState:
    """Load, validate, and clean the dataset.

    The Architect is the first agent in the pipeline. It reads the CSV file,
    performs data cleaning, and classifies column types for downstream agents.
    """
    state.current_agent = "architect"
    logger.info("Architect agent started — loading file: %s", state.file_path)

    try:
        if not state.file_path:
            raise ValueError("No file_path provided in state")

        raw_df = load_csv(state.file_path)
        state.raw_df = raw_df
        logger.info("Raw data loaded: %d rows, %d columns", len(raw_df), len(raw_df.columns))

        clean_df = clean_dataframe(raw_df.copy())
        state.clean_df = clean_df
        logger.info("Data cleaned: %d rows remaining", len(clean_df))

        state.column_types = detect_column_types(clean_df)
        logger.info("Column types detected: %s", state.column_types)

    except Exception as e:
        error_msg = f"Architect error: {e}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    state.completed_agents.append("architect")
    return state
