import logging
from core.state import AnalysisState
from core.utils import clean_dataframe, detect_column_types

logger = logging.getLogger(__name__)


def architect_agent(state: AnalysisState) -> AnalysisState:
    state.current_agent = "architect"
    logger.info("Architect agent started")

    try:
        raw_df = state.raw_df
        if raw_df is None or raw_df.empty:
            raise ValueError("No data provided in state.raw_df")

        logger.info("Raw data: %d rows, %d columns", len(raw_df), len(raw_df.columns))

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
