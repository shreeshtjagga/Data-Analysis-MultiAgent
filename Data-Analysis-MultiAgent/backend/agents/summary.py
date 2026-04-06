import logging
from ..core.state import AnalysisState

logger = logging.getLogger(__name__)


def summary_agent(state: AnalysisState) -> AnalysisState:
    """
    Summary Agent — lightweight pass.

    Executive summary generation has been removed from the insights flow.
    This agent now only ensures the insights dictionary exists and logs completion.
    """
    state.current_agent = "summary"
    logger.info("Summary agent started.")

    try:
        if state.insights is None:
            state.insights = {}
        logger.info("Summary agent complete. Executive summary is disabled.")
        state.completed_agents.append("summary")

    except Exception as e:
        logger.error("Summary error: %s", e)
        state.errors.append(f"Summary error: {e}")

    return state
