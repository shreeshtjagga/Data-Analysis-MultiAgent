import logging
from backend.core.state import AnalysisState
from backend.agents.architect import architect_agent
from backend.agents.statistician import statistician_agent
from backend.agents.visualizer import visualizer_agent
from backend.agents.insights import insights_agent
from backend.agents.summary import summary_agent

logger = logging.getLogger(__name__)


def run_pipeline(df) -> AnalysisState:
    state = AnalysisState(data=df)

    logger.info("Starting analysis pipeline")

    agents = [
        ("architect", architect_agent),
        ("statistician", statistician_agent),
        ("visualizer", visualizer_agent),
        ("insights", insights_agent),
        ("summary", summary_agent),
    ]

    for name, agent_fn in agents:
        logger.info("Running agent: %s", name)

        error_count_before = len(state.errors)

        state = agent_fn(state)

        if len(state.errors) > error_count_before:
            logger.warning(
                "Agent '%s' encountered an error, but continuing pipeline",
                name
            )

    logger.info(
        "Pipeline complete. Agents run: %s. Errors: %d",
        state.completed_agents,
        len(state.errors)
    )

    return state