import logging
from .state import AnalysisState
from ..agents.architect import architect_agent
from ..agents.statistician import statistician_agent
from ..agents.visualizer import visualizer_agent
from ..agents.insights import insights_agent
from ..agents.summary import summary_agent
from dotenv import load_dotenv
load_dotenv()
logger = logging.getLogger(__name__)


def run_pipeline(df) -> AnalysisState:
    state = AnalysisState(raw_df=df)
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

        state.current_agent = name

        error_count_before = len(state.errors)

        state = agent_fn(state)

        if len(state.errors) > error_count_before:
            logger.warning(
                "Agent '%s' encountered an error. Halting pipeline to prevent cascading failures",
                name
            )
            break

    logger.info(
        "Pipeline complete. Agents run: %s. Errors: %d",
        state.completed_agents,
        len(state.errors)
    )

    return state