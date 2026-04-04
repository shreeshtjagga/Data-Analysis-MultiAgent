import logging

from core.state import AnalysisState
from agents.architect import architect_agent
from agents.statistician import statistician_agent
from agents.visualizer import visualizer_agent
from agents.summary import summary_agent
from agents.insights import insights_agent

logger = logging.getLogger(__name__)


class AnalysisPipeline:
    """Wrapper that provides a LangGraph-style .invoke() interface."""

    def invoke(self, inputs: dict) -> dict:
        df = inputs.get("df")
        if df is None:
            raise ValueError("No DataFrame provided under key 'df'")

        state = AnalysisState(raw_df=df)

        agents = [
            ("architect", architect_agent),
            ("statistician", statistician_agent),
            ("visualizer", visualizer_agent),
            ("summary", summary_agent),
            ("insights", insights_agent),
        ]

        for name, agent_fn in agents:
            logger.info("Running agent: %s", name)
            error_count_before = len(state.errors)
            state = agent_fn(state)
            if len(state.errors) > error_count_before:
                logger.warning(
                    "Agent '%s' encountered an error, but continuing pipeline",
                    name,
                )

        logger.info(
            "Pipeline complete. Agents run: %s. Errors: %d",
            state.completed_agents,
            len(state.errors),
        )

        return {
            "raw_df": state.raw_df,
            "clean_df": state.clean_df,
            "stats_summary": state.stats_summary,
            "charts": state.charts,
            "summary": state.summary,
            "insights": state.insights,
            "errors": state.errors,
        }


def build_graph() -> AnalysisPipeline:
    """Return a ready-to-invoke analysis pipeline."""
    return AnalysisPipeline()
