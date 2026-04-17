import logging
import signal
from .state import AnalysisState
from .constants import PIPELINE_VERSION
from ..agents.architect import architect_agent
from ..agents.statistician import statistician_agent
from ..agents.visualizer import visualizer_agent
from ..agents.insights import insights_agent

logger = logging.getLogger(__name__)

_AGENT_TIMEOUT_SECONDS = 90   # max seconds a single agent may run


def _run_agent_with_timeout(agent_fn, state: AnalysisState, name: str) -> AnalysisState:
    """Run agent_fn(state) with a hard wall-clock timeout (Unix only)."""

    def _timeout_handler(signum, frame):
        raise TimeoutError(f"Agent '{name}' timed out after {_AGENT_TIMEOUT_SECONDS}s")

    # signal.SIGALRM is only available on Unix; skip on Windows
    has_alarm = hasattr(signal, "SIGALRM")
    if has_alarm:
        old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(_AGENT_TIMEOUT_SECONDS)
    try:
        return agent_fn(state)
    finally:
        if has_alarm:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)


def run_pipeline(df) -> AnalysisState:
    state = AnalysisState(raw_df=df)
    logger.info("Starting analysis pipeline (version=%s)", PIPELINE_VERSION)

    agents = [
        ("architect",    architect_agent),
        ("statistician", statistician_agent),
        ("visualizer",   visualizer_agent),
        ("insights",     insights_agent),
    ]

    for name, agent_fn in agents:
        logger.info("Running agent: %s", name)
        state.current_agent = name
        error_count_before = len(state.errors)

        try:
            state = _run_agent_with_timeout(agent_fn, state, name)
        except TimeoutError as exc:
            msg = str(exc)
            logger.error(msg)
            state.errors.append(msg)
            state.partial = True
        except Exception as exc:
            msg = f"Agent '{name}' raised an unexpected error: {exc}"
            logger.exception(msg)
            state.errors.append(msg)
            state.partial = True

        if len(state.errors) > error_count_before:
            state.partial = True
            logger.warning("Agent '%s' encountered an error but continuing pipeline", name)

    logger.info(
        "Pipeline complete. Agents run: %s. Errors: %d",
        state.completed_agents,
        len(state.errors)
    )
    return state