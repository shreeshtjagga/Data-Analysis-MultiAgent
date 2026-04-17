import logging
import signal
import concurrent.futures
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


def _run_parallel_agents(state: AnalysisState) -> AnalysisState:
    """
    Run visualizer_agent and insights_agent concurrently.

    Both agents only *read* clean_df / stats_summary (set by statistician).
    They write to disjoint fields (charts vs insights), so it is safe to run
    them on separate state copies and merge the results afterwards.
    """
    # Give each agent its own isolated copy of the state so writes don't race.
    viz_state_in  = state.model_copy(deep=False)
    ins_state_in  = state.model_copy(deep=False)

    viz_state_out: AnalysisState | None = None
    ins_state_out: AnalysisState | None = None

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        viz_future = pool.submit(
            _run_agent_with_timeout, visualizer_agent, viz_state_in, "visualizer"
        )
        ins_future = pool.submit(
            _run_agent_with_timeout, insights_agent, ins_state_in, "insights"
        )

        # Wait for both; capture exceptions individually so one failure doesn't
        # silently kill the other agent's result.
        for name, future in [("visualizer", viz_future), ("insights", ins_future)]:
            try:
                result = future.result(timeout=_AGENT_TIMEOUT_SECONDS + 5)
                if name == "visualizer":
                    viz_state_out = result
                else:
                    ins_state_out = result
            except TimeoutError as exc:
                msg = str(exc) or f"Agent '{name}' timed out"
                logger.error(msg)
                state.errors.append({"code": "TIMEOUT", "agent": name, "message": msg})
                state.partial = True
            except Exception as exc:
                msg = f"Agent '{name}' raised an unexpected error: {exc}"
                logger.exception(msg)
                state.errors.append({"code": "UNEXPECTED", "agent": name, "message": msg})
                state.partial = True

    # Merge results back into the main state
    if viz_state_out is not None:
        state.charts = viz_state_out.charts
        state.errors.extend(viz_state_out.errors)
        state.completed_agents.extend(
            a for a in viz_state_out.completed_agents if a not in state.completed_agents
        )
        if viz_state_out.partial:
            state.partial = True

    if ins_state_out is not None:
        state.insights = ins_state_out.insights
        state.errors.extend(ins_state_out.errors)
        state.completed_agents.extend(
            a for a in ins_state_out.completed_agents if a not in state.completed_agents
        )
        if ins_state_out.partial:
            state.partial = True

    return state


def run_pipeline(df) -> AnalysisState:
    state = AnalysisState(raw_df=df)
    logger.info("Starting analysis pipeline (version=%s)", PIPELINE_VERSION)

    # ── Sequential agents: each depends on the previous ───────────────────────
    sequential_agents = [
        ("architect",    architect_agent),
        ("statistician", statistician_agent),
    ]

    for name, agent_fn in sequential_agents:
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

    # ── Parallel agents: visualizer + insights run concurrently ───────────────
    logger.info(
        "Running agents concurrently: visualizer (LLM plan + heuristic + evaluate) "
        "and insights (LLM insights) — 3 LLM calls in parallel across 2 agents"
    )
    state = _run_parallel_agents(state)

    logger.info(
        "Pipeline complete. Agents run: %s. Errors: %d",
        state.completed_agents,
        len(state.errors),
    )
    return state