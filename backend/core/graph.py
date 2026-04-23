import logging
import signal
import concurrent.futures
import json
import time
import pandas as pd
from .state import AnalysisState
from .constants import PIPELINE_VERSION
from ..agents.architect import architect_agent, profile_dataset
from ..agents.statistician import statistician_agent
from ..agents.visualizer import visualizer_agent
from ..agents.insights import insights_agent

logger = logging.getLogger(__name__)

_AGENT_TIMEOUT_SECONDS = 90   # max seconds a single agent may run


def _debug_log(run_id: str, hypothesis_id: str, location: str, message: str, data: dict) -> None:
    # #region agent log
    try:
        with open("debug-da5cdd.log", "a", encoding="utf-8") as _fh:
            _fh.write(json.dumps({
                "sessionId": "da5cdd",
                "runId": run_id,
                "hypothesisId": hypothesis_id,
                "location": location,
                "message": message,
                "data": data,
                "timestamp": int(time.time() * 1000),
            }, ensure_ascii=True) + "\n")
    except Exception:
        pass
    # #endregion


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
    # We use deep=True to ensure nested lists (like .errors) are not shared.
    # FIX 9: Ensure dataframe fields are non-None before model_copy
    if state.clean_df is None:
        state.clean_df = pd.DataFrame()
    if state.raw_df is None:
        state.raw_df = pd.DataFrame()
    viz_state_in  = state.model_copy(deep=True)
    ins_state_in  = state.model_copy(deep=True)

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
                state.errors.append({"code": "TIMEOUT", "agent": name, "message": msg, "type": "pipeline"})
                state.partial = True
            except Exception as exc:
                msg = f"Agent '{name}' raised an unexpected error: {exc}"
                logger.exception(msg)
                state.errors.append({"code": "UNEXPECTED", "agent": name, "message": msg, "type": "pipeline"})
                state.partial = True

    # FIX 8: Only merge errors that are NEW (added by the agent itself).
    # Both viz_state_in and ins_state_in were deep-copied from state, so they
    # already contain all of state.errors at copy time.  If we extend with the
    # full out.errors list we double-count every pre-existing error.
    # We track the baseline error count so we can slice only the new tail.
    baseline_error_count = len(state.errors)

    if viz_state_out is not None:
        state.charts = viz_state_out.charts
        # Only take errors that the visualizer *added* (beyond what it started with)
        new_viz_errors = viz_state_out.errors[baseline_error_count:]
        state.errors.extend(new_viz_errors)
        state.completed_agents.extend(
            a for a in viz_state_out.completed_agents if a not in state.completed_agents
        )
        if viz_state_out.partial:
            state.partial = True

    if ins_state_out is not None:
        state.insights = ins_state_out.insights
        # Same deduplication for insights agent errors
        new_ins_errors = ins_state_out.errors[baseline_error_count:]
        state.errors.extend(new_ins_errors)
        state.completed_agents.extend(
            a for a in ins_state_out.completed_agents if a not in state.completed_agents
        )
        if ins_state_out.partial:
            state.partial = True

    return state


def run_pipeline(df) -> AnalysisState:
    state = AnalysisState(raw_df=df)
    logger.info("Starting analysis pipeline (version=%s)", PIPELINE_VERSION)

    # ── PHASE 1: Architect ──────────────────────────────────────────────────
    logger.info("Running agent: architect")
    state.current_agent = "architect"
    try:
        state = _run_agent_with_timeout(architect_agent, state, "architect")
    except Exception as exc:
        logger.exception("Architect failed: %s", exc)
        _debug_log("pre-fix", "H4", "backend/core/graph.py:run_pipeline", "architect failure append type", {"append_value_type": "str"})
        # FIX 11: Keep state.errors schema consistent (dict only)
        state.errors.append({
            "code": "ARCHITECT_FAILED",
            "agent": "orchestrator",
            "message": str(exc),
            "type": "pipeline",
        })
        state.partial = True
        return state

    # Usable Data Guard: If architect excludes everything, stop early.
    excluded = (state.stats_summary or {}).get("excluded_columns", [])
    # FIX 10: Compare clean columns against excluded column names set
    excluded_names = {e["column"] for e in excluded}
    clean_cols = [c for c in (state.clean_df.columns if state.clean_df is not None else [])
                  if c not in excluded_names]
    if len(clean_cols) == 0:
        msg = "No usable columns found after initial classification."
        logger.error(msg)
        state.errors.append({"code": "NO_USABLE_COLUMNS", "agent": "orchestrator", "message": msg, "type": "pipeline"})
        state.partial = True
        return state

    # ── PHASE 1.5: Run Statistician and Profiler in Parallel ────────
    logger.info("Running Statistician and Dataset Profiler concurrently...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        # Give statistician its own state copy (standard pattern here)
        if state.clean_df is None:
            state.clean_df = pd.DataFrame()
        if state.raw_df is None:
            state.raw_df = pd.DataFrame()
        stat_state_in = state.model_copy(deep=True)
        
        stat_future = pool.submit(_run_agent_with_timeout, statistician_agent, stat_state_in, "statistician")
        prof_future = pool.submit(profile_dataset, state.clean_df, state.column_types)
        
        try:
            stat_state_out = stat_future.result(timeout=_AGENT_TIMEOUT_SECONDS + 5)
            # Merge statistician results
            state.stats_summary = stat_state_out.stats_summary
            state.errors.extend(stat_state_out.errors)
            state.completed_agents.extend(
                a for a in stat_state_out.completed_agents if a not in state.completed_agents
            )
            if stat_state_out.partial:
                state.partial = True
        except Exception as exc:
            logger.exception("Statistician failed in parallel block: %s", exc)
            _debug_log("pre-fix", "H4", "backend/core/graph.py:run_pipeline", "statistician failure append type", {"append_value_type": "str"})
            state.errors.append({
                "code": "STATISTICIAN_FAILED",
                "agent": "orchestrator",
                "message": str(exc),
                "type": "pipeline",
            })
            state.partial = True

        try:
            profile_res = prof_future.result(timeout=_AGENT_TIMEOUT_SECONDS + 5)
            if state.stats_summary is None:
                state.stats_summary = {}
            state.stats_summary["dataset_profile"] = profile_res
        except Exception as exc:
            logger.exception("Dataset profiling failed in parallel block: %s", exc)
            if state.stats_summary is None:
                state.stats_summary = {}
            state.stats_summary["dataset_profile"] = {
                "label": "unknown",
                "description": "Profiling unavailable",
                "domain": "general",
            }

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