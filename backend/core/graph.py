import logging
import concurrent.futures
import pandas as pd

from .state import AnalysisState
from .constants import PIPELINE_VERSION
from ..agents.architect import architect_agent, profile_dataset
from ..agents.statistician import statistician_agent
from ..agents.visualizer import visualizer_agent
from ..agents.insights import insights_agent
logger = logging.getLogger(__name__)
_AGENT_TIMEOUT_SECONDS = 90
_RESULT_TIMEOUT_SECONDS = _AGENT_TIMEOUT_SECONDS + 5

def _run_agent_with_timeout(agent_fn, state: AnalysisState, name: str) -> AnalysisState:
    """Run an agent with a wall-clock timeout.

    Uses concurrent.futures instead of signal.SIGALRM because:
    - SIGALRM is Unix-only
    - signal.signal() can only be called from the main thread, but FastAPI
      runs the pipeline via asyncio.to_thread() which executes in a worker thread.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(agent_fn, state)
        try:
            return future.result(timeout=_AGENT_TIMEOUT_SECONDS)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise TimeoutError(f"Agent '{name}' timed out after {_AGENT_TIMEOUT_SECONDS}s")


def _run_parallel_agents(state: AnalysisState) -> AnalysisState:
    if state.clean_df is None:
        state.clean_df = pd.DataFrame()
    if state.raw_df is None:
        state.raw_df = pd.DataFrame()
    viz_state_in = state.model_copy(deep=True)
    ins_state_in = state.model_copy(deep=True)
    viz_state_out: AnalysisState | None = None
    ins_state_out: AnalysisState | None = None
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        viz_future = pool.submit(_run_agent_with_timeout, visualizer_agent, viz_state_in, 'visualizer')
        ins_future = pool.submit(_run_agent_with_timeout, insights_agent, ins_state_in, 'insights')
        for (name, future) in [('visualizer', viz_future), ('insights', ins_future)]:
            try:
                result = future.result(timeout=_RESULT_TIMEOUT_SECONDS)
                if name == 'visualizer':
                    viz_state_out = result
                else:
                    ins_state_out = result
            except TimeoutError as exc:
                msg = str(exc) or f"Agent '{name}' timed out"
                logger.error(msg)
                state.errors.append({'code': 'TIMEOUT', 'agent': name, 'message': msg, 'type': 'pipeline'})
                state.partial = True
            except Exception as exc:
                msg = f"Agent '{name}' raised an unexpected error: {exc}"
                logger.exception(msg)
                state.errors.append({'code': 'UNEXPECTED', 'agent': name, 'message': msg, 'type': 'pipeline'})
                state.partial = True
    baseline_error_count = len(state.errors)
    if viz_state_out is not None:
        state.charts = viz_state_out.charts
        new_viz_errors = viz_state_out.errors[baseline_error_count:]
        state.errors.extend(new_viz_errors)
        state.completed_agents.extend((a for a in viz_state_out.completed_agents if a not in state.completed_agents))
        if viz_state_out.partial:
            state.partial = True
    if ins_state_out is not None:
        state.insights = ins_state_out.insights
        new_ins_errors = ins_state_out.errors[baseline_error_count:]
        state.errors.extend(new_ins_errors)
        state.completed_agents.extend((a for a in ins_state_out.completed_agents if a not in state.completed_agents))
        if ins_state_out.partial:
            state.partial = True
    return state

def run_pipeline(df) -> AnalysisState:
    state = AnalysisState(raw_df=df)
    logger.info('Starting analysis pipeline (version=%s)', PIPELINE_VERSION)
    logger.info('Running agent: architect')
    state.current_agent = 'architect'
    try:
        state = _run_agent_with_timeout(architect_agent, state, 'architect')
    except Exception as exc:
        logger.exception('Architect failed: %s', exc)
        state.errors.append({'code': 'ARCHITECT_FAILED', 'agent': 'orchestrator', 'message': str(exc), 'type': 'pipeline'})
        state.partial = True
        return state
    excluded = (state.stats_summary or {}).get('excluded_columns', [])
    excluded_names = {e['column'] for e in excluded}
    clean_cols = [c for c in (state.clean_df.columns if state.clean_df is not None else []) if c not in excluded_names]
    if len(clean_cols) == 0:
        msg = 'No usable columns found after initial classification.'
        logger.error(msg)
        state.errors.append({'code': 'NO_USABLE_COLUMNS', 'agent': 'orchestrator', 'message': msg, 'type': 'pipeline'})
        state.partial = True
        return state
    logger.info('Running Statistician and Dataset Profiler concurrently...')
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        if state.clean_df is None:
            state.clean_df = pd.DataFrame()
        if state.raw_df is None:
            state.raw_df = pd.DataFrame()
        stat_state_in = state.model_copy(deep=True)
        stat_future = pool.submit(_run_agent_with_timeout, statistician_agent, stat_state_in, 'statistician')
        prof_future = pool.submit(profile_dataset, state.clean_df, state.column_types)
        try:
            stat_state_out = stat_future.result(timeout=_RESULT_TIMEOUT_SECONDS)
            state.stats_summary = stat_state_out.stats_summary
            _stat_baseline = len(state.errors)
            new_stat_errors = stat_state_out.errors[_stat_baseline:]
            state.errors.extend(new_stat_errors)
            state.completed_agents.extend((a for a in stat_state_out.completed_agents if a not in state.completed_agents))
            if stat_state_out.partial:
                state.partial = True
        except Exception as exc:
            logger.exception('Statistician failed in parallel block: %s', exc)
            state.errors.append({'code': 'STATISTICIAN_FAILED', 'agent': 'orchestrator', 'message': str(exc), 'type': 'pipeline'})
            state.partial = True
        try:
            profile_res = prof_future.result(timeout=_RESULT_TIMEOUT_SECONDS)
            if state.stats_summary is None:
                state.stats_summary = {}
            state.stats_summary['dataset_profile'] = profile_res
        except Exception as exc:
            logger.exception('Dataset profiling failed in parallel block: %s', exc)
            if state.stats_summary is None:
                state.stats_summary = {}
            state.stats_summary['dataset_profile'] = {'label': 'unknown', 'description': 'Profiling unavailable', 'domain': 'general'}
    logger.info('Running agents concurrently: visualizer (LLM plan + heuristic + evaluate) and insights (LLM insights) — 3 LLM calls in parallel across 2 agents')
    state = _run_parallel_agents(state)
    logger.info('Pipeline complete. Agents run: %s. Errors: %d', state.completed_agents, len(state.errors))
    return state