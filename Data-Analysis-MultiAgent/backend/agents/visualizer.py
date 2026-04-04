import logging
import io
import base64
import matplotlib.pyplot as plt
import seaborn as sns
from langchain_groq import ChatGroq
from core.state import AnalysisState

logger = logging.getLogger(__name__)


def _build_viz_context(state) -> dict:
    """
    Builds a minimal context dict for the visualizer prompt.
    Avoids sending full stats_summary (can be 2-4k tokens).
    Only sends what's needed to pick good chart types.
    """
    stats = state.stats_summary or {}
    numeric_cols    = list(stats.get("numeric_columns", {}).keys())
    categorical_cols = list(stats.get("categorical_columns", {}).keys())
    strong_corr     = [
        f"{c['col1']} vs {c['col2']} (r={c['correlation']:.2f})"
        for c in stats.get("strong_correlations", [])
    ]
    outlier_cols    = list(stats.get("outliers", {}).keys())

    return {
        "numeric_cols":    numeric_cols,
        "categorical_cols": categorical_cols,
        "strong_corr":     strong_corr,
        "outlier_cols":    outlier_cols,
        "column_types":    state.column_types,
    }


def visualizer_agent(state: AnalysisState) -> AnalysisState:
    """
    Visualizer Agent — token-efficient version.
    Sends only a compact context instead of the full stats_summary.
    """
    state.current_agent = "visualizer"
    logger.info("Visualizer Agent started (token-optimised).")

    if state.clean_df is None or state.clean_df.empty:
        state.errors.append("Visualizer Failure: clean_df is missing.")
        state.completed_agents.append("visualizer")
        return state

    try:
        llm = ChatGroq(model_name="llama-3.3-70b-versatile", temperature=0.2)
        ctx = _build_viz_context(state)

        prompt = f"""You are a Senior Data Scientist. Write Python code to generate a 3×2 subplot dashboard.

DATASET CONTEXT:
- Numeric columns: {ctx['numeric_cols']}
- Categorical columns: {ctx['categorical_cols']}
- Strong correlations: {ctx['strong_corr']}
- Outlier columns: {ctx['outlier_cols']}
- Column types: {ctx['column_types']}

REQUIREMENTS:
- Use: fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(20, 18))
- Use plt.tight_layout(pad=5.0)
- Use seaborn with a dark theme: sns.set_theme(style="darkgrid", palette="muted")
- Plot the 6 most informative charts for this data (distributions, correlations, categoricals, outliers)
- Every plot needs a descriptive title
- The dataframe is available as `df`
- Return ONLY executable Python code, no explanation"""

        response = llm.invoke(prompt)
        code = response.content.strip()

        if "```python" in code:
            code = code.split("```python")[1].split("```")[0].strip()
        elif "```" in code:
            code = code.split("```")[1].split("```")[0].strip()

        plt.switch_backend('Agg')
        plt.clf()

        exec_globals = {"df": state.clean_df, "plt": plt, "sns": sns}
        exec(code, exec_globals)

        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', dpi=150)
        buf.seek(0)
        img_base64 = base64.b64encode(buf.read()).decode('utf-8')
        plt.close()

        state.charts["visual_dashboard"] = img_base64
        logger.info("Visualizer Agent complete.")

    except Exception as e:
        state.errors.append(f"Visualizer error: {str(e)}")
        logger.error("Visualizer error: %s", e)

    state.completed_agents.append("visualizer")
    return state
