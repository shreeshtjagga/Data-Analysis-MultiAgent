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
        
        # 2. Build Context
        columns = list(state.clean_df.columns)
        stats_findings = state.stats_summary 
        
        # 3. The "Multi-Plot" Strategic Prompt
        prompt = f"""
        You are a Senior Data Scientist specializing in compelling data visualizations. 
        Your task is to produce a comprehensive, colorful visual gallery for the provided dataset.
        
        CONTEXT:
        - Columns: {columns}
        - Column Types: {state.column_types}
        - Statistician's Findings: {stats_findings}
        
        TASK:
        1. Deeply analyze the Statistical Insights to find the 'Primary Narrative'.

        2. Select the BEST visualization from Seaborn or Matplotlib to represent this. 
           (Examples: sns.scatterplot, sns.violinplot, sns.heatmap, sns.jointplot, sns.boxenplot, etc.)

        3. Identify the 5-6 most important visual perspectives required to fully 
           understand this data (e.g., Correlation Heatmap, Distribution of Key Metrics, 
           Categorical breakdowns, Outlier detection).

        4. Write Python code that generates these 5-6 plots as SUBPLOTS in a single figure.
        
        REQUIREMENTS:
        
        ** COLOR STRATEGY **
        - Use DIFFERENT color schemes for DIFFERENT plot types to avoid monotony
        - Use a combination of:
          * "husl" palette (vibrant, rainbow-like for diversity)
          * "RdYlGn" (red-yellow-green for heatmaps - natural)
          * "coolwarm" (blue-white-red for correlations)
          * "YlGnBu" (yellow-green-blue for gradients)
          * "Set2" (distinct, harmonious pastels)
          * "tab10" (categorical data)
        - For heatmaps: Use "RdYlGn" or "coolwarm" to show intensity naturally
        - For distributions: Use "Set2" or "husl" for aesthetically pleasing colors
        - For categorical plots: Use "tab10" or "Set3" with natural variation
        - For scatter plots: Use "viridis" or "plasma" with a colorbar showing intensity
        
        ** STYLING REQUIREMENTS **
        - Use `fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(20, 18))`
        - Set white background: `fig.patch.set_facecolor('white')`
        - Apply different sns.set_style or use matplotlib colors strategically
        - Use `plt.tight_layout(pad=5.0)`
        - Add grid lines (alpha=0.3) for better readability
        - Use grid color: #E0E0E0 (light gray) for subtle contrast
        - Use font sizes: 14px for titles, 12px for axis labels, 10px for ticks
        - Set line widths to 1.5-2.0 for better visibility
        - Use edge colors on bar plots (e.g., edgecolor='black', linewidth=0.5)
        
        ** VISUAL HIERARCHY **
        - Ensure each plot has a unique, descriptive title related to Statistician's findings
        - Add axis labels that are informative
        - Use color variations to highlight important patterns
        - Add a colorbar for heatmaps with proper labels
        
        - Return ONLY the executable Python code block.
        """

        # 4. Generate and Clean Code
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
