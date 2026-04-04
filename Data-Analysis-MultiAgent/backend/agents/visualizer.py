import logging
import io
import base64
import matplotlib.pyplot as plt
import seaborn as sns
from langchain_groq import ChatGroq
from core.state import AnalysisState

# Setup logging
logger = logging.getLogger(__name__)

def visualizer_agent(state: AnalysisState) -> AnalysisState:
    """
    Visualizer Agent: Dynamically identifies and generates the 5-6 most 
    statistically significant plots for the given dataset.
    """
    state.current_agent = "visualizer"
    logger.info("Visualizer Agent: Orchestrating a multi-plot analytical suite...")

    if state.clean_df is None or state.clean_df.empty:
        error_msg = "Visualizer Failure: clean_df is missing."
        logger.error(error_msg)
        state.errors.append(error_msg)
        return state

    try:
        # 1. Initialize Groq
        llm = ChatGroq(model_name="llama-3.3-70b-versatile", temperature=0.2)
        
        # 2. Build Context
        columns = list(state.clean_df.columns)
        stats_findings = state.stats_summary 
        
        # 3. The "Multi-Plot" Strategic Prompt
        prompt = f"""
        You are a Senior Data Scientist. Your task is to produce a comprehensive 
        visual gallery for the provided dataset.
        
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
        - Use `fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(20, 18))` (or similar grid).
        - Use `plt.tight_layout(pad=5.0)`.
        - Ensure every plot has a specific title related to the Statistician's findings.
        - Use sophisticated Seaborn themes and palettes.
        - Return ONLY the executable Python code block.
        """

        # 4. Generate and Clean Code
        response = llm.invoke(prompt)
        code = response.content.strip()
        
        if "```python" in code:
            code = code.split("```python")[1].split("```")[0].strip()
        elif "```" in code:
            code = code.split("```")[1].split("```")[0].strip()

        # 5. Execute and Capture
        plt.switch_backend('Agg') 
        plt.clf() 
        
        # We pass the dataframe as 'df' to the execution environment
        exec_globals = {"df": state.clean_df, "plt": plt, "sns": sns}
        exec(code, exec_globals)
        
        # 6. Convert the entire Grid to a single Base64 String
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', dpi=150)
        buf.seek(0)
        img_base64 = base64.b64encode(buf.read()).decode('utf-8')
        plt.close()

        # 7. Update AnalysisState
        # We store the main dashboard as 'primary_analysis'
        state.charts["visual_dashboard"] = img_base64
        logger.info("Visualizer Agent: 5-6 dynamic plots generated and stored as a dashboard.")

    except Exception as e:
        error_msg = f"Visualizer Suite Error: {str(e)}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    state.completed_agents.append("visualizer")
    return state