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

        # 2. Separate numeric and categorical columns explicitly
        df = state.clean_df
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
        stats_findings = state.stats_summary

        # 3. The "Multi-Plot" Strategic Prompt with strict column rules
        prompt = f"""

IMPORTANT: The dataframe is already loaded as `df`. Do NOT read any CSV file.

You are a Senior Data Scientist. Your task is to produce a comprehensive
visual gallery for the provided dataset.

CONTEXT:
- All Columns: {list(df.columns)}
- NUMERIC columns ONLY (use these for histograms, scatter, box, violin, heatmap): {numeric_cols}
- CATEGORICAL columns ONLY (use these for bar/count plots): {categorical_cols}
- Statistician's Findings: {stats_findings}

STRICT RULES:
- NEVER use categorical columns for scatter, histogram, violin, box, or heatmap plots
- ONLY use numeric columns for any plot that requires float/int values
- For bar/count plots, ONLY use categorical columns
- NEVER load any file, NEVER use pd.read_csv(), NEVER use open()
- The dataframe is ALREADY loaded and available as `df` — use it directly
- DO NOT import pandas, DO NOT import numpy — they are not needed
- Always use `df` as the dataframe variable name
- If numeric_cols has fewer than 2 columns, skip correlation heatmap
- All axes must be referenced as axes[row][col] or axes[index] depending on grid shape

TASK:
1. Deeply analyze the Statistical Insights to find the Primary Narrative.
2. Identify the 5-6 most important visual perspectives to understand this data.
3. Write Python code that generates these plots as SUBPLOTS in a single figure.

REQUIREMENTS:
- Use `fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(20, 18))`
- Flatten axes with `axes = axes.flatten()`
- Use `plt.tight_layout(pad=5.0)`
- Every plot must have a descriptive title
- Use seaborn themes and palettes
- Return ONLY executable Python code, no explanation, no markdown
"""

        # 4. Generate and Clean Code
        response = llm.invoke(prompt)
        code = response.content.strip()

        if "```python" in code:
            code = code.split("```python")[1].split("```")[0].strip()
        elif "```" in code:
            code = code.split("```")[1].split("```")[0].strip()

        logger.info("Generated visualization code:\n%s", code)

        # 5. Execute and Capture
        plt.switch_backend('Agg')
        plt.clf()

        exec_globals = {
            "df": df,
            "plt": plt,
            "sns": sns,
            "numeric_cols": numeric_cols,
            "categorical_cols": categorical_cols,
        }
        exec(code, exec_globals)

        # 6. Convert the entire Grid to a single Base64 String
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', dpi=150)
        buf.seek(0)
        img_base64 = base64.b64encode(buf.read()).decode('utf-8')
        plt.close()

        # 7. Update AnalysisState
        state.charts["visual_dashboard"] = img_base64
        logger.info("Visualizer Agent: Dashboard generated and stored successfully.")

    except Exception as e:
        error_msg = f"Visualizer Suite Error: {str(e)}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    state.completed_agents.append("visualizer")
    return state