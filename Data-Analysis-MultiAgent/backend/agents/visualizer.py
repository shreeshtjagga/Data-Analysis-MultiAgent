import logging
import io
import base64
import matplotlib.pyplot as plt
import seaborn as sns
from langchain_groq import ChatGroq
from backend.core.state import AnalysisState

# Setup logging to match your project's style
logger = logging.getLogger(__name__)

def visualizer_agent(state: AnalysisState) -> AnalysisState:
    """
    Visualizer Agent: Converts statistical findings into visual assets using Groq.
    """
    state.current_agent = "visualizer"
    logger.info("Visualizer Agent: Commencing chart generation via Groq.")

    # 1. Pipeline Check
    if state.clean_df is None or state.clean_df.empty:
        error_msg = "Visualizer Failure: clean_df is missing."
        logger.error(error_msg)
        state.errors.append(error_msg)
        return state

    try:
        # 2. Initialize Groq (Ensure GROQ_API_KEY is in your .env)
        # Using llama-3.3-70b-versatile or mixtral-8x7b-32768
        llm = ChatGroq(model_name="llama-3.3-70b-versatile", temperature=0)
        
        # 3. Build Context
        columns = list(state.clean_df.columns)
        stats_findings = state.stats_summary 
        
        prompt = f"""
        You are a Professional Data Visualizer.
        
        CONTEXT:
        - Available Columns: {columns}
        - Column Types: {state.column_types}
        - Statistical Insights: {stats_findings}
        
        TASK:
        Write Python code using Seaborn to create a visualization that 
        best represents the Statistical Insights provided above.
        
        REQUIREMENTS:
        - Use `plt.figure(figsize=(10, 6))`
        - Assume the dataframe is named `df`.
        - Do NOT use `plt.show()`.
        - Use `sns.set_theme(style="whitegrid")`.
        - Return ONLY the executable Python code block. No explanation.
        """

        # 4. Generate and Parse Code
        response = llm.invoke(prompt)
        # Clean the response to ensure only pure Python code remains
        code = response.content.strip()
        if "```python" in code:
            code = code.split("```python")[1].split("```")[0].strip()
        elif "```" in code:
            code = code.split("```")[1].split("```")[0].strip()

        # 5. Execute and Capture
        plt.clf()  # Clear memory
        exec_globals = {"df": state.clean_df, "plt": plt, "sns": sns}
        
        # Execute the Groq-generated code
        exec(code, exec_globals)
        
        # 6. Convert to Base64 String (The missing part)
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight')
        buf.seek(0)
        img_base64 = base64.b64encode(buf.read()).decode('utf-8')
        plt.close()

        # 7. Update AnalysisState charts dictionary
        state.charts["primary_analysis"] = img_base64
        logger.info("Visualizer Agent: Visualization successfully stored in state.")

    except Exception as e:
        error_msg = f"Visualizer Execution Error: {str(e)}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    # 8. Finalize progress
    state.completed_agents.append("visualizer")
    return state