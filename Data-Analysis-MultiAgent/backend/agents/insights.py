import logging
from langchain_groq import ChatGroq
from core.state import AnalysisState

# Setup logging to match project standards
logger = logging.getLogger(__name__)

def insights_agent(state: AnalysisState) -> AnalysisState:
    """
    Insights Agent: The Final Storyteller.
    Synthesizes multi-plot visual dashboards and statistical data into a 
    cohesive executive strategy.
    """
    state.current_agent = "insights"
    logger.info("Insights Agent: Synthesizing multi-visual report via Groq.")

    # 1. Pipeline Check
    if not state.stats_summary:
        error_msg = "Insights Failure: No statistical summary found to analyze."
        logger.error(error_msg)
        state.errors.append(error_msg)
        return state

    try:
        # 2. Initialize Groq
        llm = ChatGroq(model_name="llama-3.3-70b-versatile", temperature=0.3)
        
        # 3. Gather context (Reflecting the new visual dashboard)
        # Note: We now highlight that a multi-plot 'visual_dashboard' exists
        has_dashboard = "visual_dashboard" in state.charts
        columns = list(state.clean_df.columns) if state.clean_df is not None else "Unknown"
        
        # 4. Refined Multi-Visual Strategy Prompt
        prompt = f"""
        Act as a Senior Strategy Consultant. You are reviewing a comprehensive 
        analytical dashboard containing 5-6 specialized visualizations and 
        detailed statistical summaries.

        CONTEXT:
        - Dataset Schema: {columns}
        - Statistical Deep-Dive: {state.stats_summary}
        - Visual Assets: A comprehensive 6-plot analytical dashboard has been generated.

        TASK:
        Write a professional "Executive Insights Report" that bridges the gap 
        between the raw statistics and the multi-angle visual dashboard:
        
        1. STRATEGIC OVERVIEW:
           Provide a high-level distillation of the primary narrative. In 2-3 
           concise sentences, identify the most critical business or operational 
           reality revealed by this data.

        2. MULTI-DIMENSIONAL DRIVERS:
           Synthesize the statistical findings. Explain the 'why' behind the 
           patterns. Don't just list numbers; explain what these trends 
           mean for the organization.

        3. DASHBOARD SYNTHESIS (VISUAL EVIDENCE):
           Explain how the multi-plot dashboard validates the findings. 
           How do the distributions, correlations, and categorical breakdowns 
           collectively prove the core thesis?

        4. STRATEGIC ADVISORY:
           Based on the convergence of statistical and visual evidence, 
           provide one high-priority, actionable recommendation.

        FORMATTING RULES:
        - Use professional, executive-grade language.
        - Use Markdown headers (###) and bold text for emphasis.
        - Ensure the report feels like a "final conclusion" to the multi-agent pipeline.
        """

        # 5. Generate and clean the report
        response = llm.invoke(prompt)
        final_report = response.content.strip()

        # 6. Update AnalysisState
        state.insights["final_report"] = final_report
        logger.info("Insights Agent: Multi-dimensional report successfully generated.")

    except Exception as e:
        error_msg = f"Insights Execution Error: {str(e)}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    # 7. Finalize progress
    state.completed_agents.append("insights")
    return state