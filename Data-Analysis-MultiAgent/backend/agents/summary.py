import logging
import os
from groq import Groq
from backend.core.state import AnalysisState
 
logger = logging.getLogger(__name__)
 
 
def summary_agent(state: AnalysisState) -> AnalysisState:
    state.current_agent = "summary"
    logger.info("Summary agent started")
 
    try:
        stats = state.stats_summary
        insights = state.insights
        if not stats or not insights:
            raise ValueError("Missing stats_summary or insights — prior agents must run first")
 
        findings = insights.get("findings", [])
        recommendations = insights.get("recommendations", [])
        outliers = insights.get("outlier_summary", {})
 
        prompt = f"""You are a senior data analyst writing an executive summary for a non-technical audience.
 
Given the following analysis results, write a clear, concise paragraph (4-6 sentences) summarising:
- What the dataset contains
- The most important patterns or relationships found
- Any data quality concerns
- The top action to take next
 
Dataset overview:
- Rows: {stats.get('row_count')}
- Columns: {stats.get('column_count')}
- Columns list: {', '.join(stats.get('columns', []))}
 
Key findings:
{chr(10).join(f'- {f}' for f in findings)}
 
Recommendations:
{chr(10).join(f'- {r}' for r in recommendations)}
 
Outliers detected: {outliers if outliers else 'None'}
 
Write only the summary paragraph. No headings, no bullet points.
"""
 
        client = Groq(api_key=os.environ["GROQ_API_KEY"])
 
        response = client.chat.completions.create(
            model="llama3-70b-8192",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4,
        )
 
        summary_text = response.choices[0].message.content.strip()
        state.insights["executive_summary"] = summary_text
        logger.info("Summary agent complete")
 
    except Exception as e:
        error_msg = f"Summary error: {e}"
        logger.error(error_msg)
        state.errors.append(error_msg)
 
    state.completed_agents.append("summary")
    return state
