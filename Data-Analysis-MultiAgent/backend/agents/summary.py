import logging
import os
from groq import Groq
from core.state import AnalysisState

logger = logging.getLogger(__name__)


def summary_agent(state: AnalysisState) -> AnalysisState:
    state.current_agent = "summary"
    logger.info("Summary agent started")

    try:
        # Validate required inputs
        stats = state.stats_summary
        insights = state.insights

        if not stats:
            raise ValueError("Missing stats_summary — statistician agent must run first")

        if insights is None:
            insights = {}

        # Debug logs (VERY IMPORTANT)
        logger.info(f"Stats keys: {list(stats.keys())}")
        logger.info(f"Insights keys: {list(insights.keys())}")

        # Flexible key handling (robust against upstream differences)
        findings = insights.get("findings") or insights.get("key_findings") or []
        recommendations = insights.get("recommendations") or insights.get("actions") or []

        # Correct source for outliers
        outliers = stats.get("outliers", {})

        # Fallback text if empty
        findings_text = "\n".join(f"- {f}" for f in findings) if findings else "- No major findings available"
        recommendations_text = "\n".join(f"- {r}" for r in recommendations) if recommendations else "- No recommendations available"

        # Prompt construction
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
{findings_text}

Recommendations:
{recommendations_text}

Outliers detected: {outliers if outliers else 'None'}

Write only the summary paragraph. No headings, no bullet points.
"""

        # Initialize Groq client safely
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise ValueError("GROQ_API_KEY not set in environment variables")

        client = Groq(api_key=api_key)

        # Updated working model
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4,
        )

        summary_text = response.choices[0].message.content.strip()

        # Ensure insights dict exists before writing
        if state.insights is None:
            state.insights = {}

        state.insights["executive_summary"] = summary_text

        logger.info("Summary agent complete")

    except Exception as e:
        error_msg = f"Summary error: {e}"
        logger.error(error_msg)
        state.errors.append(error_msg)

    state.completed_agents.append("summary")
    return state