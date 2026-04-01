import sys
import os
import json
import logging
import tempfile

import streamlit as st
import plotly.io as pio

# Ensure the project root is on the path so "backend.*" imports resolve
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.core.graph import run_pipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Data Analysis Agent", layout="wide")
st.title("Data Analysis Multi-Agent System")
st.markdown("Upload a CSV file to run an automated analysis pipeline.")

uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

if uploaded_file is not None:
    # Save the upload to a temp file so agents can read it by path
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(uploaded_file.getvalue())
        tmp_path = tmp.name

    if st.button("Run Analysis"):
        with st.spinner("Running analysis pipeline..."):
            state = run_pipeline(tmp_path)

        # --- Errors ---
        if state.errors:
            st.warning("Some agents reported errors:")
            for err in state.errors:
                st.error(err)

        # --- Statistics ---
        st.header("Statistical Summary")
        stats = state.stats_summary
        if stats:
            col1, col2, col3 = st.columns(3)
            col1.metric("Rows", stats.get("row_count", "N/A"))
            col2.metric("Columns", stats.get("column_count", "N/A"))
            col3.metric("Agents Completed", len(state.completed_agents))

            if "descriptive" in stats:
                st.subheader("Descriptive Statistics")
                st.json(stats["descriptive"])

            if "normality" in stats:
                st.subheader("Normality Tests")
                st.json(stats["normality"])
        else:
            st.info("No statistics were generated.")

        # --- Charts ---
        st.header("Visualizations")
        charts = state.charts
        if charts:
            for chart_name, chart_json in charts.items():
                st.subheader(chart_name.replace("_", " ").title())
                fig = pio.from_json(chart_json)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No charts were generated.")

        # --- Insights ---
        st.header("Insights")
        insights = state.insights
        if insights:
            findings = insights.get("findings", [])
            for finding in findings:
                st.markdown(f"- {finding}")

            recommendations = insights.get("recommendations", [])
            if recommendations:
                st.subheader("Recommendations")
                for rec in recommendations:
                    st.markdown(f"- {rec}")
        else:
            st.info("No insights were generated.")

        # Clean up temp file
        os.unlink(tmp_path)
