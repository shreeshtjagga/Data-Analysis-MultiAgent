"""
AI Data Analyst · Streamlit frontend
Pipeline: Architect → Statistician → Visualizer → Summary → Insights
"""
import io
import logging
import os
from dotenv import load_dotenv

import pandas as pd
import streamlit as st

# Load environment variables from .env file
load_dotenv()

from core.graph import run_pipeline

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="AI Data Analyst",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
.block-container { padding-top: 2rem; max-width: 1280px; }

[data-testid="stSidebar"] {
    background: linear-gradient(160deg, #0f172a 0%, #1e293b 100%);
}
[data-testid="stSidebar"] * { color: #e2e8f0 !important; }

div[data-testid="stMetric"] {
    background: var(--secondary-background-color);
    border: 1px solid rgba(148,163,184,0.15);
    border-radius: 12px;
    padding: 18px 22px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    transition: box-shadow 0.2s;
}
div[data-testid="stMetric"]:hover { box-shadow: 0 6px 16px rgba(0,0,0,0.1); }
div[data-testid="stMetric"] label {
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    opacity: 0.55;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    font-size: 1.9rem !important;
    font-weight: 700 !important;
}

.health-badge {
    display: inline-flex;
    align-items: center;
    gap: 10px;
    padding: 10px 20px;
    border-radius: 999px;
    font-weight: 700;
    font-size: 1.05rem;
}
.health-great  { background:#dcfce7; color:#166534; }
.health-ok     { background:#fef9c3; color:#854d0e; }
.health-poor   { background:#fee2e2; color:#991b1b; }

.stat-card {
    background: var(--secondary-background-color);
    border: 1px solid rgba(148,163,184,0.15);
    border-left: 4px solid #6366f1;
    border-radius: 10px;
    padding: 16px 20px;
    margin-bottom: 12px;
}
.stat-card h5 {
    margin: 0 0 6px 0;
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    opacity: 0.5;
}
.stat-card .val { font-size: 1.45rem; font-weight: 700; }
.stat-card .sub { font-size: 0.83rem; opacity: 0.6; margin-top: 4px; }

.insight-card {
    background: var(--secondary-background-color);
    border-radius: 12px;
    padding: 22px 24px;
    border-top: 4px solid;
    border-left: 1px solid rgba(148,163,184,0.12);
    border-right: 1px solid rgba(148,163,184,0.12);
    border-bottom: 1px solid rgba(148,163,184,0.12);
    min-height: 220px;
}
.insight-card.findings        { border-top-color: #6366f1; }
.insight-card.anomalies       { border-top-color: #f43f5e; }
.insight-card.recommendations { border-top-color: #10b981; }
.insight-card h4 {
    margin: 0 0 14px 0;
    font-size: 0.95rem;
    font-weight: 700;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--text-color);
}
.insight-card ul {
    padding-left: 18px;
    margin: 0;
    font-size: 0.93rem;
    line-height: 1.75;
    color: var(--text-color);
}
.insight-card li { margin-bottom: 6px; }

.stTabs [data-baseweb="tab-list"] { gap: 8px; border-bottom: 2px solid rgba(148,163,184,0.15); }
.stTabs [data-baseweb="tab"] {
    height: 44px;
    border-radius: 8px 8px 0 0;
    padding: 0 18px;
    font-weight: 500;
    font-size: 0.9rem;
}
.stTabs [aria-selected="true"] { font-weight: 700 !important; }

.stButton > button {
    width: 100%;
    border-radius: 10px;
    background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
    color: white !important;
    border: none;
    font-weight: 600;
    font-size: 0.95rem;
    padding: 0.6rem 1.2rem;
    transition: opacity 0.2s, transform 0.15s;
}
.stButton > button:hover { opacity: 0.88; transform: translateY(-1px); }

.section-title {
    font-size: 1.1rem;
    font-weight: 700;
    margin-bottom: 14px;
    padding-bottom: 6px;
    border-bottom: 2px solid rgba(99,102,241,0.25);
    color: var(--text-color);
}
code { font-family: 'DM Mono', monospace; }
</style>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────────────────
_DEFAULTS = {
    "analysis_result": None,
    "uploaded_file_name": None,
    "file_bytes": None,
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:16px 4px 8px 4px">
        <p style="font-size:1.55rem;font-weight:800;margin:0;color:#f1f5f9;letter-spacing:-0.01em">
            📊 AI Data Analyst
        </p>
        <p style="font-size:0.82rem;color:#94a3b8;margin:4px 0 0 0">
            Powered by LangGraph + Groq
        </p>
    </div>
    """, unsafe_allow_html=True)
    st.divider()

    uploaded_file = st.file_uploader(
        "Upload Dataset (CSV)",
        type=["csv"],
        help="CSV files up to 200 MB.",
    )

    # Persist file bytes in session state so reruns don't lose the file
    if uploaded_file is not None:
        current_bytes = uploaded_file.getvalue()
        # Only update if a new file was uploaded
        if st.session_state["uploaded_file_name"] != uploaded_file.name:
            st.session_state["file_bytes"] = current_bytes
            st.session_state["uploaded_file_name"] = uploaded_file.name
            st.session_state["analysis_result"] = None  # clear old results
        elif st.session_state["file_bytes"] is None:
            st.session_state["file_bytes"] = current_bytes

    # Show currently loaded file
    if st.session_state["uploaded_file_name"]:
        st.success(f"✅ **{st.session_state['uploaded_file_name']}**")

    st.markdown("<br>", unsafe_allow_html=True)

    # Button is enabled as long as we have file bytes stored
    has_file = st.session_state["file_bytes"] is not None
    run_clicked = st.button(
        "⚡ Generate Analysis",
        type="primary",
        disabled=not has_file,
    )

    st.divider()
    st.markdown("""
    <div style="font-size:0.78rem;color:#64748b;line-height:1.6">
        <strong style="color:#94a3b8">Pipeline</strong><br>
        🏗 Architect &nbsp;→&nbsp; 📐 Statistician<br>
        🎨 Visualizer &nbsp;→&nbsp; 📝 Summary<br>
        💡 Insights
    </div>
    """, unsafe_allow_html=True)

# ── Run pipeline ──────────────────────────────────────────────────────────────
if run_clicked and st.session_state["file_bytes"] is not None:
    with st.spinner("🤖 Running analysis pipeline…"):
        try:
            df = pd.read_csv(io.BytesIO(st.session_state["file_bytes"]))

            state = run_pipeline(df)
            result = state.model_dump()

            st.session_state["analysis_result"] = result

        except Exception as exc:
            st.error(f"Pipeline error: {exc}")
            logger.exception("Pipeline error")

# ── Retrieve result ───────────────────────────────────────────────────────────
result = st.session_state.get("analysis_result")

# ── Empty state ───────────────────────────────────────────────────────────────
if result is None:
    st.markdown("<br><br>", unsafe_allow_html=True)
    _, mid, _ = st.columns([1, 2, 1])
    with mid:
        st.markdown("""
        <div style="text-align:center;padding:48px 32px;
                    background:var(--secondary-background-color);
                    border-radius:16px;border:1.5px dashed rgba(148,163,184,0.25)">
            <div style="font-size:3rem">📊</div>
            <h2 style="margin:12px 0 8px 0;font-weight:800">Welcome to AI Data Analyst</h2>
            <p style="opacity:0.65;font-size:1rem;max-width:380px;margin:0 auto;line-height:1.6">
                Upload a CSV file in the sidebar, then click
                <strong>Generate Analysis</strong> to get smart visualisations,
                statistical summaries, and AI-driven insights.
            </p>
        </div>
        """, unsafe_allow_html=True)
    st.stop()

# ── Header ────────────────────────────────────────────────────────────────────
file_name = st.session_state.get("uploaded_file_name", "Dataset")
st.markdown(
    f"<h1 style='font-size:1.7rem;font-weight:800;margin-bottom:4px'>"
    f"Analysis: <code style='font-size:1.4rem'>{file_name}</code></h1>",
    unsafe_allow_html=True,
)

# Warnings banner
errors = result.get("errors", [])
if errors:
    with st.expander("⚠️ Processing warnings", expanded=True):
        for err in errors:
            st.warning(err)

# ── Top metrics row ───────────────────────────────────────────────────────────
stats = result.get("stats_summary", {})
insights = result.get("insights", {})
row_count = stats.get("row_count", 0)
col_count = stats.get("column_count", 0)
missing_cells = stats.get("data_quality", {}).get("missing_cells", 0)
completeness = stats.get("data_quality", {}).get("completeness", 100)
outlier_cols = len(stats.get("outliers", {}))
strong_corrs = len(stats.get("strong_correlations", []))

st.markdown("<br>", unsafe_allow_html=True)
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Rows",             f"{row_count:,}")
c2.metric("Columns",          col_count)
c3.metric("Missing Values",   missing_cells)
c4.metric("Outlier Cols",     outlier_cols)
c5.metric("Correlations",     strong_corrs)
c6.metric("Completeness",     f"{completeness:.1f}%")
st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_summary, tab_charts, tab_insights, tab_stats, tab_data = st.tabs(
    ["📝 Summary", "📊 Charts", "💡 AI Insights", "📈 Statistics", "🔍 Data Preview"]
)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 · SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
with tab_summary:
    if not insights:
        st.warning(
            "⚠️ Summary data not yet available. "
            "Check the **Processing warnings** expander above for error details."
        )
    else:
        # Get executive summary if available
        exec_summary = insights.get("executive_summary", "")
        if exec_summary:
            st.markdown("### Executive Summary")
            st.info(exec_summary)
        
        # Show findings
        st.markdown("### Key Findings")
        findings = insights.get("findings", [])
        if findings:
            for i, finding in enumerate(findings, 1):
                st.markdown(f"**{i}. {finding}**")
        else:
            st.info("No findings generated yet.")
        
        # Show recommendations
        st.markdown("### Recommendations")
        recommendations = insights.get("recommendations", [])
        if recommendations:
            for i, rec in enumerate(recommendations, 1):
                st.markdown(f"✓ {rec}")
        else:
            st.info("No recommendations available.")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 · CHARTS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_charts:
    charts = result.get("charts") or {}
    chart_list = list(charts.values())
    if not chart_list:
        st.info("No visualisations could be generated from this dataset.", icon="ℹ️")
    else:
        # Display the visual dashboard (base64 encoded image)
        if "visual_dashboard" in charts:
            st.image(f"data:image/png;base64,{charts['visual_dashboard']}", width=1000)
        else:
            for chart in chart_list:
                st.image(f"data:image/png;base64,{chart}", width=1000)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 · AI INSIGHTS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_insights:
    insights_data = result.get("insights") or {}
    
    if not insights_data:
        st.warning("No insights generated yet. Please analyze a dataset first.")
    else:
        # Key Findings
        st.markdown("### 🔎 Key Findings")
        findings = insights_data.get("findings", [])
        if findings:
            for finding in findings:
                st.markdown(f"• {finding}")
        else:
            st.info("No findings available.")
        
        st.divider()
        
        # Correlation Insights
        st.markdown("### 📊 Correlation Insights")
        corr_insights = insights_data.get("correlation_insights", [])
        if corr_insights:
            for corr in corr_insights:
                st.markdown(f"• {corr}")
        else:
            st.info("No strong correlations detected.")
        
        st.divider()
        
        # Distribution Insights
        st.markdown("### 📈 Distribution Patterns")
        dist_insights = insights_data.get("distribution_insights", [])
        if dist_insights:
            for dist in dist_insights:
                st.markdown(f"• {dist}")
        else:
            st.info("No distributions analyzed.")
        
        st.divider()
        
        # Outlier Summary
        st.markdown("### ⚠️ Outlier Summary")
        outlier_summary = insights_data.get("outlier_summary", {})
        if outlier_summary:
            for col, summary in outlier_summary.items():
                st.markdown(f"• **{col}**: {summary}")
        else:
            st.info("No outliers detected.")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 · STATISTICS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_stats:
    if not stats:
        st.warning("No statistics available yet.")
    else:
        # Data Overview
        st.markdown("### 📐 Data Overview")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Rows", f"{stats.get('row_count', 0):,}")
        col2.metric("Columns", stats.get('column_count', 0))
        col3.metric("Memory (MB)", f"{stats.get('memory_usage_mb', 0):.2f}")
        completeness = stats.get('data_quality', {}).get('completeness', 100)
        col4.metric("Completeness", f"{completeness:.1f}%")
        
        st.divider()
        
        # Numeric Columns
        numeric_cols = stats.get("numeric_columns", {})
        if numeric_cols:
            st.markdown("### 🔢 Numeric Columns Statistics")
            numeric_stats_list = []
            for col_name, col_stats in numeric_cols.items():
                numeric_stats_list.append({
                    "Column": col_name,
                    "Mean": f"{col_stats.get('mean', 0):.2f}",
                    "Median": f"{col_stats.get('median', 0):.2f}",
                    "Std Dev": f"{col_stats.get('std', 0):.2f}",
                    "Min": f"{col_stats.get('min', 0):.2f}",
                    "Max": f"{col_stats.get('max', 0):.2f}",
                })
            st.dataframe(pd.DataFrame(numeric_stats_list), use_container_width=True)
            st.divider()
        
        # Categorical Columns
        categorical_cols = stats.get("categorical_columns", {})
        if categorical_cols:
            st.markdown("### 📋 Categorical Columns")
            for col_name, col_stats in categorical_cols.items():
                st.markdown(f"**{col_name}**")
                st.markdown(f"Unique Values: {col_stats.get('unique_values', 0)}")
                st.markdown(f"Most Common: {col_stats.get('most_common', 'N/A')} ({col_stats.get('most_common_count', 0)} times)")
                st.markdown(f"Diversity Ratio: {col_stats.get('diversity_ratio', 0):.3f}")
                st.divider()
        
        # Data Quality
        st.markdown("### ✅ Data Quality")
        dq = stats.get('data_quality', {})
        col1, col2, col3 = st.columns(3)
        col1.metric("Missing Cells", dq.get('missing_cells', 0))
        col2.metric("Duplicate Rows", dq.get('duplicate_rows', 0))
        col3.metric("Total Cells", dq.get('total_cells', 0))
        
        # Outliers
        outliers = stats.get("outliers", {})
        if outliers:
            st.divider()
            st.markdown("### ⚠️ Outliers Detected")
            outlier_list = []
            for col, info in outliers.items():
                outlier_list.append({
                    "Column": col,
                    "Count": info.get('count', 0),
                    "Percentage": f"{info.get('percentage', 0):.2f}%",
                    "Lower Bound": f"{info.get('lower_bound', 0):.2f}",
                    "Upper Bound": f"{info.get('upper_bound', 0):.2f}",
                })
            st.dataframe(pd.DataFrame(outlier_list), use_container_width=True)
        
        # Strong Correlations
        strong_corrs = stats.get("strong_correlations", [])
        if strong_corrs:
            st.divider()
            st.markdown("### 🔗 Strong Correlations")
            corr_list = []
            for corr in strong_corrs:
                corr_list.append({
                    "Column 1": corr.get("col1", ""),
                    "Column 2": corr.get("col2", ""),
                    "Correlation": f"{corr.get('correlation', 0):.3f}",
                })
            st.dataframe(pd.DataFrame(corr_list), use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 · DATA PREVIEW
# ═══════════════════════════════════════════════════════════════════════════════
with tab_data:
    raw   = result.get("raw_df")
    clean = result.get("clean_df")
    col_l, col_r = st.columns(2)

    with col_l:
        if raw is not None:
            st.markdown('<p class="section-title">Raw Dataset</p>', unsafe_allow_html=True)
            st.caption("First 100 rows before processing")
            st.dataframe(raw.head(100), use_container_width=True)
            buf = io.BytesIO()
            raw.to_csv(buf, index=False)
            st.download_button("📥 Download Raw CSV", buf.getvalue(), "raw_data.csv", "text/csv")

    with col_r:
        if clean is not None:
            st.markdown('<p class="section-title">Cleaned Dataset</p>', unsafe_allow_html=True)
            st.caption("First 100 rows after processing")
            st.dataframe(clean.head(100), use_container_width=True)
            buf = io.BytesIO()
            clean.to_csv(buf, index=False)
            st.download_button("📥 Download Cleaned CSV", buf.getvalue(), "cleaned_data.csv", "text/csv")