"""
AI Data Analyst · Streamlit frontend — Enhanced UI
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
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&family=Outfit:wght@300;400;500;600&display=swap');

/* ── Reset & Base ── */
html, body, [class*="css"] {
    font-family: 'Outfit', sans-serif;
    background-color: #080c14;
    color: #e2e8f0;
}

.block-container {
    padding-top: 1.5rem;
    max-width: 1400px;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: #080c14;
    border-right: 1px solid rgba(99, 102, 241, 0.15);
}
[data-testid="stSidebar"] * { color: #e2e8f0 !important; }
[data-testid="stSidebar"] .stFileUploader label {
    color: #94a3b8 !important;
    font-size: 0.8rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

/* ── App background ── */
.stApp {
    background: #080c14;
    background-image: radial-gradient(ellipse at 20% 10%, rgba(99,102,241,0.08) 0%, transparent 60%),
                      radial-gradient(ellipse at 80% 80%, rgba(16,185,129,0.05) 0%, transparent 60%);
}

/* ── Metric cards ── */
div[data-testid="stMetric"] {
    background: rgba(15, 23, 42, 0.8);
    border: 1px solid rgba(99, 102, 241, 0.2);
    border-top: 2px solid #6366f1;
    border-radius: 8px;
    padding: 16px 18px;
    transition: border-color 0.2s, transform 0.2s;
    backdrop-filter: blur(8px);
}
div[data-testid="stMetric"]:hover {
    border-top-color: #a5b4fc;
    transform: translateY(-2px);
    box-shadow: 0 8px 24px rgba(99,102,241,0.15);
}
div[data-testid="stMetric"] label {
    font-size: 0.68rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #64748b !important;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    font-family: 'Syne', sans-serif !important;
    font-size: 1.8rem !important;
    font-weight: 800 !important;
    color: #e2e8f0 !important;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    gap: 0;
    border-bottom: 1px solid rgba(99,102,241,0.2);
    background: transparent;
    padding: 0;
}
.stTabs [data-baseweb="tab"] {
    height: 42px;
    padding: 0 22px;
    font-size: 0.82rem;
    font-weight: 500;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: #64748b !important;
    border-bottom: 2px solid transparent;
    border-radius: 0;
    background: transparent;
    transition: all 0.2s;
}
.stTabs [data-baseweb="tab"]:hover {
    color: #a5b4fc !important;
    border-bottom-color: rgba(99,102,241,0.4);
}
.stTabs [aria-selected="true"] {
    color: #6366f1 !important;
    font-weight: 700 !important;
    border-bottom: 2px solid #6366f1 !important;
    background: transparent !important;
}
[data-testid="stTabsContent"] {
    padding-top: 1.5rem;
}

/* ── Buttons ── */
.stButton > button {
    width: 100%;
    border-radius: 6px;
    background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%);
    color: white !important;
    border: none;
    font-family: 'Syne', sans-serif;
    font-weight: 700;
    font-size: 0.85rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 0.65rem 1.2rem;
    transition: all 0.2s;
    box-shadow: 0 4px 15px rgba(99,102,241,0.3);
}
.stButton > button:hover {
    background: linear-gradient(135deg, #818cf8 0%, #6366f1 100%);
    box-shadow: 0 6px 20px rgba(99,102,241,0.45);
    transform: translateY(-1px);
}

/* ── Divider ── */
hr { border-color: rgba(99,102,241,0.15) !important; }

/* ── DataFrames ── */
[data-testid="stDataFrame"] {
    border: 1px solid rgba(99,102,241,0.15);
    border-radius: 8px;
    overflow: hidden;
}

/* ── Expander ── */
[data-testid="stExpander"] {
    border: 1px solid rgba(99,102,241,0.15) !important;
    border-radius: 8px !important;
    background: rgba(15, 23, 42, 0.5) !important;
}

/* ── Custom components ── */
.page-header {
    display: flex;
    align-items: baseline;
    gap: 14px;
    margin-bottom: 1.5rem;
    padding-bottom: 1rem;
    border-bottom: 1px solid rgba(99,102,241,0.15);
}
.page-header .title {
    font-family: 'Syne', sans-serif;
    font-size: 1.6rem;
    font-weight: 800;
    color: #f1f5f9;
    margin: 0;
}
.page-header .subtitle {
    font-size: 0.85rem;
    color: #475569;
    font-weight: 400;
}

.section-label {
    font-family: 'Syne', sans-serif;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: #475569;
    margin-bottom: 10px;
    padding-bottom: 6px;
    border-bottom: 1px solid rgba(99,102,241,0.12);
}

.kpi-card {
    background: rgba(15, 23, 42, 0.7);
    border: 1px solid rgba(99,102,241,0.18);
    border-radius: 8px;
    padding: 14px 16px;
    margin-bottom: 10px;
    transition: border-color 0.2s;
    backdrop-filter: blur(4px);
}
.kpi-card:hover { border-color: rgba(99,102,241,0.4); }
.kpi-card .label {
    font-size: 0.65rem;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #475569;
    margin-bottom: 6px;
}
.kpi-card .value {
    font-family: 'Syne', sans-serif;
    font-size: 1.35rem;
    font-weight: 800;
    color: #e2e8f0;
    line-height: 1.2;
}
.kpi-card .meta {
    font-size: 0.78rem;
    color: #64748b;
    margin-top: 4px;
    font-family: 'JetBrains Mono', monospace;
}
.kpi-card.accent-green  { border-left: 3px solid #10b981; }
.kpi-card.accent-red    { border-left: 3px solid #f43f5e; }
.kpi-card.accent-blue   { border-left: 3px solid #38bdf8; }
.kpi-card.accent-amber  { border-left: 3px solid #f59e0b; }
.kpi-card.accent-purple { border-left: 3px solid #a78bfa; }

.insight-panel {
    background: rgba(15, 23, 42, 0.7);
    border: 1px solid rgba(99,102,241,0.18);
    border-radius: 10px;
    padding: 20px 22px;
    min-height: 240px;
    backdrop-filter: blur(4px);
}
.insight-panel .panel-title {
    font-family: 'Syne', sans-serif;
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    margin-bottom: 16px;
    padding-bottom: 10px;
    border-bottom: 1px solid rgba(99,102,241,0.12);
}
.insight-panel.findings   .panel-title { color: #818cf8; border-bottom-color: rgba(99,102,241,0.25); }
.insight-panel.anomalies  .panel-title { color: #fb7185; border-bottom-color: rgba(244,63,94,0.25); }
.insight-panel.recs       .panel-title { color: #34d399; border-bottom-color: rgba(16,185,129,0.25); }
.insight-panel ul { list-style: none; padding: 0; margin: 0; }
.insight-panel ul li {
    font-size: 0.88rem;
    color: #94a3b8;
    line-height: 1.7;
    padding: 6px 0;
    border-bottom: 1px solid rgba(99,102,241,0.07);
    display: flex;
    gap: 10px;
    align-items: flex-start;
}
.insight-panel ul li::before { content: '→'; color: #4f46e5; flex-shrink: 0; margin-top: 1px; }
.insight-panel ul li:last-child { border-bottom: none; }
.insight-panel .empty-state { color: #334155; font-size: 0.85rem; font-style: italic; }

.exec-summary-box {
    background: linear-gradient(135deg, rgba(99,102,241,0.06) 0%, rgba(16,185,129,0.04) 100%);
    border: 1px solid rgba(99,102,241,0.2);
    border-left: 4px solid #6366f1;
    border-radius: 8px;
    padding: 20px 24px;
    margin-bottom: 24px;
    font-size: 0.96rem;
    line-height: 1.8;
    color: #cbd5e1;
}

.quality-bar-row {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 8px;
}
.quality-bar-label {
    font-size: 0.78rem;
    color: #64748b;
    width: 90px;
    flex-shrink: 0;
}
.quality-bar-track {
    flex: 1;
    height: 6px;
    background: rgba(99,102,241,0.1);
    border-radius: 3px;
    overflow: hidden;
}
.quality-bar-fill {
    height: 100%;
    border-radius: 3px;
    background: linear-gradient(90deg, #6366f1, #10b981);
}
.quality-bar-value {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.75rem;
    color: #94a3b8;
    width: 42px;
    text-align: right;
    flex-shrink: 0;
}

.corr-pill {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    background: rgba(15, 23, 42, 0.7);
    border: 1px solid rgba(99,102,241,0.18);
    border-radius: 6px;
    padding: 10px 14px;
    margin-bottom: 8px;
    width: 100%;
}
.corr-pill .cols { font-size: 0.88rem; color: #e2e8f0; font-weight: 500; flex: 1; }
.corr-pill .cols span { color: #475569; margin: 0 6px; }
.corr-pill .r-val {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
    font-weight: 600;
    padding: 2px 8px;
    border-radius: 4px;
}
.corr-pill .r-val.pos { background: rgba(16,185,129,0.15); color: #34d399; }
.corr-pill .r-val.neg { background: rgba(244,63,94,0.15);  color: #fb7185; }

.sidebar-logo {
    font-family: 'Syne', sans-serif;
    font-size: 1.3rem;
    font-weight: 800;
    color: #f1f5f9;
    letter-spacing: -0.02em;
    margin: 0;
}
.sidebar-tagline { font-size: 0.72rem; color: #334155; margin: 3px 0 0 0; letter-spacing: 0.06em; }

.pipeline-step {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 5px 0;
    font-size: 0.77rem;
    color: #475569;
}
.pipeline-step .dot {
    width: 6px; height: 6px;
    border-radius: 50%;
    background: #6366f1;
    flex-shrink: 0;
}

.warning-block {
    background: rgba(251,191,36,0.07);
    border: 1px solid rgba(251,191,36,0.2);
    border-left: 3px solid #fbbf24;
    border-radius: 6px;
    padding: 10px 14px;
    font-size: 0.83rem;
    color: #fde68a;
    margin-bottom: 8px;
}

.chart-frame {
    background: rgba(15, 23, 42, 0.5);
    border: 1px solid rgba(99,102,241,0.15);
    border-radius: 10px;
    padding: 16px;
    margin-bottom: 16px;
}
.chart-title {
    font-family: 'Syne', sans-serif;
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #475569;
    margin-bottom: 12px;
}

.empty-state-box {
    text-align: center;
    padding: 60px 40px;
    background: rgba(15, 23, 42, 0.4);
    border: 1px dashed rgba(99,102,241,0.2);
    border-radius: 12px;
}
.empty-state-box .icon { font-size: 2.5rem; margin-bottom: 16px; }
.empty-state-box h2 {
    font-family: 'Syne', sans-serif;
    font-size: 1.4rem;
    font-weight: 800;
    color: #e2e8f0;
    margin: 0 0 10px 0;
}
.empty-state-box p { font-size: 0.9rem; color: #475569; max-width: 380px; margin: 0 auto; line-height: 1.7; }

code, .mono { font-family: 'JetBrains Mono', monospace !important; }

/* Streamlit overrides */
[data-testid="stWarning"] {
    background: rgba(251,191,36,0.07) !important;
    border: 1px solid rgba(251,191,36,0.2) !important;
    color: #fde68a !important;
}
[data-testid="stInfo"] {
    background: rgba(56,189,248,0.06) !important;
    border: 1px solid rgba(56,189,248,0.2) !important;
    color: #7dd3fc !important;
}
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
    <div style="padding:20px 4px 12px 4px">
        <p class="sidebar-logo">◈ Data Analyst</p>
        <p class="sidebar-tagline">POWERED BY LANGGRAPH + GROQ</p>
    </div>
    """, unsafe_allow_html=True)
    st.divider()

    uploaded_file = st.file_uploader(
        "Upload Dataset",
        type=["csv"],
        help="CSV files up to 200 MB.",
        label_visibility="visible",
    )

    if uploaded_file is not None:
        current_bytes = uploaded_file.getvalue()
        if st.session_state["uploaded_file_name"] != uploaded_file.name:
            st.session_state["file_bytes"] = current_bytes
            st.session_state["uploaded_file_name"] = uploaded_file.name
            st.session_state["analysis_result"] = None
        elif st.session_state["file_bytes"] is None:
            st.session_state["file_bytes"] = current_bytes

    if st.session_state["uploaded_file_name"]:
        st.markdown(f"""
        <div style="background:rgba(16,185,129,0.08);border:1px solid rgba(16,185,129,0.25);
                    border-radius:6px;padding:8px 12px;margin:8px 0;font-size:0.78rem;color:#34d399;">
            ✓ &nbsp;<code style="color:#6ee7b7;font-size:0.76rem">{st.session_state['uploaded_file_name']}</code>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    has_file = st.session_state["file_bytes"] is not None
    run_clicked = st.button(
        "⚡  Run Analysis",
        type="primary",
        disabled=not has_file,
    )

    st.divider()

    st.markdown("""
    <div style="padding:4px 0">
        <p style="font-family:'Syne',sans-serif;font-size:0.65rem;font-weight:700;
                  letter-spacing:0.14em;text-transform:uppercase;color:#334155;margin-bottom:10px">
            Pipeline
        </p>
        <div class="pipeline-step"><span class="dot"></span> Architect — data cleaning</div>
        <div class="pipeline-step"><span class="dot"></span> Statistician — deep stats</div>
        <div class="pipeline-step"><span class="dot"></span> Visualizer — chart suite</div>
        <div class="pipeline-step"><span class="dot"></span> Summary — executive brief</div>
        <div class="pipeline-step"><span class="dot"></span> Insights — AI strategy</div>
    </div>
    """, unsafe_allow_html=True)

# ── Run pipeline ──────────────────────────────────────────────────────────────
if run_clicked and st.session_state["file_bytes"] is not None:
    with st.spinner("Running analysis pipeline…"):
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
    _, mid, _ = st.columns([1, 2.2, 1])
    with mid:
        st.markdown("""
        <div class="empty-state-box">
            <div class="icon">◈</div>
            <h2>AI Data Analyst</h2>
            <p>Upload a CSV file in the sidebar, then click
            <strong style="color:#818cf8">Run Analysis</strong> to get visualisations,
            statistical breakdowns, and AI-driven strategic insights.</p>
        </div>
        """, unsafe_allow_html=True)
    st.stop()

# ── Header ────────────────────────────────────────────────────────────────────
file_name = st.session_state.get("uploaded_file_name", "Dataset")
st.markdown(f"""
<div class="page-header">
    <p class="title">Analysis</p>
    <code style="font-size:1rem;color:#6366f1;background:rgba(99,102,241,0.1);
                 padding:3px 10px;border-radius:4px">{file_name}</code>
</div>
""", unsafe_allow_html=True)

# ── Warnings ──────────────────────────────────────────────────────────────────
errors = result.get("errors", [])
if errors:
    with st.expander("⚠️  Processing warnings", expanded=False):
        for err in errors:
            st.markdown(f'<div class="warning-block">{err}</div>', unsafe_allow_html=True)

# ── Top metrics ───────────────────────────────────────────────────────────────
stats = result.get("stats_summary", {})
insights = result.get("insights", {})
row_count = stats.get("row_count", 0)
col_count = stats.get("column_count", 0)
missing_cells = stats.get("data_quality", {}).get("missing_cells", 0)
completeness = stats.get("data_quality", {}).get("completeness", 100)
outlier_cols = len(stats.get("outliers", {}))
strong_corrs = len(stats.get("strong_correlations", []))

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Rows",             f"{row_count:,}")
c2.metric("Columns",          col_count)
c3.metric("Missing Values",   missing_cells)
c4.metric("Outlier Cols",     outlier_cols)
c5.metric("Correlations",     strong_corrs)
c6.metric("Completeness",     f"{completeness:.1f}%")
st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_summary, tab_charts, tab_insights, tab_stats, tab_data = st.tabs([
    "SUMMARY", "CHARTS", "AI INSIGHTS", "STATISTICS", "DATA PREVIEW"
])

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
        for i in range(0, len(chart_list), 2):
            row = st.columns(2)
            row[0].plotly_chart(chart_list[i], use_container_width=True)
            if i + 1 < len(chart_list):
                row[1].plotly_chart(chart_list[i + 1], use_container_width=True)
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

    if isinstance(raw, dict):
        try: raw = pd.DataFrame(raw)
        except Exception: raw = None
    if isinstance(clean, dict):
        try: clean = pd.DataFrame(clean)
        except Exception: clean = None

    col_l, col_r = st.columns(2, gap="large")

    with col_l:
        st.markdown('<p class="section-label">Raw Dataset</p>', unsafe_allow_html=True)
        st.caption("First 100 rows before processing")
        if raw is not None:
            st.dataframe(raw.head(100), use_container_width=True)
            buf = io.BytesIO()
            raw.to_csv(buf, index=False)
            st.download_button("↓  Download Raw CSV", buf.getvalue(), "raw_data.csv", "text/csv")
        else:
            st.info("Raw data not available.")

    with col_r:
        st.markdown('<p class="section-label">Cleaned Dataset</p>', unsafe_allow_html=True)
        st.caption("First 100 rows after processing")
        if clean is not None:
            st.dataframe(clean.head(100), use_container_width=True)
            buf = io.BytesIO()
            clean.to_csv(buf, index=False)
            st.download_button("↓  Download Cleaned CSV", buf.getvalue(), "cleaned_data.csv", "text/csv")
        else:
            st.info("Cleaned data not available.")
