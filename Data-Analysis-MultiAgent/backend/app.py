"""
AI Data Analyst · Streamlit frontend — Enhanced UI
Pipeline: Architect → Statistician → Visualizer → Summary → Insights
"""
import io
import logging

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
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
insights_data = result.get("insights") or {}

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Rows",          f"{stats.get('row_count', 0):,}")
c2.metric("Columns",       stats.get("column_count", 0))
c3.metric("Missing Vals",  sum(stats.get("missing_values", {}).values()))
c4.metric("Outlier Cols",  len(stats.get("outliers", {})))
c5.metric("Strong Corrs",  len(stats.get("strong_correlations", [])))
quality = stats.get("data_quality", {})
c6.metric("Completeness",  f"{quality.get('completeness', 0):.1f}%")

st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_summary, tab_charts, tab_insights, tab_stats, tab_data = st.tabs([
    "SUMMARY", "CHARTS", "AI INSIGHTS", "STATISTICS", "DATA PREVIEW"
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 · SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
with tab_summary:
    executive_summary = insights_data.get("executive_summary")

    if not executive_summary:
        st.warning("Summary agent did not return data. Check processing warnings or re-run.")
    else:
        # ── Executive Summary callout ──────────────────────────────────────
        # FIX: executive_summary is plain prose from the LLM — safe to inject.
        # Escape only angle brackets to prevent any accidental tag interpretation.
        safe_summary = executive_summary.replace("<", "&lt;").replace(">", "&gt;")
        st.markdown(f'<div class="exec-summary-box">{safe_summary}</div>', unsafe_allow_html=True)

        col_ov, col_q = st.columns([1.1, 1], gap="large")

        with col_ov:
            st.markdown('<p class="section-label">Dataset At a Glance</p>', unsafe_allow_html=True)
            r1, r2, r3 = st.columns(3)
            r1.metric("Rows",        f"{stats.get('row_count', 0):,}")
            r2.metric("Columns",     stats.get("column_count", 0))
            r3.metric("Missing",     len(stats.get("missing_values", {})))
            st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
            r4, r5, r6 = st.columns(3)
            r4.metric("Numeric",     len(stats.get("numeric_columns", {})))
            r5.metric("Categorical", len(stats.get("categorical_columns", {})))
            r6.metric("Outlier Cols",len(stats.get("outliers", {})))

        with col_q:
            if quality:
                st.markdown('<p class="section-label">Data Quality</p>', unsafe_allow_html=True)
                completeness = quality.get("completeness", 0)
                total = stats.get("row_count", 1)
                dupe = quality.get("duplicate_rows", 0)
                dupe_pct = 100 - ((dupe / total) * 100) if total > 0 else 100

                for label, val, color in [
                    ("Completeness",  completeness, "#6366f1"),
                    ("No Duplicates", dupe_pct,     "#10b981"),
                ]:
                    bar_color = color if val >= 80 else ("#f59e0b" if val >= 60 else "#f43f5e")
                    st.markdown(f"""
                    <div class="quality-bar-row">
                        <span class="quality-bar-label">{label}</span>
                        <div class="quality-bar-track">
                            <div class="quality-bar-fill"
                                 style="width:{min(val,100):.1f}%;background:{bar_color}"></div>
                        </div>
                        <span class="quality-bar-value">{val:.1f}%</span>
                    </div>
                    """, unsafe_allow_html=True)

                st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
                qa, qb, qc = st.columns(3)
                qa.metric("Duplicate Rows",  quality.get("duplicate_rows", 0))
                qb.metric("Missing Cells",   quality.get("missing_cells", 0))
                qc.metric("Total Cells",     f"{quality.get('total_cells', 0):,}")

        st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

        col_num, col_corr = st.columns([1.2, 1], gap="large")

        with col_num:
            numeric_stats = stats.get("numeric_columns", {})
            if numeric_stats:
                st.markdown('<p class="section-label">Numeric Column Breakdown</p>', unsafe_allow_html=True)
                for col_name, info in list(numeric_stats.items())[:5]:
                    skew = info.get("skewness", 0)
                    skew_label = "right-skewed" if skew > 1 else ("left-skewed" if skew < -1 else "normal")
                    skew_color = "#f59e0b" if abs(skew) > 1 else "#10b981"
                    iqr = info.get("iqr", 0)
                    spread = min((iqr / (info["max"] - info["min"] + 1e-9)) * 100, 100) if info["max"] != info["min"] else 0
                    st.markdown(f"""
                    <div class="kpi-card accent-purple" style="padding:12px 16px">
                        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
                            <span class="label" style="margin:0">{col_name}</span>
                            <span style="font-size:0.68rem;padding:2px 8px;border-radius:4px;
                                         background:rgba(99,102,241,0.1);color:{skew_color}">
                                {skew_label}
                            </span>
                        </div>
                        <div style="display:flex;align-items:baseline;gap:14px">
                            <div>
                                <span style="font-size:0.68rem;color:#475569;text-transform:uppercase;
                                             letter-spacing:0.08em">mean</span>
                                <span class="value" style="font-size:1.2rem;margin-left:5px">{info['mean']:.2f}</span>
                            </div>
                            <div>
                                <span style="font-size:0.68rem;color:#475569;text-transform:uppercase;
                                             letter-spacing:0.08em">median</span>
                                <span style="font-family:'Syne',sans-serif;font-size:1.0rem;
                                             font-weight:700;color:#94a3b8;margin-left:5px">{info['median']:.2f}</span>
                            </div>
                        </div>
                        <div class="meta" style="margin-top:6px">
                            σ {info['std']:.2f} &nbsp;·&nbsp;
                            [{info['min']:.2f} – {info['max']:.2f}] &nbsp;·&nbsp;
                            IQR {iqr:.2f}
                        </div>
                        <div style="margin-top:8px;height:3px;background:rgba(99,102,241,0.1);
                                    border-radius:2px;overflow:hidden">
                            <div style="width:{spread:.0f}%;height:100%;
                                        background:linear-gradient(90deg,#6366f1,#a78bfa)"></div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

        with col_corr:
            strong_corr = stats.get("strong_correlations", [])
            if strong_corr:
                st.markdown('<p class="section-label">Strong Correlations</p>', unsafe_allow_html=True)
                for pair in strong_corr[:6]:
                    r = pair["correlation"]
                    cls = "pos" if r > 0 else "neg"
                    bar_w = int(abs(r) * 100)
                    bar_col = "#10b981" if r > 0 else "#f43f5e"
                    st.markdown(f"""
                    <div class="corr-pill" style="flex-direction:column;align-items:stretch;gap:6px">
                        <div style="display:flex;justify-content:space-between;align-items:center">
                            <span class="cols" style="font-size:0.83rem">
                                {pair['col1']}<span style="color:#334155;margin:0 6px">↔</span>{pair['col2']}
                            </span>
                            <span class="r-val {cls}">r = {r:.2f}</span>
                        </div>
                        <div style="height:3px;background:rgba(99,102,241,0.1);
                                    border-radius:2px;overflow:hidden">
                            <div style="width:{bar_w}%;height:100%;
                                        background:{bar_col};opacity:0.7"></div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.markdown('<p class="section-label">Strong Correlations</p>', unsafe_allow_html=True)
                st.markdown('<p style="color:#334155;font-size:0.85rem;font-style:italic">No strong correlations (|r| > 0.7) found.</p>', unsafe_allow_html=True)

            outliers_dict = stats.get("outliers", {})
            if outliers_dict:
                st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
                st.markdown('<p class="section-label">Outlier Hotspots</p>', unsafe_allow_html=True)
                for col_name, ov in list(outliers_dict.items())[:4]:
                    pct = ov.get("percentage", 0)
                    severity_color = "#f43f5e" if pct > 10 else ("#f59e0b" if pct > 5 else "#6366f1")
                    st.markdown(f"""
                    <div class="kpi-card" style="border-left-color:{severity_color};padding:10px 14px;margin-bottom:8px">
                        <div style="display:flex;justify-content:space-between;align-items:center">
                            <span class="label" style="margin:0">{col_name}</span>
                            <span style="font-family:'JetBrains Mono',monospace;font-size:0.75rem;
                                         color:{severity_color};font-weight:600">{pct:.1f}%</span>
                        </div>
                        <div class="meta">{ov['count']} outlier rows · bounds [{ov['lower_bound']:.2f}, {ov['upper_bound']:.2f}]</div>
                    </div>
                    """, unsafe_allow_html=True)

        st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

        # ── Categorical breakdown ──────────────────────────────────────────
        # FIX: bars_html was built inside a nested f-string, causing Streamlit's
        # HTML sanitizer to escape the injected markup. Solution: build the entire
        # card as a plain string concatenation, then call st.markdown once per card.
        cat_stats = stats.get("categorical_columns", {})
        if cat_stats:
            st.markdown('<p class="section-label">Categorical Breakdown</p>', unsafe_allow_html=True)
            cat_col_list = list(cat_stats.items())[:4]
            cat_grid = st.columns(min(len(cat_col_list), 4), gap="small")
            total_rows = stats.get("row_count", 1)

            for i, (col_name, info) in enumerate(cat_col_list):
                top_pct = (info["most_common_count"] / total_rows) * 100
                diversity = info.get("diversity_ratio", 0)
                top5 = info.get("top_5_values", {})

                # Build bar rows as a list then join — avoids nested f-string interpolation
                bar_rows = []
                for val, cnt in list(top5.items())[:4]:
                    bar_pct = min((cnt / total_rows) * 100, 100)
                    bar_rows.append(
                        '<div style="margin-bottom:5px">'
                        '<div style="display:flex;justify-content:space-between;'
                        'font-size:0.72rem;color:#64748b;margin-bottom:2px">'
                        '<span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:70%">'
                        + str(val) +
                        '</span>'
                        '<span style="color:#94a3b8;font-family:\'JetBrains Mono\',monospace">'
                        + f"{bar_pct:.0f}%" +
                        '</span></div>'
                        '<div style="height:3px;background:rgba(99,102,241,0.1);'
                        'border-radius:2px;overflow:hidden">'
                        '<div style="width:' + f"{bar_pct:.0f}" + '%;height:100%;'
                        'background:#f59e0b;opacity:0.7"></div>'
                        '</div></div>'
                    )

                card = (
                    '<div class="kpi-card accent-amber" style="padding:14px 16px">'
                    '<div class="label">' + str(col_name) + '</div>'
                    '<div class="value" style="font-size:1.1rem;margin-bottom:2px;'
                    'overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'
                    + str(info["most_common"]) +
                    '</div>'
                    '<div class="meta" style="margin-bottom:12px">'
                    + f"{info['unique_values']} unique · {top_pct:.0f}% top value · diversity {diversity:.2f}" +
                    '</div>'
                    + "".join(bar_rows) +
                    '</div>'
                )

                with cat_grid[i]:
                    st.markdown(card, unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 · CHARTS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_charts:
    charts = result.get("charts") or {}
    if not charts:
        st.info("No visualisations could be generated from this dataset.")
    else:
        for chart_name, chart_data in charts.items():
            label = chart_name.replace("_", " ").title()
            st.markdown(f"""
            <div class="chart-frame">
                <div class="chart-title">{label}</div>
            """, unsafe_allow_html=True)
            st.image(
                f"data:image/png;base64,{chart_data}",
                use_column_width=True,
            )
            st.markdown("</div>", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 · AI INSIGHTS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_insights:
    key_findings    = insights_data.get("key_findings", [])
    anomalies       = insights_data.get("anomalies", [])
    recommendations = insights_data.get("recommendations", [])

    has_structured = any([key_findings, anomalies, recommendations])

    if not has_structured:
        st.warning("Insights agent did not return data. Check processing warnings or re-run.")
    else:
        # ── Three-panel layout ────────────────────────────────────────────
        # FIX: removed strategic report block — st.markdown cannot render
        # markdown content inside an HTML div opened/closed across separate calls.
        # The structured cards below carry the full story.
        c1, c2, c3 = st.columns(3, gap="medium")

        def _panel(col, css_cls, icon, title, items):
            bullets = "".join(f"<li>{item}</li>" for item in items) if items else \
                      '<li class="empty-state">None detected.</li>'
            col.markdown(f"""
            <div class="insight-panel {css_cls}">
                <div class="panel-title">{icon}&nbsp;&nbsp;{title}</div>
                <ul>{bullets}</ul>
            </div>
            """, unsafe_allow_html=True)

        _panel(c1, "findings",  "◈", "Key Findings",    key_findings)
        _panel(c2, "anomalies", "⚑", "Anomalies",       anomalies)
        _panel(c3, "recs",      "→", "Recommendations", recommendations)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 · STATISTICS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_stats:
    col_l, col_r = st.columns(2, gap="large")

    with col_l:
        numeric_stats = stats.get("numeric_columns", {})
        if numeric_stats:
            st.markdown('<p class="section-label">Numeric Column Stats</p>', unsafe_allow_html=True)
            rows = []
            for col_name, info in numeric_stats.items():
                rows.append({
                    "Column": col_name,
                    "Mean": f"{info['mean']:.3f}",
                    "Median": f"{info['median']:.3f}",
                    "Std": f"{info['std']:.3f}",
                    "Min": f"{info['min']:.3f}",
                    "Max": f"{info['max']:.3f}",
                    "Skew": f"{info['skewness']:.3f}",
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        outliers = stats.get("outliers", {})
        if outliers:
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown('<p class="section-label">Outlier Summary</p>', unsafe_allow_html=True)
            rows = [{
                "Column": c,
                "Count": v["count"],
                "Pct (%)": f"{v['percentage']:.2f}",
                "Lower Bound": f"{v['lower_bound']:.3f}",
                "Upper Bound": f"{v['upper_bound']:.3f}",
            } for c, v in outliers.items()]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    with col_r:
        dtypes = stats.get("dtypes", {})
        if dtypes:
            st.markdown('<p class="section-label">Column Data Types</p>', unsafe_allow_html=True)
            st.dataframe(
                pd.DataFrame.from_dict(dtypes, orient="index", columns=["Type"]),
                use_container_width=True,
            )

        cat_stats = stats.get("categorical_columns", {})
        if cat_stats:
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown('<p class="section-label">Categorical Columns</p>', unsafe_allow_html=True)
            rows = [{
                "Column": c,
                "Unique": v["unique_values"],
                "Most Common": v["most_common"],
                "Count": v["most_common_count"],
                "Diversity": f"{v['diversity_ratio']:.3f}",
            } for c, v in cat_stats.items()]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        strong_corr = stats.get("strong_correlations", [])
        if strong_corr:
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown('<p class="section-label">Strong Correlations (|r| > 0.7)</p>', unsafe_allow_html=True)
            st.dataframe(
                pd.DataFrame(strong_corr),
                use_container_width=True,
                hide_index=True,
            )

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
