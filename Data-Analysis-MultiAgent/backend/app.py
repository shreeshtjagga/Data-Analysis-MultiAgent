"""
AI Data Analyst · Streamlit frontend
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
summ  = result.get("summary") or {}
shape = stats.get("shape") or [0, 0]

st.markdown("<br>", unsafe_allow_html=True)
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Rows",             f"{shape[0]:,}")
c2.metric("Columns",          shape[1])
c3.metric("Missing Values",   sum(stats.get("nulls", {}).values()))
c4.metric("Outlier Cols",     len(stats.get("outliers", {})))
c5.metric("Correlations",     len(stats.get("top_correlations", [])))
c6.metric("Health Score",     f"{summ.get('health_score', '—')}/100")
st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_summary, tab_charts, tab_insights, tab_stats, tab_data = st.tabs(
    ["📝 Summary", "📊 Charts", "💡 AI Insights", "📈 Statistics", "🔍 Data Preview"]
)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 · SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
with tab_summary:
    insights_data = result.get("insights") or {}
    executive_summary = insights_data.get("executive_summary")
    stats = result.get("stats_summary", {})

    if not executive_summary:
        st.warning(
            "⚠️ Summary agent did not return data. "
            "Check the **Processing warnings** expander above for error details. "
            "If no warnings are shown, try clicking **Generate Analysis** again.",
        )
    else:
        # Executive Summary Text
        st.markdown("### 📝 Executive Summary")
        st.markdown(executive_summary)
        st.markdown("<br>", unsafe_allow_html=True)

        col_l, col_r = st.columns(2)

        with col_l:
            st.markdown('<p class="section-title">📐 Dataset Overview</p>', unsafe_allow_html=True)
            ov1, ov2, ov3 = st.columns(3)
            ov1.metric("Rows",            stats.get("row_count", 0))
            ov2.metric("Columns",         stats.get("column_count", 0))
            ov3.metric("Missing Values",  len(stats.get("missing_values", {})))
            ov4, ov5 = st.columns(2)
            ov4.metric("Numeric Cols",    len(stats.get("numeric_columns", {})))
            ov5.metric("Categorical Cols",len(stats.get("categorical_columns", {})))

        with col_r:
            st.markdown('<p class="section-title">🔢 Numeric Highlights</p>', unsafe_allow_html=True)
            numeric_stats = stats.get("numeric_columns", {})
            if numeric_stats:
                for col, info in list(numeric_stats.items())[:3]:
                    st.markdown(f"""
                    <div class="stat-card">
                        <h5>{col}</h5>
                        <div class="val">μ = {info['mean']:.2f}</div>
                        <div class="sub">
                            Min {info['min']:.2f} &nbsp;·&nbsp;
                            Max {info['max']:.2f} &nbsp;·&nbsp;
                            σ {info['std']:.2f}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("No numeric columns found.")

        st.markdown("<br>", unsafe_allow_html=True)
        col_l2, col_r2 = st.columns(2)

        with col_l2:
            cat_stats = stats.get("categorical_columns", {})
            if cat_stats:
                st.markdown('<p class="section-title">🏷️ Top Categories</p>', unsafe_allow_html=True)
                for col, info in list(cat_stats.items())[:3]:
                    st.markdown(f"""
                    <div class="stat-card" style="border-left-color:#f59e0b">
                        <h5>{col}</h5>
                        <div class="val">{info['most_common']}</div>
                        <div class="sub">
                            Top value · {info['unique_values']} unique values
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

        with col_r2:
            strong_corr = stats.get("strong_correlations", [])
            st.markdown('<p class="section-title">🔗 Strong Correlations</p>', unsafe_allow_html=True)
            if strong_corr:
                for pair in strong_corr:
                    r = pair["correlation"]
                    strength  = "Strong" if abs(r) >= 0.8 else "Moderate"
                    direction = "positive" if r > 0 else "negative"
                    color     = "#10b981" if r > 0 else "#f43f5e"
                    st.markdown(f"""
                    <div class="stat-card" style="border-left-color:{color}">
                        <h5>{strength} {direction} correlation</h5>
                        <div class="val">{pair['col1']} ↔ {pair['col2']}</div>
                        <div class="sub">r = {r:.2f}</div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("No strong correlations (|r| > 0.7) found.")

        # Data Quality
        st.markdown("<br>", unsafe_allow_html=True)
        quality = stats.get("data_quality", {})
        if quality:
            st.markdown('<p class="section-title">🏥 Data Quality</p>', unsafe_allow_html=True)
            q1, q2, q3 = st.columns(3)
            q1.metric("Completeness",  f"{quality.get('completeness', 0):.1f}%")
            q2.metric("Duplicate Rows", quality.get("duplicate_rows", 0))
            q3.metric("Missing Cells",  quality.get("missing_cells", 0))

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 · CHARTS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_charts:
    charts = result.get("charts") or {}
    if not charts:
        st.info("No visualisations could be generated from this dataset.", icon="ℹ️")
    else:
        for chart_name, chart_data in charts.items():
            st.markdown(f'<p class="section-title">{chart_name.replace("_", " ").title()}</p>', unsafe_allow_html=True)
            st.image(
                f"data:image/png;base64,{chart_data}",
                use_column_width=True
            )
# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 · AI INSIGHTS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_insights:
    insights_data = result.get("insights") or {}
    c1, c2, c3 = st.columns(3)

    def _insight_card(col, css_cls, icon, title, items):
        bullets = "".join(f"<li>{item}</li>" for item in items) if items else "<li>None detected.</li>"
        col.markdown(f"""
        <div class="insight-card {css_cls}">
            <h4>{icon} {title}</h4>
            <ul>{bullets}</ul>
        </div>
        """, unsafe_allow_html=True)

    _insight_card(c1, "findings",        "🔎", "Key Findings",    insights_data.get("key_findings", []))
    _insight_card(c2, "anomalies",       "🚨", "Anomalies",       insights_data.get("anomalies", []))
    _insight_card(c3, "recommendations", "🎯", "Recommendations", insights_data.get("recommendations", []))

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 · STATISTICS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_stats:
    describe = stats.get("describe", {})
    if describe:
        st.markdown('<p class="section-title">Descriptive Statistics</p>', unsafe_allow_html=True)
        st.dataframe(pd.DataFrame(describe).T, use_container_width=True)
        st.divider()

    col_l, col_r = st.columns(2)

    with col_l:
        outliers = stats.get("outliers", {})
        if outliers:
            st.markdown('<p class="section-title">Outlier Summary</p>', unsafe_allow_html=True)
            st.dataframe(
                pd.DataFrame.from_dict(
                    {c: info["count"] for c, info in outliers.items()},
                    orient="index", columns=["Count"],
                ),
                use_container_width=True,
            )

        dtypes = stats.get("dtypes", {})
        if dtypes:
            st.markdown('<p class="section-title">Column Data Types</p>', unsafe_allow_html=True)
            st.dataframe(
                pd.DataFrame.from_dict(dtypes, orient="index", columns=["Type"]),
                use_container_width=True,
            )

    with col_r:
        top_corr = stats.get("top_correlations", [])
        if top_corr:
            st.markdown('<p class="section-title">Top Correlations (|r| > 0.5)</p>', unsafe_allow_html=True)
            st.dataframe(
                pd.DataFrame(top_corr, columns=["Feature A", "Feature B", "r"]),
                use_container_width=True,
                hide_index=True,
            )

        cat_counts = stats.get("category_counts", {})
        if cat_counts:
            st.markdown('<p class="section-title">Category Value Counts (top 10)</p>', unsafe_allow_html=True)
            for col, counts in list(cat_counts.items())[:3]:
                with st.expander(f"📌 {col}"):
                    st.dataframe(
                        pd.DataFrame.from_dict(counts, orient="index", columns=["Count"]),
                        use_container_width=True,
                    )

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