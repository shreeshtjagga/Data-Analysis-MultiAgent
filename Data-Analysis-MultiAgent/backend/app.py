"""
AI Data Analyst  —  Streamlit Frontend
"""

import io
import logging
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from core.graph import build_graph

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="AI Data Analyst",
    page_icon="chart_with_upwards_trend",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'Plus Jakarta Sans', sans-serif;
}

/* ─── Sidebar ─────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: #060d1a !important;
    border-right: 1px solid rgba(124,58,237,0.18) !important;
}
[data-testid="stSidebar"] * { color: #c4cfe3 !important; }
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 { color: #f1f5f9 !important; }

/* ─── Page ─────────────────────────────────────────────── */
.block-container {
    padding-top: 2rem;
    padding-bottom: 3rem;
    max-width: 1380px;
}

/* ─── Metric cards ─────────────────────────────────────── */
div[data-testid="stMetric"] {
    background: #0c1428;
    border: 1px solid rgba(124,58,237,0.22);
    border-top: 3px solid #7C3AED;
    border-radius: 12px;
    padding: 18px 20px;
    transition: border-color 0.25s, box-shadow 0.25s;
}
div[data-testid="stMetric"]:hover {
    border-color: rgba(124,58,237,0.55);
    box-shadow: 0 0 18px rgba(124,58,237,0.15);
}
div[data-testid="stMetric"] label {
    font-size: 0.68rem !important;
    font-weight: 700 !important;
    letter-spacing: 0.11em;
    text-transform: uppercase;
    color: #4f6080 !important;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    font-family: 'Space Grotesk', sans-serif !important;
    font-size: 1.9rem !important;
    font-weight: 700 !important;
    color: #e8f0ff !important;
}

/* ─── Tab bar ──────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    gap: 2px;
    background: transparent;
    border-bottom: 1px solid rgba(124,58,237,0.18);
    padding-bottom: 0;
}
.stTabs [data-baseweb="tab"] {
    height: 40px;
    padding: 0 22px;
    font-weight: 500;
    font-size: 0.85rem;
    letter-spacing: 0.025em;
    color: #4f6080;
    background: transparent;
    border: none;
    border-radius: 8px 8px 0 0;
    transition: color 0.2s, background 0.2s;
}
.stTabs [data-baseweb="tab"]:hover { color: #94a3b8; }
.stTabs [aria-selected="true"] {
    color: #c4b5fd !important;
    font-weight: 700 !important;
    background: rgba(124,58,237,0.1) !important;
    border-bottom: 2px solid #7C3AED !important;
}

/* ─── Run button ───────────────────────────────────────── */
.stButton > button {
    width: 100%;
    border-radius: 9px;
    background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 100%);
    color: #fff !important;
    border: none;
    font-family: 'Space Grotesk', sans-serif;
    font-weight: 700;
    font-size: 0.9rem;
    padding: 0.62rem 1.2rem;
    letter-spacing: 0.04em;
    box-shadow: 0 4px 18px rgba(124,58,237,0.38);
    transition: opacity 0.2s, transform 0.15s;
}
.stButton > button:hover {
    opacity: 0.88;
    transform: translateY(-1px);
}
.stButton > button:disabled { opacity: 0.35; box-shadow: none; }

/* ─── Page title ───────────────────────────────────────── */
.page-title {
    font-family: 'Space Grotesk', sans-serif;
    font-size: 1.75rem;
    font-weight: 700;
    color: #e8f0ff;
    margin-bottom: 2px;
    letter-spacing: -0.01em;
}
.page-title span { color: #7C3AED; }

/* ─── Section label ────────────────────────────────────── */
.sec-label {
    font-size: 0.68rem;
    font-weight: 700;
    letter-spacing: 0.13em;
    text-transform: uppercase;
    color: #7C3AED;
    margin-bottom: 12px;
    padding-bottom: 7px;
    border-bottom: 1px solid rgba(124,58,237,0.18);
}

/* ─── Health badge ─────────────────────────────────────── */
.hbadge {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 7px 18px 7px 14px;
    border-radius: 999px;
    font-family: 'Space Grotesk', sans-serif;
    font-weight: 700;
    font-size: 0.88rem;
    letter-spacing: 0.02em;
}
.hbadge-great { background: rgba(5,150,105,0.12); color: #6ee7b7;
                border: 1px solid rgba(5,150,105,0.28); }
.hbadge-ok    { background: rgba(217,119,6,0.12);  color: #fcd34d;
                border: 1px solid rgba(217,119,6,0.28); }
.hbadge-poor  { background: rgba(220,38,38,0.12);  color: #fca5a5;
                border: 1px solid rgba(220,38,38,0.28); }

/* ─── Bullet summary box ───────────────────────────────── */
.bullet-box {
    background: #0c1428;
    border: 1px solid rgba(124,58,237,0.2);
    border-left: 4px solid #7C3AED;
    border-radius: 12px;
    padding: 24px 28px;
}
.bullet-item {
    display: flex;
    gap: 12px;
    align-items: flex-start;
    margin-bottom: 13px;
    font-size: 0.97rem;
    color: #cbd5e1;
    line-height: 1.65;
}
.bullet-item:last-child { margin-bottom: 0; }
.bdot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: #7C3AED;
    margin-top: 8px;
    flex-shrink: 0;
}

/* ─── Mini stat card ───────────────────────────────────── */
.mc {
    background: #0c1428;
    border: 1px solid rgba(148,163,184,0.1);
    border-left: 3px solid #7C3AED;
    border-radius: 10px;
    padding: 13px 16px;
    margin-bottom: 9px;
}
.mc-lbl {
    font-size: 0.67rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: #3d506e;
    margin-bottom: 3px;
}
.mc-val {
    font-family: 'Space Grotesk', sans-serif;
    font-size: 1.2rem;
    font-weight: 700;
    color: #e2e8f0;
}
.mc-sub { font-size: 0.78rem; color: #4f6080; margin-top: 2px; }

/* ─── Insight card ─────────────────────────────────────── */
.ic {
    background: #0c1428;
    border: 1px solid rgba(148,163,184,0.1);
    border-radius: 12px;
    padding: 20px 22px;
    min-height: 230px;
}
.ic-head {
    font-family: 'Space Grotesk', sans-serif;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.13em;
    text-transform: uppercase;
    margin-bottom: 14px;
    padding-bottom: 9px;
    border-bottom: 2px solid;
}
.ic-findings  .ic-head { color: #818cf8; border-color: #4f46e5; }
.ic-anomalies .ic-head { color: #f87171; border-color: #dc2626; }
.ic-recs      .ic-head { color: #34d399; border-color: #059669; }

.ic-item {
    display: flex;
    gap: 10px;
    align-items: flex-start;
    margin-bottom: 11px;
    font-size: 0.88rem;
    color: #8899b4;
    line-height: 1.6;
}
.ic-dot {
    width: 5px;
    height: 5px;
    border-radius: 50%;
    margin-top: 7px;
    flex-shrink: 0;
}
.ic-findings  .ic-dot { background: #4f46e5; }
.ic-anomalies .ic-dot { background: #dc2626; }
.ic-recs      .ic-dot { background: #059669; }

/* ─── Welcome screen ───────────────────────────────────── */
.welcome {
    text-align: center;
    padding: 72px 40px;
    background: #0c1428;
    border-radius: 18px;
    border: 1px solid rgba(124,58,237,0.18);
}
.welcome h2 {
    font-family: 'Space Grotesk', sans-serif;
    font-size: 2rem;
    font-weight: 700;
    color: #e8f0ff;
    margin: 16px 0 10px;
}
.welcome p { color: #4f6080; font-size: 1rem; max-width: 420px;
             margin: 0 auto; line-height: 1.7; }

code, pre { font-family: 'JetBrains Mono', monospace !important; }
</style>
""", unsafe_allow_html=True)

# ── Session state ──────────────────────────────────────────────────────────────
for k, v in {"analysis_result": None, "uploaded_file_name": None, "file_bytes": None}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:20px 0 14px">
        <div style="font-family:'Space Grotesk',sans-serif; font-size:1.5rem;
                    font-weight:700; color:#e8f0ff; line-height:1.15">
            AI Data Analyst
        </div>
        <div style="font-size:0.7rem; color:#3d506e; letter-spacing:0.1em;
                    text-transform:uppercase; margin-top:5px">
            Multi-Agent Analysis Pipeline
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

    if uploaded_file is not None:
        b = uploaded_file.getvalue()
        if st.session_state["uploaded_file_name"] != uploaded_file.name:
            st.session_state.update({
                "file_bytes": b,
                "uploaded_file_name": uploaded_file.name,
                "analysis_result": None,
            })
        elif st.session_state["file_bytes"] is None:
            st.session_state["file_bytes"] = b

    if st.session_state["uploaded_file_name"]:
        st.success(st.session_state["uploaded_file_name"])

    st.markdown("<br>", unsafe_allow_html=True)
    has_file    = st.session_state["file_bytes"] is not None
    run_clicked = st.button("Run Analysis", type="primary", disabled=not has_file)

    st.divider()
    st.markdown('<div class="sec-label">Pipeline steps</div>', unsafe_allow_html=True)
    pipeline_steps = [
        ("Architect",    "Cleans and prepares data"),
        ("Statistician", "Computes statistics"),
        ("Visualizer",   "Builds relevant charts"),
        ("Summary",      "Explains in plain English"),
        ("Insights",     "Gives recommendations"),
    ]
    for name, desc in pipeline_steps:
        st.markdown(f"""
        <div style="padding:7px 0; border-bottom:1px solid rgba(124,58,237,0.08)">
            <div style="font-size:0.8rem; font-weight:600; color:#8899b4">{name}</div>
            <div style="font-size:0.73rem; color:#3d506e">{desc}</div>
        </div>
        """, unsafe_allow_html=True)

# ── Run pipeline ───────────────────────────────────────────────────────────────
if run_clicked and st.session_state["file_bytes"] is not None:
    with st.spinner("Running analysis pipeline..."):
        try:
            df     = pd.read_csv(io.BytesIO(st.session_state["file_bytes"]))
            result = build_graph().invoke({"df": df})
            st.session_state["analysis_result"] = result
        except Exception as exc:
            st.error(f"Pipeline error: {exc}")
            logger.exception("Pipeline error")

result = st.session_state.get("analysis_result")

# ── Welcome ────────────────────────────────────────────────────────────────────
if result is None:
    st.markdown("<br><br>", unsafe_allow_html=True)
    _, mid, _ = st.columns([1, 2, 1])
    with mid:
        st.markdown("""
        <div class="welcome">
            <div style="font-size:3rem">chart_with_upwards_trend</div>
            <h2>Welcome to AI Data Analyst</h2>
            <p>Upload any CSV file in the sidebar, click <strong>Run Analysis</strong>,
               and get charts, statistics, and plain-English explanations
               — no data skills required.</p>
        </div>
        """, unsafe_allow_html=True)
    st.stop()

# ── Header ─────────────────────────────────────────────────────────────────────
fname = st.session_state.get("uploaded_file_name", "Dataset")
st.markdown(
    f'<div class="page-title">Analysis: <span><code>{fname}</code></span></div>',
    unsafe_allow_html=True,
)

errors = result.get("errors", [])
if errors:
    with st.expander("Processing warnings", expanded=False):
        for e in errors:
            st.warning(e)

# ── Top metrics ────────────────────────────────────────────────────────────────
stats = result.get("stats_summary", {})
summ  = result.get("summary") or {}
shape = stats.get("shape") or [0, 0]

st.markdown("<br>", unsafe_allow_html=True)
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Rows",           f"{shape[0]:,}")
c2.metric("Columns",        shape[1])
c3.metric("Missing Values", sum(stats.get("nulls", {}).values()))
c4.metric("Outlier Cols",   len(stats.get("outliers", {})))
c5.metric("Correlations",   len(stats.get("top_correlations", [])))
c6.metric("Health Score",   f"{summ.get('health_score', '--')}/100")
st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab_summary, tab_charts, tab_insights, tab_stats, tab_data = st.tabs([
    "Summary",
    "Charts",
    "AI Insights",
    "Statistics",
    "Data Preview",
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
with tab_summary:
    if not summ:
        st.warning("Summary agent returned no data. Check warnings above and try again.")
    else:
        hs = summ.get("health_score", 0)
        if hs >= 75:
            bc, bl = "hbadge-great", f"Data quality: Great  ({hs}/100)"
        elif hs >= 50:
            bc, bl = "hbadge-ok",    f"Data quality: Fair  ({hs}/100)"
        else:
            bc, bl = "hbadge-poor",  f"Data quality: Needs attention  ({hs}/100)"

        col_b, _ = st.columns([3, 7])
        col_b.markdown(f'<span class="hbadge {bc}">{bl}</span>', unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

        # Plain-English bullet summary
        st.markdown('<div class="sec-label">What this data tells you</div>',
                    unsafe_allow_html=True)
        bullets = summ.get("bullets", [])
        if bullets:
            items_html = "".join(
                f'<div class="bullet-item"><div class="bdot"></div>'
                f'<div>{b}</div></div>'
                for b in bullets
            )
            st.markdown(f'<div class="bullet-box">{items_html}</div>',
                        unsafe_allow_html=True)
        else:
            st.info("No summary available.")

        st.markdown("<br>", unsafe_allow_html=True)

        # Quick-look cards
        col_l, col_r = st.columns(2)

        with col_l:
            st.markdown('<div class="sec-label">Dataset overview</div>',
                        unsafe_allow_html=True)
            a1, a2, a3 = st.columns(3)
            a1.metric("Rows",    f"{summ.get('rows', 0):,}")
            a2.metric("Columns", summ.get("cols", 0))
            a3.metric("Missing", f"{summ.get('missing_rate_pct', 0)}%")
            b1, b2, b3 = st.columns(3)
            b1.metric("Numbers", len(summ.get("numeric_cols", [])))
            b2.metric("Text",    len(summ.get("cat_cols",     [])))
            b3.metric("Dates",   len(summ.get("date_cols",    [])))

            dr = summ.get("date_range")
            if dr:
                st.markdown(f"""
                <div class="mc" style="border-left-color:#0891B2; margin-top:12px">
                    <div class="mc-lbl">Date range — {dr['column']}</div>
                    <div class="mc-val">{dr['from']} to {dr['to']}</div>
                    <div class="mc-sub">Spans {dr['span_days']:,} days</div>
                </div>
                """, unsafe_allow_html=True)

        with col_r:
            st.markdown('<div class="sec-label">Numeric highlights</div>',
                        unsafe_allow_html=True)
            highlights = summ.get("highlights", [])
            if highlights:
                for h in highlights:
                    st.markdown(f"""
                    <div class="mc">
                        <div class="mc-lbl">{h['column']}</div>
                        <div class="mc-val">avg {h['mean']:,}</div>
                        <div class="mc-sub">
                            min {h['min']:,} &nbsp;&middot;&nbsp;
                            max {h['max']:,} &nbsp;&middot;&nbsp;
                            spread {h['std']:,}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("No numeric columns found.")

        st.markdown("<br>", unsafe_allow_html=True)
        col_ll, col_rr = st.columns(2)

        with col_ll:
            top_cats = summ.get("top_categories", {})
            if top_cats:
                st.markdown('<div class="sec-label">Top categories</div>',
                            unsafe_allow_html=True)
                for col, info in top_cats.items():
                    st.markdown(f"""
                    <div class="mc" style="border-left-color:#D97706">
                        <div class="mc-lbl">{col}</div>
                        <div class="mc-val">{info['top_value']}</div>
                        <div class="mc-sub">
                            {info['top_pct']}% of rows &nbsp;&middot;&nbsp;
                            {info['unique']} unique values
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

        with col_rr:
            top_corr = summ.get("top_correlations", [])
            st.markdown('<div class="sec-label">Strong relationships</div>',
                        unsafe_allow_html=True)
            if top_corr:
                for item in top_corr:
                    a, b, r = item
                    strength  = "Strong" if abs(r) >= 0.8 else "Moderate"
                    direction = "positive" if r > 0 else "negative"
                    color     = "#059669" if r > 0 else "#DC2626"
                    st.markdown(f"""
                    <div class="mc" style="border-left-color:{color}">
                        <div class="mc-lbl">{strength} {direction} relationship</div>
                        <div class="mc-val">{a} and {b}</div>
                        <div class="mc-sub">strength: {r:.2f}</div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("No strong relationships found.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — CHARTS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_charts:
    charts     = result.get("charts") or {}
    chart_list = list(charts.values())
    if not chart_list:
        st.info("No charts could be generated for this dataset.")
    else:
        for i in range(0, len(chart_list), 2):
            row = st.columns(2)
            row[0].plotly_chart(chart_list[i],     use_container_width=True)
            if i + 1 < len(chart_list):
                row[1].plotly_chart(chart_list[i + 1], use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — AI INSIGHTS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_insights:
    ins = result.get("insights") or {}
    st.markdown("<br>", unsafe_allow_html=True)
    ci1, ci2, ci3 = st.columns(3)

    def _insight_card(col, css_cls, title, items):
        rows_html = "".join(
            f'<div class="ic-item"><div class="ic-dot"></div><div>{x}</div></div>'
            for x in (items or ["Nothing detected."])
        )
        col.markdown(
            f'<div class="ic {css_cls}">'
            f'<div class="ic-head">{title}</div>'
            f'{rows_html}</div>',
            unsafe_allow_html=True,
        )

    _insight_card(ci1, "ic-findings",  "Key Findings",    ins.get("key_findings",    []))
    _insight_card(ci2, "ic-anomalies", "Anomalies",       ins.get("anomalies",       []))
    _insight_card(ci3, "ic-recs",      "Recommendations", ins.get("recommendations", []))


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — STATISTICS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_stats:
    describe = stats.get("describe", {})
    if describe:
        st.markdown('<div class="sec-label">Descriptive statistics</div>',
                    unsafe_allow_html=True)
        st.dataframe(pd.DataFrame(describe).T, use_container_width=True)
        st.divider()

    sl, sr = st.columns(2)

    with sl:
        outliers = stats.get("outliers", {})
        if outliers:
            st.markdown('<div class="sec-label">Outlier summary</div>',
                        unsafe_allow_html=True)
            st.dataframe(
                pd.DataFrame.from_dict(
                    {c: info["count"] for c, info in outliers.items()},
                    orient="index", columns=["Count"],
                ),
                use_container_width=True,
            )
        dtypes = stats.get("dtypes", {})
        if dtypes:
            st.markdown('<div class="sec-label">Column data types</div>',
                        unsafe_allow_html=True)
            st.dataframe(
                pd.DataFrame.from_dict(dtypes, orient="index", columns=["Type"]),
                use_container_width=True,
            )

    with sr:
        top_corr = stats.get("top_correlations", [])
        if top_corr:
            st.markdown('<div class="sec-label">Top correlations</div>',
                        unsafe_allow_html=True)
            st.dataframe(
                pd.DataFrame(top_corr, columns=["Column A", "Column B", "Strength"]),
                use_container_width=True,
                hide_index=True,
            )
        cat_counts = stats.get("category_counts", {})
        if cat_counts:
            st.markdown('<div class="sec-label">Category value counts</div>',
                        unsafe_allow_html=True)
            for col, counts in list(cat_counts.items())[:3]:
                with st.expander(col):
                    st.dataframe(
                        pd.DataFrame.from_dict(counts, orient="index", columns=["Count"]),
                        use_container_width=True,
                    )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — DATA PREVIEW
# ═══════════════════════════════════════════════════════════════════════════════
with tab_data:
    raw   = result.get("raw_df")
    clean = result.get("clean_df")
    dl, dr = st.columns(2)

    with dl:
        if raw is not None:
            st.markdown('<div class="sec-label">Raw dataset</div>',
                        unsafe_allow_html=True)
            st.caption("First 100 rows before cleaning")
            st.dataframe(raw.head(100), use_container_width=True)
            buf = io.BytesIO()
            raw.to_csv(buf, index=False)
            st.download_button("Download raw CSV", buf.getvalue(),
                               "raw_data.csv", "text/csv")

    with dr:
        if clean is not None:
            st.markdown('<div class="sec-label">Cleaned dataset</div>',
                        unsafe_allow_html=True)
            st.caption("First 100 rows after cleaning")
            st.dataframe(clean.head(100), use_container_width=True)
            buf = io.BytesIO()
            clean.to_csv(buf, index=False)
            st.download_button("Download cleaned CSV", buf.getvalue(),
                               "cleaned_data.csv", "text/csv")