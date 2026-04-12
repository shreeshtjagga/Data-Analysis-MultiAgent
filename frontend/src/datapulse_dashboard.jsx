/**
 * datapulse_dashboard.jsx  (v3 — Plotly-native charts)
 * ─────────────────────────────────────────────────────
 * Changes vs v2
 * ─────────────
 * • Removed recharts entirely — charts are now Plotly JSON from the backend,
 *   rendered directly with react-plotly.js (single source of truth).
 * • ChartPanel simply maps over `result.charts` and passes data/layout to Plot.
 */

import { useState, useRef, useCallback, useEffect } from "react";
import createPlotlyComponent from "react-plotly.js/factory";
import Plotly from "plotly.js-dist-min";
import { apiAnalyze, apiChat, apiHistory, apiHistoryAnalysis, apiDeleteAnalysis } from "./api.js";

const Plot = createPlotlyComponent(Plotly);

// ── Palette ───────────────────────────────────────────────────────────────────
const PALETTE = ["#6366f1","#10b981","#f59e0b","#06b6d4","#ef4444","#a78bfa","#34d399","#f472b6"];

// ── Styles ────────────────────────────────────────────────────────────────────
const s = {
  app: { minHeight: "100vh", background: "#060912", color: "#e2e8f0", fontFamily: "'Outfit', sans-serif", display: "flex", flexDirection: "column" },
  topbar: { background: "#0d1220", borderBottom: "1px solid rgba(99,102,241,0.15)", padding: "14px 28px", display: "flex", alignItems: "center", justifyContent: "space-between", position: "sticky", top: 0, zIndex: 20 },
  brand: { fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: "1.15rem", letterSpacing: "-0.02em", color: "#818cf8" },
  content: { flex: 1, padding: "28px 32px", display: "flex", flexDirection: "column", gap: "24px", maxWidth: "1600px", margin: "0 auto", width: "100%" },
  card: { background: "#0d1220", border: "1px solid rgba(99,102,241,0.13)", borderRadius: "10px", padding: "20px 24px" },
  sectionTitle: { fontFamily: "'Syne', sans-serif", fontSize: "0.7rem", fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", color: "#475569", marginBottom: "16px" },
  metric: { background: "#121929", border: "1px solid rgba(99,102,241,0.12)", borderTop: "2px solid #6366f1", borderRadius: "8px", padding: "14px 18px" },
  metricLabel: { fontSize: "0.67rem", fontWeight: 600, letterSpacing: "0.1em", textTransform: "uppercase", color: "#64748b", marginBottom: "4px" },
  metricVal: { fontFamily: "'Syne', sans-serif", fontSize: "1.6rem", fontWeight: 800, color: "#e2e8f0" },
  tab: (active) => ({ padding: "8px 18px", fontSize: "0.78rem", fontWeight: active ? 700 : 500, letterSpacing: "0.06em", textTransform: "uppercase", color: active ? "#6366f1" : "#64748b", borderBottom: active ? "2px solid #6366f1" : "2px solid transparent", background: "transparent", border: "none", cursor: "pointer", transition: "all 0.15s" }),
  btn: { display: "inline-flex", alignItems: "center", gap: "6px", background: "linear-gradient(135deg,#6366f1,#4f46e5)", color: "#fff", border: "none", borderRadius: "7px", padding: "9px 18px", fontFamily: "'Syne',sans-serif", fontWeight: 700, fontSize: "0.8rem", letterSpacing: "0.07em", textTransform: "uppercase", cursor: "pointer" },
  input: { background: "#121929", border: "1px solid rgba(99,102,241,0.2)", borderRadius: "7px", color: "#e2e8f0", padding: "10px 14px", fontSize: "0.88rem", width: "100%", fontFamily: "'Outfit',sans-serif", outline: "none" },
  tag: { display: "inline-block", background: "rgba(99,102,241,0.1)", border: "1px solid rgba(99,102,241,0.22)", color: "#818cf8", borderRadius: "4px", fontSize: "0.7rem", padding: "2px 7px", fontFamily: "monospace" },
  pill: (active) => ({ display: "inline-flex", alignItems: "center", gap: "5px", fontSize: "0.7rem", fontFamily: "monospace", padding: "3px 10px", borderRadius: "20px", background: active ? "rgba(99,102,241,0.15)" : "rgba(99,102,241,0.06)", border: "1px solid rgba(99,102,241,0.2)", color: "#818cf8" }),
};

// ── Dark-themed Plotly layout defaults ────────────────────────────────────────
const PLOTLY_DARK_LAYOUT = {
  paper_bgcolor: "rgba(0,0,0,0)",
  plot_bgcolor: "rgba(13,18,32,0.6)",
  font: { color: "#94a3b8", family: "'Outfit', sans-serif", size: 12 },
  title: { font: { color: "#e2e8f0", size: 14 } },
  xaxis: { gridcolor: "rgba(99,102,241,0.08)", zerolinecolor: "rgba(99,102,241,0.12)" },
  yaxis: { gridcolor: "rgba(99,102,241,0.08)", zerolinecolor: "rgba(99,102,241,0.12)" },
  colorway: PALETTE,
  autosize: true,
  margin: { l: 50, r: 30, t: 50, b: 40 },
};

const PLOTLY_CONFIG = {
  responsive: true,
  displayModeBar: "hover",
  displaylogo: false,
  modeBarButtonsToRemove: ["lasso2d", "select2d", "toggleSpikelines"],
};

// ── Chart panel (Plotly) ──────────────────────────────────────────────────────
function ChartPanel({ result, viewportWidth }) {
  const charts = result?.charts || {};
  const entries = Object.entries(charts);
  const columnCount = viewportWidth >= 1500 ? 3 : viewportWidth >= 980 ? 2 : 1;

  if (entries.length === 0) {
    return <div style={{ color: "#475569", fontSize: "0.88rem" }}>No charts available.</div>;
  }

  return (
    <div style={{ display: "grid", gridTemplateColumns: `repeat(${columnCount}, minmax(0,1fr))`, gap: "20px" }}>
      {entries.map(([key, fig]) => (
        <div key={key} style={s.card}>
          <Plot
            data={fig.data || []}
            layout={{
              ...PLOTLY_DARK_LAYOUT,
              ...(fig.layout || {}),
              // Force dark theme overrides regardless of backend layout
              paper_bgcolor: PLOTLY_DARK_LAYOUT.paper_bgcolor,
              plot_bgcolor: PLOTLY_DARK_LAYOUT.plot_bgcolor,
              font: { ...PLOTLY_DARK_LAYOUT.font, ...(fig.layout?.font || {}) },
              height: 380,
            }}
            config={PLOTLY_CONFIG}
            style={{ width: "100%", height: "380px" }}
            useResizeHandler
          />
        </div>
      ))}
    </div>
  );
}

// ── Main dashboard ────────────────────────────────────────────────────────────
const PRIMARY_TABS = ["overview", "charts", "insights"];
const SECONDARY_TABS = ["statistics", "quality", "chat"];

export default function DataPulse({ user, onLogout }) {
  const [phase, setPhase]     = useState("upload");
  const [result, setResult]   = useState(null);
  const [fileName, setFileName] = useState("");
  const [agentLog, setAgentLog] = useState([]);
  const [analysisError, setAnalysisError] = useState("");
  const [isDragOver, setIsDragOver] = useState(false);
  const [isMobile, setIsMobile] = useState(typeof window !== "undefined" ? window.innerWidth < 900 : false);
  const [viewportWidth, setViewportWidth] = useState(typeof window !== "undefined" ? window.innerWidth : 1280);
  const [tab, setTab]         = useState("overview");
  const [chatMsgs, setChatMsgs]   = useState([]);
  const [chatInput, setChatInput] = useState("");
  const [chatLoading, setChatLoading] = useState(false);
  const [history, setHistory] = useState([]);
  const [showHistory, setShowHistory] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [deleteLoading, setDeleteLoading] = useState(null);
  const [historySelectLoading, setHistorySelectLoading] = useState(null);
  const [statsSortKey, setStatsSortKey] = useState("outliers");
  const [statsSortDirection, setStatsSortDirection] = useState("desc");
  const [statsFilter, setStatsFilter] = useState("");
  const fileRef = useRef();
  const chatEndRef = useRef(null);
  const dragCounterRef = useRef(0);
  const stageTimersRef = useRef([]);

  const log = (msg) => setAgentLog((p) => [...p, { time: new Date().toLocaleTimeString(), msg }]);

  useEffect(() => {
    const onResize = () => {
      setIsMobile(window.innerWidth < 900);
      setViewportWidth(window.innerWidth);
    };
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  useEffect(() => {
    if (phase === "upload") {
      document.title = "Data Pulse - AI Data Analyst";
      return;
    }
    if (phase === "analyzing") {
      document.title = fileName ? `Analyzing ${fileName} - Data Pulse` : "Analyzing - Data Pulse";
      return;
    }
    document.title = fileName ? `${fileName} - Data Pulse` : "Analysis Complete - Data Pulse";
  }, [phase, fileName]);

  useEffect(() => {
    if (tab !== "chat") return;
    chatEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [chatMsgs, chatLoading, tab]);

  const clearStageTimers = () => {
    stageTimersRef.current.forEach((timerId) => clearTimeout(timerId));
    stageTimersRef.current = [];
  };

  // ── File upload → API ──────────────────────────────────────────────────────
  const analyzeFile = useCallback(async (file) => {
    if (!file) return;
    setPhase("analyzing");
    setResult(null);
    setAnalysisError("");
    setAgentLog([]);
    setTab("overview");
    setFileName(file.name);
    setChatMsgs([]);

    log("Uploading CSV to server…");
    log("Architect running…");
    clearStageTimers();
    const stageMessages = [
      "Architect complete → Statistician running…",
      "Statistician complete → Visualizer running…",
      "Visualizer complete → Insights running…",
    ];
    stageTimersRef.current = stageMessages.map((msg, idx) =>
      setTimeout(() => log(msg), 500 + idx * 900)
    );

    try {
      const data = await apiAnalyze(file);
      clearStageTimers();
      log(data.from_cache ? "Returned cached analysis." : "Pipeline complete.");
      setResult(data);
      setPhase("done");
    } catch (err) {
      clearStageTimers();
      log(`Error: ${err.message}`);
      setAnalysisError(err.message || "Analysis failed");
      setPhase("analyzing");
    }
  }, []);

  const onFile = useCallback((file) => analyzeFile(file), [analyzeFile]);
  const onDrop = useCallback((e) => {
    e.preventDefault();
    dragCounterRef.current = 0;
    setIsDragOver(false);
    onFile(e.dataTransfer.files[0]);
  }, [onFile]);
  const onDragEnter = useCallback((e) => {
    e.preventDefault();
    dragCounterRef.current += 1;
    setIsDragOver(true);
  }, []);
  const onDragLeave = useCallback((e) => {
    e.preventDefault();
    dragCounterRef.current = Math.max(0, dragCounterRef.current - 1);
    if (dragCounterRef.current === 0) setIsDragOver(false);
  }, []);

  // ── History ────────────────────────────────────────────────────────────────
  const loadHistory = async () => {
    setHistoryLoading(true);
    try {
      const resp = await apiHistory();
      setHistory(resp.analyses || []);
    } catch (err) {
      console.error("History load failed:", err.message);
    } finally {
      setHistoryLoading(false);
    }
  };

  const toggleHistory = async () => {
    const next = !showHistory;
    setShowHistory(next);
    if (next) await loadHistory();
  };

  const deleteItem = async (id) => {
    setDeleteLoading(id);
    try {
      await apiDeleteAnalysis(id);
      setHistory((h) => h.filter((x) => x.analysis_id !== id));
    } catch (err) {
      alert(`Delete failed: ${err.message}`);
    }
    setDeleteLoading(null);
  };

  const loadHistoryItem = async (item) => {
    setHistorySelectLoading(item.analysis_id);
    try {
      const full = await apiHistoryAnalysis(item.analysis_id);
      setResult(full);
      setFileName(full.file_name || item.file_name || "");
      setChatMsgs([]);
      setTab("overview");
      setPhase("done");
      setShowHistory(false);
    } catch (err) {
      alert(`Load failed: ${err.message}`);
    } finally {
      setHistorySelectLoading(null);
    }
  };

  // ── Chat ───────────────────────────────────────────────────────────────────
  const sendChat = useCallback(async () => {
    const q = chatInput.trim();
    if (!q || chatLoading || !result) return;
    setChatInput("");
    setChatMsgs((p) => [...p, { role: "user", text: q }]);
    setChatLoading(true);
    try {
      const ctx = {
        fileName,
        stats: result.stats_summary,
        insights: result.insights,
        correlations: result.stats_summary?.strong_correlations?.slice(0, 5),
      };
      const resp = await apiChat(q, ctx);
      setChatMsgs((p) => [...p, { role: "ai", text: (resp.answer || "").trim() || "No response generated." }]);
    } catch {
      setChatMsgs((p) => [...p, { role: "ai", text: "Unable to reach AI. Try again." }]);
    }
    setChatLoading(false);
  }, [chatInput, chatLoading, result, fileName]);

  // ── Computed metrics ───────────────────────────────────────────────────────
  const stats        = result?.stats_summary || {};
  const insights     = result?.insights || {};
  const dq           = stats.data_quality || {};
  const numericCols  = Object.keys(stats.numeric_columns || {});
  const catCols      = Object.keys(stats.categorical_columns || {});
  const outlierCols  = Object.keys(stats.outliers || {});
  const missingTotal = dq.missing_cells || 0;
  const completeness = dq.completeness || 100;
  const imputations  = stats.imputations || [];
  const profile      = stats.dataset_profile || {};
  const excluded     = stats.excluded_columns || [];
  const coerced      = stats.coerced_columns || [];
  const formatPercent = (value) => {
    const n = Number.isFinite(value) ? Number(value) : 100;
    return Number.isInteger(n) ? `${n}%` : `${n.toFixed(1)}%`;
  };

  const keyMetrics = [
    { label: "Rows", val: (stats.row_count || 0).toLocaleString(), tone: "#818cf8" },
    { label: "Completeness", val: formatPercent(completeness), tone: "#10b981" },
    { label: "Outlier cols", val: outlierCols.length, tone: outlierCols.length > 0 ? "#f59e0b" : "#10b981" },
  ];
  const secondaryMetrics = [
    { label: "Columns", val: stats.column_count || 0 },
    { label: "Numeric cols", val: numericCols.length },
    { label: "Categorical cols", val: catCols.length },
    { label: "Missing values", val: missingTotal },
    { label: "Correlations", val: (stats.strong_correlations || []).length },
  ];

  const numericRows = numericCols
    .map((col) => {
      const st = stats.numeric_columns?.[col];
      if (!st) return null;
      return {
        column: col,
        count: Number(st.count || 0),
        mean: Number(st.mean || 0),
        std: Number(st.std || 0),
        min: Number(st.min || 0),
        max: Number(st.max || 0),
        skewness: Number(st.skewness || 0),
        outliers: Number(stats.outliers?.[col]?.count || 0),
      };
    })
    .filter(Boolean)
    .filter((row) => row.column.toLowerCase().includes(statsFilter.trim().toLowerCase()));

  const sortedNumericRows = [...numericRows].sort((a, b) => {
    const dir = statsSortDirection === "asc" ? 1 : -1;
    if (statsSortKey === "column") return a.column.localeCompare(b.column) * dir;
    return (Number(a[statsSortKey]) - Number(b[statsSortKey])) * dir;
  });

  const toggleSort = (key) => {
    if (statsSortKey === key) {
      setStatsSortDirection((d) => (d === "asc" ? "desc" : "asc"));
      return;
    }
    setStatsSortKey(key);
    setStatsSortDirection(key === "column" ? "asc" : "desc");
  };

  const downloadDashboard = () => {
    if (!result) return;
    const payload = {
      file_name: fileName,
      exported_at: new Date().toISOString(),
      from_cache: Boolean(result?.from_cache),
      stats_summary: result?.stats_summary || {},
      insights: result?.insights || {},
      charts: Object.keys(result?.charts || {}),
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    const safeName = (fileName || "analysis").replace(/\.[^/.]+$/, "").replace(/[^a-zA-Z0-9_-]/g, "_");
    a.download = `${safeName}_dashboard.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(a.href);
  };

  // ── Upload screen ──────────────────────────────────────────────────────────
  if (phase === "upload") return (
    <div style={s.app}>
      <div style={{ ...s.topbar, flexWrap: isMobile ? "wrap" : "nowrap", gap: isMobile ? "10px" : 0 }}>
        <span style={s.brand}>◈ Data Pulse</span>
        <div style={{ display: "flex", alignItems: "center", gap: "10px", width: isMobile ? "100%" : "auto", justifyContent: isMobile ? "space-between" : "flex-end" }}>
          <span style={{ fontSize: "0.78rem", color: "#475569", fontFamily: "monospace" }}>{user?.email}</span>
          <button style={{ ...s.btn, fontSize: "0.72rem", padding: "7px 14px", background: "transparent", border: "1px solid rgba(99,102,241,0.2)", color: "#818cf8" }} onClick={onLogout}>Logout</button>
        </div>
      </div>
      <div style={{ flex: 1, display: "grid", gridTemplateColumns: isMobile ? "1fr" : "1.1fr 1fr", gap: "28px", alignItems: "stretch", padding: isMobile ? "20px" : "32px" }}>
        <div
          style={{
            ...s.card,
            display: "flex",
            flexDirection: "column",
            justifyContent: "center",
            minHeight: isMobile ? "unset" : "560px",
            textAlign: "left",
          }}
          onDrop={onDrop}
          onDragOver={(e) => e.preventDefault()}
          onDragEnter={onDragEnter}
          onDragLeave={onDragLeave}
        >
          <div style={{ fontSize: "2.6rem", marginBottom: "14px", color: "#6366f1" }}>◈</div>
          <h1 style={{ fontFamily: "'Syne',sans-serif", fontSize: "2rem", fontWeight: 800, color: "#f1f5f9", marginBottom: "10px" }}>From Spreadsheet to Insight, Fast</h1>
          <p style={{ color: "#94a3b8", fontSize: "0.92rem", marginBottom: "20px", lineHeight: 1.7 }}>
            Drop a CSV or Excel file and get instant profiling, anomaly detection, chart generation, and AI recommendations.
          </p>

          <div style={{ display: "grid", gridTemplateColumns: isMobile ? "1fr" : "repeat(3,minmax(0,1fr))", gap: "10px", marginBottom: "20px" }}>
            {[
              { k: "Profile", v: "Columns typed + cleaned" },
              { k: "Visuals", v: "Auto-ranked Plotly charts" },
              { k: "Insights", v: "Actionable findings + risks" },
            ].map((item) => (
              <div key={item.k} style={{ background: "#121929", border: "1px solid rgba(99,102,241,0.15)", borderRadius: "8px", padding: "10px" }}>
                <div style={{ fontSize: "0.68rem", color: "#818cf8", fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase" }}>{item.k}</div>
                <div style={{ fontSize: "0.78rem", color: "#cbd5e1", marginTop: "4px" }}>{item.v}</div>
              </div>
            ))}
          </div>

          <div
            onClick={() => fileRef.current.click()}
            style={{
              border: isDragOver ? "2px dashed rgba(99,102,241,0.85)" : "2px dashed rgba(99,102,241,0.3)",
              borderRadius: "12px",
              padding: "28px",
              cursor: "pointer",
              background: isDragOver ? "rgba(99,102,241,0.14)" : "rgba(99,102,241,0.03)",
              transition: "all 0.18s ease",
              textAlign: "center",
            }}
          >
            <div style={{ fontSize: "1.8rem", marginBottom: "10px", color: "#475569" }}>↑</div>
            <p style={{ color: "#94a3b8", fontSize: "0.88rem" }}>
              {isDragOver ? "Drop file to start analysis" : "Click to upload or drag CSV/Excel (.csv, .xlsx, .xls)"}
            </p>
            <input ref={fileRef} type="file" accept=".csv,.xlsx,.xls" style={{ display: "none" }} onChange={(e) => onFile(e.target.files[0])} />
          </div>

          <div style={{ marginTop: "16px", display: "flex", justifyContent: "center", gap: "8px", flexWrap: "wrap" }}>
            {["Architect","Statistician","Insights","Chat"].map((a) => (
              <span key={a} style={s.pill(false)}>{a}</span>
            ))}
          </div>
        </div>

        <div style={{ ...s.card, minHeight: isMobile ? "unset" : "560px" }}>
          <div style={s.sectionTitle}>What You Will Get</div>
          <div style={{ display: "grid", gap: "12px" }}>
            {[
              "Executive summary tailored to your dataset domain",
              "Top correlations and outlier columns with severity",
              "Interactive charts with hover, zoom, and export",
              "Data quality report with completeness and imputations",
              "Chat assistant grounded on your computed stats",
            ].map((line) => (
              <div key={line} style={{ display: "flex", gap: "10px", alignItems: "flex-start" }}>
                <span style={{ color: "#10b981", marginTop: "2px" }}>✓</span>
                <span style={{ color: "#cbd5e1", fontSize: "0.84rem", lineHeight: 1.5 }}>{line}</span>
              </div>
            ))}
          </div>

          <div style={{ marginTop: "18px", padding: "12px", borderRadius: "10px", background: "#121929", border: "1px solid rgba(99,102,241,0.15)" }}>
            <div style={{ fontSize: "0.68rem", color: "#64748b", letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: "8px" }}>Preview</div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4,minmax(0,1fr))", gap: "8px" }}>
              {[
                ["Rows", "12,540"],
                ["Cols", "27"],
                ["Outliers", "3"],
                ["Completeness", "98%"],
              ].map(([k, v]) => (
                <div key={k} style={{ background: "rgba(99,102,241,0.06)", border: "1px solid rgba(99,102,241,0.15)", borderRadius: "7px", padding: "8px" }}>
                  <div style={{ fontSize: "0.65rem", color: "#64748b" }}>{k}</div>
                  <div style={{ fontFamily: "'Syne',sans-serif", color: "#e2e8f0", fontSize: "0.95rem" }}>{v}</div>
                </div>
              ))}
            </div>
            <div style={{ marginTop: "10px", fontSize: "0.78rem", color: "#94a3b8" }}>
              Sample outcomes are illustrative. Actual metrics depend on uploaded data.
            </div>
          </div>
        </div>
      </div>
    </div>
  );

  // ── Analyzing screen ───────────────────────────────────────────────────────
  if (phase === "analyzing") return (
    <div style={s.app}>
      <div style={s.topbar}><span style={s.brand}>◈ Data Pulse</span></div>
      <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ maxWidth: "440px", width: "100%", padding: "24px" }}>
          <div style={{ fontFamily: "'Syne',sans-serif", fontSize: "1.1rem", fontWeight: 700, color: "#e2e8f0", marginBottom: "24px" }}>Running pipeline…</div>
          {agentLog.map((l, i) => (
            <div key={i} style={{ display: "flex", gap: "10px", marginBottom: "10px" }}>
              <span style={{ color: "#6366f1", fontFamily: "monospace", fontSize: "0.7rem", whiteSpace: "nowrap" }}>{l.time}</span>
              <span style={{ color: "#94a3b8", fontSize: "0.82rem", fontFamily: "monospace" }}>{l.msg}</span>
            </div>
          ))}
          <div style={{ marginTop: "16px", display: "flex", gap: "6px" }}>
            {[0,1,2].map((i) => <div key={i} style={{ width: "7px", height: "7px", borderRadius: "50%", background: "#6366f1", animation: `pulse 1.2s ${i*0.2}s infinite` }} />)}
          </div>
          {analysisError && (
            <div style={{ marginTop: "18px", background: "rgba(239,68,68,0.1)", border: "1px solid rgba(239,68,68,0.35)", borderRadius: "8px", padding: "12px" }}>
              <div style={{ color: "#fca5a5", fontSize: "0.82rem", marginBottom: "10px" }}>Analysis failed: {analysisError}</div>
              <button
                style={{ ...s.btn, fontSize: "0.72rem", padding: "7px 14px" }}
                onClick={() => {
                  setAnalysisError("");
                  setPhase("upload");
                }}
              >
                Try again
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );

  // ── Dashboard ──────────────────────────────────────────────────────────────
  return (
    <div style={s.app}>
      <style>{`
        @keyframes fadeIn { from{opacity:0;transform:translateY(4px)} to{opacity:1;transform:none} }
        @keyframes pulse  { 0%,80%,100%{opacity:.3} 40%{opacity:1} }
        *{box-sizing:border-box}
        ::-webkit-scrollbar{width:4px} ::-webkit-scrollbar-thumb{background:#1e2d47;border-radius:4px}
      `}</style>

      {/* Topbar */}
      <div style={{ ...s.topbar, flexWrap: isMobile ? "wrap" : "nowrap", gap: isMobile ? "10px" : 0 }}>
        <div style={{ display: "flex", alignItems: "center", gap: "12px", flexWrap: isMobile ? "wrap" : "nowrap" }}>
          <span style={s.brand}>◈ Data Pulse</span>
          <span style={{ fontFamily: "monospace", fontSize: "0.72rem", background: "rgba(99,102,241,0.12)", border: "1px solid rgba(99,102,241,0.22)", color: "#818cf8", padding: "2px 10px", borderRadius: "20px" }}>{fileName}</span>
          {profile.label && profile.label !== "unknown" && <span style={{ fontSize: "0.7rem", color: "#a78bfa", background: "rgba(167,139,250,0.08)", border: "1px solid rgba(167,139,250,0.2)", padding: "2px 8px", borderRadius: "12px" }}>{profile.label}</span>}
          {result?.from_cache && <span style={{ fontSize: "0.7rem", color: "#10b981", background: "rgba(16,185,129,0.08)", border: "1px solid rgba(16,185,129,0.2)", padding: "2px 8px", borderRadius: "12px" }}>CACHED</span>}
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "8px", flexWrap: isMobile ? "wrap" : "nowrap", width: isMobile ? "100%" : "auto" }}>
          <span style={{ fontSize: "0.75rem", color: "#10b981", background: "rgba(16,185,129,0.08)", border: "1px solid rgba(16,185,129,0.2)", padding: "4px 12px", borderRadius: "20px" }}>✓ Analysis Complete</span>
          <button style={{ ...s.btn, fontSize: "0.72rem", padding: "7px 14px", opacity: historyLoading ? 0.7 : 1 }} onClick={toggleHistory} disabled={historyLoading}>
            {historyLoading ? "Loading…" : "History"}
          </button>
          <button style={{ ...s.btn, fontSize: "0.72rem", padding: "7px 14px" }} onClick={downloadDashboard}>Download</button>
          <button
            style={{ ...s.btn, fontSize: "0.72rem", padding: "7px 14px" }}
            onClick={() => {
              if (!window.confirm("Clear current analysis and upload a new file?")) return;
              setResult(null);
              setPhase("upload");
            }}
          >
            New File
          </button>
          <button style={{ ...s.btn, fontSize: "0.72rem", padding: "7px 14px", background: "transparent", border: "1px solid rgba(99,102,241,0.2)", color: "#818cf8" }} onClick={onLogout}>Logout</button>
        </div>
      </div>

      {/* History drawer */}
      {showHistory && (
        <div style={{ background: "#080f1c", borderBottom: "1px solid rgba(99,102,241,0.12)", padding: "16px 32px" }}>
          <div style={s.sectionTitle}>Analysis history</div>
          {history.length === 0 ? (
            <div style={{ fontSize: "0.82rem", color: "#475569" }}>No saved analyses yet.</div>
          ) : (
            <div style={{ display: "flex", flexWrap: "wrap", gap: "10px" }}>
              {history.map((item) => (
                <div
                  key={item.analysis_id}
                  onClick={() => historySelectLoading == null && loadHistoryItem(item)}
                  style={{
                    background: "#0d1220",
                    border: "1px solid rgba(99,102,241,0.15)",
                    borderRadius: "8px",
                    padding: "10px 14px",
                    fontSize: "0.78rem",
                    display: "flex",
                    alignItems: "center",
                    gap: "14px",
                    cursor: historySelectLoading == null ? "pointer" : "wait",
                    opacity: historySelectLoading === item.analysis_id ? 0.7 : 1,
                  }}
                >
                  <span style={{ color: "#94a3b8" }}>
                    {historySelectLoading === item.analysis_id ? "Loading…" : item.file_name}
                  </span>
                  <span style={{ color: "#475569", fontFamily: "monospace", fontSize: "0.7rem" }}>{item.row_count}r × {item.column_count}c</span>
                  <button onClick={(e) => { e.stopPropagation(); deleteItem(item.analysis_id); }} disabled={deleteLoading === item.analysis_id} style={{ background: "transparent", border: "none", color: "#ef4444", cursor: deleteLoading === item.analysis_id ? "wait" : "pointer", fontSize: "0.75rem", padding: "0" }}>
                    {deleteLoading === item.analysis_id ? "..." : "✕"}
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Tab bar */}
      <div style={{ background: "#0d1220", borderBottom: "1px solid rgba(99,102,241,0.12)", padding: "6px 16px", display: "flex", gap: "14px", overflowX: "auto", alignItems: "center" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "2px" }}>
          <span style={{ fontSize: "0.65rem", color: "#64748b", letterSpacing: "0.08em", textTransform: "uppercase", marginRight: "6px" }}>Primary</span>
          {PRIMARY_TABS.map((t) => <button key={t} style={s.tab(tab === t)} onClick={() => setTab(t)}>{t}</button>)}
        </div>
        <div style={{ width: "1px", height: "22px", background: "rgba(99,102,241,0.2)" }} />
        <div style={{ display: "flex", alignItems: "center", gap: "2px" }}>
          <span style={{ fontSize: "0.65rem", color: "#64748b", letterSpacing: "0.08em", textTransform: "uppercase", marginRight: "6px" }}>Reference</span>
          {SECONDARY_TABS.map((t) => <button key={t} style={s.tab(tab === t)} onClick={() => setTab(t)}>{t}</button>)}
        </div>
      </div>

      <div style={s.content}>

        {/* OVERVIEW */}
        {tab === "overview" && (
          <>
            {insights?.headline && (
              <div style={{ ...s.card, borderLeft: "3px solid #6366f1", background: "rgba(99,102,241,0.05)" }}>
                <div style={{ fontSize: "0.72rem", fontFamily: "monospace", color: "#6366f1", letterSpacing: "0.1em", marginBottom: "6px" }}>EXECUTIVE SUMMARY</div>
                <div style={{ fontSize: "1rem", color: "#e2e8f0", lineHeight: 1.6 }}>{insights.headline}</div>
              </div>
            )}
            <div style={{ display: "grid", gridTemplateColumns: isMobile ? "1fr" : "repeat(3,minmax(0,1fr))", gap: "12px" }}>
              {keyMetrics.map(({ label, val, tone }) => (
                <div key={label} style={{ ...s.metric, borderTop: `3px solid ${tone}`, background: "linear-gradient(180deg, rgba(99,102,241,0.08), rgba(13,18,32,0.9))", padding: "16px 18px" }}>
                  <div style={{ ...s.metricLabel, color: "#94a3b8" }}>{label}</div>
                  <div style={{ ...s.metricVal, fontSize: "2rem", color: tone }}>{val}</div>
                </div>
              ))}
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(150px,1fr))", gap: "10px" }}>
              {secondaryMetrics.map(({ label, val }) => (
                <div key={label} style={s.metric}>
                  <div style={s.metricLabel}>{label}</div>
                  <div style={{ ...s.metricVal, fontSize: "1.25rem" }}>{val}</div>
                </div>
              ))}
            </div>
            {imputations.length > 0 && (
              <div style={s.card}>
                <div style={s.sectionTitle}>Data imputations applied</div>
                {imputations.map((imp, i) => (
                  <div key={i} style={{ display: "flex", gap: "10px", marginBottom: "8px", fontSize: "0.84rem", color: "#fde68a" }}>
                    <span style={{ color: "#f59e0b" }}>⚑</span>
                    <span>
                      <strong>{imp.column}</strong> — {imp.count} values filled with {imp.strategy}
                      {imp.fill_value != null && <span style={{ fontFamily: "monospace", color: "#94a3b8" }}> ({imp.fill_value})</span>}
                    </span>
                  </div>
                ))}
              </div>
            )}
            {excluded.length > 0 && (
              <div style={s.card}>
                <div style={s.sectionTitle}>Excluded columns</div>
                <div style={{ fontSize: "0.78rem", color: "#64748b", marginBottom: "10px" }}>These columns were auto-excluded from analysis (noisy / uninformative)</div>
                {excluded.map((ex, i) => (
                  <div key={i} style={{ display: "flex", gap: "10px", marginBottom: "6px", fontSize: "0.82rem" }}>
                    <span style={{ color: "#ef4444" }}>✕</span>
                    <span style={{ color: "#94a3b8" }}><strong style={{ color: "#e2e8f0" }}>{ex.column}</strong> — {ex.reason}</span>
                  </div>
                ))}
              </div>
            )}
            {coerced.length > 0 && (
              <div style={s.card}>
                <div style={s.sectionTitle}>Type-coerced columns</div>
                <div style={{ fontSize: "0.78rem", color: "#64748b", marginBottom: "10px" }}>Object columns auto-converted to numeric</div>
                {coerced.map((c, i) => (
                  <div key={i} style={{ display: "flex", gap: "10px", marginBottom: "6px", fontSize: "0.82rem" }}>
                    <span style={{ color: "#06b6d4" }}>↻</span>
                    <span style={{ color: "#94a3b8" }}><strong style={{ color: "#e2e8f0" }}>{c.column}</strong> — {(c.valid_ratio * 100).toFixed(0)}% valid numeric values</span>
                  </div>
                ))}
              </div>
            )}
            {insights?.risk_flags?.length > 0 && (
              <div style={s.card}>
                <div style={s.sectionTitle}>Risk flags</div>
                {insights.risk_flags.map((f, i) => (
                  <div key={i} style={{ display: "flex", gap: "10px", marginBottom: "8px", fontSize: "0.84rem", color: "#fde68a" }}>
                    <span style={{ color: "#f59e0b" }}>⚠</span> {f}
                  </div>
                ))}
              </div>
            )}
            {profile.description && profile.label !== "unknown" && (
              <div style={{ ...s.card, borderLeft: "3px solid #a78bfa", background: "rgba(167,139,250,0.04)" }}>
                <div style={{ fontSize: "0.72rem", fontFamily: "monospace", color: "#a78bfa", letterSpacing: "0.1em", marginBottom: "6px" }}>DATASET CLASSIFICATION</div>
                <div style={{ fontSize: "0.94rem", color: "#e2e8f0", marginBottom: "4px" }}>{profile.description}</div>
                <div style={{ fontSize: "0.76rem", color: "#64748b" }}>Domain: <span style={{ color: "#a78bfa" }}>{profile.domain}</span></div>
              </div>
            )}
            <div style={s.card}>
              <div style={s.sectionTitle}>Column inventory</div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: "7px" }}>
                {numericCols.map((c) => <span key={c} style={s.tag}>{c}</span>)}
                {catCols.map((c) => <span key={c} style={{ ...s.tag, background: "rgba(16,185,129,0.08)", border: "1px solid rgba(16,185,129,0.2)", color: "#34d399" }}>{c}</span>)}
              </div>
            </div>
          </>
        )}

        {/* CHARTS */}
        {tab === "charts" && <ChartPanel result={result} viewportWidth={viewportWidth} />}

        {/* INSIGHTS */}
        {tab === "insights" && (
          <div style={{ display: "grid", gridTemplateColumns: isMobile ? "1fr" : "minmax(0,1.45fr) minmax(0,1fr)", gap: "20px" }}>
            <div style={{ ...s.card, minHeight: "100%" }}>
              <div style={s.sectionTitle}>Key findings</div>
              {(insights?.findings || []).map((f, i) => (
                <div key={i} style={{ display: "flex", gap: "10px", marginBottom: "10px", fontSize: "0.84rem", color: "#cbd5e1", lineHeight: 1.55 }}>
                  <span style={{ color: "#6366f1", fontWeight: 700 }}>{i + 1}</span> {f}
                </div>
              ))}
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: "20px" }}>
              <div style={s.card}>
                <div style={s.sectionTitle}>Recommendations</div>
                {(insights?.recommendations || []).map((r, i) => (
                  <div key={i} style={{ display: "flex", gap: "10px", marginBottom: "10px", fontSize: "0.84rem", color: "#cbd5e1", lineHeight: 1.55 }}>
                    <span style={{ color: "#10b981" }}>→</span> {r}
                  </div>
                ))}
              </div>
              {(stats.strong_correlations || []).length > 0 && (
                <div style={s.card}>
                  <div style={s.sectionTitle}>Notable correlations</div>
                  {stats.strong_correlations.slice(0, 8).map((c, i) => (
                    <div key={i} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px", fontSize: "0.8rem" }}>
                      <span style={{ color: "#94a3b8", fontFamily: "monospace" }}>{c.col1} ↔ {c.col2}</span>
                      <span style={{ fontFamily: "monospace", fontWeight: 700, color: c.correlation > 0 ? "#10b981" : "#ef4444", background: c.correlation > 0 ? "rgba(16,185,129,0.1)" : "rgba(239,68,68,0.1)", padding: "2px 8px", borderRadius: "4px" }}>r = {c.correlation.toFixed(3)}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        )}

        {/* STATISTICS */}
        {tab === "statistics" && (
          <div style={{ display: "flex", flexDirection: "column", gap: "20px" }}>
            {numericCols.length === 0 && (
              <div style={s.card}>
                <div style={s.sectionTitle}>Numeric columns</div>
                <div style={{ fontSize: "0.84rem", color: "#64748b" }}>
                  This dataset has no numeric columns, so statistical aggregates are not available.
                </div>
              </div>
            )}
            {numericCols.length > 0 && (
              <div style={s.card}>
                <div style={s.sectionTitle}>Numeric columns</div>
                <div style={{ marginBottom: "10px", display: "flex", justifyContent: "space-between", gap: "10px", alignItems: "center", flexWrap: "wrap" }}>
                  <input
                    style={{ ...s.input, maxWidth: "260px", fontFamily: "monospace", fontSize: "0.78rem", padding: "8px 10px" }}
                    placeholder="Filter columns…"
                    value={statsFilter}
                    onChange={(e) => setStatsFilter(e.target.value)}
                  />
                  <span style={{ fontSize: "0.72rem", color: "#64748b", fontFamily: "monospace" }}>
                    Sort: {statsSortKey} ({statsSortDirection})
                  </span>
                </div>
                <div style={{ overflowX: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.78rem", fontFamily: "monospace" }}>
                    <thead>
                      <tr style={{ color: "#475569", borderBottom: "1px solid rgba(99,102,241,0.1)" }}>
                        {[
                          ["Column", "column"],
                          ["Count", "count"],
                          ["Mean", "mean"],
                          ["Std", "std"],
                          ["Min", "min"],
                          ["Max", "max"],
                          ["Skewness", "skewness"],
                          ["Outliers", "outliers"],
                        ].map(([label, key]) => (
                          <th
                            key={label}
                            onClick={() => toggleSort(key)}
                            style={{
                              padding: "8px 12px",
                              textAlign: "left",
                              fontWeight: 600,
                              fontSize: "0.68rem",
                              textTransform: "uppercase",
                              cursor: "pointer",
                              color: statsSortKey === key ? "#818cf8" : "#475569",
                              background: statsSortKey === key ? "rgba(99,102,241,0.08)" : "transparent",
                            }}
                          >
                            {label} {statsSortKey === key ? (statsSortDirection === "asc" ? "↑" : "↓") : ""}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {sortedNumericRows.map((row) => {
                        return (
                          <tr key={row.column} style={{ borderBottom: "1px solid rgba(99,102,241,0.06)" }}>
                            <td style={{ padding: "8px 12px", color: "#818cf8", fontWeight: 600 }}>{row.column}</td>
                            <td style={{ padding: "8px 12px", color: "#94a3b8", background: statsSortKey === "count" ? "rgba(99,102,241,0.06)" : "transparent" }}>{row.count}</td>
                            <td style={{ padding: "8px 12px", color: "#e2e8f0", background: statsSortKey === "mean" ? "rgba(99,102,241,0.06)" : "transparent" }}>{row.mean.toFixed(2)}</td>
                            <td style={{ padding: "8px 12px", color: "#94a3b8", background: statsSortKey === "std" ? "rgba(99,102,241,0.06)" : "transparent" }}>{row.std.toFixed(2)}</td>
                            <td style={{ padding: "8px 12px", color: "#64748b", background: statsSortKey === "min" ? "rgba(99,102,241,0.06)" : "transparent" }}>{row.min.toFixed(2)}</td>
                            <td style={{ padding: "8px 12px", color: "#64748b", background: statsSortKey === "max" ? "rgba(99,102,241,0.06)" : "transparent" }}>{row.max.toFixed(2)}</td>
                            <td style={{ padding: "8px 12px", color: Math.abs(row.skewness) > 1 ? "#f59e0b" : "#10b981", background: statsSortKey === "skewness" ? "rgba(99,102,241,0.06)" : "transparent" }}>{row.skewness.toFixed(2)}</td>
                            <td style={{ padding: "8px 12px", color: row.outliers > 0 ? "#ef4444" : "#10b981", background: statsSortKey === "outliers" ? "rgba(99,102,241,0.06)" : "transparent" }}>{row.outliers}</td>
                          </tr>
                        );
                      })}
                      {sortedNumericRows.length === 0 && (
                        <tr>
                          <td colSpan={8} style={{ padding: "12px", color: "#64748b", fontSize: "0.8rem" }}>
                            No numeric columns match the current filter.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}

        {/* QUALITY */}
        {tab === "quality" && (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(280px,1fr))", gap: "20px" }}>
            <div style={s.card}>
              <div style={s.sectionTitle}>Data quality summary</div>
              {[
                { label: "Total cells",    val: dq.total_cells || 0 },
                { label: "Missing cells",  val: dq.missing_cells || 0 },
                { label: "Duplicate rows", val: dq.duplicate_rows || 0 },
                { label: "Completeness",   val: formatPercent(dq.completeness || 100) },
              ].map(({ label, val }) => (
                <div key={label} style={{ display: "flex", justifyContent: "space-between", marginBottom: "10px", fontSize: "0.82rem" }}>
                  <span style={{ color: "#94a3b8", fontFamily: "monospace" }}>{label}</span>
                  <span style={{ color: "#e2e8f0", fontWeight: 600 }}>{val}</span>
                </div>
              ))}
            </div>
            <div style={s.card}>
              <div style={s.sectionTitle}>Outlier summary</div>
              {outlierCols.length === 0
                ? <div style={{ fontSize: "0.82rem", color: "#10b981" }}>No outliers detected.</div>
                : outlierCols.map((col) => {
                    const info = stats.outliers[col];
                    return (
                      <div key={col} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px", padding: "7px 10px", background: "rgba(239,68,68,0.06)", border: "1px solid rgba(239,68,68,0.15)", borderRadius: "6px" }}>
                        <span style={{ fontFamily: "monospace", fontSize: "0.78rem", color: "#fca5a5" }}>{col}</span>
                        <span style={{ fontFamily: "monospace", fontSize: "0.72rem", color: "#94a3b8" }}>{info.count} outliers ({info.percentage?.toFixed(1)}%)</span>
                      </div>
                    );
                  })
              }
            </div>
          </div>
        )}

        {/* CHAT */}
        {tab === "chat" && (
          <div style={s.card}>
            <div style={s.sectionTitle}>Chat with your data</div>
            {!result && (
              <div style={{ marginBottom: "12px", fontSize: "0.82rem", color: "#64748b" }}>
                Analyze a file first to chat with dataset context.
              </div>
            )}
            <div style={{ height: "340px", overflowY: "auto", marginBottom: "14px", display: "flex", flexDirection: "column", gap: "10px" }}>
              {chatMsgs.length === 0 && (
                <div style={{ textAlign: "center", padding: "40px", color: "#475569", fontSize: "0.85rem" }}>
                  <div style={{ fontSize: "1.8rem", color: "#6366f1", marginBottom: "10px" }}>◈</div>
                  Ask anything about your dataset…
                  <div style={{ display: "flex", flexWrap: "wrap", justifyContent: "center", gap: "6px", marginTop: "14px" }}>
                    {["What are the key trends?","Which column has most outliers?","Summarize data quality","Any interesting correlations?"].map((q) => (
                      <button key={q} onClick={() => setChatInput(q)} style={{ background: "rgba(99,102,241,0.1)", border: "1px solid rgba(99,102,241,0.2)", borderRadius: "20px", padding: "5px 12px", color: "#818cf8", fontSize: "0.75rem", cursor: "pointer" }}>{q}</button>
                    ))}
                  </div>
                </div>
              )}
              {chatMsgs.map((m, i) => (
                <div key={i} style={{ display: "flex", flexDirection: "column", alignSelf: m.role === "user" ? "flex-end" : "flex-start", maxWidth: "85%", gap: "3px" }}>
                  <span style={{ fontSize: "0.65rem", fontFamily: "monospace", color: "#475569", padding: "0 4px" }}>{m.role === "user" ? "You" : "AI"}</span>
                  <div style={{ background: m.role === "user" ? "linear-gradient(135deg,#6366f1,#4f46e5)" : "#121929", color: m.role === "user" ? "#fff" : "#e2e8f0", padding: "9px 13px", borderRadius: m.role === "user" ? "10px 10px 3px 10px" : "10px 10px 10px 3px", border: m.role !== "user" ? "1px solid rgba(99,102,241,0.13)" : "none", fontSize: "0.84rem", lineHeight: 1.55 }}>{m.text}</div>
                </div>
              ))}
              {chatLoading && (
                <div style={{ display: "flex", gap: "5px", padding: "10px 14px", background: "#121929", borderRadius: "10px 10px 10px 3px", border: "1px solid rgba(99,102,241,0.13)", alignSelf: "flex-start" }}>
                  {[0,1,2].map((i) => <div key={i} style={{ width: "5px", height: "5px", borderRadius: "50%", background: "#6366f1", animation: `pulse 0.9s ${i*0.15}s infinite` }} />)}
                </div>
              )}
              <div ref={chatEndRef} />
            </div>
            <div>
              <div style={{ display: "flex", gap: "8px" }}>
                <input style={s.input} value={chatInput} onChange={(e) => e.target.value.length <= 1200 && setChatInput(e.target.value)} onKeyDown={(e) => e.key === "Enter" && sendChat()} placeholder="Ask about your data…" />
                <button style={{ ...s.btn, padding: "10px 16px", flexShrink: 0 }} onClick={sendChat} disabled={chatLoading}>→</button>
              </div>
              <div style={{ fontSize: "0.7rem", color: chatInput.length >= 1200 ? "#ef4444" : "#64748b", textAlign: "right", marginTop: "6px" }}>
                {chatInput.length} / 1200
              </div>
            </div>
          </div>
        )}

      </div>
    </div>
  );
}