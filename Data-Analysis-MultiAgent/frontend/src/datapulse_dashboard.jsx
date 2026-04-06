/**
 * datapulse_dashboard.jsx  (v2 — API-wired)
 * ──────────────────────────────────────────
 * Changes vs the standalone v1
 * ─────────────────────────────
 * • Removed parseCSV / local analysis helpers — server does the heavy lifting.
 * • File upload calls apiAnalyze() from api.js (JWT auto-injected).
 * • callLLM() replaced by apiChat() — proxied through /api/chat or inline
 *   via the Anthropic endpoint (key stays server-side).
 * • Accepts  props: { user, onLogout }  from App.jsx.
 * • History panel calls apiHistory() / apiDeleteAnalysis().
 * • Charts arrive as plain Plotly JSON dicts; rendered with recharts helpers
 *   the same way as before (bar/donut/scatter/histogram/line).
 */

import { useState, useRef, useCallback, useEffect } from "react";
import {
  BarChart, Bar, LineChart, Line, ScatterChart, Scatter,
  PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend,
} from "recharts";
import { apiAnalyze, apiChat, apiHistory, apiDeleteAnalysis } from "./api.js";

// ── Palette ───────────────────────────────────────────────────────────────────
const PALETTE = ["#6366f1","#10b981","#f59e0b","#06b6d4","#ef4444","#a78bfa","#34d399","#f472b6"];
const COLORS  = { tooltip: { contentStyle: { background: "#0d1220", border: "1px solid rgba(99,102,241,0.2)", borderRadius: "7px", fontSize: "11px", fontFamily: "monospace", color: "#e2e8f0" } } };

// ── Styles ────────────────────────────────────────────────────────────────────
const s = {
  app: { minHeight: "100vh", background: "#060912", color: "#e2e8f0", fontFamily: "'Outfit', sans-serif", display: "flex", flexDirection: "column" },
  topbar: { background: "#0d1220", borderBottom: "1px solid rgba(99,102,241,0.15)", padding: "14px 28px", display: "flex", alignItems: "center", justifyContent: "space-between", position: "sticky", top: 0, zIndex: 20 },
  brand: { fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: "1.15rem", letterSpacing: "-0.02em", color: "#818cf8" },
  content: { flex: 1, padding: "28px 32px", display: "flex", flexDirection: "column", gap: "24px", maxWidth: "1300px", margin: "0 auto", width: "100%" },
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

// ── Chart panel (recharts) ────────────────────────────────────────────────────
function ChartPanel({ result }) {
  const stats   = result?.stats_summary || {};
  const numeric  = Object.keys(stats.numeric_columns || {});
  const catCols  = Object.keys(stats.categorical_columns || {});
  const rows     = result?.clean_df || result?.raw_df || [];

  if (!rows.length) return <div style={{ color: "#475569", fontSize: "0.88rem" }}>No data available for charts.</div>;

  const barData = catCols.length && numeric.length
    ? (() => {
        const col = catCols[0], val = numeric[0];
        const group = {};
        rows.forEach((r) => { if (!group[r[col]]) group[r[col]] = []; if (typeof r[val] === "number") group[r[col]].push(r[val]); });
        return Object.entries(group).slice(0, 10).map(([k, vs]) => ({ name: String(k).slice(0, 14), value: vs.length ? +(vs.reduce((a, b) => a + b, 0) / vs.length).toFixed(2) : 0 }));
      })()
    : null;

  const donutData = catCols.length
    ? (stats.categorical_columns[catCols[0]]?.top_5_values
        ? Object.entries(stats.categorical_columns[catCols[0]].top_5_values).slice(0, 6).map(([name, count]) => ({ name, count }))
        : null)
    : null;

  const scatterPair = (() => {
    for (let i = 0; i < numeric.length - 1; i++) {
      const a = numeric[i], b = numeric[i + 1];
      const d = rows.slice(0, 200).filter((r) => typeof r[a] === "number" && typeof r[b] === "number").map((r) => ({ x: r[a], y: r[b] }));
      if (d.length > 5) return { data: d, xKey: a, yKey: b };
    }
    return null;
  })();

  const histData = numeric.length
    ? (() => {
        const col = numeric[0];
        const st  = stats.numeric_columns?.[col];
        if (!st) return null;
        const vals = rows.map((r) => r[col]).filter((v) => typeof v === "number");
        const mn = st.min, mx = st.max, bins = 10;
        const size = (mx - mn) / bins || 1;
        const counts = Array(bins).fill(0);
        vals.forEach((v) => { const i = Math.min(Math.floor((v - mn) / size), bins - 1); counts[i]++; });
        return counts.map((c, i) => ({ bin: (mn + i * size).toFixed(1), count: c }));
      })()
    : null;

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(380px,1fr))", gap: "20px" }}>
      {barData && (
        <div style={s.card}>
          <div style={s.sectionTitle}>Avg {numeric[0]} by {catCols[0]}</div>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={barData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(99,102,241,0.07)" />
              <XAxis dataKey="name" tick={{ fill: "#475569", fontSize: 10, fontFamily: "monospace" }} />
              <YAxis tick={{ fill: "#475569", fontSize: 10, fontFamily: "monospace" }} />
              <Tooltip {...COLORS.tooltip} />
              <Bar dataKey="value" radius={[4, 4, 0, 0]}>{barData.map((_, i) => <Cell key={i} fill={PALETTE[i % PALETTE.length]} />)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
      {donutData && (
        <div style={s.card}>
          <div style={s.sectionTitle}>Composition — {catCols[0]}</div>
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie data={donutData.map((d) => ({ name: String(d.name).slice(0, 18), value: d.count }))} cx="50%" cy="50%" innerRadius={60} outerRadius={90} paddingAngle={2} dataKey="value">
                {donutData.map((_, i) => <Cell key={i} fill={PALETTE[i % PALETTE.length]} />)}
              </Pie>
              <Tooltip {...COLORS.tooltip} />
              <Legend wrapperStyle={{ fontSize: "11px", color: "#64748b", fontFamily: "monospace" }} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      )}
      {scatterPair && (
        <div style={s.card}>
          <div style={s.sectionTitle}>{scatterPair.xKey} vs {scatterPair.yKey}</div>
          <ResponsiveContainer width="100%" height={220}>
            <ScatterChart>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(99,102,241,0.07)" />
              <XAxis dataKey="x" name={scatterPair.xKey} tick={{ fill: "#475569", fontSize: 10, fontFamily: "monospace" }} />
              <YAxis dataKey="y" name={scatterPair.yKey} tick={{ fill: "#475569", fontSize: 10, fontFamily: "monospace" }} />
              <Tooltip cursor={{ strokeDasharray: "3 3" }} {...COLORS.tooltip} />
              <Scatter data={scatterPair.data} fill="#f59e0b" opacity={0.65} />
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      )}
      {histData && (
        <div style={s.card}>
          <div style={s.sectionTitle}>Distribution — {numeric[0]}</div>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={histData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(99,102,241,0.07)" />
              <XAxis dataKey="bin" tick={{ fill: "#475569", fontSize: 9, fontFamily: "monospace" }} />
              <YAxis tick={{ fill: "#475569", fontSize: 10, fontFamily: "monospace" }} />
              <Tooltip {...COLORS.tooltip} />
              <Bar dataKey="count" fill="#ef4444" fillOpacity={0.65} radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
      {numeric.length >= 2 && (() => {
        const lineData = rows.slice(0, 120).map((r, i) => {
          const obj = { idx: i };
          numeric.slice(0, 3).forEach((c) => { if (typeof r[c] === "number") obj[c] = r[c]; });
          return obj;
        });
        return (
          <div style={{ ...s.card, gridColumn: "1/-1" }}>
            <div style={s.sectionTitle}>Numeric trends across rows</div>
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={lineData}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(99,102,241,0.07)" />
                <XAxis dataKey="idx" tick={{ fill: "#475569", fontSize: 10 }} />
                <YAxis tick={{ fill: "#475569", fontSize: 10 }} />
                <Tooltip {...COLORS.tooltip} />
                <Legend wrapperStyle={{ fontSize: "11px", color: "#64748b" }} />
                {numeric.slice(0, 3).map((c, i) => <Line key={c} type="monotone" dataKey={c} stroke={PALETTE[i]} dot={false} strokeWidth={1.5} />)}
              </LineChart>
            </ResponsiveContainer>
          </div>
        );
      })()}
    </div>
  );
}

// ── Main dashboard ────────────────────────────────────────────────────────────
const TABS = ["overview", "charts", "insights", "statistics", "quality", "chat"];

export default function DataPulse({ user, onLogout }) {
  const [phase, setPhase]     = useState("upload");
  const [result, setResult]   = useState(null);
  const [fileName, setFileName] = useState("");
  const [agentLog, setAgentLog] = useState([]);
  const [tab, setTab]         = useState("overview");
  const [chatMsgs, setChatMsgs]   = useState([]);
  const [chatInput, setChatInput] = useState("");
  const [chatLoading, setChatLoading] = useState(false);
  const [history, setHistory] = useState([]);
  const [showHistory, setShowHistory] = useState(false);
  const fileRef = useRef();

  const log = (msg) => setAgentLog((p) => [...p, { time: new Date().toLocaleTimeString(), msg }]);

  // ── File upload → API ──────────────────────────────────────────────────────
  const analyzeFile = useCallback(async (file) => {
    if (!file) return;
    setPhase("analyzing");
    setAgentLog([]);
    setTab("overview");
    setFileName(file.name);
    setChatMsgs([]);

    log("Uploading CSV to server…");
    try {
      log("Running multi-agent pipeline (architect → statistician → visualizer → insights)…");
      const data = await apiAnalyze(file);
      log(data.from_cache ? "Returned cached analysis." : "Pipeline complete.");
      setResult(data);
      setPhase("done");
    } catch (err) {
      log(`Error: ${err.message}`);
      setPhase("upload");
      alert(`Analysis failed: ${err.message}`);
    }
  }, []);

  const onFile = useCallback((file) => analyzeFile(file), [analyzeFile]);
  const onDrop = useCallback((e) => { e.preventDefault(); onFile(e.dataTransfer.files[0]); }, [onFile]);

  // ── History ────────────────────────────────────────────────────────────────
  const loadHistory = async () => {
    try {
      const resp = await apiHistory();
      setHistory(resp.analyses || []);
    } catch (err) {
      console.error("History load failed:", err.message);
    }
  };

  const toggleHistory = async () => {
    const next = !showHistory;
    setShowHistory(next);
    if (next) await loadHistory();
  };

  const deleteItem = async (id) => {
    try {
      await apiDeleteAnalysis(id);
      setHistory((h) => h.filter((x) => x.analysis_id !== id));
    } catch (err) {
      alert(`Delete failed: ${err.message}`);
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

  // ── Upload screen ──────────────────────────────────────────────────────────
  if (phase === "upload") return (
    <div style={s.app}>
      <div style={s.topbar}>
        <span style={s.brand}>◈ DataPulse</span>
        <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
          <span style={{ fontSize: "0.78rem", color: "#475569", fontFamily: "monospace" }}>{user?.email}</span>
          <button style={{ ...s.btn, fontSize: "0.72rem", padding: "7px 14px", background: "transparent", border: "1px solid rgba(99,102,241,0.2)", color: "#818cf8" }} onClick={onLogout}>Logout</button>
        </div>
      </div>
      <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", padding: "40px" }}>
        <div style={{ textAlign: "center", maxWidth: "480px", width: "100%" }} onDrop={onDrop} onDragOver={(e) => e.preventDefault()}>
          <div style={{ fontSize: "2.8rem", marginBottom: "20px", color: "#6366f1" }}>◈</div>
          <h1 style={{ fontFamily: "'Syne',sans-serif", fontSize: "1.8rem", fontWeight: 800, color: "#f1f5f9", marginBottom: "10px" }}>AI Data Analyst</h1>
          <p style={{ color: "#64748b", fontSize: "0.9rem", marginBottom: "32px", lineHeight: 1.7 }}>Upload any CSV and get instant AI-powered insights, charts, correlations, and recommendations.</p>
          <div onClick={() => fileRef.current.click()} style={{ border: "2px dashed rgba(99,102,241,0.3)", borderRadius: "12px", padding: "40px", cursor: "pointer", background: "rgba(99,102,241,0.03)" }}>
            <div style={{ fontSize: "2rem", marginBottom: "12px", color: "#475569" }}>↑</div>
            <p style={{ color: "#94a3b8", fontSize: "0.88rem" }}>Click to upload or drag a CSV file here</p>
            <input ref={fileRef} type="file" accept=".csv" style={{ display: "none" }} onChange={(e) => onFile(e.target.files[0])} />
          </div>
          <div style={{ marginTop: "24px", display: "flex", justifyContent: "center", gap: "8px", flexWrap: "wrap" }}>
            {["Architect","Statistician","Visualizer","Insights","Chat"].map((a) => (
              <span key={a} style={s.pill(false)}>{a}</span>
            ))}
          </div>
        </div>
      </div>
    </div>
  );

  // ── Analyzing screen ───────────────────────────────────────────────────────
  if (phase === "analyzing") return (
    <div style={s.app}>
      <div style={s.topbar}><span style={s.brand}>◈ DataPulse</span></div>
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
      <div style={s.topbar}>
        <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
          <span style={s.brand}>◈ DataPulse</span>
          <span style={{ fontFamily: "monospace", fontSize: "0.72rem", background: "rgba(99,102,241,0.12)", border: "1px solid rgba(99,102,241,0.22)", color: "#818cf8", padding: "2px 10px", borderRadius: "20px" }}>{fileName}</span>
          {result?.from_cache && <span style={{ fontSize: "0.7rem", color: "#10b981", background: "rgba(16,185,129,0.08)", border: "1px solid rgba(16,185,129,0.2)", padding: "2px 8px", borderRadius: "12px" }}>CACHED</span>}
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
          <span style={{ fontSize: "0.75rem", color: "#10b981", background: "rgba(16,185,129,0.08)", border: "1px solid rgba(16,185,129,0.2)", padding: "4px 12px", borderRadius: "20px" }}>✓ Analysis Complete</span>
          <button style={{ ...s.btn, fontSize: "0.72rem", padding: "7px 14px" }} onClick={toggleHistory}>History</button>
          <button style={{ ...s.btn, fontSize: "0.72rem", padding: "7px 14px" }} onClick={() => { setResult(null); setPhase("upload"); }}>New File</button>
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
                <div key={item.analysis_id} style={{ background: "#0d1220", border: "1px solid rgba(99,102,241,0.15)", borderRadius: "8px", padding: "10px 14px", fontSize: "0.78rem", display: "flex", alignItems: "center", gap: "14px" }}>
                  <span style={{ color: "#94a3b8" }}>{item.file_name}</span>
                  <span style={{ color: "#475569", fontFamily: "monospace", fontSize: "0.7rem" }}>{item.row_count}r × {item.column_count}c</span>
                  <button onClick={() => deleteItem(item.analysis_id)} style={{ background: "transparent", border: "none", color: "#ef4444", cursor: "pointer", fontSize: "0.75rem", padding: "0" }}>✕</button>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Tab bar */}
      <div style={{ background: "#0d1220", borderBottom: "1px solid rgba(99,102,241,0.12)", padding: "0 32px", display: "flex", gap: 0 }}>
        {TABS.map((t) => <button key={t} style={s.tab(tab === t)} onClick={() => setTab(t)}>{t}</button>)}
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
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(140px,1fr))", gap: "12px" }}>
              {[
                { label: "Rows",             val: (stats.row_count || 0).toLocaleString() },
                { label: "Columns",          val: stats.column_count || 0 },
                { label: "Numeric cols",     val: numericCols.length },
                { label: "Categorical cols", val: catCols.length },
                { label: "Missing values",   val: missingTotal },
                { label: "Completeness",     val: `${completeness.toFixed(1)}%` },
                { label: "Outlier cols",     val: outlierCols.length },
                { label: "Correlations",     val: (stats.strong_correlations || []).length },
              ].map(({ label, val }) => (
                <div key={label} style={s.metric}>
                  <div style={s.metricLabel}>{label}</div>
                  <div style={s.metricVal}>{val}</div>
                </div>
              ))}
            </div>
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
        {tab === "charts" && <ChartPanel result={result} />}

        {/* INSIGHTS */}
        {tab === "insights" && (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(320px,1fr))", gap: "20px" }}>
            <div style={s.card}>
              <div style={s.sectionTitle}>Key findings</div>
              {(insights?.findings || []).map((f, i) => (
                <div key={i} style={{ display: "flex", gap: "10px", marginBottom: "10px", fontSize: "0.84rem", color: "#cbd5e1", lineHeight: 1.55 }}>
                  <span style={{ color: "#6366f1", fontWeight: 700 }}>{i + 1}</span> {f}
                </div>
              ))}
            </div>
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
        )}

        {/* STATISTICS */}
        {tab === "statistics" && (
          <div style={{ display: "flex", flexDirection: "column", gap: "20px" }}>
            {numericCols.length > 0 && (
              <div style={s.card}>
                <div style={s.sectionTitle}>Numeric columns</div>
                <div style={{ overflowX: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.78rem", fontFamily: "monospace" }}>
                    <thead>
                      <tr style={{ color: "#475569", borderBottom: "1px solid rgba(99,102,241,0.1)" }}>
                        {["Column","Count","Mean","Std","Min","Max","Skewness","Outliers"].map((h) => (
                          <th key={h} style={{ padding: "8px 12px", textAlign: "left", fontWeight: 600, fontSize: "0.68rem", textTransform: "uppercase" }}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {numericCols.map((col) => {
                        const st = stats.numeric_columns[col];
                        const outlierCount = stats.outliers?.[col]?.count || 0;
                        if (!st) return null;
                        return (
                          <tr key={col} style={{ borderBottom: "1px solid rgba(99,102,241,0.06)" }}>
                            <td style={{ padding: "8px 12px", color: "#818cf8", fontWeight: 600 }}>{col}</td>
                            <td style={{ padding: "8px 12px", color: "#94a3b8" }}>{st.count}</td>
                            <td style={{ padding: "8px 12px", color: "#e2e8f0" }}>{st.mean?.toFixed(2)}</td>
                            <td style={{ padding: "8px 12px", color: "#94a3b8" }}>{st.std?.toFixed(2)}</td>
                            <td style={{ padding: "8px 12px", color: "#64748b" }}>{st.min?.toFixed(2)}</td>
                            <td style={{ padding: "8px 12px", color: "#64748b" }}>{st.max?.toFixed(2)}</td>
                            <td style={{ padding: "8px 12px", color: Math.abs(st.skewness) > 1 ? "#f59e0b" : "#10b981" }}>{st.skewness?.toFixed(2)}</td>
                            <td style={{ padding: "8px 12px", color: outlierCount > 0 ? "#ef4444" : "#10b981" }}>{outlierCount}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}

        {/* QUALITY */}
        {tab === "quality" && (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(340px,1fr))", gap: "20px" }}>
            <div style={s.card}>
              <div style={s.sectionTitle}>Data quality summary</div>
              {[
                { label: "Total cells",    val: dq.total_cells || 0 },
                { label: "Missing cells",  val: dq.missing_cells || 0 },
                { label: "Duplicate rows", val: dq.duplicate_rows || 0 },
                { label: "Completeness",   val: `${(dq.completeness || 100).toFixed(1)}%` },
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
            </div>
            <div style={{ display: "flex", gap: "8px" }}>
              <input style={s.input} value={chatInput} onChange={(e) => setChatInput(e.target.value)} onKeyDown={(e) => e.key === "Enter" && sendChat()} placeholder="Ask about your data…" />
              <button style={{ ...s.btn, padding: "10px 16px", flexShrink: 0 }} onClick={sendChat} disabled={chatLoading}>→</button>
            </div>
          </div>
        )}

      </div>
    </div>
  );
}