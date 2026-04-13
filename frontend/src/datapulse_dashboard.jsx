import { useState, useRef, useCallback, useEffect, useMemo } from "react";
import createPlotlyComponent from "react-plotly.js/factory";
import Plotly from "plotly.js-dist-min";
import jsPDF from "jspdf";
import { apiAnalyze, apiChat, apiHistory, apiHistoryAnalysis, apiDeleteAnalysis } from "./api.js";
import ParticleBackground from "./ParticleBackground.jsx";

const Plot = createPlotlyComponent(Plotly);
const PALETTE = ["#6366f1", "#10b981", "#f59e0b", "#06b6d4", "#ef4444", "#a855f7", "#34d399", "#f472b6"];

const PLOTLY_DARK_LAYOUT = {
  paper_bgcolor: "rgba(0,0,0,0)",
  plot_bgcolor: "rgba(0,0,0,0)",
  font: { color: "#94A3B8", family: "'Inter', sans-serif", size: 12 },
  title: { font: { color: "#F1F5F9", size: 14 } },
  xaxis: { gridcolor: "rgba(99,102,241,0.1)", zerolinecolor: "rgba(99,102,241,0.2)" },
  yaxis: { gridcolor: "rgba(99,102,241,0.1)", zerolinecolor: "rgba(99,102,241,0.2)" },
  colorway: PALETTE,
  autosize: true,
  margin: { l: 40, r: 20, t: 40, b: 30 },
};

const PLOTLY_CONFIG = { responsive: true, displayModeBar: "hover", displaylogo: false, modeBarButtonsToRemove: ["lasso2d", "select2d", "toggleSpikelines"] };

function ChartPanel({ result }) {
  const charts = result?.charts || {};
  const entries = Object.entries(charts);
  
  if (entries.length === 0) {
    return <div style={{ color: "var(--text-muted)", fontSize: "14px" }}>No charts available.</div>;
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "24px" }}>
      {entries.map(([key, fig]) => (
        <div key={key} className="card" style={{ padding: '16px' }}>
          <Plot
            data={fig.data || []}
            layout={{
              ...PLOTLY_DARK_LAYOUT,
              ...(fig.layout || {}),
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

const PRIMARY_TABS = ["overview", "charts", "insights"];
const SECONDARY_TABS = ["statistics", "quality", "chat"];
const MAX_CHAT_MESSAGES = 40;

export default function DataPulse({ user, onLogout }) {
  const [phase, setPhase] = useState("upload");
  const [result, setResult] = useState(null);
  const [fileName, setFileName] = useState("");
  const [agentLog, setAgentLog] = useState([]);
  const [analysisError, setAnalysisError] = useState("");
  const [isDragOver, setIsDragOver] = useState(false);
  const [tab, setTab] = useState("overview");
  const [chatMsgs, setChatMsgs] = useState([]);
  const [chatInput, setChatInput] = useState("");
  const [chatLoading, setChatLoading] = useState(false);
  const [history, setHistory] = useState([]);
  const [historyError, setHistoryError] = useState("");
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
    if (tab !== "chat") return;
    chatEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [chatMsgs, chatLoading, tab]);

  const clearStageTimers = () => {
    stageTimersRef.current.forEach((timerId) => clearTimeout(timerId));
    stageTimersRef.current = [];
  };

  const analyzeFile = useCallback(async (file) => {
    if (!file) return;
    setPhase("analyzing");
    setResult(null);
    setAnalysisError("");
    setAgentLog([]);
    setTab("overview");
    setFileName(file.name);
    setChatMsgs([]);

    log("Uploading data to secure server…");
    log("Architect initializing models…");
    clearStageTimers();
    const stageMessages = [
      "Architect routing tasks → Statistician running…",
      "Statistician analyzing anomalies → Visualizer generating plots…",
      "Compiling AI Insights into dashboard…",
    ];
    stageTimersRef.current = stageMessages.map((msg, idx) =>
      setTimeout(() => log(msg), 500 + idx * 1200)
    );

    try {
      const data = await apiAnalyze(file);
      clearStageTimers();
      log(data.from_cache ? "Loaded accelerated cache." : "System orchestration complete.");
      setResult(data);
      if (data?.analysis_id) {
        setHistory((prev) => {
          const next = [
            {
              analysis_id: data.analysis_id,
              file_name: file.name,
              row_count: data?.stats_summary?.row_count || 0,
              column_count: data?.stats_summary?.column_count || 0,
              analyzed_at: new Date().toISOString(),
            },
            ...prev.filter((x) => x.analysis_id !== data.analysis_id),
          ];
          return next.slice(0, 20);
        });
      }
      setPhase("done");
    } catch (err) {
      clearStageTimers();
      log(`Core Failure: ${err.message}`);
      setAnalysisError(err.message || "Analysis failed");
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

  const loadHistory = async () => { /* no-op for visual changes */ };
  const toggleHistory = async () => { /* no-op for visual changes */ };
  const deleteItem = async (id) => { /* no-op */ };
  const loadHistoryItem = async (item) => { /* no-op */ };

  const chatContext = useMemo(() => {
    if (!result) return null;
    const outlierSummary = Object.entries(result.stats_summary?.outliers || {})
      .map(([column, info]) => ({ column, count: Number(info?.count || 0), percentage: Number(info?.percentage || 0) }))
      .sort((a, b) => b.count - a.count).slice(0, 10);
    return {
      fileName,
      stats: result.stats_summary,
      insights: result.insights,
      outlierSummary,
      dataQuality: result.stats_summary?.data_quality || {},
      correlations: result.stats_summary?.strong_correlations?.slice(0, 5),
    };
  }, [result, fileName]);

  const sendChat = useCallback(async () => {
    const q = chatInput.trim();
    if (!q || chatLoading || !result) return;
    setChatInput("");
    setChatMsgs((p) => [...p, { role: "user", text: q }].slice(-MAX_CHAT_MESSAGES));
    setChatLoading(true);
    try {
      const resp = await apiChat(q, chatContext || {});
      setChatMsgs((p) => [...p, { role: "ai", text: (resp.answer || "").trim() || "No response generated." }].slice(-MAX_CHAT_MESSAGES));
    } catch (err) {
      const detail = err?.message || "Unable to reach AI";
      setChatMsgs((p) => [...p, { role: "ai", text: `Chat error: ${detail}` }].slice(-MAX_CHAT_MESSAGES));
    }
    setChatLoading(false);
  }, [chatInput, chatLoading, result, chatContext]);

  const stats = result?.stats_summary || {};
  const insights = result?.insights || {};
  const dq = stats.data_quality || {};
  const numericCols = Object.keys(stats.numeric_columns || {});
  const catCols = Object.keys(stats.categorical_columns || {});
  const outlierCols = Object.keys(stats.outliers || {});
  
  const formatPercent = (value) => {
    const n = Number.isFinite(value) ? Number(value) : 100;
    return Number.isInteger(n) ? `${n}%` : `${n.toFixed(1)}%`;
  };

  const keyMetrics = [
    { label: "Total Rows", val: (stats.row_count || 0).toLocaleString() },
    { label: "Schema Columns", val: stats.column_count || 0 },
    { label: "Completeness", val: formatPercent(dq.completeness || 100) },
  ];

  const sortedNumericRows = numericCols
    .map((col) => {
      const st = stats.numeric_columns?.[col];
      return st ? { column: col, count: Number(st.count || 0), mean: Number(st.mean || 0), std: Number(st.std || 0), min: Number(st.min || 0), max: Number(st.max || 0), skewness: Number(st.skewness || 0), outliers: Number(stats.outliers?.[col]?.count || 0) } : null;
    })
    .filter(Boolean)
    .filter((row) => row.column.toLowerCase().includes(statsFilter.trim().toLowerCase()))
    .sort((a, b) => {
      const dir = statsSortDirection === "asc" ? 1 : -1;
      if (statsSortKey === "column") return a.column.localeCompare(b.column) * dir;
      return (Number(a[statsSortKey]) - Number(b[statsSortKey])) * dir;
    });

  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column', position: 'relative' }}>
      <ParticleBackground />

      {/* NAVBAR */}
      <div style={{ background: 'rgba(13, 18, 32, 0.65)', backdropFilter: 'blur(12px)', padding: '16px 48px', borderBottom: '1px solid var(--border-subtle)', display: 'flex', justifyContent: 'space-between', alignItems: 'center', position: 'sticky', top: 0, zIndex: 100 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <div style={{ color: 'var(--primary-500)', fontSize: '24px', textShadow: '0 0 10px rgba(99,102,241,0.4)' }}>◈</div>
          <strong style={{ fontSize: '18px', color: 'var(--text-main)', fontFamily: "'Syne', sans-serif" }}>DATA PULSE</strong>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '24px' }}>
          <span style={{ fontSize: '13px', color: 'var(--text-muted)' }}>{user?.email}</span>
          <button onClick={onLogout} style={{ background: 'none', border: 'none', color: 'var(--primary-500)', cursor: 'pointer', fontWeight: 600, fontSize: '13px', textTransform: 'uppercase', letterSpacing: '0.1em' }}>Logout</button>
        </div>
      </div>

      <div className="container" style={{ flex: 1, display: 'flex', flexDirection: 'column', position: 'relative', zIndex: 1 }}>
        {/* HERO SECTION REMOVED */}
        <div className="mb-32" style={{ visibility: 'hidden', height: '20px' }}></div>


        {/* MAIN SECTION (12-column grid) */}
        <div className="grid-12" style={{ alignItems: 'start' }}>
          
          {/* LEFT 5 (Upload & Chat) */}
          <div className="col-5 flex-col gap-24">
            
            {/* Upload Box */}
            <div
              className={`upload-box ${isDragOver ? 'drag-over' : ''}`}
              onDrop={onDrop}
              onDragOver={(e) => e.preventDefault()}
              onDragEnter={onDragEnter}
              onDragLeave={onDragLeave}
              onClick={() => fileRef.current.click()}
              style={{ borderColor: isDragOver ? 'var(--primary-500)' : 'var(--border-subtle)' }}
            >
              <div style={{ fontSize: '32px', color: 'var(--primary-500)', marginBottom: '16px', textShadow: '0 0 15px rgba(99,102,241,0.5)' }}>↑</div>
              <strong style={{ color: 'var(--text-main)', marginBottom: '8px', fontSize: '18px' }}>Select File</strong>
              <p className="caption" style={{ color: 'var(--text-muted)' }}>Drag (.csv, .xlsx) anywhere to begin</p>
              <input ref={fileRef} type="file" accept=".csv,.xlsx,.xls" style={{ display: "none" }} onChange={(e) => onFile(e.target.files[0])} />
            </div>

            {/* Status / File Info */}
            {fileName && (
              <div className="card" style={{ padding: '20px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                    <div style={{ padding: '8px', background: 'rgba(99,102,241,0.1)', borderRadius: '8px', fontSize: '14px', border: '1px solid rgba(99,102,241,0.2)', color: 'var(--primary-500)' }}>📄</div>
                    <strong style={{ fontSize: '14px', color: 'var(--text-main)' }}>{fileName}</strong>
                  </div>
                  {phase === "done" && <span className="data-pill success">Verified ⬢</span>}
                </div>
                
                {phase === "analyzing" && (
                  <div className="flex-col gap-8">
                    <div className="progress-container">
                      <div className="progress-bar animate-pulse" style={{ width: result ? '100%' : '65%', background: analysisError ? 'var(--error)' : 'var(--primary-500)' }} />
                    </div>
                    <div style={{ fontSize: '12px', color: 'var(--primary-500)', fontFamily: "'Outfit', monospace", textTransform: 'uppercase' }}>
                      {analysisError ? <span className="text-error">{analysisError}</span> : agentLog[agentLog.length - 1]?.msg || "Orchestrating..."}
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Chat Box (only if done) */}
            {phase === "done" && result && (
              <div className="card flex-col gap-16" style={{ padding: '20px' }}>
                <strong style={{ fontSize: '14px', color: 'var(--text-main)', fontFamily: 'Syne, sans-serif' }}>Neural Chat Uplink</strong>
                <div style={{ height: "240px", overflowY: "auto", display: "flex", flexDirection: "column", gap: "12px", background: 'var(--bg-input)', padding: '16px', borderRadius: '12px', border: '1px solid var(--border-subtle)' }}>
                   {chatMsgs.length === 0 ? (
                     <div style={{ margin: 'auto', textAlign: 'center', fontSize: '13px', color: 'var(--text-muted)' }}>Establish a query connection with your data.</div>
                   ) : chatMsgs.map((m, i) => (
                     <div key={i} style={{ alignSelf: m.role === 'user' ? 'flex-end' : 'flex-start', background: m.role === 'user' ? 'rgba(99,102,241,0.15)' : 'var(--bg-card)', color: m.role === 'user' ? '#818cf8' : 'var(--text-main)', padding: '10px 16px', borderRadius: '8px', fontSize: '13px', maxWidth: '85%', border: m.role === 'user' ? '1px solid rgba(99,102,241,0.3)' : '1px solid var(--border-subtle)', boxShadow: m.role === 'user' ? '0 0 10px rgba(99,102,241,0.1)' : 'none' }}>
                       {m.text}
                     </div>
                   ))}
                   {chatLoading && <div style={{ fontSize: '13px', color: 'var(--primary-500)', fontFamily: "'Outfit', monospace" }}>Synthesizing...</div>}
                   <div ref={chatEndRef} />
                </div>
                <div style={{ display: 'flex', gap: '12px' }}>
                  <input className="input-field" value={chatInput} onChange={e => setChatInput(e.target.value)} onKeyDown={e => e.key === 'Enter' && sendChat()} style={{ flex: 1, fontSize: '14px' }} placeholder="Inject query..." />
                  <button className="btn-primary" onClick={sendChat} disabled={chatLoading} style={{ width: '44px', padding: 0 }}>»</button>
                </div>
              </div>
            )}

          </div>

          {/* RIGHT 7 (Results Card) */}
          <div className="col-7">
            {phase === "upload" || phase === "analyzing" ? (
              <div style={{ minHeight: '440px' }} className="animate-fade-in">
                 {phase === "upload" ? null : (
                   <>
                    <div style={{ fontSize: '40px', color: 'var(--primary-500)', marginBottom: '24px', animation: 'spin 4s linear infinite', textShadow: '0 0 20px rgba(99,102,241,0.8)' }}>⊛</div>
                    <strong style={{ fontSize: '18px', color: 'var(--text-main)', marginBottom: '8px', fontFamily: "'Syne', sans-serif" }}>Processing Array...</strong>
                    <p style={{ fontSize: '14px', color: 'var(--primary-500)', fontFamily: "'Outfit', monospace" }}>{agentLog[agentLog.length - 1]?.msg || "Extracting signatures..."}</p>
                   </>
                 )}
              </div>
            ) : (
              <div className="card flex-col gap-24 animate-fade-in">
                <div style={{ display: 'flex', borderBottom: '1px solid var(--border-subtle)', gap: '16px', paddingBottom: '12px' }}>
                  {PRIMARY_TABS.map(t => <button key={t} onClick={() => setTab(t)} style={{ background: 'none', border: 'none', color: tab === t ? 'var(--text-main)' : 'var(--text-muted)', fontWeight: tab === t ? 600 : 500, fontSize: '14px', cursor: 'pointer', borderBottom: tab === t ? '2px solid var(--primary-500)' : 'none', paddingBottom: '12px', marginBottom: '-13px', textTransform: 'capitalize', letterSpacing: '0.05em' }}>{t}</button>)}
                  <div style={{ width: '1px', background: 'var(--border-subtle)', height: '20px' }} />
                  {SECONDARY_TABS.map(t => <button key={t} onClick={() => setTab(t)} style={{ background: 'none', border: 'none', color: tab === t ? 'var(--text-main)' : 'var(--text-muted)', fontWeight: tab === t ? 600 : 500, fontSize: '14px', cursor: 'pointer', borderBottom: tab === t ? '2px solid var(--primary-500)' : 'none', paddingBottom: '12px', marginBottom: '-13px', textTransform: 'capitalize', letterSpacing: '0.05em' }}>{t}</button>)}
                </div>
                
                {tab === "overview" && (
                   <div className="flex-col gap-24">
                     {insights?.headline && (
                       <div style={{ padding: '20px', background: 'rgba(99,102,241,0.05)', borderRadius: '12px', borderLeft: '4px solid var(--primary-500)' }}>
                         <strong style={{ fontSize: '12px', color: 'var(--primary-500)', textTransform: 'uppercase', display: 'block', marginBottom: '8px', letterSpacing: '0.1em' }}>Executive Matrix</strong>
                         <p style={{ fontSize: '15px', color: 'var(--text-main)' }}>{insights.headline}</p>
                       </div>
                     )}
                     <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '16px' }}>
                       {keyMetrics.map(m => (
                         <div key={m.label} style={{ padding: '20px', border: '1px solid var(--border-subtle)', borderRadius: '12px', background: 'var(--bg-input)' }}>
                           <strong style={{ fontSize: '12px', color: 'var(--text-muted)', display: 'block', marginBottom: '8px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>{m.label}</strong>
                           <span style={{ fontSize: '28px', fontFamily: "'Syne', sans-serif", fontWeight: 800, color: 'var(--text-main)', textShadow: '0 0 15px rgba(255,255,255,0.1)' }}>{m.val}</span>
                         </div>
                       ))}
                     </div>
                     <div style={{ padding: '20px', background: 'var(--bg-input)', borderRadius: '12px', border: '1px solid var(--border-subtle)' }}>
                       <strong style={{ fontSize: '15px', display: 'block', marginBottom: '16px', color: 'var(--text-main)', fontFamily: "'Syne', sans-serif" }}>Detected Vectors</strong>
                       {(insights?.findings || []).slice(0, 5).map((f, i) => (
                          <div key={i} style={{ fontSize: '15px', color: 'var(--text-muted)', marginBottom: '12px', display: 'flex', gap: '12px', alignItems: 'center' }}>
                            <div style={{ width: '6px', height: '6px', background: 'var(--primary-500)', borderRadius: '50%', boxShadow: '0 0 10px var(--primary-500)' }} /> {f}
                          </div>
                       ))}
                     </div>
                   </div>
                )}

                {tab === "charts" && <ChartPanel result={result} />}
                
                {tab === "insights" && (
                  <div className="flex-col gap-24">
                    <div style={{ padding: '20px', background: 'var(--bg-input)', borderRadius: '12px', border: '1px solid var(--border-subtle)' }}>
                       <strong style={{ fontSize: '15px', display: 'block', marginBottom: '16px', color: 'var(--text-main)', fontFamily: "'Syne', sans-serif" }}>Actionable Protocols</strong>
                       {(insights?.recommendations || []).map((r, i) => (
                          <div key={i} style={{ fontSize: '15px', color: 'var(--text-muted)', marginBottom: '12px', display: 'flex', gap: '12px' }}>
                            <span style={{ color: 'var(--primary-500)', fontSize: '18px' }}>⇥</span> {r}
                          </div>
                       ))}
                     </div>
                  </div>
                )}

                {tab === "statistics" && (
                  <div style={{ overflowX: 'auto', border: '1px solid var(--border-subtle)', borderRadius: '12px' }}>
                     <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '14px', textAlign: 'left', background: 'var(--bg-input)' }}>
                        <thead style={{ background: 'rgba(99,102,241,0.05)', borderBottom: '1px solid var(--border-subtle)' }}>
                          <tr>
                            {['Column', 'Count', 'Mean', 'Std', 'Min', 'Max', 'Outliers'].map(h => (
                               <th key={h} style={{ padding: '16px 12px', color: 'var(--text-muted)', fontWeight: 600, textTransform: 'uppercase', fontSize: '12px', letterSpacing: '0.05em' }}>{h}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {sortedNumericRows.map((r, idx) => (
                             <tr key={r.column} style={{ borderBottom: idx === sortedNumericRows.length - 1 ? 'none' : '1px solid var(--border-subtle)' }}>
                               <td style={{ padding: '16px 12px', color: '#818cf8', fontWeight: 500, fontFamily: "'Outfit', monospace" }}>{r.column}</td>
                               <td style={{ padding: '16px 12px', color: 'var(--text-muted)' }}>{r.count}</td>
                               <td style={{ padding: '16px 12px', color: 'var(--text-main)' }}>{r.mean.toFixed(2)}</td>
                               <td style={{ padding: '16px 12px', color: 'var(--text-muted)' }}>{r.std.toFixed(2)}</td>
                               <td style={{ padding: '16px 12px', color: 'var(--text-muted)' }}>{r.min.toFixed(2)}</td>
                               <td style={{ padding: '16px 12px', color: 'var(--text-muted)' }}>{r.max.toFixed(2)}</td>
                               <td style={{ padding: '16px 12px', color: r.outliers > 0 ? 'var(--error)' : 'var(--success)' }}>{r.outliers}</td>
                             </tr>
                          ))}
                        </tbody>
                     </table>
                  </div>
                )}
                
                {tab === "quality" && (
                  <div className="flex-col gap-16">
                    <strong style={{ fontSize: '15px', color: 'var(--text-main)', fontFamily: "'Syne', sans-serif" }}>Detected Anomalies</strong>
                    {outlierCols.length === 0 ? (
                      <p style={{ fontSize: '14px', color: 'var(--success)' }}>All systems nominal. No statistical outliers detected.</p>
                    ) : outlierCols.map(col => {
                       const info = stats.outliers[col];
                       return (
                         <div key={col} style={{ padding: '16px', display: 'flex', justifyContent: 'space-between', background: 'var(--bg-input)', border: '1px solid rgba(239,68,68,0.2)', borderRadius: '12px' }}>
                           <strong style={{ fontSize: '14px', color: '#fca5a5', fontFamily: "'Outfit', monospace" }}>{col}</strong>
                           <span style={{ fontSize: '14px', color: 'var(--text-muted)' }}>{info.count} signals ({info.percentage?.toFixed(1)}%)</span>
                         </div>
                       )
                    })}
                  </div>
                )}
                
              </div>
            )}
          </div>

        </div>
      </div>
    </div>
  );
}