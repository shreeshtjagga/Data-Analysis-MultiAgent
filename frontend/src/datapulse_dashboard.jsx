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
  font: { color: "#FFFFFF", family: "'Inter', sans-serif", size: 12 },
  title: { font: { color: "#FFFFFF", size: 14 } },
  xaxis: { gridcolor: "rgba(99,102,241,0.1)", zerolinecolor: "rgba(99,102,241,0.2)", tickfont: { color: "#FFFFFF" } },
  yaxis: { gridcolor: "rgba(99,102,241,0.1)", zerolinecolor: "rgba(99,102,241,0.2)", tickfont: { color: "#FFFFFF" } },
  colorway: PALETTE,
  autosize: true,
  margin: { l: 40, r: 20, t: 40, b: 30 },
};

const PLOTLY_CONFIG = { responsive: true, displayModeBar: false, displaylogo: false, modeBarButtonsToRemove: ["lasso2d", "select2d", "toggleSpikelines"] };

function ChartPanel({ result }) {
  const charts = result?.charts || {};
  const entries = Object.entries(charts)
    .map(([key, fig]) => {
      if (!fig || typeof fig !== "object") {
        return [key, { data: [], layout: {} }];
      }
      const data = Array.isArray(fig.data) ? fig.data : [];
      const layout = fig.layout && typeof fig.layout === "object" ? fig.layout : {};
      return [key, { data, layout }];
    });
  
  if (entries.length === 0) {
    return <div style={{ color: "var(--text-muted)", fontSize: "14px" }}>No charts available.</div>;
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "32px", paddingBottom: "40px" }}>
      {entries.map(([key, fig]) => (
        <div key={key} className="card" style={{ padding: '24px', backgroundColor: 'rgba(13, 18, 32, 0.7)', backdropFilter: 'blur(8px)' }}>
          <Plot
            data={fig.data}
            layout={{
              ...PLOTLY_DARK_LAYOUT,
              ...fig.layout,
              title: { 
                ...(fig.layout?.title || {}), 
                font: { color: "#FFFFFF", size: 16, weight: 'bold' } 
              },
              paper_bgcolor: "rgba(0,0,0,0)",
              plot_bgcolor: "rgba(0,0,0,0)",
              font: { color: "#FFFFFF", family: "'Inter', sans-serif" },
              height: 300,
            }}
            config={PLOTLY_CONFIG}
            style={{ width: "100%", height: "300px" }}
            useResizeHandler
          />
        </div>
      ))}
    </div>
  );
}

const PRIMARY_TABS = ["overview", "charts", "insights"];
const SECONDARY_TABS = ["statistics", "quality"];
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
  const [_historyError, setHistoryError] = useState("");
  const [showHistory, setShowHistory] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [deleteLoading, setDeleteLoading] = useState(null);
  const [historySelectLoading, setHistorySelectLoading] = useState(null);
  const [statsSortKey, _setStatsSortKey] = useState("outliers");
  const [statsSortDirection, _setStatsSortDirection] = useState("desc");
  const [statsFilter, _setStatsFilter] = useState("");
  const fileRef = useRef();
  const chatEndRef = useRef(null);
  const dragCounterRef = useRef(0);
  const stageTimersRef = useRef([]);

  useEffect(() => {
    loadHistory();
  }, []);

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

  const loadHistory = async () => {
    setHistoryLoading(true);
    try {
      const data = await apiHistory();
      const list = Array.isArray(data) ? data : (Array.isArray(data?.analyses) ? data.analyses : []);
      setHistory(list);
    } catch (err) {
      setHistoryError(err.message);
      setHistory([]);
    } finally {
      setHistoryLoading(false);
    }
  };

  const toggleHistory = async () => {
    if (!showHistory) await loadHistory();
    setShowHistory(!showHistory);
  };

  const deleteItem = async (id) => {
    setDeleteLoading(id);
    try {
      await apiDeleteAnalysis(id);
      setHistory(p => p.filter(x => x.analysis_id !== id));
    } catch (err) {
      alert("Failed to delete record");
    } finally {
      setDeleteLoading(null);
    }
  };

  const loadHistoryItem = async (item) => {
    setHistorySelectLoading(item.analysis_id);
    try {
      const data = await apiHistoryAnalysis(item.analysis_id);
      setResult(data);
      setFileName(item.file_name);
      setPhase("done");
      setShowHistory(false);
      setTab("overview");
    } catch (err) {
      alert("Failed to restore session");
    } finally {
      setHistorySelectLoading(null);
    }
  };

  const exportPDF = () => {
    if (!result) return;
    const doc = new jsPDF();
    doc.setFont("helvetica", "bold");
    doc.setFontSize(22);
    doc.setTextColor(99, 102, 241);
    doc.text("DATA PULSE — Analysis Report", 20, 25);
    
    doc.setFontSize(11);
    doc.setTextColor(148, 163, 184);
    doc.setFont("helvetica", "normal");
    doc.text(`Origin: ${fileName}`, 20, 35);
    doc.text(`Generated: ${new Date().toLocaleString()}`, 20, 42);

    let y = 60;
    if (insights?.headline) {
      doc.setFont("helvetica", "bold");
      doc.setTextColor(15, 23, 42); // Dark for body readability
      doc.text("EXECUTIVE SUMMARY", 20, y);
      y += 10;
      doc.setFont("helvetica", "normal");
      const headTxt = String(insights.headline);
      const lines = doc.splitTextToSize(headTxt, 170);
      doc.text(lines, 20, y);
      y += lines.length * 7 + 15;
    }

    if (insights?.findings) {
      doc.setFont("helvetica", "bold");
      doc.text("DETECTED VECTORS (FINDINGS)", 20, y);
      y += 10;
      doc.setFont("helvetica", "normal");
      insights.findings.forEach(f => {
        const lines = doc.splitTextToSize(`• ${f}`, 170);
        if (y > 270) { doc.addPage(); y = 20; }
        doc.text(lines, 20, y);
        y += lines.length * 7 + 5;
      });
    }

    doc.save(`DataPulse_Report_${new Date().getTime()}.pdf`);
  };

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
  const _catCols = Object.keys(stats.categorical_columns || {});
  const outlierCols = Object.keys(stats.outliers || {});
  const toTextList = (value) => {
    if (Array.isArray(value)) {
      return value
        .map((item) => {
          if (typeof item === "string") return item;
          if (item == null) return "";
          if (typeof item === "object") {
            return String(item.text || item.message || item.title || JSON.stringify(item));
          }
          return String(item);
        })
        .filter((item) => item.trim().length > 0);
    }
    if (value == null) return [];
    if (typeof value === "object") {
      return [String(value.text || value.message || value.title || JSON.stringify(value))];
    }
    return [String(value)];
  };
  const findings = toTextList(insights?.findings);
  const recommendations = toTextList(insights?.recommendations);
  const headline = (() => {
    const value = insights?.headline;
    if (value == null) return "";
    if (typeof value === "object") {
      return String(value.text || value.message || value.title || JSON.stringify(value));
    }
    return String(value);
  })();
  
  const formatPercent = (value) => {
    const n = Number.isFinite(value) ? Number(value) : 100;
    return Number.isInteger(n) ? `${n}%` : `${n.toFixed(1)}%`;
  };

  const f = (n) => (typeof n === 'number' && isFinite(n) ? n.toFixed(2) : "0.00");

  const keyMetrics = [
    { label: "Total Rows", val: (stats.row_count || 0).toLocaleString() },
    { label: "Schema Columns", val: stats.column_count || 0 },
    { label: "Completeness", val: formatPercent(dq.completeness || 100) },
  ];

  const sortedNumericRows = numericCols
    .map((col) => {
      const st = stats.numeric_columns?.[col];
      return st ? { 
        column: col, 
        count: Number(st.count || 0), 
        mean: st.mean, 
        std: st.std, 
        min: st.min, 
        max: st.max, 
        skewness: st.skewness, 
        outliers: Number(stats.outliers?.[col]?.count || 0) 
      } : null;
    })
    .filter(Boolean)
    .filter((row) => (row.column || "").toLowerCase().includes(statsFilter.trim().toLowerCase()))
    .sort((a, b) => {
      const dir = statsSortDirection === "asc" ? 1 : -1;
      if (statsSortKey === "column") return (a.column || "").localeCompare(b.column || "") * dir;
      const valA = Number(a[statsSortKey] ?? 0);
      const valB = Number(b[statsSortKey] ?? 0);
      return (valA - valB) * dir;
    });

  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column', position: 'relative' }}>
      <ParticleBackground noExclude={phase === "done"} />

      {/* NAVBAR */}
      <div style={{ background: 'rgba(13, 18, 32, 0.65)', backdropFilter: 'blur(12px)', padding: '16px 48px', borderBottom: '1px solid var(--border-subtle)', display: 'flex', justifyContent: 'space-between', alignItems: 'center', position: 'sticky', top: 0, zIndex: 100 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <div style={{ color: 'var(--primary-500)', fontSize: '24px', textShadow: '0 0 10px rgba(99,102,241,0.4)' }}>◈</div>
          <strong style={{ fontSize: '18px', color: 'var(--text-main)', fontFamily: "'Syne', sans-serif" }}>DATA PULSE</strong>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '32px' }}>
          <div style={{ display: 'flex', gap: '16px' }}>
            <button onClick={toggleHistory} style={{ background: 'none', border: 'none', color: 'var(--text-main)', cursor: 'pointer', fontWeight: 600, fontSize: '13px', textTransform: 'uppercase', letterSpacing: '0.1em', opacity: 0.8 }}>History</button>
            {result && <button onClick={exportPDF} style={{ background: 'none', border: 'none', color: 'var(--text-main)', cursor: 'pointer', fontWeight: 600, fontSize: '13px', textTransform: 'uppercase', letterSpacing: '0.1em', opacity: 0.8 }}>Download</button>}
          </div>
          <div style={{ width: '1px', height: '20px', background: 'var(--border-subtle)' }} />
          <div style={{ display: 'flex', alignItems: 'center', gap: '20px' }}>
            <span style={{ fontSize: '13px', color: 'var(--text-muted)', opacity: 0.7 }}>{user?.email}</span>
            <button onClick={onLogout} style={{ background: 'none', border: 'none', color: 'var(--primary-500)', cursor: 'pointer', fontWeight: 700, fontSize: '13px', textTransform: 'uppercase', letterSpacing: '0.1em' }}>Logout</button>
          </div>
        </div>
      </div>

      <div className="container" style={{ flex: 1, display: 'flex', flexDirection: 'column', position: 'relative', zIndex: 1, paddingBottom: '32px', minHeight: 'calc(100vh - 80px)', width: '100%' }}>
        {/* MAIN SECTION */}
        {phase === "upload" ? (
          <div className="animate-fade-in" style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', textAlign: 'center', gap: '48px', padding: '40px 0' }}>
             
             {/* Animated Welcome Section */}
             <div style={{ animation: 'slideUp 0.8s cubic-bezier(0.2, 0.8, 0.2, 1)' }}>
                <div style={{ display: 'inline-flex', alignItems: 'center', gap: '12px', padding: '6px 16px', background: 'rgba(99,102,241,0.1)', borderRadius: '100px', border: '1px solid rgba(99,102,241,0.2)', marginBottom: '24px', color: 'var(--primary-500)', fontSize: '13px', fontWeight: 600, letterSpacing: '0.1em', textTransform: 'uppercase' }}>
                   <span style={{ width: '6px', height: '6px', background: 'var(--primary-500)', borderRadius: '50%', boxShadow: '0 0 10px var(--primary-500)' }} />
                   System Ready
                </div>
                <h1 style={{ fontSize: '42px', marginBottom: '4px', letterSpacing: '-0.05em', lineHeight: 1, opacity: 0.9 }}>
                   Welcome
                </h1>
                <h2 style={{ fontSize: '52px', color: 'var(--primary-500)', textShadow: '0 0 30px rgba(99,102,241,0.4)', marginBottom: '16px', marginTop: '0' }}>
                   {user?.name || user?.email?.split('@')[0] || "Analyst"}
                </h2>
                <p style={{ fontSize: '18px', color: 'var(--text-muted)', maxWidth: '600px', margin: '0 auto', lineHeight: 1.6 }}>Ready to architect your data? Connect a datasheet to initialize multi-agent analysis.</p>
             </div>

             {/* Upload Box with Scanner Effect */}
             <div style={{ position: 'relative', width: '100%', maxWidth: '540px' }}>
                <div
                  className={`upload-box ${isDragOver ? 'drag-over' : ''}`}
                  onDrop={onDrop}
                  onDragOver={(e) => e.preventDefault()}
                  onDragEnter={onDragEnter}
                  onDragLeave={onDragLeave}
                  onClick={() => fileRef.current.click()}
                  style={{ 
                    padding: '80px 48px', 
                    borderColor: isDragOver ? 'var(--primary-500)' : 'var(--border-subtle)', 
                    background: 'rgba(13, 18, 32, 0.4)', 
                    backdropFilter: 'blur(12px)',
                    position: 'relative',
                    overflow: 'hidden'
                  }}
                >
                  {/* Subtle Scanner Line Animation */}
                  <div style={{ position: 'absolute', top: 0, left: 0, right: 0, height: '2px', background: 'linear-gradient(90deg, transparent, var(--primary-500), transparent)', opacity: 0.3, animation: 'scannerSweep 3s infinite linear' }} />
                  
                  <div style={{ fontSize: '56px', color: 'var(--primary-500)', marginBottom: '24px', textShadow: '0 0 25px rgba(99,102,241,0.6)', transform: isDragOver ? 'scale(1.1)' : 'scale(1)', transition: 'transform 0.3s ease' }}>↑</div>
                  <strong style={{ color: 'var(--text-main)', marginBottom: '8px', fontSize: '24px', fontFamily: 'Syne, sans-serif' }}>Select Data Engine</strong>
                  <p className="caption" style={{ color: 'var(--text-muted)', fontSize: '15px' }}>Drag (.csv, .xlsx) anywhere to initialize</p>
                  <input ref={fileRef} type="file" accept=".csv,.xlsx,.xls" style={{ display: "none" }} onChange={(e) => onFile(e.target.files[0])} />
                </div>
             </div>

             {/* Feature Matrix Cards */}
             <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '20px', width: '100%', maxWidth: '900px', marginTop: '16px' }}>
                {[
                  { title: "Neural Logic", desc: "Multi-agent orchestration.", icon: "◈" },
                  { title: "Deep Viz", desc: "Automated vector sets.", icon: "⬢" },
                  { title: "Secure Vault", desc: "End-to-end encryption.", icon: "⊛" }
                ].map((feat, i) => (
                  <div key={feat.title} className="card" style={{ padding: '24px', textAlign: 'center', background: 'rgba(13, 18, 32, 0.25)', animation: `slideUp 0.8s cubic-bezier(0.2, 0.8, 0.2, 1) ${0.2 + i * 0.1}s both` }}>
                    <div style={{ color: 'var(--primary-500)', fontSize: '20px', marginBottom: '12px' }}>{feat.icon}</div>
                    <strong style={{ display: 'block', fontSize: '14px', color: 'var(--text-main)', marginBottom: '4px', textTransform: 'uppercase', letterSpacing: '1px' }}>{feat.title}</strong>
                    <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{feat.desc}</span>
                  </div>
                ))}
             </div>

             {/* System Pulse Footer */}
             <div style={{ position: 'absolute', bottom: '24px', width: '100%', display: 'flex', justifyContent: 'center', gap: '32px', opacity: 0.5 }}>
                <div style={{ fontSize: '11px', textTransform: 'uppercase', letterSpacing: '2px', display: 'flex', alignItems: 'center', gap: '8px' }}>
                   <span style={{ width: '4px', height: '4px', background: 'var(--success)', borderRadius: '50%', animation: 'pulse 1.5s infinite' }} />
                   Neural Nodes: 124 Active
                </div>
                <div style={{ fontSize: '11px', textTransform: 'uppercase', letterSpacing: '2px' }}>Encryption: SHA-2048</div>
                <div style={{ fontSize: '11px', textTransform: 'uppercase', letterSpacing: '2px' }}>Latency: 14ms</div>
             </div>
          </div>
        ) : (
          <div className="grid-12 animate-fade-in" style={{ alignItems: 'start', width: '100%', flex: 1 }}>
            
            {/* LEFT 3 (Chat & Status) */}
            <div className="col-3 flex-col gap-24">
               <div
                className="card"
                onClick={() => fileRef.current?.click()}
                style={{
                  padding: '20px',
                  cursor: 'pointer',
                  border: '1px dashed rgba(99,102,241,0.45)',
                  background: 'rgba(13, 18, 32, 0.55)',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '12px' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                    <div style={{ padding: '8px', background: 'rgba(99,102,241,0.1)', borderRadius: '8px', fontSize: '14px', border: '1px solid rgba(99,102,241,0.2)', color: 'var(--primary-500)' }}>＋</div>
                    <div className="flex-col gap-4">
                      <strong style={{ fontSize: '14px', color: 'var(--text-main)' }}>Add New File</strong>
                      <span className="caption" style={{ fontSize: '12px' }}>Upload another CSV/XLSX file</span>
                    </div>
                  </div>
                  <span className="data-pill" style={{ borderColor: 'rgba(99,102,241,0.3)', color: 'var(--primary-500)' }}>Upload</span>
                </div>
                <input
                  ref={fileRef}
                  type="file"
                  accept=".csv,.xlsx,.xls"
                  style={{ display: "none" }}
                  onChange={(e) => onFile(e.target.files?.[0])}
                />
              </div>

               {/* File Info */}
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
                  <strong style={{ fontSize: '14px', color: 'var(--text-main)', fontFamily: 'Syne, sans-serif' }}>Analyst Advisor</strong>
                  <div style={{ height: "300px", overflowY: "auto", display: "flex", flexDirection: "column", gap: "12px", background: 'var(--bg-input)', padding: '16px', borderRadius: '12px', border: '1px solid var(--border-subtle)' }}>
                     {chatMsgs.length === 0 ? (
                       <div style={{ margin: 'auto', textAlign: 'center', fontSize: '13px', color: 'var(--text-muted)' }}>Establish a query connection with your data.</div>
                     ) : chatMsgs.map((m, i) => (
                       <div 
                        key={i} 
                        style={{ 
                          alignSelf: m.role === 'user' ? 'flex-end' : 'flex-start', 
                          background: m.role === 'user' 
                            ? 'linear-gradient(135deg, #6366f1 0%, #4f46e5 100%)' 
                            : 'rgba(30, 41, 59, 0.5)', 
                          color: m.role === 'user' ? '#FFFFFF' : 'var(--text-main)', 
                          padding: '12px 18px', 
                          borderRadius: m.role === 'user' ? '18px 18px 4px 18px' : '18px 18px 18px 4px', 
                          fontSize: '13px', 
                          maxWidth: '85%', 
                          border: m.role === 'user' ? '1px solid rgba(255,255,255,0.1)' : '1px solid rgba(99,102,241,0.2)', 
                          boxShadow: m.role === 'user' ? '0 4px 15px rgba(99,102,241,0.3)' : '0 4px 15px rgba(0,0,0,0.2)',
                          lineHeight: 1.5,
                          marginBottom: '4px'
                        }}
                       >
                         {m.text}
                       </div>
                     ))}
                     {chatLoading && <div style={{ fontSize: '13px', color: 'var(--primary-500)', fontFamily: "'Outfit', monospace" }}>Synthesizing...</div>}
                     <div ref={chatEndRef} />
                  </div>
                  <div style={{ display: 'flex', gap: '12px' }}>
                    <input className="input-field" value={chatInput} onChange={e => setChatInput(e.target.value)} onKeyDown={e => e.key === 'Enter' && sendChat()} style={{ flex: 1, fontSize: '14px' }} placeholder="Query data..." />
                    <button className="btn-primary" onClick={sendChat} disabled={chatLoading} style={{ width: '44px', padding: 0 }}>»</button>
                  </div>
                </div>
              )}
            </div>

            {/* RIGHT (Results / Loading) */}
            <div className="col-9">
              {analysisError ? (
                <div className="card flex-col align-center justify-center animate-fade-in" style={{ minHeight: '500px', textAlign: 'center', border: '1px solid rgba(239,68,68,0.2)' }}>
                  <div style={{ fontSize: '48px', color: 'var(--error)', marginBottom: '16px' }}>⚠</div>
                  <h3 style={{ color: 'var(--text-main)', marginBottom: '8px' }}>Analysis Protocol Interrupted</h3>
                  <p style={{ color: 'var(--error)', fontSize: '14px', maxWidth: '400px', marginBottom: '24px' }}>{analysisError}</p>
                  <button className="btn-primary" onClick={() => { setPhase("upload"); setAnalysisError(""); }}>Reconnect Array</button>
                </div>
              ) : phase === "analyzing" ? (
                <div style={{ minHeight: '500px', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }} className="animate-fade-in">
                    <div style={{ fontSize: '40px', color: 'var(--primary-500)', marginBottom: '24px', animation: 'spin 4s linear infinite', textShadow: '0 0 20px rgba(99,102,241,0.8)' }}>⊛</div>
                    <strong style={{ fontSize: '18px', color: 'var(--text-main)', marginBottom: '8px', fontFamily: "'Syne', sans-serif" }}>Processing Array...</strong>
                    <p style={{ fontSize: '14px', color: 'var(--primary-500)', fontFamily: "'Outfit', monospace" }}>{agentLog[agentLog.length - 1]?.msg || "Extracting signatures..."}</p>
                </div>
              ) : !result ? (
                 <div className="card animate-fade-in" style={{ minHeight: '500px', display: 'flex', alignItems: 'center', justifyContent: 'center', width: '100%' }}>
                   <p style={{ color: 'var(--text-muted)' }}>Synchronizing data array... Standby.</p>
                 </div>
              ) : (

              <div className="panel-flat flex-col gap-24 animate-fade-in">
                <div style={{ display: 'flex', borderBottom: '1px solid var(--border-subtle)', gap: '16px', paddingBottom: '12px' }}>
                  {PRIMARY_TABS.map(t => <button key={t} onClick={() => setTab(t)} style={{ background: 'none', border: 'none', color: tab === t ? 'var(--text-main)' : 'var(--text-muted)', fontWeight: tab === t ? 600 : 500, fontSize: '14px', cursor: 'pointer', borderBottom: tab === t ? '2px solid var(--primary-500)' : 'none', paddingBottom: '12px', marginBottom: '-13px', textTransform: 'capitalize', letterSpacing: '0.05em' }}>{t}</button>)}
                  <div style={{ width: '24px' }} />
                  {SECONDARY_TABS.map(t => <button key={t} onClick={() => setTab(t)} style={{ background: 'none', border: 'none', color: tab === t ? 'var(--text-main)' : 'var(--text-muted)', fontWeight: tab === t ? 600 : 500, fontSize: '14px', cursor: 'pointer', borderBottom: tab === t ? '2px solid var(--primary-500)' : 'none', paddingBottom: '12px', marginBottom: '-13px', textTransform: 'capitalize', letterSpacing: '0.05em' }}>{t}</button>)}
                </div>
                
                {tab === "overview" && (
                   <div className="flex-col gap-24">
                     {headline && (
                       <div style={{ padding: '20px', background: 'rgba(99,102,241,0.05)', borderRadius: '12px', borderLeft: '4px solid var(--primary-500)' }}>
                         <strong style={{ fontSize: '12px', color: 'var(--primary-500)', textTransform: 'uppercase', display: 'block', marginBottom: '8px', letterSpacing: '0.1em' }}>Executive Matrix</strong>
                         <p style={{ fontSize: '15px', color: 'var(--text-main)' }}>{headline}</p>
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
                      {findings.slice(0, 5).map((f, i) => (
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
                       {recommendations.map((r, i) => (
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
                               <td style={{ padding: '16px 12px', color: 'var(--text-main)' }}>{f(r.mean)}</td>
                               <td style={{ padding: '16px 12px', color: 'var(--text-muted)' }}>{f(r.std)}</td>
                               <td style={{ padding: '16px 12px', color: 'var(--text-muted)' }}>{f(r.min)}</td>
                               <td style={{ padding: '16px 12px', color: 'var(--text-muted)' }}>{f(r.max)}</td>
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
                       const pct = Number(info?.percentage ?? 0);
                       const pctText = Number.isFinite(pct) ? pct.toFixed(1) : "0.0";
                       return (
                         <div key={col} style={{ padding: '16px', display: 'flex', justifyContent: 'space-between', background: 'var(--bg-input)', border: '1px solid rgba(239,68,68,0.2)', borderRadius: '12px' }}>
                           <strong style={{ fontSize: '14px', color: '#fca5a5', fontFamily: "'Outfit', monospace" }}>{col}</strong>
                           <span style={{ fontSize: '14px', color: 'var(--text-muted)' }}>{Number(info?.count ?? 0)} signals ({pctText}%)</span>
                         </div>
                       )
                    })}
                  </div>
                )}
                
              </div>
            )}
          </div>

          </div>
        )}
      </div>
      
      {/* HISTORY SIDEBAR OVERLAY */}
    {showHistory && (
      <div style={{ position: 'fixed', inset: 0, zIndex: 1000, display: 'flex', justifyContent: 'flex-end' }}>
        <div style={{ position: 'absolute', inset: 0, background: 'rgba(0,0,0,0.5)', backdropFilter: 'blur(6px)' }} onClick={() => setShowHistory(false)} />
        <div className="animate-fade-in" style={{ width: '400px', background: 'var(--bg-card)', borderLeft: '1px solid var(--border-subtle)', position: 'relative', zIndex: 1, padding: '32px', display: 'flex', flexDirection: 'column', gap: '24px', boxShadow: '-20px 0 50px rgba(0,0,0,0.5)' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
             <h2 style={{ fontSize: '20px' }}>Analysis Vault</h2>
             <button onClick={() => setShowHistory(false)} style={{ background: 'none', border: 'none', color: 'var(--text-muted)', cursor: 'pointer', fontSize: '20px' }}>✕</button>
          </div>
          
          <div style={{ flex: 1, overflowY: 'auto' }} className="flex-col gap-12">
            {historyLoading ? <div style={{ color: 'var(--primary-500)' }}>Syncing history...</div> : (
              history.length === 0 ? <div style={{ color: 'var(--text-muted)' }}>No recorded sessions found.</div> : (
                history.map(item => (
                  <div key={item.analysis_id} className="card" style={{ padding: '16px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', cursor: 'pointer', border: historySelectLoading === item.analysis_id ? '1px solid var(--primary-500)' : '1px solid var(--border-subtle)' }} onClick={() => loadHistoryItem(item)}>
                    <div className="flex-col gap-4">
                      <strong style={{ fontSize: '14px', color: 'var(--text-main)', display: 'block' }}>{item.file_name}</strong>
                      <span className="caption">{new Date(item.analyzed_at).toLocaleDateString()} • {item.row_count} rows</span>
                    </div>
                    <button 
                      onClick={(e) => { e.stopPropagation(); deleteItem(item.analysis_id); }} 
                      disabled={deleteLoading === item.analysis_id}
                      style={{ background: 'none', border: 'none', color: 'var(--text-muted)', cursor: 'pointer', padding: '8px' }}
                    >
                      {deleteLoading === item.analysis_id ? "..." : "🗑"}
                    </button>
                  </div>
                ))
              )
            )}
          </div>
        </div>
      </div>
    )}
    </div>
  );
}