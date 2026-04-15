import { useState, useRef, useCallback, useEffect, useMemo } from "react";
import jsPDF from "jspdf";
import { apiAnalyze, apiChat, apiHistory, apiHistoryAnalysis, apiDeleteAnalysis } from "./api.js";
import ParticleBackground from "./ParticleBackground.jsx";

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

function ChartPanel({ result, PlotComponent }) {
  if (!PlotComponent) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
        {[1, 2, 3].map(i => (
          <div key={i} style={{ height: '300px', borderRadius: '14px', background: 'rgba(99,102,241,0.05)', border: '1px solid var(--border-subtle)', animation: 'pulse 1.5s infinite' }} />
        ))}
      </div>
    );
  }

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
    <div style={{ display: "grid", gridTemplateColumns: "repeat(12, 1fr)", gap: "24px", paddingBottom: "40px", alignItems: "start" }}>
      {entries.map(([key, fig], idx) => {
        const isMatrix = key.startsWith("scatter_matrix") || key.startsWith("heatmap") || key.includes("matrix");
        const isWide = isMatrix || key.startsWith("timeseries") || key.startsWith("line");
        const gridSpan = isWide ? "span 12" : "span 6";
        const chartHeight = isMatrix ? 600 : isWide ? 400 : 350;

        return (
          <div
            key={key}
            style={{
              gridColumn: gridSpan,
              minWidth: 0, 
              padding: '24px',
              backgroundColor: 'rgba(13, 18, 32, 0.7)',
              backdropFilter: 'blur(8px)',
              borderRadius: '14px',
              border: '1px solid var(--border-subtle)',
              boxShadow: 'var(--shadow-card)',
              animation: 'fadeIn 0.35s cubic-bezier(0.2, 0.8, 0.2, 1) both',
              animationDelay: `${idx * 90}ms`,
            }}
          >
            <PlotComponent
              data={fig.data}
              layout={{
                ...PLOTLY_DARK_LAYOUT,
                ...fig.layout,
                authorise : true,
                title: { 
                  ...(fig.layout?.title || {}), 
                  font: { color: "#FFFFFF", size: 16, weight: 'bold' } 
                },
                paper_bgcolor: "rgba(0,0,0,0)",
                plot_bgcolor: "rgba(0,0,0,0)",
                font: { color: "#FFFFFF", family: "'Inter', sans-serif" },
                height: chartHeight,
                margin: { l: 50, r: 20, t: 60, b: 80 },
                legend: {
                  ...(fig.layout?.legend || {}),
                  orientation: "h",
                  yanchor: "top",
                  y: -0.15,
                  xanchor: "center",
                  x: 0.5,
                  font: { size: 11, color: "rgba(255,255,255,0.7)" }
                }
              }}
              config={PLOTLY_CONFIG}
              style={{ width: "100%", height: `${chartHeight}px` }}
            />
          </div>
        );
      })}
    </div>
  );
}

const PRIMARY_TABS = ["overview", "charts", "insights"];
const SECONDARY_TABS = ["data"];
const MAX_CHAT_MESSAGES = 40;

export default function DataPulse({ user, onLogout }) {
  const [PlotComponent, setPlotComponent] = useState(null);
  const [phase, setPhase] = useState("upload");
  const [result, setResult] = useState(null);
  const [fileName, setFileName] = useState("");
  const [agentLog, setAgentLog] = useState([]);
  const [analysisError, setAnalysisError] = useState("");
  const [isDragOver, setIsDragOver] = useState(false);
  const [showAllFindings, setShowAllFindings] = useState(false);
  const [tab, setTab] = useState("overview");
  const [chatMsgs, setChatMsgs] = useState([]);
  const [historyStale, setHistoryStale] = useState(false);
  const [chatInput, setChatInput] = useState("");
  const [chatLoading, setChatLoading] = useState(false);
  const [history, setHistory] = useState([]);
  const [historyError, setHistoryError] = useState("");
  const [historyActionError, setHistoryActionError] = useState("");
  const [showHistory, setShowHistory] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [deleteLoading, setDeleteLoading] = useState(null);
  const [historySelectLoading, setHistorySelectLoading] = useState(null);
  const [statsSortKey, setStatsSortKey] = useState("outliers");
  const [statsSortDirection, setStatsSortDirection] = useState("desc");
  const [statsFilter, setStatsFilter] = useState("");
  const fileRef = useRef();
  const chatContainerRef = useRef(null);
  const dragCounterRef = useRef(0);
  const stageTimersRef = useRef([]);
  const plotlyPreloadRef = useRef(null);

  useEffect(() => {
    loadHistory();
  }, []);

  const log = useCallback((msg) => {
    setAgentLog((p) => {
      const next = [...p, msg];
      return next.length > 20 ? next.slice(-20) : next;
    });
  }, []);

  useEffect(() => {
    if (chatContainerRef.current) {
        // Use requestAnimationFrame instead of setTimeout to guarantee browser paint cycle has completed
        requestAnimationFrame(() => {
            if (chatContainerRef.current) {
                chatContainerRef.current.scrollTop = chatContainerRef.current.scrollHeight;
            }
        });
    }
  }, [chatMsgs, chatLoading]);

  useEffect(() => {
    if (!result || PlotComponent || plotlyPreloadRef.current) return;

    plotlyPreloadRef.current = Promise.all([
      import("plotly.js-dist-min"),
      import("react-plotly.js/factory"),
    ])
      .then(([PlotlyModule, factoryModule]) => {
        const createPlotlyComponent = factoryModule.default;
        const PlotlyLib = PlotlyModule.default;
        setPlotComponent(() => createPlotlyComponent(PlotlyLib));
      })
      .catch(() => {
        plotlyPreloadRef.current = null;
      });
  }, [result, PlotComponent]);
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

    setAgentLog([
      "Uploading data to secure server…",
      "Architect initializing models…",
    ]);
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
          const prevItems = Array.isArray(prev) ? prev : [];
          const next = [
            {
              analysis_id: data.analysis_id,
              file_name: file.name,
              row_count: data?.stats_summary?.row_count || 0,
              column_count: data?.stats_summary?.column_count || 0,
              analyzed_at: new Date().toISOString(),
            },
            ...prevItems.filter((x) => x.analysis_id !== data.analysis_id),
          ];
          return next.slice(0, 20);
        });
      }
      setPhase("done");
      setHistoryStale(true);
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
    dragCounterRef.current -= 1;
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
    if (!showHistory && (history.length === 0 || historyStale)) {
      await loadHistory();
      setHistoryStale(false);
    }
    setShowHistory(!showHistory);
  };

  const deleteItem = async (id) => {
  setDeleteLoading(id);
  setHistoryActionError("");
  try {
    await apiDeleteAnalysis(id);
    setHistory(p => p.filter(x => x.analysis_id !== id));
  } catch (err) {
    setHistoryActionError("Failed to delete record. Please try again.");
  } finally {
    setDeleteLoading(null);
  }
};

  const loadHistoryItem = async (item) => {
  setHistorySelectLoading(item.analysis_id);
  setHistoryActionError("");
  try {
    const data = await apiHistoryAnalysis(item.analysis_id);
    setResult(data);
    setFileName(item.file_name);
    setPhase("done");
    setShowHistory(false);
    setTab("overview");
  } catch (err) {
    setHistoryActionError("Failed to restore session. Please try again.");
  } finally {
    setHistorySelectLoading(null);
  }
};

  const exportPDF = useCallback(() => {
    if (!result) return;
    const insights = result?.insights || {};
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
  }, [result]);

  const downloadCleanedData = useCallback(() => {
    if (!result || !result.clean_df || result.clean_df.length === 0) return;
    
    const df = result.clean_df;
    const headers = Object.keys(df[0]);
    const csvContent = [
      headers.join(","),
      ...df.map(row => headers.map(h => {
        const val = row[h];
        if (typeof val === 'string' && val.includes(',')) {
          return `"${val}"`;
        }
        return val;
      }).join(","))
    ].join("\n");

    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.setAttribute("download", `cleaned_data_${new Date().getTime()}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }, [result]);

  const chatStats = useMemo(() => result?.stats_summary || {}, [result?.stats_summary]);
  const chatInsights = useMemo(() => result?.insights || {}, [result?.insights]);

  const chatContext = useMemo(() => {
    if (!chatStats) return null;
    const outlierSummary = Object.entries(chatStats?.outliers || {})
      .map(([column, info]) => ({ column, count: Number(info?.count || 0), percentage: Number(info?.percentage || 0) }))
      .sort((a, b) => b.count - a.count).slice(0, 10);
    return {
      fileName,
      stats: chatStats,
      insights: chatInsights,
      outlierSummary,
      dataQuality: chatStats?.data_quality || {},
      correlations: chatStats?.strong_correlations?.slice(0, 5),
      charts: result?.charts || {},
    };
  }, [chatStats, chatInsights, fileName, result?.charts]);

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
  const numericColumns = stats?.numeric_columns || {};
  const outliersByColumn = stats?.outliers || {};
  const dq = stats.data_quality || {};
  const numericCols = Object.keys(stats.numeric_columns || {});
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

  const sortedNumericRows = useMemo(() => {
    return Object.keys(numericColumns)
      .map((col) => {
        const st = numericColumns?.[col];
        return st ? {
          column: col,
          count: Number(st.count || 0),
          mean: st.mean,
          std: st.std,
          min: st.min,
          max: st.max,
          skewness: st.skewness,
          outliers: Number(outliersByColumn?.[col]?.count || 0)
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
  }, [numericColumns, outliersByColumn, statsFilter, statsSortKey, statsSortDirection]);
  return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column', position: 'relative', overflow: 'hidden' }}>
      <ParticleBackground noExclude={phase === "done"} />

      {/* NAVBAR */}
      <div style={{ background: 'rgba(6, 9, 18, 0.90)', backdropFilter: 'blur(12px)', WebkitBackdropFilter: 'blur(12px)', padding: '16px 48px', borderBottom: '1px solid var(--border-subtle)', display: 'flex', justifyContent: 'space-between', alignItems: 'center', position: 'sticky', top: 0, zIndex: 100 }}>
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

      <div className="container" style={{ flex: 1, display: 'flex', flexDirection: 'column', position: 'relative', zIndex: 1, padding: 0, overflow: 'hidden' }}>
        {/* MAIN SECTION */}
        {phase === "upload" ? (
          <div className="animate-fade-in" style={{ flex: 1, display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '48px', alignItems: 'center', maxWidth: '1400px', margin: '0 auto', padding: '40px 64px', height: '100%', overflow: 'hidden' }}>
             
             {/* LEFT SIDE: AI Animated Visual */}
             <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
                <div style={{ position: 'relative', width: '100%', maxWidth: '450px' }}>
                  {/* Outer pulse ring */}
                  <div style={{ position: 'absolute', inset: 0, borderRadius: '50%', background: 'var(--primary-500)', filter: 'blur(80px)', opacity: 0.15, animation: 'pulse 4s infinite' }} />
                  
                  {/* The AI Image Hologram Container */}
                  <div className="ai-hologram-layer" style={{ zIndex: 1 }}>
                     {/* Base Image */}
                     <img 
                        src="/ai_hologram.png" 
                        alt="AI Matrix Core" 
                        style={{ width: '100%', borderRadius: '24px', position: 'relative', zIndex: 2, border: '1px solid rgba(99,102,241,0.2)' }} 
                     />
                     
                     {/* Glitch Overlay for Animated Effect */}
                     <img 
                        className="ai-glitch-layer"
                        src="/ai_hologram.png" 
                        alt="" 
                        style={{ zIndex: 3 }}
                     />
                  </div>
                </div>
             </div>

             {/* RIGHT SIDE: Upload Logic */}
             <div style={{ display: 'flex', flexDirection: 'column', justifyContent: 'center', gap: '24px' }}>
               <div style={{ animation: 'slideUp 0.8s cubic-bezier(0.2, 0.8, 0.2, 1)' }}>
                  <div style={{ display: 'inline-flex', alignItems: 'center', gap: '12px', padding: '6px 16px', background: 'rgba(99,102,241,0.1)', borderRadius: '100px', border: '1px solid rgba(99,102,241,0.2)', marginBottom: '16px', color: 'var(--primary-500)', fontSize: '13px', fontWeight: 600, letterSpacing: '0.1em', textTransform: 'uppercase' }}>
                     <span style={{ width: '6px', height: '6px', background: 'var(--primary-500)', borderRadius: '50%', boxShadow: '0 0 10px var(--primary-500)' }} />
                     System Ready
                  </div>
                  <h1 style={{ fontSize: '46px', margin: '0 0 4px 0', letterSpacing: '-0.05em', lineHeight: 1, color: 'var(--text-main)', opacity: 0.9 }}>
                     Welcome
                  </h1>
                  <h2 style={{ fontSize: '56px', margin: '0 0 16px 0', color: 'var(--primary-500)', textShadow: '0 0 30px rgba(99,102,241,0.4)', lineHeight: 1 }}>
                     {user?.name || user?.email?.split('@')[0] || "Analyst"}
                  </h2>
                  <p style={{ fontSize: '16px', color: 'var(--text-muted)', lineHeight: 1.6, margin: 0 }}>
                    Ready to architect your data? Connect a datasheet to initialize multi-agent analysis.
                  </p>
               </div>

               {/* Upload Box */}
               <div style={{ position: 'relative', width: '100%' }}>
                  <div
                    className={`upload-box ${isDragOver ? 'drag-over' : ''}`}
                    onDrop={onDrop}
                    onDragOver={(e) => e.preventDefault()}
                    onDragEnter={onDragEnter}
                    onDragLeave={onDragLeave}
                    onClick={() => fileRef.current.click()}
                    style={{ 
                      padding: '50px 32px', 
                      borderColor: isDragOver ? 'var(--primary-500)' : 'var(--border-subtle)', 
                      background: 'rgba(13, 18, 32, 0.4)', 
                      backdropFilter: 'blur(12px)',
                      position: 'relative',
                      overflow: 'hidden',
                      textAlign: 'center',
                      borderRadius: '16px'
                    }}
                  >
                    <div style={{ fontSize: '48px', color: 'var(--primary-500)', marginBottom: '16px', textShadow: '0 0 25px rgba(99,102,241,0.6)', transform: isDragOver ? 'scale(1.1)' : 'scale(1)', transition: 'transform 0.3s ease' }}>↑</div>
                    <strong style={{ color: 'var(--text-main)', fontSize: '20px', fontFamily: "'Syne', sans-serif", display: 'block', marginBottom: '8px' }}>Select Data Engine</strong>
                    <p className="caption" style={{ color: 'var(--text-muted)', fontSize: '13px', margin: 0 }}>Drag (.csv, .xlsx) anywhere to initialize</p>
                    <input ref={fileRef} type="file" accept=".csv,.xlsx,.xls" style={{ display: "none" }} onChange={(e) => onFile(e.target.files[0])} />
                  </div>
               </div>

               {/* Metrics */}
               <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '16px', marginTop: '8px' }}>
                  {[
                    { title: "Neural Logic", desc: "Multi-agent orchestration.", icon: "◈" },
                    { title: "Deep Viz", desc: "Automated vector sets.", icon: "⬢" },
                    { title: "Secure Vault", desc: "End-to-end encryption.", icon: "⊛" }
                  ].map((feat, i) => (
                    <div key={feat.title} className="card" style={{ padding: '16px', textAlign: 'center', background: 'rgba(13, 18, 32, 0.25)', animation: `slideUp 0.8s cubic-bezier(0.2, 0.8, 0.2, 1) ${0.2 + i * 0.1}s both` }}>
                      <div style={{ color: 'var(--primary-500)', fontSize: '18px', marginBottom: '8px' }}>{feat.icon}</div>
                      <strong style={{ display: 'block', fontSize: '12px', color: 'var(--text-main)', marginBottom: '2px', textTransform: 'uppercase', letterSpacing: '1px' }}>{feat.title}</strong>
                      <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>{feat.desc}</span>
                    </div>
                  ))}
               </div>
             </div>
          </div>
        ) : phase === "analyzing" || analysisError ? (
          <div className="animate-fade-in" style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', textAlign: 'center', maxWidth: '800px', margin: '0 auto', height: '100%', width: '100%' }}>
            {analysisError ? (
                <div className="card flex-col align-center justify-center animate-fade-in" style={{ padding: '60px', textAlign: 'center', border: '1px solid rgba(239,68,68,0.2)', background: 'rgba(13,18,32,0.6)', width: '100%' }}>
                  <div style={{ fontSize: '56px', color: 'var(--error)', marginBottom: '24px' }}>⚠</div>
                  <h3 style={{ color: 'var(--text-main)', marginBottom: '12px', fontSize: '24px' }}>Analysis Protocol Interrupted</h3>
                  <p style={{ color: 'var(--error)', fontSize: '15px', maxWidth: '500px', marginBottom: '32px', margin: '0 auto 32px auto', lineHeight: 1.6 }}>{analysisError}</p>
                  <button className="btn-primary" style={{ padding: '12px 32px', fontSize: '15px', margin: '0 auto' }} onClick={() => { setPhase("upload"); setAnalysisError(""); }}>Upload New Dataset</button>
                </div>
            ) : (
                <div style={{ width: '100%', maxWidth: '600px', display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                  {/* Dynamic Animation */}
                  <div style={{ position: 'relative', width: '120px', height: '120px', marginBottom: '40px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                    <div style={{ position: 'absolute', inset: 0, borderRadius: '50%', border: '2px solid rgba(99,102,241,0.2)', borderTop: '2px solid var(--primary-500)', animation: 'spin 1.5s linear infinite' }} />
                    <div style={{ position: 'absolute', inset: '15px', borderRadius: '50%', border: '2px solid rgba(16,185,129,0.2)', borderBottom: '2px solid var(--success)', animation: 'spin 2s linear infinite reverse' }} />
                    <div style={{ fontSize: '32px', color: 'var(--primary-500)', animation: 'pulse 2s infinite' }}>◈</div>
                  </div>
                  
                  <h2 style={{ fontSize: '28px', color: 'var(--text-main)', marginBottom: '12px', letterSpacing: '0.05em' }}>Analyzing Dataset</h2>
                  
                  {/* Agent Logs Component */}
                  <div style={{ width: '100%', background: 'rgba(13,18,32,0.8)', border: '1px solid var(--border-subtle)', borderRadius: '16px', padding: '24px', position: 'relative', overflow: 'hidden' }}>
                    <div className="progress-container" style={{ marginBottom: '20px', height: '4px', background: 'rgba(255,255,255,0.05)' }}>
                      <div className="progress-bar animate-pulse" style={{ width: '65%', background: 'linear-gradient(90deg, var(--primary-600), var(--info))' }} />
                    </div>
                    
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', minHeight: '60px', justifyContent: 'center' }}>
                      <span style={{ fontSize: '12px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '2px' }}>Active Agent Node</span>
                      <strong style={{ fontSize: '16px', color: 'var(--primary-500)', fontFamily: "'Outfit', monospace", textShadow: '0 0 10px rgba(99,102,241,0.3)' }}>
                        {agentLog[agentLog.length - 1]?.msg || "Orchestrating Neural Paths..."}
                      </strong>
                    </div>
                  </div>
                  
                  <p style={{ marginTop: '24px', fontSize: '14px', color: 'var(--text-muted)', opacity: 0.8 }}>Data transparency protocols engaged. Preparing visualizations...</p>
                </div>
            )}
          </div>
        ) : (
          <div className="grid-12 animate-fade-in" style={{ alignItems: 'start', width: '100%', flex: 1, overflow: 'hidden', height: '100%' }}>
            
            {/* LEFT 4 (Chat & Status) */}
            <div className="col-4 flex-col gap-24" style={{ height: '100%', overflowY: 'auto', paddingRight: '12px' }}>
               <div
                    onClick={() => fileRef.current?.click()}
                    style={{
                      padding: '20px',
                      cursor: 'pointer',
                      border: '1px dashed rgba(99,102,241,0.45)',
                      background: 'rgba(13, 18, 32, 0.55)',
                      borderRadius: '14px',
                      transition: 'border-color 0.2s ease, background 0.2s ease',
                    }}
                    onMouseEnter={e => e.currentTarget.style.borderColor = 'var(--primary-500)'}
                    onMouseLeave={e => e.currentTarget.style.borderColor = 'rgba(99,102,241,0.45)'}
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
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                      <div style={{ padding: '8px', background: 'rgba(99,102,241,0.1)', borderRadius: '8px', fontSize: '14px', border: '1px solid rgba(99,102,241,0.2)', color: 'var(--primary-500)' }}>📄</div>
                      <strong style={{ fontSize: '14px', color: 'var(--text-main)' }}>{fileName}</strong>
                    </div>
                    {phase === "done" && <span className="data-pill success">Verified ⬢</span>}
                  </div>
                </div>
              )}

              {/* Chat Box (only if done) */}
              {phase === "done" && result && (
                <div className="card flex-col gap-16" style={{ padding: '20px', flex: 1, display: 'flex', overflow: 'hidden' }}>
                  <strong style={{ fontSize: '14px', color: 'var(--text-main)', fontFamily: 'Syne, sans-serif' }}>Analyst Advisor</strong>
                  <div ref={chatContainerRef} style={{ flex: 1, overflowY: "auto", display: "flex", flexDirection: "column", gap: "12px", background: 'var(--bg-input)', padding: '16px', borderRadius: '12px', border: '1px solid var(--border-subtle)' }}>
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
                          maxWidth: m.role === 'user' ? '90%' : '100%', 
                          border: m.role === 'user' ? '1px solid rgba(255,255,255,0.1)' : '1px solid rgba(99,102,241,0.2)', 
                          boxShadow: m.role === 'user' ? '0 4px 15px rgba(99,102,241,0.3)' : '0 4px 15px rgba(0,0,0,0.2)',
                          lineHeight: 1.5,
                          marginBottom: '4px'
                        }}
                       >
                         {(() => {
                           if (m.role !== 'ai' || !m.text.includes('[CHART:')) return m.text;
                           const parts = m.text.split(/(\[CHART:\s*[^\]]+\])/);
                           return parts.map((part, pIdx) => {
                             const match = part.match(/\[CHART:\s*([^\]]+)\]/);
                             if (match && result?.charts?.[match[1]]) {
                               const figStr = result.charts[match[1]];
                               let parsedFig = typeof figStr === "string" ? JSON.parse(figStr) : figStr;
                               const data = Array.isArray(parsedFig?.data) ? parsedFig.data : [];
                               const layout = (parsedFig?.layout && typeof parsedFig.layout === 'object') ? parsedFig.layout : {};
                               return PlotComponent ? (
                                 <div key={pIdx} style={{ margin: '16px 0', border: '1px solid rgba(99,102,241,0.2)', borderRadius: '12px', overflow: 'hidden', padding: '12px', background: 'rgba(0,0,0,0.3)', width: '100%' }}>
                                   <PlotComponent
                                     data={data.map(t => ({ ...t, textfont: { color: "#FFFFFF" } }))}
                                     layout={{ 
                                       ...PLOTLY_DARK_LAYOUT, 
                                       ...layout, 
                                       paper_bgcolor: "rgba(0,0,0,0)",
                                       plot_bgcolor: "rgba(0,0,0,0)",
                                       font: { color: "#FFFFFF", family: "'Inter', sans-serif" },
                                       hoverlabel: { bgcolor: "#0d1220", font: { color: "#FFFFFF" }, bordercolor: "rgba(99,102,241,0.5)" },
                                       height: 280, 
                                       margin: {l: 40, r: 20, t: 40, b: 40}, 
                                       title: { ...(layout.title || {}), font: { size: 14, color: '#fff', weight: 'bold' }, y: 0.95, yanchor: 'top' }, 
                                       legend: { orientation: "h", yanchor: "top", y: -0.2, xanchor: "center", x: 0.5, font: { size: 10, color: "rgba(255,255,255,0.7)" } } 
                                     }}
                                     config={PLOTLY_CONFIG}
                                     style={{ width: "100%", height: "280px" }}
                                   />
                                 </div>
                               ) : <div key={pIdx} style={{ color: 'var(--primary-500)' }}>[Rendering Chart...]</div>;
                             }
                             if (match) return null; // hide broken chart tags if it doesn't exist
                             return <span key={pIdx}>{part}</span>;
                           });
                         })()}
                       </div>
                     ))}
                     {chatLoading && <div style={{ fontSize: '13px', color: 'var(--primary-500)', fontFamily: "'Outfit', monospace" }}>Synthesizing...</div>}
                     {chatMsgs.length >= MAX_CHAT_MESSAGES && (
                        <div style={{ fontSize: '11px', color: 'var(--text-muted)', textAlign: 'center', padding: '4px 0', borderTop: '1px solid var(--border-subtle)', marginTop: '4px' }}>
                          Showing last {MAX_CHAT_MESSAGES} messages
                        </div>
                      )}
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                    <div style={{ display: 'flex', gap: '12px' }}>
                      <input className="input-field" value={chatInput} onChange={e => setChatInput(e.target.value)} onKeyDown={e => e.key === 'Enter' && sendChat()} style={{ flex: 1, fontSize: '14px' }} placeholder="Query data..." maxLength={1200} />
                      <button className="btn-primary" onClick={sendChat} disabled={chatLoading} style={{ width: '44px', padding: 0 }}>»</button>
                    </div>
                    {chatInput.length > 900 && (
                      <div style={{ fontSize: '11px', textAlign: 'right', color: chatInput.length > 1100 ? 'var(--error)' : 'var(--text-muted)' }}>
                        {chatInput.length}/1200
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>

            {/* RIGHT (Results / Loading) */}
            <div className="col-8" style={{ height: '100%', overflowY: 'auto', paddingRight: '12px', paddingBottom: '32px' }}>
              {!result ? (
                 <div className="card animate-fade-in" style={{ minHeight: '500px', display: 'flex', alignItems: 'center', justifyContent: 'center', width: '100%' }}>
                   <p style={{ color: 'var(--text-muted)' }}>Synchronizing data... Standby.</p>
                 </div>
              ) : (

              <div className="panel-flat flex-col gap-24 animate-fade-in">
                <div style={{ display: 'flex', borderBottom: '1px solid var(--border-subtle)', gap: '16px', paddingBottom: '12px' }}>
                  {PRIMARY_TABS.map(t => <button key={t} onClick={() => setTab(t)} style={{ background: 'none', border: 'none', color: tab === t ? 'var(--text-main)' : 'var(--text-muted)', fontWeight: tab === t ? 600 : 500, fontSize: '14px', cursor: 'pointer', borderBottom: tab === t ? '2px solid var(--primary-500)' : 'none', paddingBottom: '12px', marginBottom: '-13px', textTransform: 'capitalize', letterSpacing: '0.05em' }}>{t}</button>)}
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
                       {(showAllFindings ? findings : findings.slice(0, 5)).map((f, i) => (
                          <div key={i} style={{ fontSize: '15px', color: 'var(--text-muted)', marginBottom: '12px', display: 'flex', gap: '12px', alignItems: 'center' }}>
                            <div style={{ width: '6px', height: '6px', background: 'var(--primary-500)', borderRadius: '50%', boxShadow: '0 0 10px var(--primary-500)' }} /> {f}
                          </div>
                        ))}
                        {findings.length > 5 && (
                          <button onClick={() => setShowAllFindings(v => !v)} style={{ background: 'none', border: 'none', color: 'var(--primary-500)', cursor: 'pointer', fontSize: '13px', fontWeight: 600, textAlign: 'left', padding: '4px 0', marginTop: '4px' }}>
                            {showAllFindings ? "Show less" : `+ ${findings.length - 5} more findings`}
                          </button>
                        )}
                     </div>
                   </div>
                )}

                {tab === "charts" && <ChartPanel result={result} PlotComponent={PlotComponent} />} 
                
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
                
                {tab === "data" && (
                  <div className="flex-col gap-16">
                     <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                       <strong style={{ fontSize: '15px', color: 'var(--text-main)', fontFamily: "'Syne', sans-serif" }}>Cleaned Data Preview</strong>
                       <button onClick={downloadCleanedData} className="btn-primary" style={{ padding: '0 16px', height: '36px', fontSize: '12px' }}>Download Data (CSV)</button>
                     </div>
                     <p style={{ fontSize: '13px', color: 'var(--text-muted)' }}>This preview shows up to 100 array segments from your engine after cleaning and imputation algorithms have run.</p>
                     <div style={{ overflowX: 'auto', border: '1px solid var(--border-subtle)', borderRadius: '12px' }}>
                        {result?.clean_df && result.clean_df.length > 0 ? (
                          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px', textAlign: 'left', background: 'var(--bg-input)' }}>
                            <thead style={{ background: 'rgba(99,102,241,0.05)', borderBottom: '1px solid var(--border-subtle)' }}>
                              <tr>
                                {Object.keys(result.clean_df[0]).map(k => (
                                  <th key={k} style={{ padding: '12px 14px', color: 'var(--text-muted)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', whiteSpace: 'nowrap' }}>{k}</th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {result.clean_df.map((row, i) => (
                                <tr key={i} style={{ borderBottom: i === result.clean_df.length - 1 ? 'none' : '1px solid var(--border-subtle)' }}>
                                  {Object.values(row).map((v, j) => (
                                    <td key={j} style={{ padding: '12px 14px', color: 'var(--text-main)', whiteSpace: 'nowrap' }}>{String(v)}</td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        ) : (
                          <div style={{ padding: '24px', textAlign: 'center', color: 'var(--text-muted)' }}>Cleaned data unavailable.</div>
                        )}
                     </div>
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
        <div className="animate-fade-in" style={{ width: 'min(400px, 100vw)', background: 'var(--bg-card)', borderLeft: '1px solid var(--border-subtle)', position: 'relative', zIndex: 1, padding: '24px', display: 'flex', flexDirection: 'column', gap: '24px', boxShadow: '-20px 0 50px rgba(0,0,0,0.5)' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
             <h2 style={{ fontSize: '20px' }}>Analysis Vault</h2>
             <button onClick={() => setShowHistory(false)} style={{ background: 'none', border: 'none', color: 'var(--text-muted)', cursor: 'pointer', fontSize: '20px' }}>✕</button>
          </div>
          
          <div style={{ flex: 1, overflowY: 'auto' }} className="flex-col gap-12">
            {historyError && (
              <div style={{ color: 'var(--error)', fontSize: '13px', padding: '12px', background: 'rgba(239,68,68,0.1)', borderRadius: '8px', border: '1px solid rgba(239,68,68,0.2)' }}>
                {historyError}
              </div>
            )}
            {historyActionError && (
              <div style={{ color: 'var(--error)', fontSize: '13px', padding: '12px', background: 'rgba(239,68,68,0.1)', borderRadius: '8px', border: '1px solid rgba(239,68,68,0.2)' }}>
                {historyActionError}
              </div>
            )}
            {historyLoading ? <div style={{ color: 'var(--primary-500)' }}>Syncing history...</div> : (
              (Array.isArray(history) ? history.length : 0) === 0 ? <div style={{ color: 'var(--text-muted)' }}>No recorded sessions found.</div> : (
                (Array.isArray(history) ? history : []).map(item => (
                  <div key={item.analysis_id} className="card" style={{ padding: '16px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', cursor: 'pointer', border: historySelectLoading === item.analysis_id ? '1px solid var(--primary-500)' : '1px solid var(--border-subtle)' }} onClick={() => loadHistoryItem(item)}>
                    <div className="flex-col gap-4">
                      <strong style={{ fontSize: '14px', color: 'var(--text-main)', display: 'block' }}>{item.file_name}</strong>
                      <span className="caption">{new Date(item.analyzed_at).toLocaleDateString()} • {item.row_count} rows</span>
                    </div>
                    <button 
                      onClick={(e) => { e.stopPropagation(); deleteItem(item.analysis_id); }} 
                      disabled={deleteLoading === item.analysis_id}
                      style={{ background: 'none', border: 'none', color: 'var(--text-muted)', cursor: 'pointer', padding: '8px', fontSize: '24px' }}
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