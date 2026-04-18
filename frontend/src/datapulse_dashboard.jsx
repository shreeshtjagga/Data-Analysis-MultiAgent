import { useState, useRef, useCallback, useEffect, useMemo } from "react";
import jsPDF from "jspdf";
import { apiAnalyze, apiChat, apiHistory, apiHistoryAnalysis, apiDeleteAnalysis } from "./api.js";
import ParticleBackground from "./ParticleBackground.jsx";
import GlobeCanvas from "./GlobeCanvas.jsx";


const PALETTE = ["#6366f1", "#10b981", "#f59e0b", "#06b6d4", "#ef4444", "#a855f7", "#34d399", "#f472b6"];

const PLOTLY_DARK_LAYOUT = {
  paper_bgcolor: "rgba(0,0,0,0)",
  plot_bgcolor: "rgba(0,0,0,0)",
  font: { color: "architect#FFFFFF", family: "'Inter', sans-serif", size: 12 },
  title: { font: { color: "#FFFFFF", size: 14 } },
  xaxis: { gridcolor: "rgba(99,102,241,0.1)", zerolinecolor: "rgba(99,102,241,0.2)", tickfont: { color: "#FFFFFF" } },
  yaxis: { gridcolor: "rgba(99,102,241,0.1)", zerolinecolor: "rgba(99,102,241,0.2)", tickfont: { color: "#FFFFFF" } },
  hoverlabel: {
    bgcolor: "rgba(8,12,24,0.98)",
    bordercolor: "rgba(99,102,241,0.85)",
    font: { color: "#F8FAFC", size: 12 },
  },
  colorway: PALETTE,
  autosize: true,
  margin: { l: 40, r: 20, t: 40, b: 30 },
};

const PLOTLY_CONFIG = {
  responsive: true,
  displayModeBar: "hover",
  displaylogo: false,
  modeBarButtons: [["zoomIn2d", "zoomOut2d", "resetScale2d"]]
};

function truncateLabel(value, max = 26) {
  const text = String(value ?? "").trim();
  if (text.length <= max) return text;
  return `${text.slice(0, max - 1)}…`;
}

function cleanQuestionLabel(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return "";
  // Remove survey prefixes like "Q4 - " and leading punctuation artifacts.
  let text = raw
    .replace(/^Q\d+\s*[-:]*\s*/i, "")
    .replace(/^[-\s.:]+/, "")
    .trim();
  // Keep labels concise and avoid trailing question mark in legends.
  text = text.replace(/\?+$/, "").trim();
  return text || raw;
}

function cleanAxisTitle(value) {
  return truncateLabel(cleanQuestionLabel(value), 42);
}

function normalizeTraceData(data) {
  return (Array.isArray(data) ? data : []).map((trace) => {
    const next = { ...trace };
    if (typeof next.name === "string") next.name = truncateLabel(cleanQuestionLabel(next.name), 24);
    if (next.type === "pie") {
      next.textinfo = next.textinfo || "percent+label";
      next.textposition = next.textposition || "outside";
      next.hole = typeof next.hole === "number" ? next.hole : 0.45;
    }
    return next;
  });
}

function hasLongCategoryLabels(data) {
  const samples = [];
  (Array.isArray(data) ? data : []).forEach((trace) => {
    if (Array.isArray(trace?.x)) samples.push(...trace.x.slice(0, 15));
  });
  return samples.some((v) => String(v ?? "").length > 14);
}

function getLegendConfig(traceCount, isMatrix) {
  if (isMatrix || traceCount <= 1) {
    return { showlegend: false, legend: {}, legendRows: 0 };
  }

  const legendRows = Math.max(1, Math.ceil(traceCount / 4));
  const legendYOffset = -0.14 - ((legendRows - 1) * 0.11);

  return {
    showlegend: true,
    legendRows,
    legend: {
      orientation: "h",
      yanchor: "top",
      y: legendYOffset,
      xanchor: "center",
      x: 0.5,
      font: { size: 11, color: "rgba(255,255,255,0.8)" },
      tracegroupgap: 6,
    },
  };
}

function shouldHideLegend(data, traceCount) {
  const traces = Array.isArray(data) ? data : [];
  if (traceCount > 6) return true;
  const barCount = traces.filter((t) => t?.type === "bar").length;
  if (barCount >= 6) return true;
  return false;
}

function ChartPanel({ result, PlotComponent }) {
  const [flipped, setFlipped] = useState({});

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

  // Priority order: most informative chart types first
  const CHART_PRIORITY = {
    timeseries: 0, line: 1, scatter: 2, heatmap: 3,
    ranked_bar: 4, grouped_bar: 5, stacked: 6, box: 7,
    violin: 8, likert: 9, freq: 10, histogram: 11,
    donut: 12, pie: 13,
  };
  const getChartPriority = (key) => {
    for (const prefix of Object.keys(CHART_PRIORITY)) {
      if (key.startsWith(prefix)) return CHART_PRIORITY[prefix];
    }
    return 99;
  };

  const entries = Object.entries(charts)
    .map(([key, value]) => {
      let fig = value;
      let desc = "";
      if (value && typeof value === "object" && value.fig) {
        fig = value.fig;
        desc = value.description || "";
      }
      if (!fig || typeof fig !== "object") {
        return [key, { data: [], layout: {} }, ""];
      }
      const data = Array.isArray(fig.data) ? fig.data : [];
      const layout = fig.layout && typeof fig.layout === "object" ? fig.layout : {};
      return [key, { data, layout }, desc];
    })
    .sort((a, b) => getChartPriority(a[0]) - getChartPriority(b[0]));

  if (entries.length === 0) {
    return <div style={{ color: "var(--text-muted)", fontSize: "14px" }}>No charts available.</div>;
  }

  const toggleFlip = (key) => {
    setFlipped(prev => ({ ...prev, [key]: !prev[key] }));
  };

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(12, 1fr)", gap: "24px", paddingBottom: "40px", alignItems: "start" }}>
      {entries.map(([key, fig, desc], idx) => {
        const isMatrix = key.startsWith("scatter_matrix") || key.startsWith("heatmap") || key.includes("matrix");
        const isWide = isMatrix || key.startsWith("timeseries") || key.startsWith("line");
        const traceCount = Array.isArray(fig.data) ? fig.data.length : 0;
        const hasLongLabels = hasLongCategoryLabels(fig.data);
        const { showlegend, legend, legendRows } = getLegendConfig(traceCount, isMatrix);
        const hideLegend = shouldHideLegend(fig.data, traceCount);
        const effectiveShowLegend = hideLegend ? false : showlegend;
        const gridSpan = isWide ? "span 12" : "span 6";
        const chartHeight = isMatrix ? 620 : isWide ? 500 : 450;
        const normalizedData = normalizeTraceData(fig.data);
        const margin = {
          l: 60,
          r: 24,
          t: 70,
          b: effectiveShowLegend ? (95 + (legendRows * 24)) : (hasLongLabels ? 90 : 68),
        };

        return (
          <div
            key={key}
            className={`chart-flip-wrapper ${flipped[key] ? 'flipped' : ''}`}
            style={{
              gridColumn: gridSpan,
              minWidth: 0,
              height: `${chartHeight + 110}px`, // Accommodate padding and title padding
              animation: 'fadeIn 0.35s cubic-bezier(0.2, 0.8, 0.2, 1) both',
              animationDelay: `${idx * 90}ms`,
            }}
          >
            <div className="chart-flip-inner">
              {/* FRONT SIDE */}
              <div className="chart-flip-front" style={{ padding: '24px' }}>
                <button
                  className="chart-info-btn"
                  onClick={() => toggleFlip(key)}
                  data-tooltip="View Details"
                >
                  ℹ
                </button>

                <PlotComponent
                  data={normalizedData}
                  layout={{
                    ...PLOTLY_DARK_LAYOUT,
                    ...fig.layout,
                    authorise: true,
                    title: {
                      ...(fig.layout?.title || {}),
                      text: truncateLabel(cleanQuestionLabel(fig.layout?.title?.text || fig.layout?.title || key.replaceAll("_", " ")), 85),
                      font: { color: "#FFFFFF", size: 16, weight: 'bold' },
                      x: 0.5,
                      xanchor: "center",
                    },
                    paper_bgcolor: "rgba(0,0,0,0)",
                    plot_bgcolor: "rgba(0,0,0,0)",
                    font: { color: "#FFFFFF", family: "'Inter', sans-serif" },
                    hovermode: fig.layout?.hovermode || "closest",
                    hoverlabel: {
                      ...PLOTLY_DARK_LAYOUT.hoverlabel,
                      ...(fig.layout?.hoverlabel || {}),
                    },
                    height: chartHeight,
                    showlegend: effectiveShowLegend,
                    margin,
                    legend: {
                      ...(fig.layout?.legend || {}),
                      ...legend,
                    },
                    xaxis: {
                      ...(fig.layout?.xaxis || {}),
                      title: {
                        ...(fig.layout?.xaxis?.title || {}),
                        text: cleanAxisTitle(fig.layout?.xaxis?.title?.text || fig.layout?.xaxis?.title || ""),
                      },
                      automargin: true,
                      tickangle: hasLongLabels ? -28 : (fig.layout?.xaxis?.tickangle ?? 0),
                      tickfont: { color: "#FFFFFF", size: 11 },
                    },
                    yaxis: {
                      ...(fig.layout?.yaxis || {}),
                      title: {
                        ...(fig.layout?.yaxis?.title || {}),
                        text: cleanAxisTitle(fig.layout?.yaxis?.title?.text || fig.layout?.yaxis?.title || ""),
                      },
                      automargin: true,
                      tickfont: { color: "#FFFFFF", size: 11 },
                    },
                    uniformtext: { minsize: 10, mode: "hide" },
                  }}
                  config={PLOTLY_CONFIG}
                  style={{ width: "100%", height: `${chartHeight}px` }}
                />
              </div>

              {/* BACK SIDE */}
              <div className="chart-flip-back">
                <button
                  className="chart-info-btn"
                  onClick={() => toggleFlip(key)}
                  data-tooltip="Flip Back"
                >
                  ✕
                </button>

                <div className="chart-back-badge">
                  <span style={{ fontSize: '10px' }}>◈</span> {
                    fig.data?.[0]?.type
                      ? fig.data[0].type.charAt(0).toUpperCase() + fig.data[0].type.slice(1).replace('scatter', 'Scatter Plot').replace('bar', 'Bar Chart').replace('pie', 'Pie Chart').replace('histogram', 'Histogram')
                      : 'Data Insight'
                  }
                </div>

                <h3 className="chart-back-title">
                  {cleanQuestionLabel(fig.layout?.title?.text || fig.layout?.title || key.replaceAll("_", " "))}
                </h3>

                <div className="chart-back-divider" />

                <div className="chart-back-section-label">AI Narrative</div>
                <p className="chart-back-description">
                  {desc || "Our multi-agent orchestration performed a deep vector analysis on this distribution. The patterns identified suggest a strong alignment with normalized expectations for this domain."}
                </p>

                <div className="chart-back-section-label">Core Revelation</div>
                <div className="chart-back-insight-item">
                  <div className="chart-back-insight-dot" />
                  <span>{desc ? "The statistical significance of this variance suggests a primary pivot point for strategy." : "Automated outlier detection confirmed data integrity within this specific dimension."}</span>
                </div>

                <div style={{ marginTop: 'auto', paddingTop: '20px', display: 'flex', alignItems: 'center', gap: '8px', opacity: 0.5 }}>
                  <div style={{ height: '1px', flex: 1, background: 'var(--border-subtle)' }} />
                  <span style={{ fontSize: '10px', textTransform: 'uppercase', letterSpacing: '1px' }}>Processed via DataPulse v5.0</span>
                  <div style={{ height: '1px', flex: 1, background: 'var(--border-subtle)' }} />
                </div>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

const PRIMARY_TABS = ["overview", "charts", "insights"];
const SECONDARY_TABS = ["data"];
const MAX_CHAT_MESSAGES = 40;

function inferDatasetType(result, fileName) {
  const profile = result?.stats_summary?.dataset_profile || {};
  const label = String(profile?.label || "").trim();
  const domain = String(profile?.domain || "").trim();

  if (label && label.toLowerCase() !== "unknown") return label;
  if (domain) return `${domain} dataset`;

  const lower = String(fileName || "").toLowerCase();
  if (lower.endsWith(".csv")) return "tabular csv dataset";
  if (lower.endsWith(".xlsx") || lower.endsWith(".xls")) return "tabular excel dataset";
  return "structured dataset";
}

export default function DataPulse({ user, onLogout }) {
  const [PlotComponent, setPlotComponent] = useState(null);
  const [phase, setPhase] = useState("upload");
  const [result, setResult] = useState(null);
  const [fileName, setFileName] = useState("");
  const [agentLog, setAgentLog] = useState([]);
  const [analysisError, setAnalysisError] = useState("");
  const [isDragOver, setIsDragOver] = useState(false);
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
  const [progress, setProgress] = useState(0);
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
    stageTimersRef.current.forEach((timerId) => {
      clearTimeout(timerId);
      clearInterval(timerId);
    });
    stageTimersRef.current = [];
  };

  const analyzeFile = useCallback(async (file) => {
    if (!file) return;
    setPhase("analyzing");
    setResult(null);
    setAnalysisError("");
    setAgentLog([]);
    setProgress(10);
    setTab("overview");
    setFileName(file.name);
    setChatMsgs([]);

    setAgentLog([
      "Uploading data to secure server…",
      "Architect initializing models…",
    ]);
    clearStageTimers();

    // Smoother progress increment
    const progInterval = setInterval(() => {
      setProgress(p => {
        if (p < 92) return p + Math.random() * 2;
        return p;
      });
    }, 400);
    stageTimersRef.current.push(progInterval);

    const stageMessages = [
      "Architect routing tasks → Statistician running…",
      "Statistician analyzing anomalies → Visualizer generating plots…",
      "Compiling AI Insights into dashboard…",
    ];
    stageMessages.forEach((msg, idx) => {
      const t = setTimeout(() => {
        log(msg);
        setProgress(30 + (idx * 20));
      }, 800 + idx * 1500);
      stageTimersRef.current.push(t);
    });

    try {
      const data = await apiAnalyze(file);
      clearStageTimers();
      setProgress(100);
      log(data.from_cache ? "Loaded accelerated cache." : "System orchestration complete.");

      // Delay slightly so user sees 100%
      setTimeout(() => {
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
      }, 600);
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

  const exportPDF = useCallback(async () => {
    if (!result) return;

    try {
      const { default: Plotly } = await import("plotly.js-dist-min");

      // Initialize Landscape A4
      const doc = new jsPDF("l", "mm", "a4");
      const pageWidth = doc.internal.pageSize.getWidth();  // ~297mm
      const pageHeight = doc.internal.pageSize.getHeight(); // ~210mm

      // 0. Background Color (Pale Teal/Cream)
      doc.setFillColor(236, 244, 243);
      doc.rect(0, 0, pageWidth, pageHeight, 'F');

      const margin = 12;

      // 1. TITLE
      doc.setFont("times", "bold"); // Serif font for dashboard title
      doc.setFontSize(22);
      doc.setTextColor(34, 49, 63); // Dark slate
      const displayTitle = fileName ? `${fileName.replace(/\.[^/.]+$/, "")} Dashboard` : "Analytics Dashboard";
      doc.text(truncateLabel(displayTitle, 45), margin, 20);

      // DataPulse Watermark
      doc.setFont("helvetica", "bolditalic");
      doc.setFontSize(14);
      doc.setTextColor(180, 200, 195);
      doc.text("DataPulse", pageWidth - margin - 20, margin + 4);

      // 2. SUBTITLE / HEADLINE
      const insights = result.insights || {};
      let headlineText = "Data processed successfully via automated AI analysis.";
      if (insights.headline) {
        headlineText = typeof insights.headline === 'object' ? String(insights.headline.text || "") : String(insights.headline);
      }

      doc.setFont("helvetica", "bold");
      doc.setFontSize(11); // Bold explicit heading
      doc.setTextColor(30, 40, 40);
      doc.text("Executive Summary", margin, 26);

      doc.setFont("helvetica", "normal");
      doc.setFontSize(10);
      doc.setTextColor(40, 40, 40);
      const splitSubtitle = doc.splitTextToSize(headlineText, pageWidth - margin * 2 - 20);
      doc.text(splitSubtitle, margin, 31);

      // 3. TOP METRICS ROW (Business Insights)
      const stats = result.stats_summary || {};
      const findingsList = [];
      if (insights.findings && Array.isArray(insights.findings)) {
        insights.findings.forEach(f => {
          findingsList.push(typeof f === 'object' ? String(f.text || f.message || "") : String(f));
        });
      }
      if (insights.recommendations && Array.isArray(insights.recommendations)) {
        insights.recommendations.forEach(f => {
          findingsList.push(typeof f === 'object' ? String(f.text || f.message || "") : String(f));
        });
      }

      const topFinding = findingsList.length > 0 ? findingsList[0] : "Data processed and structured successfully.";
      const secondFinding = findingsList.length > 1 ? findingsList[1] : `Analyzed ${stats.row_count || 0} rows across ${stats.column_count || 0} variables.`;

      let metricY = 41;
      const numMetrics = 3;
      const metricBoxWidth = (pageWidth - margin * 2 - 20) / numMetrics;

      for (let i = 0; i < numMetrics; i++) {
        const mX = margin + i * (metricBoxWidth + 10);

        // Label
        doc.setFont("helvetica", "bold");
        doc.setFontSize(10);
        doc.setTextColor(30, 50, 50);
        const label = i === 0 ? "Strategic Insight" : (i === 1 ? "Key Finding" : "Dataset Scope");
        doc.text(label, mX, metricY);

        // Value
        doc.setFont("helvetica", "normal");
        doc.setFontSize(10);
        doc.setTextColor(20, 30, 30);
        let valStr = "";
        if (i === 0) valStr = topFinding;
        else if (i === 1) valStr = secondFinding;
        else valStr = `${(stats.row_count || 0).toLocaleString()} records processed accurately.`;

        const splitVal = doc.splitTextToSize(valStr, metricBoxWidth);
        doc.text(splitVal, mX, metricY + 6);
      }

      // 4. CHARTS GRID
      const charts = result.charts || {};
      const chartKeys = Object.keys(charts);

      if (chartKeys.length > 0) {
        const totalCharts = Math.min(chartKeys.length, 6);
        let cols = 3;
        if (totalCharts <= 4) cols = 2;
        if (totalCharts === 1) cols = 1;

        const gap = 12;
        const availableWidth = pageWidth - margin * 2;
        const chartBoxWidth = (availableWidth - gap * (cols - 1)) / cols;

        const startY = 62; // Shifted down to accommodate standalone Executive Summary
        const availableHeight = pageHeight - startY - margin;
        const totalRows = Math.ceil(totalCharts / cols);
        const yGap = 12;
        const chartBoxHeight = totalRows > 1 ? (availableHeight - yGap) / totalRows : Math.min(availableHeight, 100);

        for (let i = 0; i < totalCharts; i++) {
          const key = chartKeys[i];
          const fig = charts[key];
          if (!fig || !fig.data) continue;

          try {
            // Calculate positions
            let col = i % cols;
            let row = Math.floor(i / cols);

            let boxX = margin + col * (chartBoxWidth + gap);

            // Center bottom row if exactly 3 charts (2 top, 1 bottom)
            if (totalCharts === 3 && i === 2) {
              boxX = margin + (availableWidth / 2) - (chartBoxWidth / 2);
            }
            // Center bottom row if exactly 5 charts (3 top, 2 bottom)
            if (totalCharts === 5 && i >= 3) {
              // For 5 charts, cols=3. The bottom row has 2 charts.
              // The width of 2 charts + 1 gap is (2*chartBoxWidth + gap).
              const bottomRowWidth = (2 * chartBoxWidth) + gap;
              const startOff = margin + (availableWidth - bottomRowWidth) / 2;
              const bottomCol = i - 3;
              boxX = startOff + bottomCol * (chartBoxWidth + gap);
            }

            const boxY = startY + row * (chartBoxHeight + yGap);

            // Draw White Rounded Container
            doc.setFillColor(255, 255, 255);
            doc.setDrawColor(200, 215, 215);
            doc.roundedRect(boxX, boxY, chartBoxWidth, chartBoxHeight, 3, 3, 'FD');

            // Draw Dark Title Pill Overlapping Top
            const pillWidth = chartBoxWidth * 0.95;
            const pillX = boxX + (chartBoxWidth - pillWidth) / 2;
            const pillY = boxY - 3;
            const pillHeight = 6;
            doc.setFillColor(4, 59, 72); // dark teal
            doc.roundedRect(pillX, pillY, pillWidth, pillHeight, 2, 2, 'F');

            // Pill Text Cleansing
            let baseTitle = key;
            if (fig.layout && fig.layout.title) {
              baseTitle = typeof fig.layout.title === 'string' ? fig.layout.title : (fig.layout.title.text || key.replaceAll("_", " "));
            }
            let strippedTitle = baseTitle.replace(/^(timeseries|scatter|bar\s?counts?|frequency\s?of|composition\s?of|donut|pie|line|heatmap)(?:\s+multi)?[\s-:]*/gi, "").trim() || baseTitle;
            // Remove instances of "Q10 - ", "Q4 - " anywhere in the title
            strippedTitle = strippedTitle.replace(/\bQ\d+\s*[-:]*\s*/gi, "").trim() || strippedTitle;
            // Remove aggregation suffixes e.g., "(agg to 16 pts)" and trailing question marks
            strippedTitle = strippedTitle.replace(/\(agg[^)]+\)/gi, "").replace(/\?+$/, "").trim() || strippedTitle;

            doc.setFontSize(7.5); // Reduced slightly for better fit
            doc.setTextColor(255, 255, 255);
            doc.setFont("helvetica", "bold");
            const titleStr = strippedTitle.charAt(0).toUpperCase() + strippedTitle.slice(1);
            const trTitle = titleStr.length > 60 ? titleStr.slice(0, 57) + '...' : titleStr;
            doc.text(trTitle, boxX + chartBoxWidth / 2, pillY + 4.0, { align: 'center' });

            const innerWidth = chartBoxWidth - 4;
            const innerHeight = chartBoxHeight - 8;

            // Base rendering height for Plotly to maintain good font sizes
            const scaleFactor = 320 / innerHeight;
            const renderWidth = Math.round(innerWidth * scaleFactor);
            const renderHeight = Math.round(innerHeight * scaleFactor);

            // Prepare Plotly Configuration for Light Mode
            const pdfLayout = {
              ...fig.layout,
              paper_bgcolor: "rgba(0,0,0,0)",
              plot_bgcolor: "rgba(0,0,0,0)",
              font: { color: "#333333", family: "Helvetica", size: 14 },
              xaxis: {
                ...fig.layout.xaxis,
                tickfont: { color: "#555555", size: 13 },
                title: { ...(fig.layout.xaxis?.title || {}), font: { color: "#333333", size: 15 } }
              },
              yaxis: {
                ...fig.layout.yaxis,
                tickfont: { color: "#555555", size: 13 },
                title: { ...(fig.layout.yaxis?.title || {}), font: { color: "#333333", size: 15 } }
              },
              width: renderWidth,
              height: renderHeight,
              showlegend: false,
              margin: { l: 45, r: 25, t: 20, b: 45 },
              title: null // Title is handled by pdf pill
            };

            // Generate Image with retuned resolution ratio
            const imgData = await Plotly.toImage(
              { data: fig.data, layout: pdfLayout },
              { format: 'png', width: renderWidth, height: renderHeight, scale: 3 }
            );

            // Add Image to Box
            doc.addImage(imgData, 'PNG', boxX + 2, boxY + 4, innerWidth, innerHeight);

          } catch (chartErr) {
            console.warn(`Skipped chart ${key}`, chartErr);
          }
        }
      }

      const cleanFileName = fileName.replace(/\.[^/.]+$/, "");
      doc.save(`${cleanFileName}_Dashboard.pdf`);

    } catch (err) {
      console.error("PDF Generation Error:", err);
      alert("Failed to compile PDF document. Please try again.");
    }
  }, [result, fileName]);

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
  const datasetTypeLabel = useMemo(() => inferDatasetType(result, fileName), [result, fileName]);

  const chatContext = useMemo(() => {
    if (!chatStats) return null;
    const outlierSummary = Object.entries(chatStats?.outliers || {})
      .map(([column, info]) => ({ column, count: Number(info?.count || 0), percentage: Number(info?.percentage || 0) }))
      .sort((a, b) => b.count - a.count).slice(0, 10);
    return {
      fileName,
      datasetTypeLabel,
      datasetProfile: chatStats?.dataset_profile || null,
      stats: chatStats,
      insights: chatInsights,
      outlierSummary,
      dataQuality: chatStats?.data_quality || {},
      correlations: chatStats?.strong_correlations?.slice(0, 5),
      charts: result?.charts || {},
      // Include the 100-row clean_df preview so the backend can generate new charts on demand
      clean_df: result?.clean_df || [],
      file_hash: result?.file_hash || null,
    };
  }, [chatStats, chatInsights, fileName, result?.charts, result?.clean_df, result?.file_hash, datasetTypeLabel]);

  const sendChat = useCallback(async () => {
    const q = chatInput.trim();
    if (!q || chatLoading || !result) return;
    
    // Prepare history before updating local state with the new message
    const history = chatMsgs.map(m => ({
      role: m.role === 'ai' ? 'assistant' : 'user',
      content: m.text
    })).slice(-10); // Last 10 messages for context

    setChatInput("");
    setChatMsgs((p) => [...p, { role: "user", text: q }].slice(-MAX_CHAT_MESSAGES));
    setChatLoading(true);
    try {
      const resp = await apiChat(q, chatContext || {}, history);
      const aiMessage = {
        role: "ai",
        text: (resp.answer || "").trim() || "No response generated.",
        // Attach any on-demand generated charts so they render inside this bubble
        generatedCharts: Array.isArray(resp.generated_charts) ? resp.generated_charts : [],
      };
      setChatMsgs((p) => [...p, aiMessage].slice(-MAX_CHAT_MESSAGES));
    } catch (err) {
      const detail = err?.message || "Unable to reach AI";
      setChatMsgs((p) => [...p, { role: "ai", text: `Chat error: ${detail}`, generatedCharts: [] }].slice(-MAX_CHAT_MESSAGES));
    }
    setChatLoading(false);
  }, [chatInput, chatLoading, result, chatContext]);

  const stats = result?.stats_summary || {};
  const insights = result?.insights || {};
  const dq = stats.data_quality || {};
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

  const keyMetrics = [
    { label: "Total Rows", val: (stats.row_count || 0).toLocaleString() },
    { label: "Schema Columns", val: stats.column_count || 0 },
    { label: "Completeness", val: formatPercent(dq.completeness || 100) },
  ];
  return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column', position: 'relative', overflow: 'hidden' }}>
      <ParticleBackground
        noExclude={false}
        exclusionSelectors={phase === "done"
          ? [
            ".col-4 > div:first-child",
            ".col-4 .card",
            ".panel-flat",
          ]
          : []}
        exclusionPadding={14}
      />

      {/* NAVBAR */}
      <div style={{ background: 'rgba(6, 9, 18, 0.90)', backdropFilter: 'blur(12px)', WebkitBackdropFilter: 'blur(12px)', padding: '16px 48px', borderBottom: '1px solid var(--border-subtle)', display: 'flex', justifyContent: 'space-between', alignItems: 'center', position: 'sticky', top: 0, zIndex: 100 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <div style={{ color: 'var(--primary-500)', fontSize: '24px', textShadow: '0 0 10px rgba(99,102,241,0.4)' }}>◈</div>
          <strong style={{ fontSize: '18px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif" }}>DATA PULSE</strong>
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
          <div className="animate-fade-in" style={{ flex: 1, display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 'clamp(22px, 3vw, 48px)', alignItems: 'center', maxWidth: '1400px', margin: '0 auto', padding: 'clamp(22px, 3vw, 40px) clamp(20px, 4vw, 64px)', height: '100%', overflow: 'hidden' }}>

            {/* LEFT SIDE: AI Animated Visual */}
            <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
              <div style={{ position: 'relative', width: '100%', maxWidth: '390px' }}>
                {/* Outer pulse ring */}
                <div style={{ position: 'absolute', inset: 0, borderRadius: '50%', background: 'var(--primary-500)', filter: 'blur(40px)', opacity: 0.05, animation: 'pulse 4s infinite' }} />

                {/* The Rotating AI Globe Core Container */}
                <div className="ai-hologram-layer" style={{ zIndex: 1 }}>
                  <GlobeCanvas size={390} />
                </div>
              </div>
            </div>

            {/* RIGHT SIDE: Upload Logic */}
            <div style={{ display: 'flex', flexDirection: 'column', justifyContent: 'center', alignItems: 'center', gap: '24px' }}>
              <div style={{ animation: 'slideUp 0.8s cubic-bezier(0.2, 0.8, 0.2, 1)', textAlign: 'center', width: '100%' }}>
                <div style={{ display: 'inline-flex', alignItems: 'center', gap: '12px', padding: '6px 16px', background: 'rgba(99,102,241,0.1)', borderRadius: '100px', border: '1px solid rgba(99,102,241,0.2)', marginBottom: '16px', color: 'var(--primary-500)', fontSize: '13px', fontWeight: 600, letterSpacing: '0.1em', textTransform: 'uppercase', marginInline: 'auto' }}>
                  <span style={{ width: '6px', height: '6px', background: 'var(--primary-500)', borderRadius: '50%', boxShadow: '0 0 10px var(--primary-500)' }} />
                  System Ready
                </div>
                <h1 style={{ fontSize: 'clamp(34px, 4vw, 42px)', margin: '0 0 4px 0', letterSpacing: '-0.02em', lineHeight: 1, color: 'var(--text-main)', opacity: 0.9, fontFamily: "'Inter', sans-serif", fontWeight: 700 }}>
                  Welcome
                </h1>
                <h2 style={{ fontSize: 'clamp(38px, 4.6vw, 48px)', margin: '0 0 16px 0', color: 'var(--primary-500)', textShadow: '0 0 20px rgba(99,102,241,0.3)', lineHeight: 1, fontFamily: "'Inter', sans-serif", fontWeight: 700 }}>
                  {user?.name || user?.email?.split('@')[0] || "Analyst"}
                </h2>
                <p style={{ fontSize: '16px', color: 'var(--text-muted)', lineHeight: 1.6, margin: 0 }}>
                  Ready to analyze your data? Connect a datasheet to initialize multi-agent analysis.
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
                  <strong style={{ color: 'var(--text-main)', fontSize: '20px', fontFamily: "'Inter', sans-serif", display: 'block', marginBottom: '8px' }}>Select File to upload</strong>
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
                    <div className="progress-bar" style={{ width: `${progress}%`, transition: 'width 0.5s cubic-bezier(0.4, 0, 0.2, 1)', background: 'linear-gradient(90deg, var(--primary-600), var(--info))' }} />
                  </div>

                  <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', minHeight: '60px', justifyContent: 'center' }}>
                    <span style={{ fontSize: '12px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '2px' }}>Active Agent Node</span>
                    <strong style={{ fontSize: '16px', color: 'var(--primary-500)', fontFamily: "'Outfit', monospace", textShadow: '0 0 10px rgba(99,102,241,0.3)' }}>
                      {agentLog[agentLog.length - 1] || "Analysis Pipeline running..."}
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
                    {phase === "done" && (
                      <span
                        className="data-pill"
                        style={{
                          color: '#7dd3fc',
                          borderColor: 'rgba(6,182,212,0.35)',
                          background: 'rgba(6,182,212,0.1)'
                        }}
                      >
                        {datasetTypeLabel}
                      </span>
                    )}
                  </div>
                </div>
              )}

              {/* Chat Box (only if done) */}
              {phase === "done" && result && (
                <div className="card flex-col gap-16" style={{ padding: '20px', flex: 1, display: 'flex', overflow: 'hidden' }}>
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <strong style={{ fontSize: '14px', color: 'var(--text-main)', fontFamily: 'Syne, sans-serif' }}>Expert Analyst Advisor</strong>
                    <span
                      title="This advisor only answers questions about the active dataset."
                      style={{
                        fontSize: '10px',
                        padding: '3px 8px',
                        background: 'rgba(99,102,241,0.12)',
                        border: '1px solid rgba(99,102,241,0.25)',
                        borderRadius: '100px',
                        color: 'var(--primary-500)',
                        letterSpacing: '0.08em',
                        textTransform: 'uppercase',
                        cursor: 'default',
                      }}
                    >Dataset Scope</span>
                  </div>
                  <div ref={chatContainerRef} style={{ flex: 1, overflowY: "auto", display: "flex", flexDirection: "column", gap: "12px", background: 'var(--bg-input)', padding: '16px', borderRadius: '12px', border: '1px solid var(--border-subtle)' }}>
                    {chatMsgs.length === 0 ? (
                      <div style={{ margin: 'auto', textAlign: 'center', display: 'flex', flexDirection: 'column', gap: '10px', maxWidth: '310px', background: 'linear-gradient(180deg, rgba(99,102,241,0.08), rgba(6,9,18,0.05))', border: '1px solid rgba(99,102,241,0.2)', borderRadius: '12px', padding: '16px 18px' }}>
                        <strong style={{ fontSize: '16px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif", letterSpacing: '0.02em' }}>Expert Data Analyst Advisor</strong>
                        <p style={{ fontSize: '13px', color: '#cbd5e1', lineHeight: 1.55, fontFamily: "'Inter', sans-serif" }}>Ask me anything about your dataset — insights, trends, outliers, or which chart reveals the story best.</p>
                        <div style={{ fontSize: '11px', color: 'var(--text-muted)', borderTop: '1px solid rgba(99,102,241,0.15)', paddingTop: '8px', opacity: 0.8 }}>⚠ Responses are limited to the active dataset only.</div>
                      </div>
                    ) : chatMsgs.map((m, i) => (
                      <div
                        key={i}
                        style={{
                          alignSelf: m.role === 'user' ? 'flex-end' : 'flex-start',
                          display: 'flex',
                          flexDirection: 'column',
                          gap: '8px',
                          maxWidth: m.role === 'user' ? '90%' : '100%',
                          marginBottom: '4px',
                        }}
                      >
                        {/* Text bubble */}
                        <div
                          style={{
                            background: m.role === 'user'
                              ? 'linear-gradient(135deg, #6366f1 0%, #4f46e5 100%)'
                              : 'rgba(30, 41, 59, 0.5)',
                            color: m.role === 'user' ? '#FFFFFF' : 'var(--text-main)',
                            padding: '12px 18px',
                            borderRadius: m.role === 'user' ? '18px 18px 4px 18px' : '18px 18px 18px 4px',
                            fontSize: '13px',
                            border: m.role === 'user' ? '1px solid rgba(255,255,255,0.1)' : '1px solid rgba(99,102,241,0.2)',
                            boxShadow: m.role === 'user' ? '0 4px 15px rgba(99,102,241,0.3)' : '0 4px 15px rgba(0,0,0,0.2)',
                            lineHeight: 1.5,
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
                                        hoverlabel: { bgcolor: "rgba(8,12,24,0.98)", font: { color: "#F8FAFC", size: 12 }, bordercolor: "rgba(99,102,241,0.85)" },
                                        height: 280,
                                        margin: { l: 40, r: 20, t: 40, b: 40 },
                                        title: { ...(layout.title || {}), font: { size: 14, color: '#fff', weight: 'bold' }, y: 0.95, yanchor: 'top' },
                                        legend: { orientation: "h", yanchor: "top", y: -0.2, xanchor: "center", x: 0.5, font: { size: 10, color: "rgba(255,255,255,0.7)" } }
                                      }}
                                      config={PLOTLY_CONFIG}
                                      style={{ width: "100%", height: "280px" }}
                                    />
                                  </div>
                                ) : <div key={pIdx} style={{ color: 'var(--primary-500)' }}>[Rendering Chart...]</div>;
                              }
                              if (match) return null;
                              return <span key={pIdx}>{part}</span>;
                            });
                          })()}
                        </div>

                        {/* On-demand generated charts — rendered BELOW the text bubble */}
                        {m.role === 'ai' && Array.isArray(m.generatedCharts) && m.generatedCharts.map((gc, gcIdx) => (
                          <div key={`gc-${gcIdx}`} style={{ width: '100%' }}>
                            {gc.error ? (
                              /* Error state */
                              <div style={{
                                padding: '12px 16px',
                                background: 'rgba(239,68,68,0.08)',
                                border: '1px solid rgba(239,68,68,0.25)',
                                borderRadius: '12px',
                                fontSize: '12px',
                                color: 'var(--error)',
                                lineHeight: 1.5,
                              }}>
                                <span style={{ fontWeight: 700, marginRight: '6px' }}>⚠ Chart Error:</span>{gc.error}
                              </div>
                            ) : gc.fig && PlotComponent ? (
                              /* Successful chart */
                              <div style={{
                                border: '1px solid rgba(99,102,241,0.25)',
                                borderRadius: '14px',
                                overflow: 'hidden',
                                background: 'rgba(0,0,0,0.35)',
                              }}>
                                {/* Generated chart badge */}
                                <div style={{
                                  padding: '6px 14px',
                                  borderBottom: '1px solid rgba(99,102,241,0.15)',
                                  display: 'flex',
                                  alignItems: 'center',
                                  gap: '6px',
                                  background: 'rgba(99,102,241,0.08)',
                                }}>
                                  <span style={{ fontSize: '10px', color: 'var(--primary-500)' }}>✦</span>
                                  <span style={{
                                    fontSize: '10px',
                                    color: 'var(--primary-500)',
                                    textTransform: 'uppercase',
                                    letterSpacing: '0.1em',
                                    fontWeight: 700,
                                  }}>Generated Chart</span>
                                  <span style={{ fontSize: '10px', color: 'var(--text-muted)', marginLeft: 'auto' }}>{gc.id}</span>
                                </div>
                                <div style={{ padding: '8px' }}>
                                  <PlotComponent
                                    data={(gc.fig.data || []).map(t => ({ ...t, textfont: { color: "#FFFFFF" } }))}
                                    layout={{
                                      ...PLOTLY_DARK_LAYOUT,
                                      ...(gc.fig.layout || {}),
                                      paper_bgcolor: "rgba(0,0,0,0)",
                                      plot_bgcolor: "rgba(0,0,0,0)",
                                      font: { color: "#FFFFFF", family: "'Inter', sans-serif", size: 11 },
                                      hoverlabel: { bgcolor: "rgba(8,12,24,0.98)", font: { color: "#F8FAFC", size: 12 }, bordercolor: "rgba(99,102,241,0.85)" },
                                      height: 300,
                                      margin: { l: 45, r: 16, t: 36, b: 45 },
                                      title: {
                                        ...(gc.fig.layout?.title || {}),
                                        font: { size: 13, color: '#FFFFFF', weight: 'bold' },
                                        y: 0.97, yanchor: 'top',
                                      },
                                      xaxis: { ...(gc.fig.layout?.xaxis || {}), tickfont: { color: "#FFFFFF", size: 10 }, gridcolor: "rgba(99,102,241,0.1)", automargin: true },
                                      yaxis: { ...(gc.fig.layout?.yaxis || {}), tickfont: { color: "#FFFFFF", size: 10 }, gridcolor: "rgba(99,102,241,0.1)", automargin: true },
                                      showlegend: false,
                                    }}
                                    config={{ ...PLOTLY_CONFIG, modeBarButtons: [['zoomIn2d', 'zoomOut2d', 'resetScale2d', 'toImage']] }}
                                    style={{ width: "100%", height: "300px" }}
                                  />
                                </div>
                              </div>
                            ) : null}
                          </div>
                        ))}
                      </div>
                    ))}
                    {chatLoading && <div style={{ fontSize: '13px', color: 'var(--primary-500)', fontFamily: "'Outfit', monospace" }}>Gener...</div>}
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
            <div className="col-8" style={{ height: '100%', overflowY: 'auto', paddingRight: '12px', paddingBottom: '32px', display: 'flex', flexDirection: 'column' }}>
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
                            <span style={{ fontSize: '28px', fontFamily: "'Inter', sans-serif", fontWeight: 800, color: 'var(--text-main)', textShadow: '0 0 15px rgba(255,255,255,0.1)' }}>{m.val}</span>
                          </div>
                        ))}
                      </div>
                      <div style={{ padding: '20px', background: 'var(--bg-input)', borderRadius: '12px', border: '1px solid var(--border-subtle)' }}>
                        <strong style={{ fontSize: '15px', display: 'block', marginBottom: '16px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif" }}>Data Info</strong>
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px' }}>
                          {[
                            {
                              label: 'Total Records',
                              value: (stats.row_count || 0).toLocaleString(),
                              color: '#818cf8',
                              icon: (
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                  <ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0 0 18 0V5"/><path d="M3 12a9 3 0 0 0 18 0"/>
                                </svg>
                              )
                            },
                            {
                              label: 'Total Columns',
                              value: stats.column_count || 0,
                              color: '#a78bfa',
                              icon: (
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                  <rect x="3" y="3" width="5" height="18" rx="1"/><rect x="10" y="3" width="5" height="18" rx="1"/><rect x="17" y="3" width="4" height="18" rx="1"/>
                                </svg>
                              )
                            },
                            {
                              label: 'Numeric Columns',
                              value: Object.keys(stats.numeric_columns || {}).length || '—',
                              color: '#60a5fa',
                              icon: (
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                  <line x1="4" y1="9" x2="20" y2="9"/><line x1="4" y1="15" x2="20" y2="15"/><line x1="10" y1="3" x2="8" y2="21"/><line x1="16" y1="3" x2="14" y2="21"/>
                                </svg>
                              )
                            },
                            {
                              label: 'Categorical Columns',
                              value: Object.keys(stats.categorical_columns || {}).length || '—',
                              color: '#c084fc',
                              icon: (
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                  <path d="M20.59 13.41l-7.17 7.17a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z"/><line x1="7" y1="7" x2="7.01" y2="7"/>
                                </svg>
                              )
                            },
                            {
                              label: 'Missing Values',
                              value: dq.total_missing != null ? dq.total_missing.toLocaleString() : (dq.missing_count != null ? dq.missing_count.toLocaleString() : '0'),
                              color: '#fbbf24',
                              icon: (
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                  <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>
                                </svg>
                              )
                            },
                            {
                              label: 'Duplicate Rows',
                              value: dq.duplicate_rows != null ? dq.duplicate_rows.toLocaleString() : '0',
                              color: '#f472b6',
                              icon: (
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                  <rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
                                </svg>
                              )
                            },
                            {
                              label: 'Data Completeness',
                              value: formatPercent(dq.completeness || 100),
                              color: '#34d399',
                              icon: (
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                  <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/>
                                </svg>
                              )
                            },
                            {
                              label: 'Dataset Type',
                              value: datasetTypeLabel || '—',
                              color: '#38bdf8',
                              icon: (
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                  <line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/>
                                </svg>
                              )
                            },
                          ].map(item => (
                            <div key={item.label} style={{ display: 'flex', alignItems: 'center', gap: '12px', padding: '12px 14px', background: 'rgba(99,102,241,0.04)', borderRadius: '10px', border: '1px solid rgba(99,102,241,0.1)', transition: 'background 0.2s' }}
                              onMouseEnter={e => e.currentTarget.style.background = 'rgba(99,102,241,0.09)'}
                              onMouseLeave={e => e.currentTarget.style.background = 'rgba(99,102,241,0.04)'}
                            >
                              <div style={{
                                width: '32px', height: '32px', borderRadius: '8px', flexShrink: 0,
                                background: `${item.color}18`,
                                border: `1px solid ${item.color}35`,
                                display: 'flex', alignItems: 'center', justifyContent: 'center',
                                color: item.color,
                              }}>
                                {item.icon}
                              </div>
                              <div style={{ display: 'flex', flexDirection: 'column', gap: '2px', minWidth: 0 }}>
                                <span style={{ fontSize: '10px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.08em', whiteSpace: 'nowrap' }}>{item.label}</span>
                                <span style={{ fontSize: '15px', fontWeight: 700, color: 'var(--text-main)', fontFamily: "'Inter', sans-serif", overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{String(item.value)}</span>
                              </div>
                            </div>
                          ))}
                        </div>
                        {stats.column_types && Object.keys(stats.column_types).length > 0 && (
                          <div style={{ marginTop: '14px' }}>
                            <span style={{ fontSize: '11px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.07em', display: 'block', marginBottom: '8px' }}>Column Overview</span>
                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
                              {Object.entries(stats.column_types).slice(0, 16).map(([col, dtype]) => (
                                <span key={col} style={{ fontSize: '11px', padding: '3px 10px', borderRadius: '100px', background: 'rgba(99,102,241,0.08)', border: '1px solid rgba(99,102,241,0.15)', color: 'var(--text-muted)' }}>
                                  <span style={{ color: 'var(--text-main)', fontWeight: 600 }}>{col}</span>
                                  <span style={{ opacity: 0.6 }}> · {dtype}</span>
                                </span>
                              ))}
                              {Object.keys(stats.column_types).length > 16 && (
                                <span style={{ fontSize: '11px', padding: '3px 10px', borderRadius: '100px', background: 'rgba(255,255,255,0.04)', color: 'var(--text-muted)' }}>+{Object.keys(stats.column_types).length - 16} more</span>
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  )}

                  {tab === "charts" && <ChartPanel result={result} PlotComponent={PlotComponent} />}

                  {tab === "insights" && (
                    <div className="flex-col gap-24">
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '16px' }}>
                        <div style={{ padding: '18px', borderRadius: '12px', border: '1px solid var(--border-subtle)', background: 'var(--bg-input)' }}>
                          <strong style={{ fontSize: '12px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Strong Correlations</strong>
                          <div style={{ marginTop: '8px', fontSize: '26px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif" }}>{(stats?.strong_correlations || []).length}</div>
                        </div>
                        <div style={{ padding: '18px', borderRadius: '12px', border: '1px solid var(--border-subtle)', background: 'var(--bg-input)' }}>
                          <strong style={{ fontSize: '12px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Columns With Outliers</strong>
                          <div style={{ marginTop: '8px', fontSize: '26px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif" }}>{outlierCols.length}</div>
                        </div>
                        <div style={{ padding: '18px', borderRadius: '12px', border: '1px solid var(--border-subtle)', background: 'var(--bg-input)' }}>
                          <strong style={{ fontSize: '12px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Data Completeness</strong>
                          <div style={{ marginTop: '8px', fontSize: '26px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif" }}>{formatPercent(dq.completeness || 100)}</div>
                        </div>
                      </div>

                      <div style={{ padding: '20px', background: 'var(--bg-input)', borderRadius: '12px', border: '1px solid var(--border-subtle)' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '16px' }}>
                          <span style={{ fontSize: '18px' }}>🔍</span>
                          <strong style={{ fontSize: '15px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif" }}>Analyst Findings</strong>
                          <span style={{ marginLeft: 'auto', fontSize: '11px', padding: '3px 10px', borderRadius: '100px', background: 'rgba(6,182,212,0.1)', border: '1px solid rgba(6,182,212,0.25)', color: '#7dd3fc', textTransform: 'uppercase', letterSpacing: '0.08em' }}>AI Generated</span>
                        </div>
                        {findings.length ? findings.map((f, i) => (
                            <div key={`ins-${i}`} style={{ marginBottom: '14px', padding: '14px 16px', borderRadius: '10px', background: 'rgba(255,255,255,0.03)', border: '1px solid rgba(99,102,241,0.12)', display: 'flex', gap: '14px', alignItems: 'flex-start' }}>
                              <div style={{ width: '26px', height: '26px', borderRadius: '50%', background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.12)', display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0, fontSize: '12px', fontWeight: 700, color: 'var(--text-muted)' }}>{i + 1}</div>
                              <div style={{ fontSize: '14px', color: 'var(--text-main)', lineHeight: 1.6, paddingTop: '4px' }}>{f}</div>
                            </div>
                        )) : <div style={{ fontSize: '14px', color: 'var(--text-muted)', fontStyle: 'italic' }}>No analyst findings were generated for this dataset.</div>}
                      </div>

                      <div style={{ padding: '20px', background: 'var(--bg-input)', borderRadius: '12px', border: '1px solid var(--border-subtle)' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '16px' }}>
                          <span style={{ fontSize: '18px' }}>🚀</span>
                          <strong style={{ fontSize: '15px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif" }}>Strategic Recommendations</strong>
                          <span style={{ marginLeft: 'auto', fontSize: '11px', padding: '3px 10px', borderRadius: '100px', background: 'rgba(99,102,241,0.1)', border: '1px solid rgba(99,102,241,0.25)', color: 'var(--primary-500)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Actionable</span>
                        </div>
                        {recommendations.length ? recommendations.map((r, i) => (
                          <div key={`rec-${i}`} style={{ marginBottom: '14px', padding: '14px 16px', borderRadius: '10px', background: 'rgba(99,102,241,0.04)', border: '1px solid rgba(99,102,241,0.12)', display: 'flex', gap: '14px', alignItems: 'flex-start' }}>
                            <div style={{ width: '26px', height: '26px', borderRadius: '50%', background: 'rgba(99,102,241,0.2)', border: '1px solid rgba(99,102,241,0.4)', display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0, fontSize: '12px', fontWeight: 800, color: 'var(--primary-500)' }}>{i + 1}</div>
                            <div style={{ flex: 1 }}>
                              <div style={{ fontSize: '11px', color: 'var(--primary-500)', textTransform: 'uppercase', letterSpacing: '0.1em', fontWeight: 700, marginBottom: '4px' }}>Action Item</div>
                              <div style={{ fontSize: '14px', color: 'var(--text-main)', lineHeight: 1.6 }}>{r}</div>
                            </div>
                          </div>
                        )) : <div style={{ fontSize: '14px', color: 'var(--text-muted)', fontStyle: 'italic' }}>No strategic recommendations were generated.</div>}
                      </div>
                    </div>
                  )}

                  {tab === "data" && (
                    <div className="flex-col gap-16">
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <strong style={{ fontSize: '15px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif" }}>Cleaned Data Preview</strong>
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