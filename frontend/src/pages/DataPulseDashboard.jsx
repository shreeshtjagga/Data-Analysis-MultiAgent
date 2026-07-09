import { useState, useRef, useCallback, useEffect, useMemo, memo } from "react";
import jsPDF from "jspdf";
import { apiAnalyze, apiChat, apiHistory, apiHistoryAnalysis, apiDeleteAnalysis } from "../api.js";
import ParticleBackground from "../components/ParticleBackground.jsx";
import GlobeCanvas from "../components/GlobeCanvas.jsx";
import PlotComponent from "react-plotly.js";
const PALETTE = ["#6366f1", "#10b981", "#f59e0b", "#06b6d4", "#ef4444", "#a855f7", "#34d399", "#f472b6"];
const PLOTLY_DARK_LAYOUT = {
  paper_bgcolor: "rgba(0,0,0,0)",
  plot_bgcolor: "rgba(0,0,0,0)",
  font: { color: "#FFFFFF", family: "'Inter', sans-serif", size: 12 },
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
  displayModeBar: false,
  scrollZoom: true,
  displaylogo: false,
  doubleClick: "reset+autosize"
};
const MIN_ZOOM_SPAN_RATIO = 0.12;
const MAX_ZOOM_OUT_MULTIPLIER = 1.0;
const ZOOM_BOUNDARY_PADDING_RATIO = 0.0;
function truncateLabel(value, max = 26) {
  const text = String(value ?? "").trim();
  if (text.length <= max) return text;
  return `${text.slice(0, max - 1)}…`;
}
function cleanQuestionLabel(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return "";
  let text = raw
    .replace(/^Q\d+\s*[-:]*\s*/i, "")
    .replace(/^[-\s.:]+/, "")
    .trim();
  text = text.replace(/\?+$/, "").trim();
  return text || raw;
}
function cleanAxisTitle(value) {
  return truncateLabel(cleanQuestionLabel(value), 42);
}
function getFigureTitleText(fig, fallback = "") {
  const title = fig?.layout?.title;
  if (typeof title === "string") return title;
  return title?.text || fallback;
}
function stopPageZoomOnCtrlWheel(event) {
  if (event.ctrlKey || event.metaKey) {
    event.preventDefault();
  }
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
function parseAxisValue(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return { value, kind: "number" };
  }
  const parsedDate = Date.parse(value);
  if (Number.isFinite(parsedDate)) {
    return { value: parsedDate, kind: "date" };
  }
  return null;
}
function normalizeRangePair(range) {
  if (!Array.isArray(range) || range.length !== 2) return null;
  const first = parseAxisValue(range[0]);
  const second = parseAxisValue(range[1]);
  if (!first || !second || first.kind !== second.kind) return null;
  const low = Math.min(first.value, second.value);
  const high = Math.max(first.value, second.value);
  if (!Number.isFinite(low) || !Number.isFinite(high) || low === high) return null;
  return { low, high, kind: first.kind };
}
function collectAxisValues(data, axisKey) {
  const sourceKey = axisKey === "x" ? "x" : "y";
  const traces = Array.isArray(data) ? data : [];
  const parsed = [];
  let categoricalExtent = 0;
  traces.forEach((trace) => {
    const raw = trace?.[sourceKey];
    if (!Array.isArray(raw)) return;
    if (raw.length > categoricalExtent) categoricalExtent = raw.length;
    raw.forEach((item) => {
      const next = parseAxisValue(item);
      if (next) parsed.push(next);
    });
  });
  if (parsed.length < 2) return null;
  const kind = parsed[0].kind;
  const filtered = parsed.filter((p) => p.kind === kind).map((p) => p.value);
  if (filtered.length < 2) {
    if (categoricalExtent > 1) {
      return { min: -0.5, max: categoricalExtent - 0.5, kind: "number" };
    }
    return null;
  }
  return { min: Math.min(...filtered), max: Math.max(...filtered), kind };
}
function buildAxisConstraint(layoutAxis, dataAxisValues) {
  const layoutRange = normalizeRangePair(layoutAxis?.range);
  const base = layoutRange || dataAxisValues;
  if (!base) return null;
  let baseMin = base.low ?? base.min;
  let baseMax = base.high ?? base.max;
  if (!Number.isFinite(baseMin) || !Number.isFinite(baseMax)) return null;
  if (baseMax === baseMin) {
    baseMax = baseMin + 1;
  }
  const span = Math.max(1e-9, baseMax - baseMin);
  return {
    kind: base.kind,
    minSpan: span * MIN_ZOOM_SPAN_RATIO,
    maxSpan: span * MAX_ZOOM_OUT_MULTIPLIER,
    hardMin: baseMin - span * ZOOM_BOUNDARY_PADDING_RATIO,
    hardMax: baseMax + span * ZOOM_BOUNDARY_PADDING_RATIO,
  };
}
function formatAxisValue(value, kind) {
  if (kind === "date") return new Date(value).toISOString();
  return value;
}
function clampRangeToConstraint(range, constraint) {
  const normalized = normalizeRangePair(range);
  if (!normalized || !constraint || normalized.kind !== constraint.kind) return null;
  let low = normalized.low;
  let high = normalized.high;
  let span = high - low;
  const center = (low + high) / 2;
  if (span < constraint.minSpan) {
    span = constraint.minSpan;
    low = center - span / 2;
    high = center + span / 2;
  } else if (span > constraint.maxSpan) {
    span = constraint.maxSpan;
    low = center - span / 2;
    high = center + span / 2;
  }
  if (low < constraint.hardMin) {
    const delta = constraint.hardMin - low;
    low += delta;
    high += delta;
  }
  if (high > constraint.hardMax) {
    const delta = high - constraint.hardMax;
    low -= delta;
    high -= delta;
  }
  if (low < constraint.hardMin) low = constraint.hardMin;
  if (high > constraint.hardMax) high = constraint.hardMax;
  return [formatAxisValue(low, constraint.kind), formatAxisValue(high, constraint.kind)];
}
function getRelayoutRange(eventData, axisName) {
  const direct = eventData?.[`${axisName}.range`];
  if (Array.isArray(direct) && direct.length === 2) return direct;
  const start = eventData?.[`${axisName}.range[0]`];
  const end = eventData?.[`${axisName}.range[1]`];
  if (start !== undefined && end !== undefined) return [start, end];
  return null;
}
function rangesEqual(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b) || a.length !== 2 || b.length !== 2) return false;
  const left = normalizeRangePair(a);
  const right = normalizeRangePair(b);
  if (!left || !right || left.kind !== right.kind) return false;
  return Math.abs(left.low - right.low) < 1e-9 && Math.abs(left.high - right.high) < 1e-9;
}
function isRangeAtZoomBoundary(range, constraint, direction) {
  const normalized = normalizeRangePair(range);
  if (!normalized || !constraint || normalized.kind !== constraint.kind) return false;
  const span = normalized.high - normalized.low;
  if (direction === "in") {
    return span <= (constraint.minSpan * 1.02);
  }
  if (direction === "out") {
    return span >= (constraint.maxSpan * 0.98);
  }
  return false;
}
function scaleRangeByFactor(range, constraint, factor) {
  const normalized = normalizeRangePair(range);
  if (!normalized || !constraint || normalized.kind !== constraint.kind) return null;
  const center = (normalized.low + normalized.high) / 2;
  const nextSpan = (normalized.high - normalized.low) * factor;
  const rawRange = [
    formatAxisValue(center - (nextSpan / 2), constraint.kind),
    formatAxisValue(center + (nextSpan / 2), constraint.kind),
  ];
  return clampRangeToConstraint(rawRange, constraint);
}
function getLegendConfig(traceCount, isMatrix) {
  if (isMatrix || traceCount <= 1) {
    return { showlegend: false, legend: {}, legendRows: 0 };
  }
  const legendRows = Math.max(1, Math.ceil(traceCount / 4));
  const legendYOffset = -0.16 - ((legendRows - 1) * 0.08);
  return {
    showlegend: true,
    legendRows,
    legend: {
      orientation: "h",
      yanchor: "top",
      y: legendYOffset,
      xanchor: "left",
      x: 0,
      font: { size: 11, color: "rgba(255,255,255,0.8)" },
      tracegroupgap: 10,
      entrywidthmode: "pixels",
      entrywidth: 92,
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
function cleanPlotSummaryText(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return "";
  return raw
    .replace(/^#+\s*/gm, "")
    .replace(/\bAI[_\s-]*NARRATIVE\b\s*[:-]*/gi, "")
    .replace(/\bPLOT[_\s-]*SUMMARY\b\s*[:-]*/gi, "")
    .replace(/^\s*summary\s*[:-]\s*/i, "")
    .replace(/\s{2,}/g, " ")
    .trim();
}
function getPlotSummary(desc, fig, key) {
  const cleaned = cleanPlotSummaryText(desc);
  if (cleaned) return cleaned;
  const traceType = String(fig?.data?.[0]?.type || "chart").toLowerCase();
  const xTitle = cleanAxisTitle(fig?.layout?.xaxis?.title?.text || fig?.layout?.xaxis?.title || "");
  const yTitle = cleanAxisTitle(fig?.layout?.yaxis?.title?.text || fig?.layout?.yaxis?.title || "");
  const chartTitle = cleanQuestionLabel(fig?.layout?.title?.text || fig?.layout?.title || key.replaceAll("_", " "));
  if (traceType === "pie") return `${chartTitle} highlights category share distribution across the selected groups.`;
  if (traceType === "histogram") return `${chartTitle} shows frequency spread${xTitle ? ` for ${xTitle}` : ""}, helping identify skew and concentration.`;
  if (traceType === "box") return `${chartTitle} summarizes median, spread, and outliers${xTitle ? ` across ${xTitle}` : ""}.`;
  if (traceType === "heatmap") return `${chartTitle} maps intensity patterns to expose high and low concentration zones.`;
  if (xTitle && yTitle) {
    return `${chartTitle} compares ${yTitle} across ${xTitle} to surface key differences and trends.`;
  }
  return `${chartTitle} provides a focused visual summary of the most relevant variation in this dataset segment.`;
}
function getNumericExtent(trace) {
  const candidates = [];
  const yVals = Array.isArray(trace?.y) ? trace.y : [];
  const xVals = Array.isArray(trace?.x) ? trace.x : [];
  yVals.forEach((v) => {
    if (typeof v === "number" && Number.isFinite(v)) candidates.push(v);
  });
  xVals.forEach((v) => {
    if (typeof v === "number" && Number.isFinite(v)) candidates.push(v);
  });
  if (candidates.length < 2) return null;
  return { min: Math.min(...candidates), max: Math.max(...candidates), count: candidates.length };
}
function getCoreRevelations(desc, fig, key) {
  const traces = Array.isArray(fig?.data) ? fig.data : [];
  const traceType = String(traces[0]?.type || "chart").toLowerCase();
  const xTitle = cleanAxisTitle(fig?.layout?.xaxis?.title?.text || fig?.layout?.xaxis?.title || "x-axis");
  const yTitle = cleanAxisTitle(fig?.layout?.yaxis?.title?.text || fig?.layout?.yaxis?.title || "y-axis");
  const chartTitle = cleanQuestionLabel(fig?.layout?.title?.text || fig?.layout?.title || key.replaceAll("_", " "));
  const cleanedSummary = cleanPlotSummaryText(desc);
  const summarySentences = cleanedSummary
    ? cleanedSummary.split(/(?<=[.!?])\s+/).map((s) => s.trim()).filter(Boolean)
    : [];
  const firstTrace = traces[0] || {};
  const dataPointCount = Math.max(
    Array.isArray(firstTrace?.x) ? firstTrace.x.length : 0,
    Array.isArray(firstTrace?.y) ? firstTrace.y.length : 0,
  );
  const extent = getNumericExtent(firstTrace);
  const insights = [];
  if (summarySentences.length > 0) insights.push(summarySentences[0]);
  if (summarySentences.length > 1) insights.push(summarySentences[1]);
  if (traceType === "histogram") {
    insights.push(`The distribution across ${xTitle} highlights where observations are most concentrated.`);
  } else if (traceType === "box" || traceType === "violin") {
    insights.push(`Spread and quartile structure indicate how variable ${yTitle} is across groups.`);
  } else if (traceType === "pie" || traceType === "donut") {
    insights.push(`Category proportions reveal which segments dominate the overall composition.`);
  } else if (traceType === "heatmap") {
    insights.push(`Color intensity shows where pairings of ${xTitle} and ${yTitle} are strongest or weakest.`);
  } else {
    insights.push(`${chartTitle} compares ${yTitle} across ${xTitle}, exposing meaningful differences between categories.`);
  }
  if (traces.length > 1) {
    insights.push(`This view overlays ${traces.length} series, making cross-series comparison easier at a glance.`);
  }
  if (dataPointCount > 0) {
    insights.push(`The chart summarizes ${dataPointCount.toLocaleString()} plotted observations in the primary series.`);
  }
  if (extent) {
    insights.push(`Observed numeric range spans from ${extent.min.toFixed(2)} to ${extent.max.toFixed(2)}, indicating notable spread.`);
  }
  return Array.from(new Set(insights)).slice(0, 5);
}
function isChartZoomable(data) {
  const traces = Array.isArray(data) ? data : [];
  if (traces.length === 0) return false;
  const nonZoomableTypes = new Set([
    "pie",
    "sunburst",
    "treemap",
    "funnelarea",
    "parcats",
    "parcoords",
    "sankey",
    "table",
    "indicator",
  ]);
  return traces.some((trace) => {
    const traceType = String(trace?.type || "scatter").toLowerCase();
    if (nonZoomableTypes.has(traceType)) return false;
    if (Array.isArray(trace?.x) || Array.isArray(trace?.y)) return true;
    if (trace?.xaxis || trace?.yaxis) return true;
    return [
      "scatter",
      "bar",
      "histogram",
      "box",
      "violin",
      "heatmap",
      "contour",
      "candlestick",
      "ohlc",
      "waterfall",
      "funnel",
    ].includes(traceType);
  });
}
function _parseBoldInline(str) {
  const parts = str.split(/\*\*(.+?)\*\*/);
  if (parts.length === 1) return str;
  return parts.map((part, idx) =>
    idx % 2 === 1
      ? <strong key={idx} style={{ color: '#e2e8f0', fontWeight: 700 }}>{part}</strong>
      : (part || null)
  );
}
function renderMarkdown(text) {
  if (!text) return null;
  const lines = text.split('\n');
  const elements = [];
  let ki = 0;
  lines.forEach((raw, i) => {
    const line = raw.trim();
    if (!line) {
      elements.push(<div key={ki++} style={{ height: '5px' }} />);
      return;
    }

    const bulletMatch = line.match(/^[•\-*]\s+(.*)$/);
    if (bulletMatch) {
      elements.push(
        <div key={ki++} style={{ display: 'flex', gap: '8px', alignItems: 'flex-start', marginTop: '3px' }}>
          <span style={{ color: '#818cf8', fontWeight: 700, flexShrink: 0, marginTop: '1px' }}>•</span>
          <span style={{ flex: 1, lineHeight: 1.55 }}>{_parseBoldInline(bulletMatch[1])}</span>
        </div>
      );
      return;
    }

    const numMatch = line.match(/^(\d+)\.\s+(.*)$/);
    if (numMatch) {
      elements.push(
        <div key={ki++} style={{ display: 'flex', gap: '8px', alignItems: 'flex-start', marginTop: '3px' }}>
          <span style={{ color: '#818cf8', fontWeight: 700, minWidth: '20px', flexShrink: 0 }}>{numMatch[1]}.</span>
          <span style={{ flex: 1, lineHeight: 1.55 }}>{_parseBoldInline(numMatch[2])}</span>
        </div>
      );
      return;
    }

    elements.push(
      <p key={ki++} style={{ margin: 0, marginTop: i === 0 ? 0 : '4px', lineHeight: 1.55, color: 'inherit' }}>
        {_parseBoldInline(line)}
      </p>
    );
  });
  return elements;
}
const ChatBubble = memo(({ m, PlotComponent, result, stopPageZoomOnCtrlWheel, onSuggestionClick }) => {
  const [expandedChartKey, setExpandedChartKey] = useState(null);
  useEffect(() => {
    if (expandedChartKey) {
      const originalOverflow = document.body.style.overflow;
      document.body.style.overflow = "hidden";
      return () => {
        document.body.style.overflow = originalOverflow;
      };
    }
    return undefined;
  }, [expandedChartKey]);
  return (
    <div
      style={{
        alignSelf: m.role === 'user' ? 'flex-end' : 'flex-start',
        display: 'flex',
        flexDirection: 'column',
        gap: '8px',
        maxWidth: m.role === 'user' ? '90%' : '100%',
        marginBottom: '4px',
      }}
    >
      { }
      {m.role === 'ai' && m.newChart?.fig && PlotComponent && (
        <div style={{ width: '100%' }}>
          <div style={{
            border: '1px solid rgba(99,102,241,0.25)',
            borderRadius: '14px',
            overflow: 'hidden',
            background: 'rgba(0,0,0,0.35)',
          }}>
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
              <span style={{ fontSize: '10px', color: 'var(--text-muted)', marginLeft: 'auto' }}>{m.newChart.id}</span>
              <button
                onClick={() => setExpandedChartKey(m.newChart.id)}
                className="topbar-btn"
                style={{ padding: '4px 10px', fontSize: '11px', background: 'rgba(99,102,241,0.1)', marginLeft: '8px' }}
              >
                Expand
              </button>
            </div>
            <div style={{ padding: '8px' }}>
              <div onWheel={stopPageZoomOnCtrlWheel}>
                <PlotComponent
                  data={(m.newChart.fig.data || []).map(t => ({ ...t, textfont: { color: "#FFFFFF" } }))}
                  layout={{
                    ...PLOTLY_DARK_LAYOUT,
                    ...(m.newChart.fig.layout || {}),
                    paper_bgcolor: "rgba(0,0,0,0)",
                    plot_bgcolor: "rgba(0,0,0,0)",
                    font: { color: "#FFFFFF", family: "'Inter', sans-serif", size: 11 },
                    autosize: true,
                    width: undefined,
                    dragmode: false,
                    hoverlabel: { bgcolor: "rgba(8,12,24,0.98)", font: { color: "#F8FAFC", size: 12 }, bordercolor: "rgba(99,102,241,0.85)" },
                    height: 360,
                    margin: { r: 16, t: 36, b: 45 },
                    title: {
                      ...(typeof m.newChart.fig.layout?.title === "object" ? m.newChart.fig.layout.title : {}),
                      text: getFigureTitleText(m.newChart.fig, ""),
                      font: { size: 13, color: '#FFFFFF', weight: 'bold' },
                      y: 0.97, yanchor: 'top',
                    },
                    xaxis: { ...(m.newChart.fig.layout?.xaxis || {}), tickfont: { color: "#FFFFFF", size: 10 }, gridcolor: "rgba(99,102,241,0.1)", automargin: true },
                    yaxis: { ...(m.newChart.fig.layout?.yaxis || {}), tickfont: { color: "#FFFFFF", size: 10 }, gridcolor: "rgba(99,102,241,0.1)", automargin: true, tickmode: "auto", nticks: 10 },
                    showlegend: false,
                  }}
                  config={{ ...PLOTLY_CONFIG, scrollZoom: false, staticPlot: true, displayModeBar: false }}
                  useResizeHandler
                  style={{ width: "100%", height: "360px" }}
                />
              </div>
            </div>

            {expandedChartKey === m.newChart.id && (
              <div style={{ position: 'fixed', inset: 0, zIndex: 9999, background: 'rgba(0,0,0,0.85)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '40px' }} onClick={() => setExpandedChartKey(null)}>
                <div style={{ background: 'var(--bg-deep)', border: '1px solid var(--border-subtle)', borderRadius: '16px', width: '90%', maxWidth: '1000px', height: '80vh', display: 'flex', flexDirection: 'column', overflow: 'hidden', boxShadow: '0 20px 50px rgba(0,0,0,0.6)' }} onClick={e => e.stopPropagation()}>
                  <div style={{ padding: '16px 24px', borderBottom: '1px solid var(--border-subtle)', display: 'flex', justifyContent: 'space-between', alignItems: 'center', background: 'var(--bg-card)' }}>
                    <strong style={{ fontSize: '16px', color: 'var(--text-main)' }}>{getFigureTitleText(m.newChart.fig, m.newChart.id)}</strong>
                    <button onClick={() => setExpandedChartKey(null)} style={{ background: 'none', border: 'none', color: 'var(--text-muted)', fontSize: '24px', cursor: 'pointer' }}>×</button>
                  </div>
                  <div style={{ flex: 1, padding: '24px' }}>
                    <PlotComponent
                      data={(m.newChart.fig.data || []).map(t => ({ ...t, textfont: { color: "#FFFFFF" } }))}
                      layout={{
                        ...PLOTLY_DARK_LAYOUT,
                        ...(m.newChart.fig.layout || {}),
                        paper_bgcolor: "rgba(0,0,0,0)",
                        plot_bgcolor: "rgba(0,0,0,0)",
                        font: { color: "#FFFFFF", family: "'Inter', sans-serif" },
                        autosize: true,
                        width: undefined,
                        dragmode: 'zoom',
                        hoverlabel: { bgcolor: "rgba(8,12,24,0.98)", font: { color: "#F8FAFC", size: 12 }, bordercolor: "rgba(99,102,241,0.85)" },
                        height: undefined,
                        margin: { r: 24, t: 40, b: 60, l: 60 },
                        title: { text: '' },
                        xaxis: { ...(m.newChart.fig.layout?.xaxis || {}), tickfont: { color: "#FFFFFF", size: 11 }, automargin: true },
                        yaxis: { ...(m.newChart.fig.layout?.yaxis || {}), tickfont: { color: "#FFFFFF", size: 11 }, automargin: true },
                        legend: { orientation: 'h', yanchor: 'top', y: -0.15, xanchor: 'center', x: 0.5, font: { size: 12, color: 'rgba(255,255,255,0.7)' } },
                      }}
                      config={{ ...PLOTLY_CONFIG, displayModeBar: false, scrollZoom: true, doubleClick: 'reset' }}
                      useResizeHandler
                      style={{ width: "100%", height: "100%" }}
                    />
                  </div>
                </div>
              </div>
            )}

          </div>
        </div>
      )}
      { }
      <div
        className={m.role === 'ai' ? 'ai-message' : ''}
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
          display: 'flex',
          flexDirection: 'column',
          gap: '10px',
        }}
      >
        {(() => {
          if (m.role !== 'ai') {
            return renderMarkdown(m.text);
          }

          let cleanText = m.text;
          let parsedJson = null;
          try {
            let maybeJson = cleanText.replace(/```json/g, '').replace(/```/g, '').trim();
            if (maybeJson.startsWith('{') && maybeJson.endsWith('}')) {
              parsedJson = JSON.parse(maybeJson);
            }
          } catch (e) {
            parsedJson = null;
          }

          let hasChart = false;
          let inlineChartJSX = null;
          const CHART_TAG_RE = /\[CHART:\s*([^\]]+)\]/g;
          let tagM;

          while ((tagM = CHART_TAG_RE.exec(cleanText)) !== null) {
            hasChart = true;
            const key = tagM[1].trim();
            if (result?.charts?.[key]) {
              const figRaw = result.charts[key];
              if (figRaw === true || figRaw == null || typeof figRaw === 'boolean') break;
              let parsedFig = figRaw;
              if (typeof figRaw === 'string') {
                try { parsedFig = JSON.parse(figRaw); }
                catch (e) { console.warn('Failed to parse chart JSON:', e); parsedFig = {}; }
              }
              const cData = Array.isArray(parsedFig?.data) ? parsedFig.data : [];
              const cLayout = (parsedFig?.layout && typeof parsedFig.layout === 'object') ? parsedFig.layout : {};
              inlineChartJSX = (
                <div style={{ border: '1px solid rgba(99,102,241,0.2)', borderRadius: '12px', overflow: 'hidden', background: 'rgba(0,0,0,0.3)', width: '100%', padding: '12px', display: 'flex', flexDirection: 'column', gap: '8px' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span style={{ color: 'var(--primary-500)', fontSize: '16px' }}>📊</span>
                      <strong style={{ fontSize: '13px', color: 'var(--text-main)' }}>{cLayout.title?.text || key.replace(/_/g, ' ')}</strong>
                    </div>
                    <button onClick={() => setExpandedChartKey(key)} className="topbar-btn" style={{ padding: '4px 10px', fontSize: '11px', background: 'rgba(99,102,241,0.1)' }}>Expand</button>
                  </div>
                  <div style={{ height: '240px', pointerEvents: 'none', opacity: 0.95 }} onWheel={stopPageZoomOnCtrlWheel}>
                    <PlotComponent
                      data={cData.map(t => ({ ...t, textfont: { color: '#FFFFFF' } }))}
                      layout={{
                        ...PLOTLY_DARK_LAYOUT,
                        ...cLayout,
                        paper_bgcolor: 'rgba(0,0,0,0)',
                        plot_bgcolor: 'rgba(0,0,0,0)',
                        font: { color: '#FFFFFF', family: "'Inter', sans-serif" },
                        autosize: true,
                        width: undefined,
                        dragmode: false,
                        height: 240,
                        margin: { r: 15, t: 15, b: 35, l: 35 },
                        title: { text: '' },
                        showlegend: false,
                      }}
                      config={{ ...PLOTLY_CONFIG, staticPlot: true, displayModeBar: false }}
                      useResizeHandler
                      style={{ width: '100%', height: '100%' }}
                    />
                  </div>
                  {expandedChartKey === key && (
                    <div style={{ position: 'fixed', inset: 0, zIndex: 9999, background: 'rgba(0,0,0,0.85)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '40px' }} onClick={() => setExpandedChartKey(null)}>
                      <div style={{ background: 'var(--bg-deep)', border: '1px solid var(--border-subtle)', borderRadius: '16px', width: '90%', maxWidth: '1000px', height: '80vh', display: 'flex', flexDirection: 'column', overflow: 'hidden', boxShadow: '0 20px 50px rgba(0,0,0,0.6)' }} onClick={e => e.stopPropagation()}>
                        <div style={{ padding: '16px 24px', borderBottom: '1px solid var(--border-subtle)', display: 'flex', justifyContent: 'space-between', alignItems: 'center', background: 'var(--bg-card)' }}>
                          <strong style={{ fontSize: '16px', color: 'var(--text-main)' }}>{cLayout.title?.text || key.replace(/_/g, ' ')}</strong>
                          <button onClick={() => setExpandedChartKey(null)} style={{ background: 'none', border: 'none', color: 'var(--text-muted)', fontSize: '24px', cursor: 'pointer' }}>×</button>
                        </div>
                        <div style={{ flex: 1, padding: '24px' }}>
                          <PlotComponent
                            data={cData.map(t => ({ ...t, textfont: { color: '#FFFFFF' } }))}
                            layout={{
                              ...PLOTLY_DARK_LAYOUT,
                              ...cLayout,
                              paper_bgcolor: 'rgba(0,0,0,0)',
                              plot_bgcolor: 'rgba(0,0,0,0)',
                              font: { color: '#FFFFFF', family: "'Inter', sans-serif" },
                              autosize: true,
                              width: undefined,
                              dragmode: 'zoom',
                              hoverlabel: { bgcolor: 'rgba(8,12,24,0.98)', font: { color: '#F8FAFC', size: 12 }, bordercolor: 'rgba(99,102,241,0.85)' },
                              height: undefined,
                              margin: { r: 24, t: 40, b: 60, l: 60 },
                              xaxis: { ...(cLayout.xaxis || {}), tickfont: { color: '#FFFFFF', size: 11 }, automargin: true },
                              yaxis: { ...(cLayout.yaxis || {}), tickfont: { color: '#FFFFFF', size: 11 }, automargin: true },
                              title: { text: '' },
                              legend: { orientation: 'h', yanchor: 'top', y: -0.15, xanchor: 'center', x: 0.5, font: { size: 12, color: 'rgba(255,255,255,0.7)' } },
                            }}
                            config={{ ...PLOTLY_CONFIG, displayModeBar: false, scrollZoom: true, doubleClick: 'reset' }}
                            useResizeHandler
                            style={{ width: '100%', height: '100%' }}
                          />
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              );
              break;
            }
          }

          if (parsedJson) {
            const finalAnswerText = String(parsedJson.direct_answer || '').replace(/\[CHART:\s*[^\]]+\]/g, '').trim();

            return (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
                {finalAnswerText && (
                  <div style={{ fontSize: '15px', color: 'var(--text-main)', lineHeight: 1.6, fontWeight: 500 }}>
                    {_parseBoldInline(finalAnswerText)}
                  </div>
                )}
                {inlineChartJSX}
                {parsedJson.proactive_insight && (
                  <div style={{ padding: '12px', background: 'rgba(99,102,241,0.08)', borderRadius: '8px', borderLeft: '3px solid var(--primary-500)', fontSize: '14px', lineHeight: 1.5 }}>
                    <span style={{ color: 'var(--primary-500)', fontWeight: 700, marginRight: '6px' }}>Insight:</span>
                    {_parseBoldInline(String(parsedJson.proactive_insight))}
                  </div>
                )}
                {(parsedJson.confidence || parsedJson.suggestion) && (
                  <div style={{ display: 'flex', gap: '12px', alignItems: 'center', marginTop: '4px', flexWrap: 'wrap' }}>
                    {parsedJson.confidence && (
                      <div style={{ display: 'flex', alignItems: 'center', gap: '6px', background: 'rgba(0,0,0,0.2)', padding: '4px 8px', borderRadius: '4px', border: '1px solid var(--border-subtle)' }}>
                        <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: parsedJson.confidence > 85 ? 'var(--success)' : (parsedJson.confidence > 60 ? 'var(--warning)' : 'var(--error)'), animation: 'pulse 2s infinite' }} />
                        <span style={{ fontSize: '11px', color: 'var(--text-muted)', fontFamily: "'Inter', sans-serif" }}>{parsedJson.confidence}% CONFIDENCE</span>
                      </div>
                    )}
                    {parsedJson.suggestion && (
                      <button onClick={() => onSuggestionClick && onSuggestionClick(parsedJson.suggestion)} style={{ background: 'none', border: 'none', color: 'var(--primary-500)', fontSize: '12px', cursor: 'pointer', padding: 0, textDecoration: 'underline' }}>
                        Try: {parsedJson.suggestion}
                      </button>
                    )}
                  </div>
                )}
              </div>
            );
          }

          const finalCleanText = cleanText.replace(/\[CHART:\s*[^\]]+\]/g, '').replace(/[ \t]{2,}/g, ' ').trim();
          return (
            <>
              {finalCleanText && renderMarkdown(finalCleanText)}
              {inlineChartJSX}
            </>
          );
        })()}
      </div>
    </div>
  );
});
ChatBubble.displayName = 'ChatBubble';
const ChartPanel = memo(({ result, PlotComponent }) => {
  const [flipped, setFlipped] = useState({});
  const [chartRevisions, setChartRevisions] = useState({});
  const [spotlightViewports, setSpotlightViewports] = useState({});
  const [chartInitialBounds, setChartInitialBounds] = useState({});
  const [chartInteractionMode, setChartInteractionMode] = useState({});
  const [spotlightChartKey, setSpotlightChartKey] = useState(null);
  const relayoutTimeoutRefs = useRef({});
  const currentViewportsRef = useRef({});
  const wrapperRefs = useRef({});

  useEffect(() => {
    if (!spotlightChartKey) return undefined;
    const el = wrapperRefs.current[spotlightChartKey];
    if (!el) return undefined;

    const handleWheelCapture = (event) => {
      // If user zooms, prevent page scroll
      if (!event.ctrlKey && !event.metaKey) {
        event.preventDefault();
      } else {
        return;
      }

      const key = spotlightChartKey;
      const fig = result?.charts?.[key];
      if (!fig) return;

      const initialBounds = chartInitialBounds[key];
      const xBaseAxis = initialBounds?.x ? { range: initialBounds.x } : fig.layout?.xaxis;
      const yBaseAxis = initialBounds?.y ? { range: initialBounds.y } : fig.layout?.yaxis;

      const xConstraint = buildAxisConstraint(xBaseAxis, collectAxisValues(fig.data, "x"));
      const yConstraint = buildAxisConstraint(yBaseAxis, collectAxisValues(fig.data, "y"));

      if (!xConstraint && !yConstraint) return;

      const direction = event.deltaY < 0 ? "in" : "out";
      const viewport = spotlightViewports[key] || {};
      const currentXRange = currentViewportsRef.current[key]?.x || viewport.x || initialBounds?.x || fig.layout?.xaxis?.range || null;
      const currentYRange = currentViewportsRef.current[key]?.y || viewport.y || initialBounds?.y || fig.layout?.yaxis?.range || null;

      const axesAtBoundary = [];
      if (xConstraint && currentXRange) {
        axesAtBoundary.push(isRangeAtZoomBoundary(currentXRange, xConstraint, direction));
      }
      if (yConstraint && currentYRange) {
        axesAtBoundary.push(isRangeAtZoomBoundary(currentYRange, yConstraint, direction));
      }

      if (axesAtBoundary.length > 0 && axesAtBoundary.every(Boolean)) {
        // Stop propagation so Plotly doesn't zoom past the boundary!
        event.stopPropagation();
      }
    };

    el.addEventListener("wheel", handleWheelCapture, { capture: true, passive: false });
    return () => {
      el.removeEventListener("wheel", handleWheelCapture, { capture: true });
    };
  }, [spotlightChartKey, chartInitialBounds, result?.charts, spotlightViewports]);

  useEffect(() => {
    if (!spotlightChartKey) return undefined;
    const handleEscape = (event) => {
      if (event.key === "Escape") {
        setSpotlightChartKey(null);
      }
    };
    window.addEventListener("keydown", handleEscape);
    return () => {
      window.removeEventListener("keydown", handleEscape);
    };
  }, [spotlightChartKey]);
  useEffect(() => {
    if (spotlightChartKey) {
      const originalOverflow = document.body.style.overflow;
      document.body.style.overflow = "hidden";
      return () => {
        document.body.style.overflow = originalOverflow;
      };
    }
    return undefined;
  }, [spotlightChartKey]);
  const captureInitialBounds = useCallback((key, figure) => {
    const xRange = figure?.layout?.xaxis?.range
      ? normalizeRangePair(figure.layout.xaxis.range)
      : null;
    const yRange = normalizeRangePair(figure?.layout?.yaxis?.range);
    if (!xRange && !yRange) return;
    setChartInitialBounds((prev) => {
      if (prev[key]) return prev;
      return {
        ...prev,
        [key]: {
          x: xRange ? [formatAxisValue(xRange.low, xRange.kind), formatAxisValue(xRange.high, xRange.kind)] : null,
          y: yRange ? [formatAxisValue(yRange.low, yRange.kind), formatAxisValue(yRange.high, yRange.kind)] : null,
        },
      };
    });
  }, []);
  const resetChartView = useCallback((key) => {
    currentViewportsRef.current[key] = null;
    if (relayoutTimeoutRefs.current[key]) {
      clearTimeout(relayoutTimeoutRefs.current[key]);
      delete relayoutTimeoutRefs.current[key];
    }
    setSpotlightViewports((prev) => {
      if (!prev[key]) return prev;
      const next = { ...prev };
      delete next[key];
      return next;
    });
    setChartRevisions((prev) => ({ ...prev, [key]: (prev[key] || 0) + 1 }));
  }, []);
  const setChartMode = useCallback((key, mode) => {
    setChartInteractionMode((prev) => ({ ...prev, [key]: mode }));
  }, []);
  const entries = useMemo(() => {
    const CHART_PRIORITY = {
      timeseries: 0, heatmap: 1, line: 2, scatter: 3,
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
    const charts = result?.charts || {};
    return Object.entries(charts)
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
  }, [result?.charts]);
  const renderedEntries = useMemo(() => {
    if (!spotlightChartKey) return entries;
    const spotlightIndex = entries.findIndex(([key]) => key === spotlightChartKey);
    if (spotlightIndex <= 0) return entries;
    const current = entries[spotlightIndex];
    const previous = entries[spotlightIndex - 1];
    if (!current || !previous) return entries;
    const currentKey = current[0];
    const previousKey = previous[0];
    const currentIsWide = currentKey.startsWith("scatter_matrix") || currentKey.startsWith("heatmap") || currentKey.includes("matrix") || currentKey.startsWith("timeseries") || currentKey.startsWith("line");
    const previousIsWide = previousKey.startsWith("scatter_matrix") || previousKey.startsWith("heatmap") || previousKey.includes("matrix") || previousKey.startsWith("timeseries") || previousKey.startsWith("line");
    if (currentIsWide || previousIsWide) return entries;
    const next = [...entries];
    next[spotlightIndex - 1] = current;
    next[spotlightIndex] = previous;
    return next;
  }, [entries, spotlightChartKey]);
  if (!PlotComponent) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
        {[1, 2, 3].map(i => (
          <div key={i} style={{ height: '300px', borderRadius: '14px', background: 'rgba(99,102,241,0.05)', border: '1px solid var(--border-subtle)', animation: 'pulse 1.5s infinite' }} />
        ))}
      </div>
    );
  }
  if (entries.length === 0) {
    return <div style={{ color: "var(--text-muted)", fontSize: "14px" }}>No charts available.</div>;
  }
  const toggleFlip = (key) => {
    setFlipped(prev => ({ ...prev, [key]: !prev[key] }));
  };
  return (
    <>
      <div
        className={`chart-grid ${spotlightChartKey ? "chart-grid-spotlight-active" : ""}`}
        style={{ display: "grid", gridTemplateColumns: "repeat(12, 1fr)", gap: "24px", paddingBottom: "40px", alignItems: "start" }}
      >
        {spotlightChartKey && (
          <div
            className="chart-spotlight-backdrop"
            onClick={() => setSpotlightChartKey(null)}
            aria-label="Exit chart focus"
          />
        )}
        {renderedEntries.map(([key, fig, desc], idx) => {
          const isMatrix = key.startsWith("scatter_matrix") || key.startsWith("heatmap") || key.includes("matrix");
          const isWide = isMatrix || key.startsWith("timeseries") || key.startsWith("line");
          const traceCount = Array.isArray(fig.data) ? fig.data.length : 0;
          const hasLongLabels = hasLongCategoryLabels(fig.data);
          const { showlegend, legend, legendRows } = getLegendConfig(traceCount, isMatrix);
          const hideLegend = shouldHideLegend(fig.data, traceCount);
          const effectiveShowLegend = hideLegend ? false : showlegend;
          const showViewportControls = isChartZoomable(fig.data);
          const isSpotlighted = spotlightChartKey === key;
          const isDimmed = Boolean(spotlightChartKey) && !isSpotlighted;
          const gridSpan = isWide ? "span 12" : "span 6";
          const baseChartHeight = isMatrix ? 560 : isWide ? 455 : 405;
          const spotlightChartHeight = isMatrix ? 720 : isWide ? 640 : 560;
          const chartHeight = isSpotlighted ? spotlightChartHeight : baseChartHeight;
          const cardExtraHeight = effectiveShowLegend ? (isSpotlighted ? 90 : 72) : (isSpotlighted ? 72 : 58);
          const normalizedData = normalizeTraceData(fig.data);
          const margin = {
            l: 60,
            r: 24,
            t: 70,
            b: effectiveShowLegend ? (72 + (legendRows * 18)) : (hasLongLabels ? 78 : 58),
          };
          const initialBounds = chartInitialBounds[key] || {};
          const optimizedData = normalizedData.map(trace => {
            if (trace.type === "scatter" || trace.type === "scattergl") {
              const baseSize = trace.marker?.size || 8;
              return {
                ...trace,
                marker: {
                  ...(trace.marker || {}),
                  size: isSpotlighted ? baseSize : Math.max(4, baseSize - 2),
                  opacity: isSpotlighted ? 0.8 : 0.6,
                  line: {
                    width: isSpotlighted ? (trace.marker?.line?.width || 0.5) : 0,
                    color: trace.marker?.line?.color || "rgba(255,255,255,0.2)"
                  }
                }
              };
            }
            return trace;
          });
          const xBaseAxis = initialBounds.x ? { range: initialBounds.x } : fig.layout?.xaxis;
          const yBaseAxis = initialBounds.y ? { range: initialBounds.y } : fig.layout?.yaxis;
          const xConstraint = buildAxisConstraint(xBaseAxis, collectAxisValues(fig.data, "x"));
          const yConstraint = buildAxisConstraint(yBaseAxis, collectAxisValues(fig.data, "y"));
          const viewport = isSpotlighted ? (spotlightViewports[key] || {}) : {};
          const chartMode = chartInteractionMode[key] || (isSpotlighted ? "pan" : "zoom");
          const canBoundedZoom = Boolean(xConstraint || yConstraint);
          const canUseSpotlightZoom = isSpotlighted && showViewportControls && canBoundedZoom;
          const currentXRange = viewport.x || initialBounds.x || fig.layout?.xaxis?.range || null;
          const currentYRange = viewport.y || initialBounds.y || fig.layout?.yaxis?.range || null;
          const zoomChart = (factor) => {
            if (!canUseSpotlightZoom) return;
            setSpotlightViewports((prev) => {
              const current = prev[key] || {};
              const sourceX = currentViewportsRef.current[key]?.x || current.x || currentXRange;
              const sourceY = currentViewportsRef.current[key]?.y || current.y || currentYRange;
              const nextX = xConstraint ? scaleRangeByFactor(sourceX, xConstraint, factor) : null;
              const nextY = yConstraint ? scaleRangeByFactor(sourceY, yConstraint, factor) : null;
              if (!nextX && !nextY) return prev;
              const nextViewport = {
                ...current,
                ...(nextX ? { x: nextX } : {}),
                ...(nextY ? { y: nextY } : {}),
              };
              if (rangesEqual(current.x, nextViewport.x) && rangesEqual(current.y, nextViewport.y)) {
                return prev;
              }
              // Update the ref immediately
              currentViewportsRef.current[key] = { x: nextViewport.x, y: nextViewport.y };
              return { ...prev, [key]: nextViewport };
            });
          };
          const onChartRelayout = (eventData) => {
            if (!eventData) return;
            if (!canUseSpotlightZoom) return;

            if (eventData["xaxis.autorange"] || eventData["yaxis.autorange"]) {
              currentViewportsRef.current[key] = null;
              setSpotlightViewports((prev) => {
                if (!prev[key]) return prev;
                const next = { ...prev };
                delete next[key];
                return next;
              });
              return;
            }

            const rawX = getRelayoutRange(eventData, "xaxis");
            const rawY = getRelayoutRange(eventData, "yaxis");
            const clampedX = xConstraint ? clampRangeToConstraint(rawX, xConstraint) : null;
            const clampedY = yConstraint ? clampRangeToConstraint(rawY, yConstraint) : null;
            if (!clampedX && !clampedY) return;

            const nextX = clampedX || rawX || currentXRange;
            const nextY = clampedY || rawY || currentYRange;

            // Update the ref immediately so subsequent wheel/relayout events have the correct current range
            currentViewportsRef.current[key] = { x: nextX, y: nextY };

            if (relayoutTimeoutRefs.current[key]) {
              clearTimeout(relayoutTimeoutRefs.current[key]);
            }

            relayoutTimeoutRefs.current[key] = setTimeout(() => {
              setSpotlightViewports((prev) => {
                const current = prev[key] || {};
                const nextViewport = {
                  ...current,
                  ...(clampedX ? { x: clampedX } : {}),
                  ...(clampedY ? { y: clampedY } : {}),
                };
                if (rangesEqual(current.x, nextViewport.x) && rangesEqual(current.y, nextViewport.y)) {
                  return prev;
                }
                return { ...prev, [key]: nextViewport };
              });
            }, 300); // 300ms debounce
          };
          return (
            <div
              key={key}
              ref={el => { wrapperRefs.current[key] = el; }}
              className={`chart-flip-wrapper chart-card ${flipped[key] ? 'flipped' : ''} ${isSpotlighted ? 'chart-spotlighted' : ''} ${isDimmed ? 'chart-dimmed' : ''}`}
              data-mode={isSpotlighted ? chartMode : undefined}
              style={{
                gridColumn: isSpotlighted ? "1 / -1" : gridSpan,
                minWidth: 0,
                height: `${chartHeight + cardExtraHeight}px`,
                width: isSpotlighted ? "min(90vw, 1160px)" : undefined,
                justifySelf: isSpotlighted ? "center" : undefined,
                animation: isSpotlighted ? "none" : 'fadeIn 0.24s ease-out both',
                animationDelay: isSpotlighted ? undefined : `${Math.min(idx * 28, 260)}ms`,
              }}
            >
              <div className="chart-flip-inner">
                { }
                <div className="chart-flip-front" style={{ padding: '24px' }}>
                  <button
                    className="chart-info-btn"
                    onClick={() => toggleFlip(key)}
                    data-tooltip="View Details"
                  >
                    ℹ
                  </button>
                  <button
                    className="chart-focus-btn"
                    onClick={() => setSpotlightChartKey(isSpotlighted ? null : key)}
                    data-tooltip={isSpotlighted ? "Exit Focus" : "Focus Chart"}
                  >
                    {isSpotlighted ? "⤡" : "⤢"}
                  </button>
                  {canUseSpotlightZoom && (
                    <div className="chart-action-group">
                      <button
                        className="chart-action-btn"
                        onClick={() => zoomChart(0.8)}
                      >
                        +
                      </button>
                      <button
                        className="chart-action-btn"
                        onClick={() => zoomChart(1.25)}
                      >
                        -
                      </button>
                      <button
                        className={`chart-action-btn chart-pan-btn ${chartMode === "pan" ? "active" : ""}`}
                        onClick={() => setChartMode(key, chartMode === "pan" ? "zoom" : "pan")}
                      >
                        Pan
                      </button>
                      <button
                        className="chart-action-btn chart-reset-btn"
                        onClick={() => resetChartView(key)}
                      >
                        Reset
                      </button>
                    </div>
                  )}
                  <div>
                    <PlotComponent
                      data={optimizedData}
                      revision={chartRevisions[key] || 0}
                      onInitialized={(figure) => captureInitialBounds(key, figure)}
                      onRelayout={onChartRelayout}
                      layout={{
                        ...PLOTLY_DARK_LAYOUT,
                        ...fig.layout,
                        authorise: true,
                        title: {
                          ...(typeof fig.layout?.title === "object" ? fig.layout.title : {}),
                          text: truncateLabel(cleanQuestionLabel(getFigureTitleText(fig, key.replaceAll("_", " "))), 85),
                          font: { color: "#FFFFFF", size: 16, weight: 'bold' },
                          x: 0.5,
                          xanchor: "center",
                        },
                        paper_bgcolor: "rgba(0,0,0,0)",
                        plot_bgcolor: "rgba(0,0,0,0)",
                        font: { color: "#FFFFFF", family: "'Inter', sans-serif" },
                        uniformtext: { mode: 'hide', minsize: 10 },
                        dragmode: canUseSpotlightZoom ? chartMode : false,
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
                          ...(xConstraint ? {
                            minallowed: formatAxisValue(xConstraint.hardMin, xConstraint.kind),
                            maxallowed: formatAxisValue(xConstraint.hardMax, xConstraint.kind),
                          } : {}),
                          ...(viewport.x ? { range: viewport.x, autorange: false } : {}),
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
                          ...(yConstraint ? {
                            minallowed: formatAxisValue(yConstraint.hardMin, yConstraint.kind),
                            maxallowed: formatAxisValue(yConstraint.hardMax, yConstraint.kind),
                          } : {}),
                          ...(viewport.y ? { range: viewport.y, autorange: false } : {}),
                          automargin: true,
                          tickfont: { color: "#FFFFFF", size: 11 },
                        },
                      }}
                      config={{
                        ...PLOTLY_CONFIG,
                        scrollZoom: canUseSpotlightZoom,
                      }}
                      style={{ width: "100%", height: `${chartHeight}px` }}
                    />
                  </div>
                </div>
                { }
                <div className="chart-flip-back">
                  <button
                    className="chart-info-btn"
                    onClick={() => toggleFlip(key)}
                    data-tooltip="Flip Back"
                  >
                    ✕
                  </button>
                  <button
                    className="chart-focus-btn"
                    onClick={() => setSpotlightChartKey(isSpotlighted ? null : key)}
                    data-tooltip={isSpotlighted ? "Exit Focus" : "Focus Chart"}
                  >
                    {isSpotlighted ? "⤡" : "⤢"}
                  </button>
                  <div className="chart-back-badge">
                    <span style={{ fontSize: '10px' }}>◈</span> {
                      fig.data?.[0]?.type
                        ? fig.data[0].type.charAt(0).toUpperCase() + fig.data[0].type.slice(1).replace('scatter', 'Scatter Plot').replace('bar', 'Bar Chart').replace('pie', 'Pie Chart').replace('histogram', 'Histogram')
                        : 'Data Insight'
                    }
                  </div>
                  <h3 className="chart-back-title">
                    {cleanQuestionLabel(getFigureTitleText(fig, key.replaceAll("_", " ")))}
                  </h3>
                  <div className="chart-back-divider" />
                  <div className="chart-back-section-label">Plot Summary</div>
                  <p className="chart-back-description">
                    {getPlotSummary(desc, fig, key)}
                  </p>
                  <div className="chart-back-section-label">Core Revelation</div>
                  {getCoreRevelations(desc, fig, key).map((insight, insightIdx) => (
                    <div className="chart-back-insight-item" key={`${key}-insight-${insightIdx}`}>
                      <div className="chart-back-insight-dot" />
                      <span>{insight}</span>
                    </div>
                  ))}
                  <div className="chart-footer">
                    PROCESSED VIA DATAPULSE V5.0
                  </div>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </>
  );
});
ChartPanel.displayName = 'ChartPanel';
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
  const [phase, setPhase] = useState("upload");
  const [result, setResult] = useState(null);
  const [fileName, setFileName] = useState("");
  const [agentLog, setAgentLog] = useState([]);
  const [analysisError, setAnalysisError] = useState("");
  const [isDragOver, setIsDragOver] = useState(false);
  const [tab, setTab] = useState("overview");
  const [chatMsgs, setChatMsgs] = useState([]);
  const [generatedChartKeys, setGeneratedChartKeys] = useState([]);
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
  const [tableSort, setTableSort] = useState({ col: null, dir: 'none' });
  const [showExportModal, setShowExportModal] = useState(false);
  const [selectedExportKeys, setSelectedExportKeys] = useState([]);
  const fileRef = useRef();
  const chatContainerRef = useRef(null);
  const dragCounterRef = useRef(0);
  const stageTimersRef = useRef([]);
  useEffect(() => {
    loadHistory();
  }, []);
  const log = useCallback((msg) => {
    setAgentLog((p) => {
      const next = [...p, msg];
      return next.length > 20 ? next.slice(-20) : next;
    });
  }, []);
  const handleTableSort = useCallback((col) => {
    setTableSort(prev => {
      if (prev.col !== col) return { col, dir: 'asc' };
      if (prev.dir === 'asc') return { col, dir: 'desc' };
      return { col: null, dir: 'none' };
    });
  }, []);
  const sortedCleanDf = useMemo(() => {
    const df = result?.clean_df;
    if (!df || df.length === 0) return [];
    if (tableSort.dir === 'none' || !tableSort.col) return df;
    return [...df].sort((a, b) => {
      const va = a[tableSort.col];
      const vb = b[tableSort.col];
      const na = typeof va === 'number' ? va : parseFloat(va);
      const nb = typeof vb === 'number' ? vb : parseFloat(vb);
      const isNum = !isNaN(na) && !isNaN(nb);
      const cmp = isNum ? na - nb : String(va ?? '').localeCompare(String(vb ?? ''));
      return tableSort.dir === 'asc' ? cmp : -cmp;
    });
  }, [result?.clean_df, tableSort]);
  useEffect(() => {
    if (chatContainerRef.current) {
      requestAnimationFrame(() => {
        if (chatContainerRef.current) {
          chatContainerRef.current.scrollTop = chatContainerRef.current.scrollHeight;
        }
      });
    }
  }, [chatMsgs, chatLoading]);
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
    setGeneratedChartKeys([]);
    setAgentLog([
      "Uploading data to secure server…",
      "Architect initializing models…",
    ]);
    clearStageTimers();
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
      const raw = err?.message || "";
      log(`Core Failure: ${raw}`);
      // Sanitize raw backend/DB errors — never show internal details to users
      let userMsg = "Analysis failed. Please try again or upload a different file.";
      if (raw.includes("413") || raw.toLowerCase().includes("too large") || raw.toLowerCase().includes("file size")) {
        userMsg = "File is too large. Maximum size is 10 MB.";
      } else if (raw.includes("400") || raw.toLowerCase().includes("invalid") || raw.toLowerCase().includes("could not parse")) {
        userMsg = "Could not read the file. Make sure it's a valid CSV or Excel file.";
      } else if (raw.includes("413") || raw.toLowerCase().includes("rows") || raw.toLowerCase().includes("columns")) {
        userMsg = "Dataset is too large to analyze. Please reduce the number of rows or columns.";
      } else if (raw.includes("401") || raw.toLowerCase().includes("session expired") || raw.toLowerCase().includes("unauthorized")) {
        userMsg = "Your session has expired. Please log in again.";
      } else if (raw.includes("503") || raw.toLowerCase().includes("database") || raw.toLowerCase().includes("unavailable")) {
        userMsg = "Service temporarily unavailable. Please try again in a moment.";
      } else if (raw.includes("429") || raw.toLowerCase().includes("rate")) {
        userMsg = "Too many requests. Please wait a moment before trying again.";
      } else if (raw.includes("500")) {
        userMsg = "An unexpected server error occurred. Please try again.";
      }
      setAnalysisError(userMsg);
    }
  }, [log]);
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
      setHistoryError(err?.message?.includes("401") || err?.message?.toLowerCase().includes("session") ? "Session expired — please log in again." : "Could not load history. Check your connection and try again.");

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
      setGeneratedChartKeys([]);
      setChatMsgs([]);
      setChatInput("");
    } catch (err) {
      setHistoryActionError("Failed to restore session. Please try again.");
    } finally {
      setHistorySelectLoading(null);
    }
  };

  const exportPDF = useCallback(async (keysToExport) => {
    if (!result) return;
    setShowExportModal(false);
    try {
      const { default: Plotly } = await import("plotly.js-dist-min");
      const doc = new jsPDF("l", "mm", "a4");
      const pageWidth = doc.internal.pageSize.getWidth();
      const pageHeight = doc.internal.pageSize.getHeight();
      doc.setFillColor(236, 244, 243);
      doc.rect(0, 0, pageWidth, pageHeight, 'F');
      const margin = 12;
      doc.setFont("times", "bold");
      doc.setFontSize(22);
      doc.setTextColor(34, 49, 63);
      const displayTitle = fileName ? `${fileName.replace(/\.[^/.]+$/, "")} Dashboard` : "Analytics Dashboard";
      doc.text(truncateLabel(displayTitle, 45), margin, 20);
      doc.setFont("helvetica", "bolditalic");
      doc.setFontSize(14);
      doc.setTextColor(180, 200, 195);
      doc.text("DataPulse", pageWidth - margin - 20, margin + 4);
      const insights = result.insights || {};
      let headlineText = "Data processed successfully via automated AI analysis.";
      if (insights.headline) {
        headlineText = typeof insights.headline === 'object' ? String(insights.headline.text || "") : String(insights.headline);
      }
      doc.setFont("helvetica", "bold");
      doc.setFontSize(11);
      doc.setTextColor(30, 40, 40);
      doc.text("Executive Summary", margin, 26);
      doc.setFont("helvetica", "normal");
      doc.setFontSize(10);
      doc.setTextColor(40, 40, 40);
      const splitSubtitle = doc.splitTextToSize(headlineText, pageWidth - margin * 2 - 20);
      doc.text(splitSubtitle, margin, 31);
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
        doc.setFont("helvetica", "bold");
        doc.setFontSize(10);
        doc.setTextColor(30, 50, 50);
        const label = i === 0 ? "Strategic Insight" : (i === 1 ? "Key Finding" : "Dataset Scope");
        doc.text(label, mX, metricY);
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
      const charts = result.charts || {};
      const chartKeys = Array.isArray(keysToExport) && keysToExport.length > 0
        ? keysToExport.filter(k => charts[k])
        : Object.keys(charts);
      if (chartKeys.length > 0) {
        const totalCharts = Math.min(chartKeys.length, 6);
        let cols = 3;
        if (totalCharts <= 4) cols = 2;
        if (totalCharts === 1) cols = 1;
        const gap = 12;
        const availableWidth = pageWidth - margin * 2;
        const chartBoxWidth = (availableWidth - gap * (cols - 1)) / cols;
        const startY = 62;
        const availableHeight = pageHeight - startY - margin;
        const totalRows = Math.ceil(totalCharts / cols);
        const yGap = 12;
        const chartBoxHeight = totalRows > 1 ? (availableHeight - yGap) / totalRows : Math.min(availableHeight, 100);
        for (let i = 0; i < totalCharts; i++) {
          const key = chartKeys[i];
          const fig = charts[key];
          if (!fig || !fig.data) continue;
          try {
            let col = i % cols;
            let row = Math.floor(i / cols);
            let boxX = margin + col * (chartBoxWidth + gap);
            if (totalCharts === 3 && i === 2) {
              boxX = margin + (availableWidth / 2) - (chartBoxWidth / 2);
            }
            if (totalCharts === 5 && i >= 3) {
              const bottomRowWidth = (2 * chartBoxWidth) + gap;
              const startOff = margin + (availableWidth - bottomRowWidth) / 2;
              const bottomCol = i - 3;
              boxX = startOff + bottomCol * (chartBoxWidth + gap);
            }
            const boxY = startY + row * (chartBoxHeight + yGap);
            doc.setFillColor(255, 255, 255);
            doc.setDrawColor(200, 215, 215);
            doc.roundedRect(boxX, boxY, chartBoxWidth, chartBoxHeight, 3, 3, 'FD');
            const pillWidth = chartBoxWidth * 0.95;
            const pillX = boxX + (chartBoxWidth - pillWidth) / 2;
            const pillY = boxY - 3;
            const pillHeight = 6;
            doc.setFillColor(4, 59, 72);
            doc.roundedRect(pillX, pillY, pillWidth, pillHeight, 2, 2, 'F');
            let baseTitle = key;
            if (fig.layout && fig.layout.title) {
              baseTitle = typeof fig.layout.title === 'string' ? fig.layout.title : (fig.layout.title.text || key.replaceAll("_", " "));
            }
            let strippedTitle = baseTitle.replace(/^(timeseries|scatter|bar\s?counts?|frequency\s?of|composition\s?of|donut|pie|line|heatmap)(?:\s+multi)?[\s-:]*/gi, "").trim() || baseTitle;
            strippedTitle = strippedTitle.replace(/\bQ\d+\s*[-:]*\s*/gi, "").trim() || strippedTitle;
            strippedTitle = strippedTitle.replace(/\(agg[^)]+\)/gi, "").replace(/\?+$/, "").trim() || strippedTitle;
            doc.setFontSize(7.5);
            doc.setTextColor(255, 255, 255);
            doc.setFont("helvetica", "bold");
            const titleStr = strippedTitle.charAt(0).toUpperCase() + strippedTitle.slice(1);
            const trTitle = titleStr.length > 60 ? titleStr.slice(0, 57) + '...' : titleStr;
            doc.text(trTitle, boxX + chartBoxWidth / 2, pillY + 4.0, { align: 'center' });
            const innerWidth = chartBoxWidth - 4;
            const innerHeight = chartBoxHeight - 8;
            const scaleFactor = 320 / innerHeight;
            const renderWidth = Math.round(innerWidth * scaleFactor);
            const renderHeight = Math.round(innerHeight * scaleFactor);
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
              title: null
            };
            const imgData = await Plotly.toImage(
              { data: fig.data, layout: pdfLayout },
              { format: 'png', width: renderWidth, height: renderHeight, scale: 3 }
            );
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
    if (result?.chat_context_pack) {
      return {
        chat_context_pack: result.chat_context_pack,
        fileName,
        file_hash: result?.file_hash || null,
        generated_chart_keys: generatedChartKeys,
        charts: Object.keys(result?.charts || {}).reduce((acc, k) => ({ ...acc, [k]: true }), {}),
      };
    }
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
      charts: Object.keys(result?.charts || {}).reduce((acc, k) => ({ ...acc, [k]: true }), {}),
      file_hash: result?.file_hash || null,
      generated_chart_keys: generatedChartKeys,
    };
  }, [chatStats, chatInsights, fileName, result?.charts, result?.file_hash, result?.chat_context_pack, datasetTypeLabel, generatedChartKeys]);
  const sendChat = useCallback(async () => {
    const q = chatInput.trim();
    if (!q || chatLoading || !result) return;
    const chatHistory = chatMsgs.map(m => ({
      role: m.role === 'ai' ? 'assistant' : 'user',
      content: (m.text || '').replace(/\[CHART:\s*[^\]]+\]/g, '').trim(),
    })).filter(m => m.content).slice(-10);
    setChatInput("");
    const userMsgId = `msg-${Date.now()}-u`;
    setChatMsgs((p) => [...p, { id: userMsgId, role: "user", text: q }].slice(-MAX_CHAT_MESSAGES));
    setChatLoading(true);
    try {
      const resp = await apiChat(q, chatContext || {}, chatHistory);
      if (resp?.new_chart?.id && resp?.new_chart?.fig) {
        const chartId = resp.new_chart.id;
        setGeneratedChartKeys((prev) => (prev.includes(chartId) ? prev : [...prev, chartId]));
        setResult((prev) => prev ? ({
          ...prev,
          charts: { ...(prev.charts || {}), [chartId]: resp.new_chart.fig },
        }) : prev);
      }
      const rawAnswer = (resp.answer || "").trim() || "No response generated.";
      const cleanAnswer = rawAnswer.replace(/\*\*/g, '');
      const aiMsgId = `msg-${Date.now()}-a`;
      setChatMsgs((p) => [...p, {
        id: aiMsgId,
        role: "ai",
        text: cleanAnswer,
        newChart: resp?.new_chart?.fig ? resp.new_chart : null,
      }].slice(-MAX_CHAT_MESSAGES));
    } catch (err) {
      const raw = err?.message || "";
      // Show backend's own message if it's already user-friendly (rate limit, session, etc.)
      // Otherwise show a clean generic fallback
      const userMsg =
        raw.includes("429") || raw.toLowerCase().includes("too many")
          ? "You're sending messages too fast. Please wait a moment and try again."
          : raw.includes("401") || raw.toLowerCase().includes("session expired")
          ? "Your session has expired. Please refresh the page and log in again."
          : raw.includes("403")
          ? "Access denied to this dataset."
          : raw.includes("413") || raw.toLowerCase().includes("too long")
          ? "Your message is too long. Please shorten it and try again."
          : raw.includes("503") || raw.toLowerCase().includes("unavailable")
          ? "The AI service is temporarily unavailable. Please try again in a moment."
          : raw.length > 0 && raw.length < 200
          ? raw  // backend already gave a short, readable message
          : "Something went wrong. Please try again.";
      const errMsgId = `msg-${Date.now()}-e`;
      setChatMsgs((p) => [...p, { id: errMsgId, role: "ai", text: userMsg, newChart: null }].slice(-MAX_CHAT_MESSAGES));

    } finally {
      setChatLoading(false);
    }
  }, [chatInput, chatLoading, result, chatContext, chatMsgs]);
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
    <div className="dashboard-reveal" style={{ height: '100vh', display: 'flex', flexDirection: 'column', position: 'relative', overflow: 'hidden' }}>

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
      { }
      <div style={{ background: 'rgba(6, 9, 18, 0.90)', backdropFilter: 'blur(12px)', WebkitBackdropFilter: 'blur(12px)', padding: '16px 48px', borderBottom: '1px solid var(--border-subtle)', display: 'flex', justifyContent: 'space-between', alignItems: 'center', position: 'sticky', top: 0, zIndex: 100 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <div style={{ color: 'var(--primary-500)', fontSize: '24px', textShadow: '0 0 10px rgba(99,102,241,0.4)' }}>◈</div>
          <strong style={{ fontSize: '18px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif" }}>DATA PULSE</strong>
        </div>
        <div className="topbar-actions">
          {phase !== "analyzing" && <button onClick={toggleHistory} className="topbar-btn">History</button>}
          {result && <button onClick={() => setShowExportModal(true)} className="topbar-btn">Download</button>}
          <button onClick={onLogout} className="topbar-btn" style={{ marginLeft: '12px', background: 'rgba(239, 68, 68, 0.1)', color: 'var(--error)' }}>Logout</button>
        </div>
        <div style={{ fontSize: '14px', color: 'var(--text-muted)' }}>{user?.email}</div>
      </div>
      <div className="container" style={{ flex: 1, display: 'flex', flexDirection: 'column', position: 'relative', zIndex: 1, padding: 0, overflow: 'hidden' }}>
        { }
        {phase === "upload" ? (
          <div className="animate-fade-in" style={{ flex: 1, display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 'clamp(22px, 3vw, 48px)', alignItems: 'center', maxWidth: '1400px', margin: '0 auto', padding: 'clamp(22px, 3vw, 40px) clamp(20px, 4vw, 64px)', height: '100%', overflow: 'hidden' }}>
            { }
            <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
              <div style={{ position: 'relative', width: '100%', maxWidth: '390px' }}>
                { }
                <div style={{ position: 'absolute', inset: 0, borderRadius: '50%', background: 'var(--primary-500)', filter: 'blur(40px)', opacity: 0.05, animation: 'pulse 4s infinite' }} />
                { }
                <div className="ai-hologram-layer" style={{ zIndex: 1 }}>
                  { }
                  <GlobeCanvas size={390} />
                </div>
              </div>
            </div>
            { }
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
              { }
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
              { }
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
                <div style={{ fontSize: '56px', marginBottom: '24px' }}>⚠️</div>
                <h3 style={{ color: 'var(--text-main)', marginBottom: '12px', fontSize: '22px' }}>Analysis Failed</h3>
                <p style={{ color: 'var(--text-muted)', fontSize: '15px', maxWidth: '480px', margin: '0 auto 8px auto', lineHeight: 1.6 }}>{analysisError}</p>
                <p style={{ color: 'rgba(148,163,184,0.5)', fontSize: '12px', marginBottom: '32px' }}>Check the file format and try again, or upload a different dataset.</p>
                <button className="btn-primary" style={{ padding: '12px 32px', fontSize: '14px', margin: '0 auto' }} onClick={() => { setPhase("upload"); setAnalysisError(""); }}>Upload New Dataset</button>
              </div>
            ) : (
              <div style={{ width: '100%', maxWidth: '600px', display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                { }
                <div style={{ position: 'relative', width: '120px', height: '120px', marginBottom: '40px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  <div style={{ position: 'absolute', inset: 0, borderRadius: '50%', border: '2px solid rgba(99,102,241,0.2)', borderTop: '2px solid var(--primary-500)', animation: 'spin 1.5s linear infinite' }} />
                  <div style={{ position: 'absolute', inset: '15px', borderRadius: '50%', border: '2px solid rgba(16,185,129,0.2)', borderBottom: '2px solid var(--success)', animation: 'spin 2s linear infinite reverse' }} />
                  <div style={{ fontSize: '32px', color: 'var(--primary-500)', animation: 'pulse 2s infinite' }}>◈</div>
                </div>
                <h2 style={{ fontSize: '28px', color: 'var(--text-main)', marginBottom: '12px', letterSpacing: '0.05em' }}>Analyzing Dataset</h2>
                { }
                <div style={{ width: '100%', background: 'rgba(13,18,32,0.8)', border: '1px solid var(--border-subtle)', borderRadius: '16px', padding: '24px', position: 'relative', overflow: 'hidden' }}>
                  <div className="progress-container" style={{ marginBottom: '20px', height: '4px', background: 'rgba(255,255,255,0.05)' }}>
                    <div className="progress-bar" style={{ width: `${progress}%`, transition: 'width 0.5s cubic-bezier(0.4, 0, 0.2, 1)', background: 'linear-gradient(90deg, var(--primary-600), var(--info))' }} />
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', minHeight: '60px', justifyContent: 'center' }}>
                    <span style={{ fontSize: '12px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '2px' }}>Active Agent Node</span>
                    <strong style={{ fontSize: '16px', color: 'var(--primary-500)', fontFamily: "'Inter', sans-serif", textShadow: '0 0 10px rgba(99,102,241,0.3)' }}>
                      {agentLog[agentLog.length - 1] || "Analysis Pipeline running..."}
                    </strong>
                  </div>
                </div>
                <p style={{ marginTop: '24px', fontSize: '14px', color: 'var(--text-muted)', opacity: 0.8 }}>Data transparency protocols engaged. Preparing visualizations...</p>
              </div>
            )}
          </div>
        ) : (
          <div className="grid-12 animate-fade-in dashboard-grid">
            { }
            <div className="col-4 flex-col gap-24 dashboard-sidebar">
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
              { }
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
              { }
              {phase === "done" && result && (
                <div className="card flex-col gap-16" style={{ padding: '20px', flex: 1, display: 'flex', overflow: 'hidden' }}>
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <strong style={{ fontSize: '14px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif" }}>Expert Analyst Advisor</strong>
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
                  <div ref={chatContainerRef} className="chat-container">
                    {chatMsgs.length === 0 ? (
                      <div style={{ margin: 'auto', display: 'flex', flexDirection: 'column', gap: '16px', width: '100%', maxWidth: '340px' }}>
                        <div style={{ background: 'linear-gradient(180deg, rgba(99,102,241,0.08), rgba(6,9,18,0.05))', border: '1px solid rgba(99,102,241,0.2)', borderRadius: '12px', padding: '16px 18px', textAlign: 'center' }}>
                          <strong style={{ fontSize: '15px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif", letterSpacing: '0.02em', display: 'block', marginBottom: '12px' }}>💡 Suggested Questions</strong>
                          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                            {[
                              "What's the top category?",
                              "Show me the overall trend.",
                              "Are there any outliers?",
                              "Which factors have highest correlation?"
                            ].map((q, i) => (
                              <button key={i} onClick={() => { setChatInput(q); }} style={{ background: 'rgba(255,255,255,0.03)', border: '1px solid var(--border-subtle)', borderRadius: '8px', padding: '10px 14px', color: 'var(--text-main)', fontSize: '13px', cursor: 'pointer', textAlign: 'left', transition: 'all 0.2s' }} onMouseOver={e => { e.currentTarget.style.borderColor = 'var(--primary-500)'; e.currentTarget.style.background = 'rgba(99,102,241,0.08)'; }} onMouseOut={e => { e.currentTarget.style.borderColor = 'var(--border-subtle)'; e.currentTarget.style.background = 'rgba(255,255,255,0.03)'; }}>
                                › {q}
                              </button>
                            ))}
                          </div>
                        </div>
                      </div>
                    ) : chatMsgs.map((m, i) => (
                      <ChatBubble
                        key={m.id || i}
                        m={m}
                        PlotComponent={PlotComponent}
                        result={result}
                        stopPageZoomOnCtrlWheel={stopPageZoomOnCtrlWheel}
                        onSuggestionClick={(q) => { setChatInput(q); setTimeout(() => document.getElementById('chat-send-btn')?.click(), 50); }}
                      />
                    ))}
                    {chatLoading && (
                      <div className="ai-typing-pulse" style={{ padding: '12px 18px', margin: '4px 0', fontSize: '13px', color: 'var(--primary-500)', fontFamily: "'Inter', sans-serif" }}>
                        Architecting response <span></span><span></span><span></span>
                      </div>
                    )}
                    {chatMsgs.length >= MAX_CHAT_MESSAGES && (
                      <div style={{ fontSize: '11px', color: 'var(--text-muted)', textAlign: 'center', padding: '4px 0', borderTop: '1px solid var(--border-subtle)', marginTop: '4px' }}>
                        Showing last {MAX_CHAT_MESSAGES} messages
                      </div>
                    )}
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                    <div style={{ display: 'flex', gap: '12px' }}>
                      <input className="input-field" value={chatInput} onChange={e => setChatInput(e.target.value)} onKeyDown={e => e.key === 'Enter' && document.getElementById('chat-send-btn')?.click()} style={{ flex: 1, fontSize: '14px' }} placeholder="Query data..." maxLength={1200} />
                      <button id="chat-send-btn" className="btn-primary" onClick={sendChat} disabled={chatLoading} style={{ width: '44px', padding: 0 }}>»</button>
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
            { }
            <div className="col-8 dashboard-main">
              {!result ? (
                <div className="card animate-fade-in" style={{ minHeight: '500px', display: 'flex', alignItems: 'center', justifyContent: 'center', width: '100%' }}>
                  <p style={{ color: 'var(--text-muted)' }}>Synchronizing data... Standby.</p>
                </div>
              ) : (
                <div className="panel-flat flex-col gap-24 animate-fade-in">
                  <div className="tabs-nav">
                    {PRIMARY_TABS.map(t => <button key={t} onClick={() => setTab(t)} className={`tab ${tab === t ? 'active' : ''}`}>{t}</button>)}
                    {SECONDARY_TABS.map(t => <button key={t} onClick={() => setTab(t)} className={`tab ${tab === t ? 'active' : ''}`}>{t}</button>)}
                  </div>
                  {tab === "overview" && (
                    <div className="flex-col gap-24 tab-content-fade-in">
                      {headline && (
                        <div style={{ padding: '20px', background: 'rgba(99,102,241,0.05)', borderRadius: '12px', borderLeft: '4px solid var(--primary-500)' }}>
                          <strong style={{ fontSize: '12px', color: 'var(--primary-500)', textTransform: 'uppercase', display: 'block', marginBottom: '8px', letterSpacing: '0.1em' }}>Data Synopsis</strong>
                          <p style={{ fontSize: '15px', color: 'var(--text-main)', marginBottom: (insights.data_info && insights.data_info.length > 0) ? '12px' : '0' }}>{headline}</p>
                          {insights.data_info && insights.data_info.length > 0 && (
                            <div style={{ display: 'flex', flexDirection: 'column', gap: '6px', paddingTop: '12px', borderTop: '1px solid rgba(99,102,241,0.1)' }}>
                              {toTextList(insights.data_info).map((info, i) => (
                                <div key={`di-${i}`} style={{ fontSize: '13px', color: 'var(--text-muted)', display: 'flex', gap: '8px', alignItems: 'flex-start' }}>
                                  <span style={{ color: 'var(--primary-500)', fontSize: '14px', lineHeight: '18px' }}>•</span>
                                  <span>{info}</span>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      )}
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '16px' }}>
                        {keyMetrics.map(m => (
                          <div key={m.label} className="kpi-card">
                            <strong className="kpi-label">{m.label}</strong>
                            <span className="kpi-value">{m.val}</span>
                          </div>
                        ))}
                      </div>
                      <div style={{ padding: '20px', background: 'var(--bg-input)', borderRadius: '12px', border: '1px solid var(--border-subtle)' }}>
                        <strong style={{ fontSize: '15px', display: 'block', marginBottom: '16px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif" }}>Data Info</strong>
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px' }}>
                          {[
                            {
                              label: 'Numeric Columns',
                              value: Object.keys(stats.numeric_columns || {}).length || '—',
                              color: '#60a5fa',
                              icon: (
                                <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                                  <line x1="4" y1="9" x2="20" y2="9" /><line x1="4" y1="15" x2="20" y2="15" /><line x1="10" y1="3" x2="8" y2="21" /><line x1="16" y1="3" x2="14" y2="21" />
                                </svg>
                              )
                            },
                            {
                              label: 'Categorical Columns',
                              value: Object.keys(stats.categorical_columns || {}).length || '—',
                              color: '#c084fc',
                              icon: (
                                <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                                  <path d="M20.59 13.41l-7.17 7.17a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z" /><line x1="7" y1="7" x2="7.01" y2="7" />
                                </svg>
                              )
                            },
                            {
                              label: 'Missing Values',
                              value: dq.total_missing != null ? dq.total_missing.toLocaleString() : (dq.missing_count != null ? dq.missing_count.toLocaleString() : '0'),
                              color: '#fbbf24',
                              icon: (
                                <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                                  <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" /><line x1="12" y1="9" x2="12" y2="13" /><line x1="12" y1="17" x2="12.01" y2="17" />
                                </svg>
                              )
                            },
                            {
                              label: 'Duplicate Rows',
                              value: dq.duplicate_rows != null ? dq.duplicate_rows.toLocaleString() : '0',
                              color: '#f472b6',
                              icon: (
                                <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                                  <rect x="9" y="9" width="13" height="13" rx="2" /><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
                                </svg>
                              )
                            },
                            {
                              label: 'Dataset Type',
                              value: datasetTypeLabel || '—',
                              color: '#38bdf8',
                              icon: (
                                <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                                  <line x1="18" y1="20" x2="18" y2="10" /><line x1="12" y1="20" x2="12" y2="4" /><line x1="6" y1="20" x2="6" y2="14" />
                                </svg>
                              )
                            },
                            {
                              label: 'Noise Filtered',
                              value: (stats.excluded_columns || []).length,
                              color: '#94a3b8',
                              icon: (
                                <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                                  <path d="M22 3H2l8 9.46V19l4 2v-8.54L22 3z" />
                                </svg>
                              )
                            },
                            {
                              label: 'Detected Outlier Count',
                              value: Object.values(stats.outliers || {}).reduce((acc, curr) => acc + (curr.count || 0), 0).toLocaleString(),
                              color: '#f43f5e',
                              icon: (
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                  <circle cx="12" cy="12" r="10" /><line x1="12" y1="8" x2="12" y2="12" /><line x1="12" y1="16" x2="12.01" y2="16" />
                                </svg>
                              )
                            },
                            {
                              label: 'Strong Correlations',
                              value: (stats.strong_correlations || []).length,
                              color: '#8b5cf6',
                              icon: (
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                  <path d="M18 8A3 3 0 1018 2a3 3 0 000 6zM6 15A3 3 0 106 9a3 3 0 000 6zM18 22A3 3 0 1018 16a3 3 0 000 6z" /><path d="M9 12l6-4M9 12l6 7" />
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
                  {tab === "charts" && <div className="tab-content-fade-in"><ChartPanel result={result} PlotComponent={PlotComponent} /></div>}
                  {tab === "insights" && (
                    <div className="flex-col gap-24 tab-content-fade-in">
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '16px' }}>
                        <div style={{ padding: '18px', borderRadius: '12px', border: '1px solid var(--border-subtle)', background: 'var(--bg-input)' }}>
                          <strong style={{ fontSize: '12px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Strong Correlations</strong>
                          <div style={{ marginTop: '8px', fontSize: '26px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif" }}>{(stats?.strong_correlations || []).length}</div>
                        </div>
                        <div style={{ padding: '18px', borderRadius: '12px', border: '1px solid var(--border-subtle)', background: 'var(--bg-input)' }}>
                          <strong style={{ fontSize: '12px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Columns With Outliers</strong>
                          <div style={{ marginTop: '8px', fontSize: '26px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif" }}>{outlierCols.length}</div>
                        </div>
                        <div className={dq.completeness >= 99.5 ? "completeness-pulse" : ""} style={{ padding: '18px', borderRadius: '12px', border: '1px solid var(--border-subtle)', background: 'var(--bg-input)' }}>
                          <strong style={{ fontSize: '12px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Data Completeness</strong>
                          <div style={{ marginTop: '8px', fontSize: '26px', color: 'var(--text-main)', fontFamily: "'Inter', sans-serif" }}>{formatPercent(dq.completeness || 100)}</div>
                        </div>
                      </div>
                      <div style={{ border: '1px solid var(--border-subtle)', borderRadius: '12px', background: 'var(--bg-input)', overflow: 'hidden' }}>
                        <div style={{ padding: '16px 20px', borderBottom: '1px solid var(--border-subtle)', background: 'rgba(255,255,255,0.02)' }}>
                          <strong style={{ fontSize: '14px', color: 'var(--text-main)', letterSpacing: '0.05em', textTransform: 'uppercase', display: 'flex', alignItems: 'center', gap: '8px' }}>
                            <span></span> ABOUT DATA
                          </strong>
                        </div>
                        {!findings.length ? (
                          <div style={{ padding: '32px', color: 'var(--text-muted)', textAlign: 'center' }}>No analyst findings were generated for this dataset.</div>
                        ) : (
                          <div style={{ padding: '24px' }}>
                            <div className="pull-quote">
                              {findings[0].split(/([₹$€£]?-?\d+(?:,\d{3})*(?:\.\d+)?(?:%|k|M|B)?)/g).map((part, index) =>
                                /^[₹$€£]?-?\d+(?:,\d{3})*(?:\.\d+)?(?:%|k|M|B)?$/.test(part) ? <strong key={index} style={{ color: '#00d4a8', fontWeight: 700 }}>{part}</strong> : part
                              )}
                            </div>

                            {findings.length > 1 && (
                              <div style={{ marginBottom: '24px' }}>
                                <strong style={{ fontSize: '12px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.1em', display: 'block', marginBottom: '12px' }}>WHY THIS MATTERS</strong>
                                <p style={{ color: 'var(--text-main)', lineHeight: 1.6, fontSize: '15px' }}>
                                  {findings.slice(1, 3).map((f, i) => (
                                    <span key={i} style={{ display: 'block', marginBottom: '8px' }}>
                                      {f.split(/([₹$€£]?-?\d+(?:,\d{3})*(?:\.\d+)?(?:%|k|M|B)?)/g).map((part, index) =>
                                        /^[₹$€£]?-?\d+(?:,\d{3})*(?:\.\d+)?(?:%|k|M|B)?$/.test(part) ? <strong key={index} style={{ color: '#00d4a8', fontWeight: 700 }}>{part}</strong> : part
                                      )}
                                    </span>
                                  ))}
                                </p>
                              </div>
                            )}

                            {findings.length > 3 && (
                              <div>
                                <strong style={{ fontSize: '12px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.1em', display: 'block', marginBottom: '12px' }}>WHAT TO WATCH</strong>
                                <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                                  {findings.slice(3).map((info, i) => (
                                    <div key={`watch-${i}`} style={{ display: 'flex', gap: '12px', alignItems: 'flex-start' }}>
                                      <span style={{ color: 'var(--primary-500)', fontSize: '18px', lineHeight: '20px' }}>›</span>
                                      <span style={{ color: 'var(--text-main)', lineHeight: 1.6, fontSize: '14.5px' }}>
                                        {info.split(/([₹$€£]?-?\d+(?:,\d{3})*(?:\.\d+)?(?:%|k|M|B)?)/g).map((part, index) =>
                                          /^[₹$€£]?-?\d+(?:,\d{3})*(?:\.\d+)?(?:%|k|M|B)?$/.test(part) ? <strong key={index} style={{ color: '#00d4a8', fontWeight: 700 }}>{part}</strong> : part
                                        )}
                                      </span>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                      <div style={{ padding: '20px', background: 'var(--bg-input)', borderRadius: '12px', border: '1px solid var(--border-subtle)' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '16px' }}>
                          <div style={{
                            width: '32px',
                            height: '32px',
                            borderRadius: '10px',
                            border: '1px solid rgba(99,102,241,0.35)',
                            background: 'rgba(99,102,241,0.10)',
                            color: 'var(--primary-500)',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            flexShrink: 0,
                          }}>
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                              <path d="M4.5 19.5l6.2-2.1 8.8-8.8a2.1 2.1 0 0 0-3-3l-8.8 8.8-2.1 6.2z" />
                              <path d="M12.5 11.5l2 2" />
                            </svg>
                          </div>
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
                                {Object.keys(result.clean_df[0]).map(k => {
                                  const isActive = tableSort.col === k;
                                  const icon = isActive ? (tableSort.dir === 'asc' ? '▲' : '▼') : '⇅';
                                  return (
                                    <th
                                      key={k}
                                      onClick={() => handleTableSort(k)}
                                      style={{
                                        padding: '12px 14px',
                                        color: isActive ? '#818cf8' : 'var(--text-muted)',
                                        fontWeight: 600,
                                        textTransform: 'uppercase',
                                        letterSpacing: '0.05em',
                                        whiteSpace: 'nowrap',
                                        cursor: 'pointer',
                                        userSelect: 'none',
                                        transition: 'color 0.15s ease',
                                      }}
                                    >
                                      {k}
                                      <span style={{ opacity: isActive ? 1 : 0.35, fontSize: '9px', marginLeft: '5px', verticalAlign: 'middle' }}>{icon}</span>
                                    </th>
                                  );
                                })}
                              </tr>
                            </thead>
                            <tbody>
                              {sortedCleanDf.map((row, i) => (
                                <tr key={i} style={{ borderBottom: i === sortedCleanDf.length - 1 ? 'none' : '1px solid var(--border-subtle)' }}>
                                  {Object.values(row).map((v, j) => (
                                    <td key={j} style={{ padding: '12px 14px', color: 'var(--text-main)', whiteSpace: 'nowrap' }}>{String(v ?? '')}</td>
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
      { }
      {showExportModal && (
        <div style={{ position: 'fixed', inset: 0, zIndex: 2000, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          { }
          <div
            onClick={() => setShowExportModal(false)}
            style={{ position: 'absolute', inset: 0, background: 'rgba(0,0,0,0.65)', backdropFilter: 'blur(8px)' }}
          />
          { }
          <div className="animate-fade-in" style={{
            position: 'relative', zIndex: 1, width: 'min(560px, 94vw)',
            background: 'var(--bg-card)', border: '1px solid rgba(99,102,241,0.25)',
            borderRadius: '20px', padding: '32px', display: 'flex', flexDirection: 'column', gap: '20px',
            boxShadow: '0 24px 60px rgba(0,0,0,0.6), 0 0 0 1px rgba(99,102,241,0.1)',
          }}>
            { }
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <div>
                <h2 style={{ fontSize: '18px', fontWeight: 700, margin: 0, color: 'var(--text-main)' }}>Export Dashboard PDF</h2>
                <p style={{ margin: '4px 0 0', fontSize: '13px', color: 'var(--text-muted)' }}>
                  {selectedExportKeys.length} of {Object.keys(result?.charts || {}).length} charts selected
                </p>
              </div>
              <button onClick={() => setShowExportModal(false)} style={{ background: 'none', border: 'none', color: 'var(--text-muted)', cursor: 'pointer', fontSize: '20px', lineHeight: 1, padding: '2px 6px' }}>✕</button>
            </div>
            { }
            <div style={{ display: 'flex', gap: '10px' }}>
              <button
                onClick={() => setSelectedExportKeys(Object.keys(result?.charts || {}))}
                style={{
                  padding: '6px 14px', borderRadius: '8px', fontSize: '12px', fontWeight: 600,
                  border: '1px solid rgba(99,102,241,0.4)', background: 'rgba(99,102,241,0.1)',
                  color: '#818cf8', cursor: 'pointer', letterSpacing: '0.03em',
                }}
              >Select All</button>
              <button
                onClick={() => setSelectedExportKeys([])}
                style={{
                  padding: '6px 14px', borderRadius: '8px', fontSize: '12px', fontWeight: 600,
                  border: '1px solid rgba(255,255,255,0.1)', background: 'transparent',
                  color: 'var(--text-muted)', cursor: 'pointer',
                }}
              >Select None</button>
            </div>
            { }
            <div style={{
              display: 'flex', flexDirection: 'column', gap: '8px',
              maxHeight: '340px', overflowY: 'auto', paddingRight: '4px',
            }}>
              {Object.entries(result?.charts || {}).map(([key, fig]) => {
                const isChecked = selectedExportKeys.includes(key);
                const chartType = (Array.isArray(fig?.data) ? fig.data[0]?.type : null) || key.split('_')[0];
                const rawTitle = fig?.layout?.title?.text || fig?.layout?.title || '';
                const displayTitle = (() => {
                  let t = typeof rawTitle === 'string' ? rawTitle : String(rawTitle || key.replaceAll('_', ' '));
                  t = t.replace(/^(timeseries|scatter|bar|freq|donut|pie|line|heatmap|histogram|box|violin)[\s_-]*/gi, '').trim();
                  return t || key.replaceAll('_', ' ');
                })();
                const TYPE_BADGE = { scatter: '⬡ Scatter', heatmap: '▦ Heatmap', bar: '▬ Bar', histogram: '▤ Histogram', box: '⊡ Box', violin: '◈ Violin', pie: '◉ Pie', line: '⌇ Line', timeseries: '⌇ Time Series' };
                const badgeLabel = TYPE_BADGE[chartType] || `◈ ${chartType}`;
                return (
                  <label
                    key={key}
                    style={{
                      display: 'flex', alignItems: 'center', gap: '12px', padding: '10px 14px',
                      borderRadius: '10px', cursor: 'pointer', userSelect: 'none',
                      background: isChecked ? 'rgba(99,102,241,0.08)' : 'rgba(255,255,255,0.02)',
                      border: `1px solid ${isChecked ? 'rgba(99,102,241,0.35)' : 'rgba(255,255,255,0.06)'}`,
                      transition: 'all 0.15s ease',
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={isChecked}
                      onChange={() =>
                        setSelectedExportKeys(prev =>
                          isChecked ? prev.filter(k => k !== key) : [...prev, key]
                        )
                      }
                      style={{ width: '16px', height: '16px', accentColor: '#6366f1', cursor: 'pointer', flexShrink: 0 }}
                    />
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-main)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {displayTitle.charAt(0).toUpperCase() + displayTitle.slice(1)}
                      </div>
                      <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '2px' }}>{badgeLabel}</div>
                    </div>
                    {isChecked && <span style={{ color: '#6366f1', fontSize: '16px', flexShrink: 0 }}>✓</span>}
                  </label>
                );
              })}
            </div>
            { }
            <div style={{ display: 'flex', gap: '12px', justifyContent: 'flex-end', paddingTop: '4px', borderTop: '1px solid var(--border-subtle)' }}>
              <button
                onClick={() => setShowExportModal(false)}
                style={{
                  padding: '10px 20px', borderRadius: '10px', fontSize: '13px', fontWeight: 600,
                  border: '1px solid rgba(255,255,255,0.12)', background: 'transparent',
                  color: 'var(--text-muted)', cursor: 'pointer',
                }}
              >Cancel</button>
              <button
                onClick={() => exportPDF(selectedExportKeys)}
                disabled={selectedExportKeys.length === 0}
                style={{
                  padding: '10px 24px', borderRadius: '10px', fontSize: '13px', fontWeight: 700,
                  border: 'none', cursor: selectedExportKeys.length === 0 ? 'not-allowed' : 'pointer',
                  background: selectedExportKeys.length === 0 ? 'rgba(99,102,241,0.3)' : 'linear-gradient(135deg, #6366f1, #4f46e5)',
                  color: selectedExportKeys.length === 0 ? 'rgba(255,255,255,0.4)' : '#fff',
                  boxShadow: selectedExportKeys.length > 0 ? '0 4px 16px rgba(99,102,241,0.4)' : 'none',
                  transition: 'all 0.2s ease',
                }}
              >
                ↓ Generate PDF ({selectedExportKeys.length} chart{selectedExportKeys.length !== 1 ? 's' : ''})
              </button>
            </div>
          </div>
        </div>
      )}
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
                  (Array.isArray(history) ? history : []).map(item => {
                    const isThisLoading = historySelectLoading === item.analysis_id;
                    const isAnyLoading = historySelectLoading !== null;
                    const isThisDeleting = deleteLoading === item.analysis_id;
                    const isDisabled = isAnyLoading || isThisDeleting;
                    return (
                      <div key={item.analysis_id} className="card" style={{
                        padding: '16px',
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center',
                        cursor: isDisabled ? 'wait' : 'pointer',
                        border: isThisLoading ? '1px solid var(--primary-500)' : '1px solid var(--border-subtle)',
                        opacity: isThisDeleting ? 0.5 : (isAnyLoading && !isThisLoading) ? 0.4 : 1,
                        pointerEvents: isDisabled ? 'none' : 'auto',
                        transition: 'opacity 0.2s ease, border-color 0.2s ease',
                        boxShadow: isThisLoading ? '0 0 20px rgba(99,102,241,0.25)' : undefined,
                        position: 'relative',
                        overflow: 'hidden',
                      }} onClick={() => !isDisabled && loadHistoryItem(item)}>
                        {isThisLoading && (
                          <div style={{
                            position: 'absolute',
                            top: 0,
                            left: 0,
                            height: '3px',
                            background: 'linear-gradient(90deg, transparent, var(--primary-500), transparent)',
                            animation: 'historyLoadSweep 1.2s ease-in-out infinite',
                            width: '60%',
                            borderRadius: '2px',
                          }} />
                        )}
                        <div className="flex-col gap-12" style={{ flex: 1, minWidth: 0, width: '100%' }}>
                          <div>
                            <strong style={{ fontSize: '14px', color: 'var(--text-main)', display: 'block', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{item.file_name}</strong>
                            <span className="caption" style={{ marginTop: '2px', display: 'block', color: 'var(--text-muted)' }}>{new Date(item.analyzed_at).toLocaleDateString()} • {(item.row_count || 0).toLocaleString()} rows</span>
                          </div>
                          <div style={{ height: '1px', background: 'var(--border-subtle)', width: '100%' }}></div>
                          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: '12px' }}>
                            <span style={{ color: 'var(--text-muted)', fontWeight: 500 }}>{item.column_count || 'Multi'} cols • {isThisLoading ? 'Restoring session…' : 'Analysis ready'}</span>
                            <div style={{ display: 'flex', gap: '8px' }}>
                              <button
                                disabled={isDisabled}
                                onClick={(e) => { e.stopPropagation(); if (!isDisabled) loadHistoryItem(item); }}
                                style={{
                                  padding: '6px 12px',
                                  background: isThisLoading ? 'rgba(99,102,241,0.25)' : 'rgba(99,102,241,0.1)',
                                  color: '#818cf8',
                                  border: isThisLoading ? '1px solid rgba(99,102,241,0.5)' : '1px solid rgba(99,102,241,0.3)',
                                  borderRadius: '6px',
                                  cursor: isDisabled ? 'not-allowed' : 'pointer',
                                  fontWeight: 600,
                                  minWidth: '62px',
                                  display: 'inline-flex',
                                  alignItems: 'center',
                                  justifyContent: 'center',
                                  gap: '6px',
                                  transition: 'all 0.2s ease',
                                  opacity: (isAnyLoading && !isThisLoading) ? 0.5 : 1,
                                }}
                              >
                                {isThisLoading ? (
                                  <>
                                    <span style={{
                                      width: '12px',
                                      height: '12px',
                                      border: '2px solid rgba(129,140,248,0.3)',
                                      borderTopColor: '#818cf8',
                                      borderRadius: '50%',
                                      animation: 'spin 0.8s linear infinite',
                                      display: 'inline-block',
                                    }} />
                                    Loading
                                  </>
                                ) : 'Load'}
                              </button>
                              <button
                                className="history-delete-btn"
                                onClick={(e) => { e.stopPropagation(); deleteItem(item.analysis_id); }}
                                disabled={isDisabled}
                                title="Delete Analysis"
                                style={{ opacity: isAnyLoading && !isThisDeleting ? 0.5 : 1 }}
                              >
                                {isThisDeleting ? (
                                  <span style={{ fontSize: '10px', fontWeight: 'bold' }}>...</span>
                                ) : (
                                  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ width: '15px', height: '15px' }}>
                                    <polyline points="3 6 5 6 21 6"></polyline>
                                    <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
                                    <line x1="10" y1="11" x2="10" y2="17"></line>
                                    <line x1="14" y1="11" x2="14" y2="17"></line>
                                  </svg>
                                )}
                              </button>

                            </div>
                          </div>
                        </div>
                      </div>
                    );
                  })
                )
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}