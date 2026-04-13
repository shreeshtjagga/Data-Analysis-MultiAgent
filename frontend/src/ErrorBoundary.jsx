import React from "react";

export default class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, message: "" };
  }

  static getDerivedStateFromError(error) {
    return {
      hasError: true,
      message: error?.message || "Unexpected dashboard render error",
    };
  }

  componentDidCatch(error, info) {
    // Keep a console trail for debugging dataset-specific crashes.
    console.error("Dashboard render error:", error, info);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div
          style={{
            minHeight: "100vh",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            background: "var(--bg-deep)",
            color: "var(--text-main)",
            padding: "24px",
          }}
        >
          <div
            style={{
              maxWidth: "720px",
              width: "100%",
              border: "1px solid rgba(239,68,68,0.35)",
              background: "rgba(13, 18, 32, 0.75)",
              borderRadius: "12px",
              padding: "24px",
            }}
          >
            <h3 style={{ marginTop: 0, marginBottom: "10px", color: "#fca5a5" }}>
              Dashboard Render Error
            </h3>
            <p style={{ margin: 0, color: "var(--text-muted)" }}>
              {this.state.message}
            </p>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
