import { useEffect, useState, Component } from "react";
import { Navigate, Route, Routes, useNavigate } from "react-router-dom";
import { getToken, setToken, clearToken, apiMe } from "./api.js";
import Login from "./pages/Login.jsx";
import DataPulse from "./pages/DataPulseDashboard.jsx";
import AuthCallback from "./pages/AuthCallback.jsx";
export default function App() {
  const [authState, setAuthState] = useState({
    checked: false,
    user: null,
  });
  const navigate = useNavigate();
  useEffect(() => {
    const token = getToken();
    if (!token) {
      setAuthState({ checked: true, user: null });
      return;
    }
    apiMe()
      .then((user) => setAuthState({ checked: true, user }))
      .catch(() => {
        clearToken();
        setAuthState({ checked: true, user: null });
      });
  }, []);
  const handleLogin = (user, token) => {
    setToken(token);
    setAuthState({ checked: true, user });
    navigate("/", { replace: true });
  };
  const handleLogout = () => {
    clearToken();
    setAuthState({ checked: true, user: null });
    navigate("/login");
  };
  if (!authState.checked) {
    return (
      <div
        style={{
          minHeight: "100vh",
          background: "var(--bg-deep)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          color: "var(--text-muted)",
          fontFamily: "'Outfit', monospace",
          fontSize: "14px",
          letterSpacing: "0.1em",
          textTransform: "uppercase"
        }}
      >
        Loading System
      </div>
    );
  }
  return (
    <Routes>
      <Route
        path="/auth/callback"
        element={<AuthCallback onLogin={handleLogin} />}
      />
      <Route
        path="/login"
        element={
          authState.user
            ? <Navigate to="/" replace />
            : <Login onLogin={handleLogin} />
        }
      />
      <Route
        path="/register"
        element={
          authState.user
            ? <Navigate to="/" replace />
            : <Login onLogin={handleLogin} />
        }
      />
      <Route
        path="/forgot-password"
        element={
          authState.user
            ? <Navigate to="/" replace />
            : <Login onLogin={handleLogin} />
        }
      />
      <Route
        path="/reset-password"
        element={
          
          <Login onLogin={handleLogin} />
        }
      />
      <Route
        path="/"
        element={
          authState.user
            ? (
              <ErrorBoundary>
                <DataPulse user={authState.user} onLogout={handleLogout} />
              </ErrorBoundary>
            )
            : <Navigate to="/login" replace />
        }
      />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}

class ErrorBoundary extends Component {
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