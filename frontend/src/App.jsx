import { useEffect, useState } from "react";
import { Navigate, Route, Routes, useNavigate } from "react-router-dom";
import { getToken, setToken, clearToken, apiMe } from "./api.js";
import Login from "./pages/Login.jsx";
import DataPulse from "./pages/DataPulseDashboard.jsx";
import ErrorBoundary from "./components/ErrorBoundary.jsx";
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
          /* Always accessible — Supabase recovery sessions count as logged-in,
             so redirecting away would prevent the user from resetting their password. */
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