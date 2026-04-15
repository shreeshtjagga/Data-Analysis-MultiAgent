import { useEffect, useState } from "react";
import { Navigate, Route, Routes, useNavigate } from "react-router-dom";
import { getToken, setToken, clearToken, apiMe } from "./services/api.js";
import Login from "./login.jsx";
import DataPulse from "./pages/DataPulseDashboard.jsx";
import ErrorBoundary from "./ErrorBoundary.jsx";

/**
 * App.jsx
 * ───────
 * Handles top-level routing:
 *   /login      → <Login />      (public)
 *   /           → <DataPulse />  (protected — redirects to /login if no token)
 *
 * Auth state is stored in React state (NOT localStorage / sessionStorage).
 * The token lives in the api.js module-level variable (_token) and is lost
 * on a hard refresh — the user must re-login, which is intentional.
 */
export default function App() {
  const [authState, setAuthState] = useState({
    checked: false,   // true once we have attempted a token check
    user: null,       // null = not logged in
  });
  const navigate = useNavigate();

  // On mount: if a token is already held in memory (e.g. hot-reload),
  // verify it is still valid by hitting /auth/me.
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
    navigate("/");
  };

  const handleLogout = () => {
    clearToken();
    setAuthState({ checked: true, user: null });
    navigate("/login");
  };

  // Wait for the initial auth check before rendering anything
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
        path="/login"
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
          authState.user
            ? <Navigate to="/" replace />
            : <Login onLogin={handleLogin} />
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
      {/* Catch-all */}
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}