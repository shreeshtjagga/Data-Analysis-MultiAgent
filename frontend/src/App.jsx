import { useEffect, useState, Component, useCallback, useRef } from "react";
import { Navigate, Route, Routes, useNavigate } from "react-router-dom";
import { getToken, setToken, clearToken, apiMe, apiSyncSession, onSessionExpired, recordActivity, supabase } from "./api.js";
import Login from "./pages/Login.jsx";
import DataPulse from "./pages/DataPulseDashboard.jsx";
import AuthCallback from "./pages/AuthCallback.jsx";
export default function App() {
  const [authState, setAuthState] = useState({
    checked: false,
    user: null,
  });
  const [sessionMessage, setSessionMessage] = useState(null);
  const navigate = useNavigate();
  const logoutRef = useRef(null);

  // ── Auto-logout handler (called by session timeout system) ──
  const handleLogout = useCallback((reason) => {
    clearToken();
    setAuthState({ checked: true, user: null });
    if (reason === 'session_expired') {
      setSessionMessage('Your session has expired. Please log in again.');
    } else if (reason === 'inactivity') {
      setSessionMessage('You were logged out due to inactivity.');
    } else {
      setSessionMessage(null);
    }
    navigate("/login");
  }, [navigate]);

  // Keep ref in sync so the session monitor callback always has latest version
  logoutRef.current = handleLogout;

  // ── Register session-expired callback & activity listeners ──
  useEffect(() => {
    // Session timeout callback
    onSessionExpired((reason) => {
      if (logoutRef.current) logoutRef.current(reason);
    });

    // Activity listeners — throttled to avoid performance issues
    let lastRecord = 0;
    const throttledRecord = () => {
      const now = Date.now();
      if (now - lastRecord > 5000) { // record at most every 5 seconds
        lastRecord = now;
        recordActivity();
      }
    };
    const events = ['mousemove', 'keydown', 'click', 'touchstart', 'scroll'];
    events.forEach((evt) => window.addEventListener(evt, throttledRecord, { passive: true }));

    return () => {
      events.forEach((evt) => window.removeEventListener(evt, throttledRecord));
    };
  }, []);

  // ── Initial auth check (with Supabase session restore on page refresh) ──
  useEffect(() => {
    const restoreSession = async () => {
      // Strategy 1: valid in-memory token → verify with server
      const token = getToken();
      if (token) {
        try {
          const user = await apiMe();
          setAuthState({ checked: true, user });
          return;
        } catch (err) {
          // 503 = DB down but token may still be valid — try Supabase session below
          if (err?.message?.includes('503') || err?.status === 503) {
            console.info('[Auth] apiMe DB unavailable, trying Supabase session fallback');
          } else {
            clearToken();
            setAuthState({ checked: true, user: null });
            return;
          }
        }
      }

      // Strategy 2: restore from Supabase localStorage session (page refresh / DB down)
      try {
        const { data } = await supabase.auth.getSession();
        if (data?.session) {
          try {
            const synced = await apiSyncSession(
              data.session.access_token,
              data.session.refresh_token
            );
            setToken(synced.access_token || data.session.access_token);
            setAuthState({ checked: true, user: synced.user });
            return;
          } catch {
            // Strategy 3: sync failed (DB down) — use Supabase token directly so user stays logged in
            setToken(data.session.access_token);
            setAuthState({
              checked: true,
              user: {
                id: 0,
                email: data.session.user?.email || '',
                name: data.session.user?.user_metadata?.name || data.session.user?.email?.split('@')[0] || '',
              },
            });
            return;
          }
        }
      } catch (err) {
        console.info('[Auth] Session restore failed:', err?.message);
      }

      setAuthState({ checked: true, user: null });
    };

    restoreSession();
  }, []);


  const handleLogin = (user, token) => {
    setToken(token);
    setSessionMessage(null); // clear any previous session message
    setAuthState({ checked: true, user });
    navigate("/", { replace: true });
  };
  const handleManualLogout = () => {
    handleLogout();
  };
  if (!authState.checked) {
    // Always render the OAuth callback handler immediately — never block it with null
    if (window.location.pathname === '/auth/callback') {
      return <AuthCallback onLogin={handleLogin} />;
    }
    return null;
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
            : <Login onLogin={handleLogin} sessionMessage={sessionMessage} />
        }
      />
      <Route
        path="/register"
        element={
          authState.user
            ? <Navigate to="/" replace />
            : <Login onLogin={handleLogin} sessionMessage={sessionMessage} />
        }
      />
      <Route
        path="/forgot-password"
        element={
          authState.user
            ? <Navigate to="/" replace />
            : <Login onLogin={handleLogin} sessionMessage={sessionMessage} />
        }
      />
      <Route
        path="/reset-password"
        element={
          
          <Login onLogin={handleLogin} sessionMessage={sessionMessage} />
        }
      />
      <Route
        path="/"
        element={
          authState.user
            ? (
              <ErrorBoundary>
                <DataPulse user={authState.user} onLogout={handleManualLogout} />
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
              Something went wrong
            </h3>
            <p style={{ margin: "0 0 20px", color: "var(--text-muted)", lineHeight: 1.6 }}>
              The dashboard encountered an unexpected error. Try refreshing the page — your data is safe.
            </p>
            <button
              onClick={() => window.location.reload()}
              style={{
                padding: "10px 24px", borderRadius: "9px",
                background: "linear-gradient(135deg, #6366f1, #4f46e5)",
                color: "#fff", border: "none", cursor: "pointer",
                fontWeight: 700, fontSize: "13px", letterSpacing: "0.06em",
              }}
            >
              Reload Page
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}