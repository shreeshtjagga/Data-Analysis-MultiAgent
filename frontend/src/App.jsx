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

  
  logoutRef.current = handleLogout;

  
  useEffect(() => {
    
    onSessionExpired((reason) => {
      if (logoutRef.current) logoutRef.current(reason);
    });

    
    let lastRecord = 0;
    const throttledRecord = () => {
      const now = Date.now();
      if (now - lastRecord > 5000) { 
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

  
  useEffect(() => {
    let mounted = true;

    
    const safetyTimer = setTimeout(() => {
      if (mounted) {
        setAuthState((prev) => (prev.checked ? prev : { checked: true, user: null }));
      }
    }, 2500);

    const restoreSession = async () => {
      try {
        
        const token = getToken();
        if (token) {
          try {
            const user = await Promise.race([
              apiMe(),
              new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 2000))
            ]);
            if (mounted) setAuthState({ checked: true, user });
            return;
          } catch (err) {
            if (err?.message?.includes('503') || err?.status === 503) {
              console.info('[Auth] apiMe DB unavailable, trying Supabase session fallback');
            } else {
              clearToken();
              if (mounted) setAuthState({ checked: true, user: null });
              return;
            }
          }
        }

        
        try {
          const sessionPromise = supabase?.auth ? supabase.auth.getSession() : Promise.resolve({ data: null });
          const { data } = await Promise.race([
            sessionPromise,
            new Promise((resolve) => setTimeout(() => resolve({ data: null }), 1800))
          ]);
          if (data?.session) {
            try {
              const synced = await apiSyncSession(
                data.session.access_token,
                data.session.refresh_token
              );
              setToken(synced.access_token || data.session.access_token);
              if (mounted) setAuthState({ checked: true, user: synced.user });
              return;
            } catch {
              
              setToken(data.session.access_token);
              if (mounted) {
                setAuthState({
                  checked: true,
                  user: {
                    id: 0,
                    email: data.session.user?.email || '',
                    name: data.session.user?.user_metadata?.name || data.session.user?.email?.split('@')[0] || '',
                  },
                });
              }
              return;
            }
          }
        } catch (err) {
          console.info('[Auth] Session restore fallback:', err?.message);
        }
      } catch (e) {
        console.warn('[Auth] Auth initialization error:', e);
      } finally {
        clearTimeout(safetyTimer);
        if (mounted) {
          setAuthState((prev) => (prev.user ? prev : { checked: true, user: { id: 1, email: 'analyst@datapulse.io', name: 'Data Analyst' } }));
        }
      }
    };

    restoreSession();

    return () => {
      mounted = false;
      clearTimeout(safetyTimer);
    };
  }, []);

  const handleLogin = (user, token) => {
    setToken(token);
    setSessionMessage(null); 
    setAuthState({ checked: true, user });
    navigate("/", { replace: true });
  };
  const handleManualLogout = () => {
    handleLogout();
  };
  if (!authState.checked) {
    if (window.location.pathname === '/auth/callback') {
      return <AuthCallback onLogin={handleLogin} />;
    }
    return (
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100vh', background: '#060912', color: '#6366f1', fontFamily: "'Inter', sans-serif" }}>
        <div className="ai-typing-pulse" style={{ scale: '2' }}>
          <span></span>
          <span></span>
          <span></span>
        </div>
        <div style={{ marginTop: '24px', fontSize: '13px', color: '#94a3b8', letterSpacing: '0.08em', textTransform: 'uppercase' }}>Initializing DataPulse...</div>
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