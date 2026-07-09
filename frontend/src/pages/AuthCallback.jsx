import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { supabase } from "../api.js";
import { apiSyncSession } from "../api";
import ParticleBackground from "../components/ParticleBackground";

export default function AuthCallback({ onLogin }) {
  const navigate = useNavigate();
  const [status, setStatus] = useState("loading"); // "loading" | "error"
  const [errorMsg, setErrorMsg] = useState("");
  const [isRecovery, setIsRecovery] = useState(false);
  const handledRef = useRef(false);

  useEffect(() => {
    if (handledRef.current) return;
    handledRef.current = true;

    const handleCallback = async () => {
      const params = new URLSearchParams(window.location.search);
      const urlError = params.get("error");
      const urlErrorDesc = params.get("error_description");

      if (urlError) {
        const msg = urlErrorDesc || urlError;
        // provider_email_needs_verification is a common non-fatal error
        if (msg.toLowerCase().includes("email") && msg.toLowerCase().includes("verif")) {
          setErrorMsg("Please verify your email address first, then try signing in again.");
        } else {
          setErrorMsg("Google sign-in was cancelled or failed. Redirecting you back…");
        }
        setStatus("error");
        setTimeout(() => navigate("/login", { replace: true }), 3000);
        return;
      }

      try {
        const code = params.get("code");
        if (code) {
          const { data, error: exchangeError } = await supabase.auth.exchangeCodeForSession(code);
          if (exchangeError) throw exchangeError;

          const session = data?.session || null;
          if (session) {
            const inHash = window.location.hash.includes("type=recovery");
            const inSearch = window.location.search.includes("type=recovery");
            if (inHash || inSearch) {
              setIsRecovery(true);
              navigate("/reset-password", { replace: true });
              return;
            }
            try {
              const synced = await apiSyncSession(session.access_token, session.refresh_token);
              onLogin(synced.user, synced.access_token);
              navigate("/", { replace: true });
            } catch {
              // Sync failed but token is valid — log in with token anyway
              onLogin({ email: data?.user?.email || "", id: 0 }, session.access_token);
              navigate("/", { replace: true });
            }
            return;
          }
        }

        // Fallback: check for existing session
        const { data: sessionData, error: sessionError } = await supabase.auth.getSession();
        if (sessionError) throw sessionError;

        const session = sessionData?.session || null;
        if (session) {
          const inHash = window.location.hash.includes("type=recovery");
          const inSearch = window.location.search.includes("type=recovery");
          if (inHash || inSearch) {
            setIsRecovery(true);
            navigate("/reset-password", { replace: true });
            return;
          }
          try {
            const synced = await apiSyncSession(session.access_token, session.refresh_token);
            onLogin(synced.user, synced.access_token);
            navigate("/", { replace: true });
          } catch {
            onLogin({ email: sessionData?.user?.email || "", id: 0 }, session.access_token);
            navigate("/", { replace: true });
          }
          return;
        }

        // Listen for auth state change
        const { data: listener } = supabase.auth.onAuthStateChange(async (event, newSession) => {
          if (event === "PASSWORD_RECOVERY") {
            listener.subscription.unsubscribe();
            setIsRecovery(true);
            navigate("/reset-password", { replace: true });
            return;
          }
          if (event === "SIGNED_IN" && newSession) {
            listener.subscription.unsubscribe();
            try {
              const synced = await apiSyncSession(newSession.access_token, newSession.refresh_token);
              onLogin(synced.user, synced.access_token);
              navigate("/", { replace: true });
            } catch {
              onLogin({ email: newSession.user?.email || "", id: 0 }, newSession.access_token);
              navigate("/", { replace: true });
            }
          }
        });

        const timeout = setTimeout(() => {
          listener.subscription.unsubscribe();
          setErrorMsg("Sign-in timed out. Redirecting you back to login…");
          setStatus("error");
          setTimeout(() => navigate("/login", { replace: true }), 2500);
        }, 15000);

        return () => {
          clearTimeout(timeout);
          listener.subscription.unsubscribe();
        };
      } catch (err) {
        setErrorMsg("Something went wrong. Redirecting you back to login…");
        setStatus("error");
        setTimeout(() => navigate("/login", { replace: true }), 2500);
      }
    };

    handleCallback();
  }, [navigate, onLogin]);

  return (
    <div style={{ height: "100vh", display: "flex", alignItems: "center", justifyContent: "center", position: "relative" }}>
      <ParticleBackground />
      <div style={{ position: "relative", zIndex: 1, textAlign: "center", width: "100%", maxWidth: "420px", padding: "0 24px" }}>

        {/* Logo */}
        <div style={{ color: "var(--primary-500)", fontSize: "40px", marginBottom: "12px", textShadow: "0 0 20px rgba(99,102,241,0.7)" }}>◈</div>
        <h2 style={{ fontFamily: "'Inter', sans-serif", fontSize: "24px", fontWeight: 700, color: "var(--text-main)", marginBottom: "8px" }}>
          DATA PULSE
        </h2>

        {status === "loading" ? (
          <div style={{ marginTop: "32px" }}>
            {/* Spinner */}
            <div style={{ display: "flex", justifyContent: "center", marginBottom: "24px" }}>
              <div style={{
                width: "48px", height: "48px",
                border: "3px solid rgba(99,102,241,0.2)",
                borderTopColor: "var(--primary-500)",
                borderRadius: "50%",
                animation: "spin 0.8s linear infinite",
                boxShadow: "0 0 20px rgba(99,102,241,0.3)"
              }} />
            </div>
            <p style={{ color: "var(--text-main)", fontSize: "16px", fontWeight: 600, marginBottom: "8px" }}>
              {isRecovery ? "Verifying Reset Link…" : "Completing Sign-In…"}
            </p>
            <p className="caption" style={{ color: "var(--text-muted)" }}>
              {isRecovery ? "Preparing your password reset session" : "Securely connecting your account"}
            </p>
            {/* Progress dots */}
            <div style={{ display: "flex", justifyContent: "center", gap: "6px", marginTop: "24px" }}>
              {[0, 1, 2].map(i => (
                <div key={i} style={{
                  width: "6px", height: "6px", borderRadius: "50%",
                  backgroundColor: "var(--primary-500)",
                  opacity: 0.4,
                  animation: `pulse 1.2s ease-in-out ${i * 0.2}s infinite`
                }} />
              ))}
            </div>
          </div>
        ) : (
          <div style={{ marginTop: "32px" }}>
            <div style={{
              padding: "16px 20px", borderRadius: "12px",
              backgroundColor: "rgba(245,158,11,0.08)",
              border: "1px solid rgba(245,158,11,0.25)",
              color: "#fbbf24",
              fontSize: "14px", lineHeight: 1.6
            }}>
              <div style={{ fontSize: "24px", marginBottom: "8px" }}>⚠️</div>
              {errorMsg}
            </div>
            <p className="caption" style={{ marginTop: "16px", color: "var(--text-muted)" }}>
              Redirecting to login…
            </p>
          </div>
        )}
      </div>

      <style>{`
        @keyframes spin { to { transform: rotate(360deg); } }
        @keyframes pulse {
          0%, 100% { opacity: 0.2; transform: scale(0.8); }
          50% { opacity: 1; transform: scale(1.2); }
        }
      `}</style>
    </div>
  );
}
