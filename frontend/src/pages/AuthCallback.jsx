import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { supabase } from "../supabaseClient";
import { apiSyncSession } from "../api";
import ParticleBackground from "../components/ParticleBackground";

export default function AuthCallback({ onLogin }) {
  const navigate = useNavigate();
  const [error, setError] = useState("");
  const [isRecovery, setIsRecovery] = useState(false);
  const handledRef = useRef(false); // prevent double-fire

  useEffect(() => {
    if (handledRef.current) return;
    handledRef.current = true;

    const handleCallback = async () => {
      // Check for OAuth errors in query string
      const params = new URLSearchParams(window.location.search);
      const urlError = params.get("error");
      const urlErrorDesc = params.get("error_description");
      if (urlError) {
        setError(
          `OAuth Error: ${urlErrorDesc || urlError}. Please check your Supabase Google provider configuration.`
        );
        return;
      }

      // Supabase may have already parsed the hash and established a session
      try {
        const {
          data: { session },
          error: sessionError,
        } = await supabase.auth.getSession();

        if (sessionError) throw sessionError;

        if (session) {
          // Check for password recovery in BOTH hash (implicit flow) and
          // query string (PKCE flow — Supabase v2 default)
          const inHash   = window.location.hash.includes("type=recovery");
          const inSearch = window.location.search.includes("type=recovery");
          if (inHash || inSearch) {
            setIsRecovery(true);
            navigate("/reset-password", { replace: true });
            return;
          }
          // Normal OAuth sign-in — sync with our backend
          const data = await apiSyncSession(
            session.access_token,
            session.refresh_token
          );
          onLogin(data.user, data.access_token);
          navigate("/", { replace: true });
          return;
        }

        // No session yet — listen for the auth state change event
        const { data: listener } = supabase.auth.onAuthStateChange(
          async (event, newSession) => {
            if (event === "PASSWORD_RECOVERY") {
              // Supabase recovery link — redirect to reset form; session is live
              listener.subscription.unsubscribe();
              setIsRecovery(true);
              navigate("/reset-password", { replace: true });
              return;
            }

            if (event === "SIGNED_IN" && newSession) {
              listener.subscription.unsubscribe();
              try {
                const data = await apiSyncSession(
                  newSession.access_token,
                  newSession.refresh_token
                );
                onLogin(data.user, data.access_token);
                navigate("/", { replace: true });
              } catch (err) {
                setError(
                  err.message || "Failed to synchronize session with server."
                );
              }
            }
          }
        );

        // Safety timeout — if nothing fires in 6 s, show error
        const timeout = setTimeout(() => {
          listener.subscription.unsubscribe();
          setError("Authentication timed out. No session received.");
          setTimeout(() => navigate("/login", { replace: true }), 2500);
        }, 6000);

        return () => {
          clearTimeout(timeout);
          listener.subscription.unsubscribe();
        };
      } catch (err) {
        setError(err.message || "Authentication failed.");
      }
    };

    handleCallback();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div
      style={{
        height: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        position: "relative",
      }}
    >
      <ParticleBackground />
      <div
        style={{
          position: "relative",
          zIndex: 1,
          textAlign: "center",
          color: "var(--text-main)",
        }}
      >
        {error ? (
          <div
            style={{
              padding: "16px 24px",
              borderRadius: "10px",
              backgroundColor: "rgba(239,68,68,0.1)",
              border: "1px solid rgba(239,68,68,0.3)",
              color: "#fca5a5",
              maxWidth: "420px",
            }}
          >
            <h3 style={{ marginBottom: "8px" }}>Authentication Error</h3>
            <p>{error}</p>
          </div>
        ) : (
          <div className="flex-col gap-16" style={{ alignItems: "center" }}>
            <div
              style={{
                width: "40px",
                height: "40px",
                border: "3px solid rgba(99,102,241,0.3)",
                borderTopColor: "var(--primary-500)",
                borderRadius: "50%",
                animation: "spin 1s linear infinite",
              }}
            />
            <h2>{isRecovery ? "Verifying Reset Link…" : "Completing Login…"}</h2>
            <p className="caption">{isRecovery ? "Preparing your password reset session" : "Synchronizing your secure session"}</p>
          </div>
        )}
      </div>
    </div>
  );
}
