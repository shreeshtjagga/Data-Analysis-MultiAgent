import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { supabase } from "../supabaseClient";
import { apiSyncSession } from "../api";
import ParticleBackground from "../components/ParticleBackground";

export default function AuthCallback({ onLogin }) {
  const navigate = useNavigate();
  const [error, setError] = useState("");

  useEffect(() => {
    const handleCallback = async () => {
      const params = new URLSearchParams(window.location.search);
      const urlError = params.get('error');
      const urlErrorDesc = params.get('error_description');
      
      if (urlError) {
        setError(`OAuth Error: ${urlErrorDesc || urlError}. Please check your Supabase Google provider configuration.`);
        return;
      }
      
      try {
        const { data: { session }, error: sessionError } = await supabase.auth.getSession();
        
        if (sessionError) throw sessionError;
        
        if (session) {
          const data = await apiSyncSession(session.access_token, session.refresh_token);
          onLogin(data.user, data.access_token);
          navigate("/", { replace: true });
        } else {
          const { data: listener } = supabase.auth.onAuthStateChange(async (event, newSession) => {
            if (event === 'SIGNED_IN' && newSession) {
              try {
                const data = await apiSyncSession(newSession.access_token, newSession.refresh_token);
                onLogin(data.user, data.access_token);
                navigate("/", { replace: true });
              } catch (err) {
                setError(err.message || "Failed to synchronize session with server.");
              }
            }
          });
          
          setTimeout(() => {
            if (!error) {
              setError("Authentication failed. No session found.");
              setTimeout(() => navigate("/login", { replace: true }), 2000);
            }
          }, 3000);
          
          return () => listener.subscription.unsubscribe();
        }
      } catch (err) {
        setError(err.message || "Authentication failed.");
      }
    };

    handleCallback();
  }, [navigate, onLogin, error]);

  return (
    <div style={{ height: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
      <ParticleBackground />
      <div style={{ position: 'relative', zIndex: 1, textAlign: 'center', color: 'var(--text-main)' }}>
        {error ? (
          <div style={{ padding: '16px', borderRadius: '8px', backgroundColor: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)', color: '#fca5a5' }}>
            <h3 style={{ marginBottom: '8px' }}>Authentication Error</h3>
            <p>{error}</p>
          </div>
        ) : (
          <div className="flex-col gap-16" style={{ alignItems: 'center' }}>
            <div style={{ width: '40px', height: '40px', border: '3px solid rgba(99,102,241,0.3)', borderTopColor: 'var(--primary-500)', borderRadius: '50%', animation: 'spin 1s linear infinite' }} />
            <h2>Completing Login...</h2>
            <p className="caption">Synchronizing your secure session</p>
          </div>
        )}
      </div>
    </div>
  );
}
