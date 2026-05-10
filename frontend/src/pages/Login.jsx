import { useEffect, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { apiLogin, apiRegister, apiForgotPassword } from "../api.js";
import ParticleBackground from "../components/ParticleBackground.jsx";
import { supabase } from "../api.js";
function PasswordInput({ id, placeholder, value, onChange, onKeyDown, disabled, autoComplete }) {
  const [show, setShow] = useState(false);
  return (
    <div style={{ position: "relative", opacity: disabled ? 0.6 : 1 }}>
      <input
        id={id}
        className="input-field"
        style={{ width: "100%", paddingRight: "55px", fontFamily: "'Outfit', monospace", backgroundColor: '#f1f5f9', color: '#0f172a' }}
        type={show ? "text" : "password"}
        placeholder={placeholder}
        value={value}
        onChange={onChange}
        onKeyDown={onKeyDown}
        disabled={disabled}
        autoComplete={autoComplete}
      />
      <button
        type="button"
        tabIndex="-1"
        onClick={(e) => { e.preventDefault(); setShow(!show); }}
        style={{
          position: "absolute",
          right: "12px",
          top: "50%",
          transform: "translateY(-50%)",
          background: "none",
          border: "none",
          color: "var(--primary-500)",
          cursor: "pointer",
          fontSize: "11px",
          fontWeight: 700,
          textTransform: "uppercase",
          letterSpacing: "0.05em",
          padding: "4px"
        }}
        disabled={disabled}
      >
        {show ? "Hide" : "Show"}
      </button>
    </div>
  );
}
function GoogleAuthComponent({ loading, setError }) {
  const handleGoogleLogin = async () => {
    setError("");
    try {
      const { error } = await supabase.auth.signInWithOAuth({
        provider: 'google',
        options: {
          redirectTo: `${window.location.origin}/auth/callback`
        }
      });
      if (error) throw error;
    } catch (err) {
      setError(err.message || "Google Single Sign-On failed.");
    }
  };
  return (
    <>
      <div style={{ display: 'flex', alignItems: 'center', gap: '16px', margin: '24px 0', color: 'var(--border-subtle)' }}>
        <div style={{ flex: 1, height: '1px', backgroundColor: 'var(--border-subtle)' }} />
        <span style={{ fontSize: '13px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>or</span>
        <div style={{ flex: 1, height: '1px', backgroundColor: 'var(--border-subtle)' }} />
      </div>
      <div style={{ marginBottom: '24px', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '16px' }}>
        <button
          type="button"
          onClick={handleGoogleLogin}
          disabled={loading}
          style={{
            width: "100%",
            borderRadius: "9px", 
            boxShadow: '0 0 35px rgba(99,102,241,0.6)',
            border: '1px solid rgba(99,102,241,0.4)',
            padding: '12px', 
            backgroundColor: '#ffffff',
            color: '#334155',
            fontWeight: 600,
            fontSize: '14px',
            transition: 'transform 0.2s ease, box-shadow 0.2s ease',
            pointerEvents: loading ? 'none' : 'auto',
            opacity: loading ? 0.6 : 1,
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: '12px'
          }}
        >
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z" fill="#4285F4" />
            <path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853" />
            <path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z" fill="#FBBC05" />
            <path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335" />
          </svg>
          Continue with Google
        </button>
      </div>
    </>
  );
}
function LoginForm({ onLogin, onForgot }) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const submit = async () => {
    setError("");
    if (!email || !password) { setError("Please fill in both fields"); return; }
    setLoading(true);
    try {
      const data = await apiLogin(email, password);
      onLogin(data.user, data.access_token);
    } catch (err) {
      setError(err.message || "Login failed");
    } finally {
      setLoading(false);
    }
  };
  return (
    <div className="flex-col gap-16">
      {error && <div style={{ padding: '12px', borderRadius: '8px', backgroundColor: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)', color: '#fca5a5', fontSize: '14px' }}>{error}</div>}
      <div className="flex-col gap-8">
        <label style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Email address</label>
        <input
          id="login-email"
          className="input-field"
          style={{ width: "100%", backgroundColor: '#f1f5f9', color: '#0f172a', opacity: loading ? 0.6 : 1 }}
          type="email"
          placeholder="you@example.com"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && submit()}
          disabled={loading}
          autoComplete="username"
        />
      </div>
      <div className="flex-col gap-12">
        <label style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Password</label>
        <PasswordInput
          id="login-password"
          placeholder="••••••••"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && submit()}
          disabled={loading}
          autoComplete="current-password"
        />
      </div>
      <button id="login-submit" className="btn-primary" style={{ width: "100%", marginTop: '24px' }} onClick={submit} disabled={loading}>
        {loading ? "LOGGING IN…" : "LOGIN"}
      </button>
      <button
        type="button"
        onClick={onForgot}
        style={{ background: 'none', border: 'none', color: 'var(--primary-500)', cursor: 'pointer', fontSize: '13px', textAlign: 'left', padding: 0 }}
      >
        Forgot your password?
      </button>
      <GoogleAuthComponent setError={setError} loading={loading} />
    </div>
  );
}
function ForgotPasswordForm({ onBackToLogin }) {
  const [email, setEmail] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const submit = async () => {
    setError("");
    setSuccess("");
    if (!email) {
      setError("Please enter your email");
      return;
    }
    setLoading(true);
    try {
      const data = await apiForgotPassword(email);
      setSuccess(data?.message || "If an account exists for that email, a reset link has been sent.");
    } catch (err) {
      setError(err.message || "Could not process request");
    } finally {
      setLoading(false);
    }
  };
  return (
    <div className="flex-col gap-16">
      {error && <div style={{ padding: '12px', borderRadius: '8px', backgroundColor: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)', color: '#fca5a5', fontSize: '14px' }}>{error}</div>}
      {success && <div style={{ padding: '12px', borderRadius: '8px', backgroundColor: 'rgba(16,185,129,0.1)', border: '1px solid rgba(16,185,129,0.3)', color: '#6ee7b7', fontSize: '14px' }}>{success}</div>}

      <div className="flex-col gap-8">
        <label style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Account Email</label>
        <input
          className="input-field"
          style={{ width: "100%", backgroundColor: '#f1f5f9', color: '#0f172a' }}
          type="email"
          placeholder="you@example.com"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && submit()}
        />
      </div>
      <button className="btn-primary" style={{ width: "100%" }} onClick={submit} disabled={loading}>
        {loading ? "SENDING…" : "SEND RESET LINK"}
      </button>
      <button type="button" onClick={onBackToLogin} style={{ background: 'none', border: 'none', color: 'var(--primary-500)', cursor: 'pointer', fontSize: '13px', textAlign: 'left', padding: 0 }}>
        Back to Login
      </button>
    </div>
  );
}
function ResetPasswordForm({ onBackToLogin }) {
  const navigate = useNavigate();
  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const [countdown, setCountdown] = useState(null);

  useEffect(() => {
    if (countdown === null) return;
    if (countdown <= 0) {
      navigate("/login", { replace: true });
      return;
    }
    const t = setTimeout(() => setCountdown((c) => c - 1), 1000);
    return () => clearTimeout(t);
  }, [countdown, navigate]);

  const submit = async () => {
    setError("");
    setSuccess("");
    if (!password || !confirm) {
      setError("Please fill in both password fields");
      return;
    }
    if (password !== confirm) {
      setError("Passwords do not match");
      return;
    }
    if (password.length < 6) {
      setError("Password must be at least 6 characters");
      return;
    }
    setLoading(true);
    try {

      const { error: sbError } = await supabase.auth.updateUser({ password });
      if (sbError) throw sbError;

      await supabase.auth.signOut();

      setSuccess("Password updated! Redirecting to login in 3 seconds…");
      setCountdown(3);
    } catch (err) {
      const msg = err?.message || "Could not reset password";
      if (msg.toLowerCase().includes("session") || msg.toLowerCase().includes("expired")) {
        setError("Your reset link has expired. Please request a new one.");
      } else {
        setError(msg);
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex-col gap-16">
      {error && (
        <div style={{ padding: '12px', borderRadius: '8px', backgroundColor: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)', color: '#fca5a5', fontSize: '14px' }}>
          {error}
        </div>
      )}
      {success && (
        <div style={{ padding: '12px', borderRadius: '8px', backgroundColor: 'rgba(16,185,129,0.1)', border: '1px solid rgba(16,185,129,0.3)', color: '#6ee7b7', fontSize: '14px' }}>
          ✓ {success}
        </div>
      )}
      {!success && (
        <>
          <div className="flex-col gap-8">
            <label style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>New Password</label>
            <PasswordInput
              id="reset-password"
              placeholder="••••••••"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              disabled={loading}
              autoComplete="new-password"
            />
          </div>
          <div className="flex-col gap-8">
            <label style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Confirm Password</label>
            <PasswordInput
              id="reset-confirm"
              placeholder="••••••••"
              value={confirm}
              onChange={(e) => setConfirm(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && submit()}
              disabled={loading}
              autoComplete="new-password"
            />
          </div>
          <button
            id="reset-submit"
            className="btn-primary"
            style={{ width: "100%", marginTop: '8px' }}
            onClick={submit}
            disabled={loading}
          >
            {loading ? "UPDATING PASSWORD…" : "SET NEW PASSWORD"}
          </button>
        </>
      )}
      <button
        type="button"
        onClick={onBackToLogin}
        style={{ background: 'none', border: 'none', color: 'var(--primary-500)', cursor: 'pointer', fontSize: '13px', textAlign: 'left', padding: 0 }}
      >
        ← Back to Login
      </button>
    </div>
  );
}
function RegisterForm({ onLogin }) {
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const submit = async () => {
    setError(""); setSuccess("");
    if (!email || !password || !confirm) { setError("Please fill in all fields"); return; }
    if (password !== confirm) { setError("Passwords do not match"); return; }
    if (password.length < 6) { setError("Password must be at least 6 characters"); return; }
    setLoading(true);
    try {
      await apiRegister(email, password, name);
      setSuccess("Account established! Logging you in...");
      const loginData = await apiLogin(email, password);
      onLogin(loginData.user, loginData.access_token);
    } catch (err) {
      const msg = err.message || "Registration failed";
      setError(msg.includes("rate") ? "Registered! Please try logging in manually." : msg);
    } finally {
      setLoading(false);
    }
  };
  return (
    <div className="flex-col gap-16">
      {error && <div style={{ padding: '12px', borderRadius: '8px', backgroundColor: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)', color: '#fca5a5', fontSize: '14px' }}>{error}</div>}
      {success && <div style={{ padding: '12px', borderRadius: '8px', backgroundColor: 'rgba(16,185,129,0.1)', border: '1px solid rgba(16,185,129,0.3)', color: '#6ee7b7', fontSize: '14px' }}>{success}</div>}
      <div className="flex-col gap-8">
        <label style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Username</label>
        <input id="reg-name" className="input-field" style={{ width: "100%", backgroundColor: '#f1f5f9', color: '#0f172a', opacity: loading ? 0.6 : 1 }} type="text" placeholder="johndoe123" value={name} onChange={(e) => setName(e.target.value)} disabled={loading} autoComplete="off" />
      </div>
      <div className="flex-col gap-8">
        <label style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Email address</label>
        <input id="reg-email" className="input-field" style={{ width: "100%", backgroundColor: '#f1f5f9', color: '#0f172a', opacity: loading ? 0.6 : 1 }} type="email" placeholder="you@example.com" value={email} onChange={(e) => setEmail(e.target.value)} disabled={loading} autoComplete="off" />
      </div>
      <div className="flex-col gap-8">
        <label style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Secure Password</label>
        <PasswordInput id="reg-password" placeholder="••••••••" value={password} onChange={(e) => setPassword(e.target.value)} disabled={loading} autoComplete="new-password" />
      </div>
      <div className="flex-col gap-8">
        <label style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Confirm Password</label>
        <PasswordInput id="reg-confirm" placeholder="••••••••" value={confirm} onChange={(e) => setConfirm(e.target.value)} onKeyDown={(e) => e.key === "Enter" && submit()} disabled={loading} autoComplete="new-password" />
      </div>
      <button id="reg-submit" className="btn-primary" style={{ width: "100%", marginTop: '16px' }} onClick={submit} disabled={loading}>
        {loading ? "REGISTERING…" : "REGISTER"}
      </button>
      <GoogleAuthComponent setError={setError} loading={loading} />
    </div>
  );
}
export default function Login({ onLogin }) {
  const location = useLocation();
  const navigate = useNavigate();
  const deriveTabFromPath = (pathname) => {
    if (pathname === "/reset-password") return "reset";
    if (pathname === "/forgot-password") return "forgot";
    if (pathname === "/register") return "register";
    return "login";
  };
  const [tab, setTab] = useState(deriveTabFromPath(location.pathname));
  useEffect(() => {
    const routeTab = deriveTabFromPath(location.pathname);
    if (routeTab !== tab) {
      setTab(routeTab);
    }
  }, [location.pathname, tab]);
  const goToLogin = () => {
    setTab("login");
    navigate("/login", { replace: true });
  };
  const goToForgot = () => {
    setTab("forgot");
    navigate("/forgot-password", { replace: true });
  };
  return (
    <div style={{ height: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '24px', position: 'relative', overflowY: 'auto' }}>
      <ParticleBackground />
      <div className="animate-fade-in" style={{ width: '100%', maxWidth: '550px', position: 'relative', zIndex: 1, backgroundColor: 'transparent', boxShadow: 'none', border: 'none', padding: '0' }}>
        <div style={{ textAlign: 'center', marginBottom: '32px' }}>
          <div style={{ color: 'var(--primary-500)', fontSize: '40px', marginBottom: '8px', textShadow: '0 0 20px rgba(99,102,241,0.7)' }}>◈</div>
          <h2 style={{ fontFamily: "'Syne', sans-serif", fontSize: '28px', color: 'var(--text-main)', textShadow: '0 0 10px rgba(255,255,255,0.1)' }}>DATA PULSE</h2>
          <p className="caption" style={{ textTransform: 'uppercase', letterSpacing: '0.1em', marginTop: '4px', color: 'var(--text-muted)' }}>Secure Analytics Portal</p>
        </div>
        <div style={{ display: 'flex', borderBottom: '1px solid var(--border-subtle)', marginBottom: '32px' }}>
          <button 
            id="tab-login"
            style={{ 
              flex: 1, padding: '12px 0', background: 'none', border: 'none', 
              borderBottom: tab === 'login' ? '3px solid var(--primary-500)' : '2px solid transparent',
              color: tab === 'login' ? 'var(--text-main)' : 'var(--text-muted)',
              textShadow: tab === 'login' ? '0 0 10px rgba(99,102,241,0.5)' : 'none',
              fontWeight: 700,
              cursor: 'pointer', fontSize: '13px', textTransform: 'uppercase', letterSpacing: '0.08em'
            }} 
            onClick={goToLogin}
          >
            Login
          </button>
          <button 
            id="tab-register"
            style={{ 
              flex: 1, padding: '12px 0', background: 'none', border: 'none', 
              borderBottom: tab === 'register' ? '3px solid var(--primary-500)' : '2px solid transparent',
              color: tab === 'register' ? 'var(--text-main)' : 'var(--text-muted)',
              textShadow: tab === 'register' ? '0 0 10px rgba(99,102,241,0.5)' : 'none',
              fontWeight: 700,
              cursor: 'pointer', fontSize: '13px', textTransform: 'uppercase', letterSpacing: '0.08em'
            }} 
            onClick={() => { 
                setTab("register"); 
                navigate("/register", { replace: true }); 
            }}
          >
            Register
          </button>
        </div>
        {tab === "login" && <LoginForm onLogin={onLogin} onForgot={goToForgot} />}
        {tab === "register" && (
          <>
            <RegisterForm onLogin={onLogin} setTab={setTab} />
          </>
        )}
        {tab === "forgot" && <ForgotPasswordForm onBackToLogin={goToLogin} />}
        {tab === "reset" && <ResetPasswordForm onBackToLogin={goToLogin} />}
      </div>
    </div>
  );
}
