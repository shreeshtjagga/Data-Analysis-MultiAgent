import { useEffect, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { GoogleLogin } from '@react-oauth/google';
import { apiLogin, apiRegister, apiGoogleLogin, apiForgotPassword, apiResetPassword } from "./api.js";
import ParticleBackground from "./ParticleBackground.jsx";

const GOOGLE_CLIENT_ID = import.meta.env.VITE_GOOGLE_CLIENT_ID;

/**
 * Login.jsx — DataPulse auth page (Premium Dark SaaS System)
 */

function PasswordInput({ id, placeholder, value, onChange, onKeyDown }) {
  const [show, setShow] = useState(false);
  return (
    <div style={{ position: "relative" }}>
      <input
        id={id}
        className="input-field"
        style={{ width: "100%", paddingRight: "55px", fontFamily: "'Outfit', monospace", backgroundColor: '#f1f5f9', color: '#0f172a' }}
        type={show ? "text" : "password"}
        placeholder={placeholder}
        value={value}
        onChange={onChange}
        onKeyDown={onKeyDown}
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
      >
        {show ? "Hide" : "Show"}
      </button>
    </div>
  );
}

function GoogleAuthComponent({ onLogin, setError, setLoading, setTab }) {
  if (!GOOGLE_CLIENT_ID) {
    return null;
  }

  return (
    <>
      <div style={{ display: 'flex', alignItems: 'center', gap: '16px', margin: '24px 0', color: 'var(--border-subtle)' }}>
        <div style={{ flex: 1, height: '1px', backgroundColor: 'var(--border-subtle)' }} />
        <span style={{ fontSize: '13px', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>or</span>
        <div style={{ flex: 1, height: '1px', backgroundColor: 'var(--border-subtle)' }} />
      </div>
      <div style={{ marginBottom: '24px', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '16px' }}>
        <div style={{
          borderRadius: "9px", 
          boxShadow: '0 0 35px rgba(99,102,241,0.6)',
          border: '1px solid rgba(99,102,241,0.4)',
          padding: '2px', 
          backgroundColor: '#ffffff',
          transition: 'transform 0.2s ease, box-shadow 0.2s ease'
        }}>
          <GoogleLogin
            onSuccess={async (credentialResponse) => {
              setError("");
              setLoading(true);
              try {
                const data = await apiGoogleLogin(credentialResponse.credential, GOOGLE_CLIENT_ID);
                onLogin(data.user, data.access_token);
              } catch (err) {
                setError(err.message || "Google Single Sign-On failed.");
              } finally {
                setLoading(false);
              }
            }}
            onError={() => {
              setError("Google initialization failed.");
              setTab("login");
            }}
            size="large"
            width="400"
            text="continue_with"
          />
        </div>
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
          style={{ width: "100%", backgroundColor: '#f1f5f9', color: '#0f172a' }}
          type="email"
          placeholder="you@example.com"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && submit()}
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
      
      <GoogleAuthComponent onLogin={onLogin} setError={setError} setLoading={setLoading} setTab={() => {}} />
    </div>
  );
}

function ForgotPasswordForm({ onBackToLogin }) {
  const [email, setEmail] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const [debugToken, setDebugToken] = useState("");

  const submit = async () => {
    setError("");
    setSuccess("");
    setDebugToken("");
    if (!email) {
      setError("Please enter your email");
      return;
    }
    setLoading(true);
    try {
      const data = await apiForgotPassword(email);
      setSuccess(data?.message || "If an account exists for that email, a reset link has been sent.");
      if (data?.debug_reset_token) {
        setDebugToken(data.debug_reset_token);
      }
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
      {!!debugToken && (
        <div style={{ padding: '10px', borderRadius: '8px', backgroundColor: 'rgba(99,102,241,0.1)', border: '1px solid rgba(99,102,241,0.3)', color: '#c7d2fe', fontSize: '12px', wordBreak: 'break-all' }}>
          Dev token: {debugToken}
        </div>
      )}
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

function ResetPasswordForm({ token, onBackToLogin }) {
  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");

  const submit = async () => {
    setError("");
    setSuccess("");
    if (!token) {
      setError("Reset token is missing. Please open the link from your email.");
      return;
    }
    if (!password || !confirm) {
      setError("Please fill in all fields");
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
      const data = await apiResetPassword(token, password);
      setSuccess(data?.message || "Password reset successful.");
    } catch (err) {
      setError(err.message || "Could not reset password");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex-col gap-16">
      {error && <div style={{ padding: '12px', borderRadius: '8px', backgroundColor: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)', color: '#fca5a5', fontSize: '14px' }}>{error}</div>}
      {success && <div style={{ padding: '12px', borderRadius: '8px', backgroundColor: 'rgba(16,185,129,0.1)', border: '1px solid rgba(16,185,129,0.3)', color: '#6ee7b7', fontSize: '14px' }}>{success}</div>}
      <div className="flex-col gap-8">
        <label style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>New Password</label>
        <PasswordInput id="reset-password" placeholder="••••••••" value={password} onChange={(e) => setPassword(e.target.value)} />
      </div>
      <div className="flex-col gap-8">
        <label style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Confirm Password</label>
        <PasswordInput id="reset-confirm" placeholder="••••••••" value={confirm} onChange={(e) => setConfirm(e.target.value)} onKeyDown={(e) => e.key === "Enter" && submit()} />
      </div>
      <button className="btn-primary" style={{ width: "100%" }} onClick={submit} disabled={loading}>
        {loading ? "UPDATING…" : "RESET PASSWORD"}
      </button>
      <button type="button" onClick={onBackToLogin} style={{ background: 'none', border: 'none', color: 'var(--primary-500)', cursor: 'pointer', fontSize: '13px', textAlign: 'left', padding: 0 }}>
        Back to Login
      </button>
    </div>
  );
}

function RegisterForm({ onLogin, setTab }) {
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
      setError(err.message || "Registration failed");
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
        <input id="reg-name" className="input-field" style={{ width: "100%", backgroundColor: '#f1f5f9', color: '#0f172a' }} type="text" placeholder="johndoe123" value={name} onChange={(e) => setName(e.target.value)} />
      </div>

      <div className="flex-col gap-8">
        <label style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Email address</label>
        <input id="reg-email" className="input-field" style={{ width: "100%", backgroundColor: '#f1f5f9', color: '#0f172a' }} type="email" placeholder="you@example.com" value={email} onChange={(e) => setEmail(e.target.value)} />
      </div>

      <div className="flex-col gap-8">
        <label style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Secure Password</label>
        <PasswordInput id="reg-password" placeholder="••••••••" value={password} onChange={(e) => setPassword(e.target.value)} />
      </div>

      <div className="flex-col gap-8">
        <label style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Confirm Password</label>
        <PasswordInput id="reg-confirm" placeholder="••••••••" value={confirm} onChange={(e) => setConfirm(e.target.value)} onKeyDown={(e) => e.key === "Enter" && submit()} />
      </div>

      <button id="reg-submit" className="btn-primary" style={{ width: "100%", marginTop: '16px' }} onClick={submit} disabled={loading}>
        {loading ? "REGISTERING…" : "REGISTER"}
      </button>

      <GoogleAuthComponent onLogin={onLogin} setError={setError} setLoading={setLoading} setTab={setTab} />
    </div>
  );
}

export default function Login({ onLogin }) {
  const location = useLocation();
  const navigate = useNavigate();
  const params = new URLSearchParams(location.search);
  const resetToken = params.get("token") || "";
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
            style={{ 
              flex: 1, padding: '12px 0', background: 'none', border: 'none', 
              borderBottom: tab === 'register' ? '3px solid var(--primary-500)' : '2px solid transparent',
              color: tab === 'register' ? 'var(--text-main)' : 'var(--text-muted)',
              textShadow: tab === 'register' ? '0 0 10px rgba(99,102,241,0.5)' : 'none',
              fontWeight: 700,
              cursor: 'pointer', fontSize: '13px', textTransform: 'uppercase', letterSpacing: '0.08em'
            }} 
            onClick={() => { setTab("register"); navigate("/register", { replace: true }); }}
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
        {tab === "reset" && <ResetPasswordForm token={resetToken} onBackToLogin={goToLogin} />}
      </div>
    </div>
  );
}
