import { useState } from "react";
import { GoogleLogin } from '@react-oauth/google';
import { apiLogin, apiRegister, apiGoogleLogin } from "./api.js";

/**
 * Login.jsx — DataPulse auth page
 * Design: dark-navy + indigo accent (unchanged)
 * Changes: card wider (500px), inputs/fonts slightly larger, OAuth placeholder added
 */

const s = {
  page: {
    minHeight: "100vh",
    background: "#060912",
    backgroundImage:
      "radial-gradient(ellipse at 20% 15%, rgba(99,102,241,0.12) 0%, transparent 55%), radial-gradient(ellipse at 80% 85%, rgba(99,102,241,0.06) 0%, transparent 45%)",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    fontFamily: "'Outfit', sans-serif",
    padding: "24px",
  },
  card: {
    width: "100%",
    maxWidth: "500px",          // was 420px
    background: "#0d1220",
    border: "1px solid rgba(99,102,241,0.22)",
    borderRadius: "16px",
    padding: "56px 48px 50px",  // was 48px 36px 40px
    boxShadow: "0 28px 70px rgba(0,0,0,0.55), 0 0 0 1px rgba(99,102,241,0.08)",
  },
  logo: {
    textAlign: "center",
    fontSize: "3rem",           // was 2.6rem
    color: "#6366f1",
    marginBottom: "16px",
  },
  title: {
    fontFamily: "'Syne', sans-serif",
    fontSize: "1.9rem",         // was 1.6rem
    fontWeight: 800,
    color: "#f1f5f9",
    textAlign: "center",
    marginBottom: "4px",
    letterSpacing: "-0.02em",
  },
  subtitle: {
    fontSize: "0.84rem",
    color: "#475569",
    textAlign: "center",
    marginBottom: "34px",
    letterSpacing: "0.06em",
  },
  tabRow: {
    display: "flex",
    borderBottom: "1px solid rgba(99,102,241,0.15)",
    marginBottom: "30px",
  },
  tab: (active) => ({
    flex: 1,
    padding: "11px 0",
    fontSize: "0.80rem",
    fontWeight: active ? 700 : 500,
    letterSpacing: "0.08em",
    textTransform: "uppercase",
    color: active ? "#6366f1" : "#475569",
    background: "transparent",
    border: "none",
    borderBottom: active ? "2px solid #6366f1" : "2px solid transparent",
    cursor: "pointer",
    transition: "color 0.15s",
  }),
  label: {
    display: "block",
    fontSize: "0.74rem",
    fontWeight: 600,
    letterSpacing: "0.1em",
    textTransform: "uppercase",
    color: "#94a3b8",
    marginBottom: "8px",
  },
  input: {
    width: "100%",
    background: "#121929",
    border: "1px solid rgba(99,102,241,0.22)",
    borderRadius: "9px",
    color: "#e2e8f0",
    padding: "13px 16px",       // was 11px 14px
    fontSize: "1rem",           // was 0.92rem
    fontFamily: "'Outfit', sans-serif",
    outline: "none",
    boxSizing: "border-box",
    marginBottom: "20px",
    transition: "border-color 0.15s",
  },
  btn: {
    width: "100%",
    background: "linear-gradient(135deg,#6366f1,#4f46e5)",
    color: "#fff",
    border: "none",
    borderRadius: "9px",
    padding: "14px",            // was 12px
    fontFamily: "'Syne', sans-serif",
    fontWeight: 700,
    fontSize: "0.88rem",
    letterSpacing: "0.08em",
    textTransform: "uppercase",
    cursor: "pointer",
    marginTop: "6px",
    transition: "opacity 0.15s, transform 0.1s",
  },
  divider: {
    display: "flex",
    alignItems: "center",
    gap: "12px",
    margin: "22px 0",
    color: "#334155",
    fontSize: "0.78rem",
    letterSpacing: "0.06em",
  },
  dividerLine: {
    flex: 1,
    height: "1px",
    background: "rgba(99,102,241,0.12)",
  },
  oauthBtn: {
    width: "100%",
    background: "transparent",
    color: "#475569",
    border: "1px solid rgba(99,102,241,0.15)",
    borderRadius: "9px",
    padding: "13px 16px",
    fontFamily: "'Outfit', sans-serif",
    fontWeight: 500,
    fontSize: "0.92rem",
    cursor: "not-allowed",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    gap: "10px",
    opacity: 0.55,
    position: "relative",
  },
  oauthBadge: {
    fontSize: "0.65rem",
    background: "rgba(99,102,241,0.15)",
    color: "#6366f1",
    padding: "2px 7px",
    borderRadius: "4px",
    letterSpacing: "0.06em",
    textTransform: "uppercase",
    fontWeight: 600,
  },
  alert: (type) => ({
    padding: "11px 15px",
    borderRadius: "8px",
    fontSize: "0.84rem",
    marginBottom: "20px",
    background: type === "error" ? "rgba(239,68,68,0.1)" : "rgba(16,185,129,0.1)",
    border: `1px solid ${type === "error" ? "rgba(239,68,68,0.3)" : "rgba(16,185,129,0.3)"}`,
    color: type === "error" ? "#fca5a5" : "#6ee7b7",
  }),
};

/** Function passing Google credentials to the backend */
function GoogleAuthComponent({ onLogin, setError, setLoading }) {
  return (
    <>
      <div style={s.divider}>
        <div style={s.dividerLine} />
        <span>or</span>
        <div style={s.dividerLine} />
      </div>
      <div style={{ display: "flex", justifyContent: "center", width: "100%", overflow: "hidden", borderRadius: "9px" }}>
        <GoogleLogin
          onSuccess={async (credentialResponse) => {
            setError("");
            setLoading(true);
            try {
              const data = await apiGoogleLogin(credentialResponse.credential);
              onLogin(data.user, data.access_token);
            } catch (err) {
              setError(err.message || "Google Single Sign-On failed.");
            } finally {
              setLoading(false);
            }
          }}
          onError={() => {
            setError("Google initialization failed.");
          }}
          theme="filled_black"
          size="large"
          width="100%"
          text="continue_with"
        />
      </div>
    </>
  );
}

function LoginForm({ onLogin }) {
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
    <>
      {error && <div style={s.alert("error")}>{error}</div>}
      <label style={s.label}>Email address</label>
      <input
        id="login-email"
        style={s.input}
        type="email"
        placeholder="you@example.com"
        value={email}
        onChange={(e) => setEmail(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && submit()}
      />
      <label style={s.label}>Password</label>
      <input
        id="login-password"
        style={s.input}
        type="password"
        placeholder="••••••••"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && submit()}
      />
      <button id="login-submit" style={{ ...s.btn, opacity: loading ? 0.65 : 1 }} onClick={submit} disabled={loading}>
        {loading ? "Signing in…" : "Sign in"}
      </button>
      <GoogleAuthComponent onLogin={onLogin} setError={setError} setLoading={setLoading} />
    </>
  );
}

function RegisterForm() {
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
      setSuccess("Account created! Switch to the Sign In tab to log in.");
      setName(""); setEmail(""); setPassword(""); setConfirm("");
    } catch (err) {
      setError(err.message || "Registration failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <>
      {error   && <div style={s.alert("error")}>{error}</div>}
      {success && <div style={s.alert("success")}>{success}</div>}
      <label style={s.label}>Full Name</label>
      <input id="reg-name" style={s.input} type="text" placeholder="John Doe" value={name} onChange={(e) => setName(e.target.value)} />
      <label style={s.label}>Email address</label>
      <input id="reg-email" style={s.input} type="email" placeholder="you@example.com" value={email} onChange={(e) => setEmail(e.target.value)} />
      <label style={s.label}>Password</label>
      <input id="reg-password" style={s.input} type="password" placeholder="••••••••" value={password} onChange={(e) => setPassword(e.target.value)} />
      <label style={s.label}>Confirm password</label>
      <input
        id="reg-confirm"
        style={s.input}
        type="password"
        placeholder="••••••••"
        value={confirm}
        onChange={(e) => setConfirm(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && submit()}
      />
      <button id="reg-submit" style={{ ...s.btn, opacity: loading ? 0.65 : 1 }} onClick={submit} disabled={loading}>
        {loading ? "Creating account…" : "Create account"}
      </button>
      <GoogleAuthComponent onLogin={(u, t) => {}} setError={setError} setLoading={setLoading} />
    </>
  );
}

export default function Login({ onLogin }) {
  const [tab, setTab] = useState("login");

  return (
    <div style={s.page}>
      <div style={s.card}>
        <div style={s.logo}>◈</div>
        <div style={s.title}>Data Pulse</div>
        <div style={s.subtitle}>AI-POWERED DATA ANALYSIS</div>

        <div style={s.tabRow}>
          <button id="tab-signin"   style={s.tab(tab === "login")}    onClick={() => setTab("login")}>Sign in</button>
          <button id="tab-register" style={s.tab(tab === "register")} onClick={() => setTab("register")}>Register</button>
        </div>

        {tab === "login"
          ? <LoginForm onLogin={onLogin} />
          : <RegisterForm />}
      </div>
    </div>
  );
}
