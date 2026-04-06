import { useState } from "react";
import { apiLogin, apiRegister } from "./api.js";

/**
 * Login.jsx
 * ─────────
 * Renders the login / registration page.
 * On successful login, calls props.onLogin(user, token) which is handled
 * by App.jsx — it stores the token in the api.js module and navigates to /.
 *
 * Styling matches the existing DataPulse dark-navy aesthetic:
 *   • Background:  #060912
 *   • Accent:      #6366f1 (indigo)
 *   • Font stack:  Outfit + Syne (loaded via index.html)
 */

const s = {
  page: {
    minHeight: "100vh",
    background: "#060912",
    backgroundImage:
      "radial-gradient(ellipse at 20% 15%, rgba(99,102,241,0.10) 0%, transparent 55%)",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    fontFamily: "'Outfit', sans-serif",
    padding: "24px",
  },
  card: {
    width: "100%",
    maxWidth: "420px",
    background: "#0d1220",
    border: "1px solid rgba(99,102,241,0.2)",
    borderRadius: "14px",
    padding: "48px 36px 40px",
    boxShadow: "0 24px 60px rgba(0,0,0,0.5)",
  },
  logo: {
    textAlign: "center",
    fontSize: "2.6rem",
    color: "#6366f1",
    marginBottom: "14px",
  },
  title: {
    fontFamily: "'Syne', sans-serif",
    fontSize: "1.6rem",
    fontWeight: 800,
    color: "#f1f5f9",
    textAlign: "center",
    marginBottom: "4px",
    letterSpacing: "-0.02em",
  },
  subtitle: {
    fontSize: "0.82rem",
    color: "#475569",
    textAlign: "center",
    marginBottom: "32px",
    letterSpacing: "0.05em",
  },
  tabRow: {
    display: "flex",
    borderBottom: "1px solid rgba(99,102,241,0.15)",
    marginBottom: "28px",
  },
  tab: (active) => ({
    flex: 1,
    padding: "10px 0",
    fontSize: "0.78rem",
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
    fontSize: "0.72rem",
    fontWeight: 600,
    letterSpacing: "0.1em",
    textTransform: "uppercase",
    color: "#94a3b8",
    marginBottom: "7px",
  },
  input: {
    width: "100%",
    background: "#121929",
    border: "1px solid rgba(99,102,241,0.2)",
    borderRadius: "8px",
    color: "#e2e8f0",
    padding: "11px 14px",
    fontSize: "0.92rem",
    fontFamily: "'Outfit', sans-serif",
    outline: "none",
    boxSizing: "border-box",
    marginBottom: "18px",
    transition: "border-color 0.15s",
  },
  btn: {
    width: "100%",
    background: "linear-gradient(135deg,#6366f1,#4f46e5)",
    color: "#fff",
    border: "none",
    borderRadius: "8px",
    padding: "12px",
    fontFamily: "'Syne', sans-serif",
    fontWeight: 700,
    fontSize: "0.85rem",
    letterSpacing: "0.08em",
    textTransform: "uppercase",
    cursor: "pointer",
    marginTop: "6px",
    transition: "opacity 0.15s",
  },
  alert: (type) => ({
    padding: "10px 14px",
    borderRadius: "7px",
    fontSize: "0.82rem",
    marginBottom: "18px",
    background: type === "error" ? "rgba(239,68,68,0.1)" : "rgba(16,185,129,0.1)",
    border: `1px solid ${type === "error" ? "rgba(239,68,68,0.3)" : "rgba(16,185,129,0.3)"}`,
    color: type === "error" ? "#fca5a5" : "#6ee7b7",
  }),
};

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
        style={s.input}
        type="email"
        placeholder="you@example.com"
        value={email}
        onChange={(e) => setEmail(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && submit()}
      />
      <label style={s.label}>Password</label>
      <input
        style={s.input}
        type="password"
        placeholder="••••••••"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && submit()}
      />
      <button style={{ ...s.btn, opacity: loading ? 0.65 : 1 }} onClick={submit} disabled={loading}>
        {loading ? "Signing in…" : "Sign in"}
      </button>
    </>
  );
}

function RegisterForm() {
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
      await apiRegister(email, password);
      setSuccess("Account created! Switch to the Sign In tab to log in.");
      setEmail(""); setPassword(""); setConfirm("");
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
      <label style={s.label}>Email address</label>
      <input style={s.input} type="email" placeholder="you@example.com" value={email} onChange={(e) => setEmail(e.target.value)} />
      <label style={s.label}>Password</label>
      <input style={s.input} type="password" placeholder="••••••••" value={password} onChange={(e) => setPassword(e.target.value)} />
      <label style={s.label}>Confirm password</label>
      <input
        style={s.input}
        type="password"
        placeholder="••••••••"
        value={confirm}
        onChange={(e) => setConfirm(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && submit()}
      />
      <button style={{ ...s.btn, opacity: loading ? 0.65 : 1 }} onClick={submit} disabled={loading}>
        {loading ? "Creating account…" : "Create account"}
      </button>
    </>
  );
}

export default function Login({ onLogin }) {
  const [tab, setTab] = useState("login");

  return (
    <div style={s.page}>
      <div style={s.card}>
        <div style={s.logo}>◈</div>
        <div style={s.title}>DataPulse</div>
        <div style={s.subtitle}>AI-POWERED DATA ANALYSIS</div>

        <div style={s.tabRow}>
          <button style={s.tab(tab === "login")}   onClick={() => setTab("login")}>Sign in</button>
          <button style={s.tab(tab === "register")} onClick={() => setTab("register")}>Register</button>
        </div>

        {tab === "login"
          ? <LoginForm onLogin={onLogin} />
          : <RegisterForm />}
      </div>
    </div>
  );
}