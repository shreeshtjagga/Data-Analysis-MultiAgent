import { useEffect, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { apiLogin, apiRegister, apiForgotPassword, supabase } from "../api.js";
import ParticleBackground from "../components/ParticleBackground.jsx";

// ── Shared sub-components ─────────────────────────────────────────────────────

function Alert({ type, children }) {
  if (!children) return null;
  return <div className={`auth-alert ${type}`}>{children}</div>;
}

function Field({ label, children }) {
  return (
    <div className="auth-field">
      <label className="auth-label">{label}</label>
      {children}
    </div>
  );
}

function PasswordInput({ id, placeholder, value, onChange, onKeyDown, disabled, name, autoComplete }) {
  const [show, setShow] = useState(false);
  return (
    <div className="auth-input-wrap">
      <input
        id={id}
        className="auth-input"
        type={show ? "text" : "password"}
        placeholder={placeholder}
        value={value}
        onChange={onChange}
        onKeyDown={onKeyDown}
        disabled={disabled}
        autoComplete={autoComplete || "new-password"}
        name={name}
      />
      <button
        type="button"
        tabIndex={-1}
        className="auth-show-btn"
        onClick={(e) => { e.preventDefault(); setShow(s => !s); }}
        disabled={disabled}
      >
        {show ? "Hide" : "Show"}
      </button>
    </div>
  );
}

function Divider() {
  return (
    <div className="auth-divider">
      <div className="auth-divider-line" />
      <span className="auth-divider-text">or</span>
      <div className="auth-divider-line" />
    </div>
  );
}

function GoogleButton({ loading, setError }) {
  const handleClick = async () => {
    setError("");
    try {
      const { error } = await supabase.auth.signInWithOAuth({
        provider: "google",
        options: { redirectTo: `${window.location.origin}/auth/callback` },
      });
      if (error) throw error;
    } catch (err) {
      setError(err.message || "Google sign-in failed.");
    }
  };
  return (
    <button type="button" className="auth-google-btn" onClick={handleClick} disabled={loading}>
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none">
        <path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z" fill="#4285F4" />
        <path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853" />
        <path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z" fill="#FBBC05" />
        <path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335" />
      </svg>
      Continue with Google
    </button>
  );
}

// ── Forms ─────────────────────────────────────────────────────────────────────

function LoginForm({ onLogin, onForgot }) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const submit = async () => {
    setError("");
    if (!email || !password) { setError("Please fill in both fields."); return; }
    setLoading(true);
    try {
      const data = await apiLogin(email, password);
      onLogin(data.user, data.access_token);
    } catch (err) {
      setError(err.message || "Login failed. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <form className="auth-form" autoComplete="off" onSubmit={(e) => { e.preventDefault(); submit(); }}>
      {/* Honeypot fields */}
      <input type="text" style={{ display: "none" }} tabIndex={-1} autoComplete="off" />
      <input type="password" style={{ display: "none" }} tabIndex={-1} autoComplete="off" />

      <Alert type="error">{error}</Alert>

      <Field label="Email address">
        <input
          id="login-email"
          className="auth-input"
          type="email"
          placeholder="you@example.com"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && submit()}
          disabled={loading}
          autoComplete="off"
        />
      </Field>

      <Field label="Password">
        <PasswordInput
          id="login-password"
          placeholder="••••••••"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && submit()}
          disabled={loading}
        />
      </Field>

      <button id="login-submit" type="submit" className="btn-primary" style={{ width: "100%", height: 46 }} disabled={loading}>
        {loading ? "Signing in…" : "Sign In"}
      </button>

      <button type="button" className="auth-link-btn" onClick={onForgot}>
        Forgot your password?
      </button>

      <Divider />
      <GoogleButton loading={loading} setError={setError} />
    </form>
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
    if (!email || !password || !confirm) { setError("Please fill in all fields."); return; }
    if (password !== confirm) { setError("Passwords do not match."); return; }
    if (password.length < 6) { setError("Password must be at least 6 characters."); return; }
    setLoading(true);
    try {
      await apiRegister(email, password, name);
      setSuccess("Account created! Signing you in…");
      const loginData = await apiLogin(email, password);
      onLogin(loginData.user, loginData.access_token);
    } catch (err) {
      const msg = err.message || "Registration failed.";
      setError(msg.includes("rate") ? "Account created! Please sign in manually." : msg);
    } finally {
      setLoading(false);
    }
  };

  return (
    <form className="auth-form" autoComplete="off" onSubmit={(e) => { e.preventDefault(); submit(); }}>
      {/* Honeypot fields */}
      <input type="text" style={{ display: "none" }} tabIndex={-1} autoComplete="off" />
      <input type="password" style={{ display: "none" }} tabIndex={-1} autoComplete="off" />

      <Alert type="error">{error}</Alert>
      <Alert type="success">{success}</Alert>

      <Field label="Username (optional)">
        <input
          id="reg-name"
          className="auth-input"
          type="text"
          placeholder="johndoe"
          value={name}
          onChange={(e) => setName(e.target.value)}
          disabled={loading}
          autoComplete="off"
        />
      </Field>

      <Field label="Email address">
        <input
          id="reg-email"
          className="auth-input"
          type="email"
          placeholder="you@example.com"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          disabled={loading}
          autoComplete="off"
        />
      </Field>

      <Field label="Password">
        <PasswordInput
          id="reg-password"
          placeholder="••••••••"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          disabled={loading}
        />
      </Field>

      <Field label="Confirm Password">
        <PasswordInput
          id="reg-confirm"
          placeholder="••••••••"
          value={confirm}
          onChange={(e) => setConfirm(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && submit()}
          disabled={loading}
        />
      </Field>

      <button id="reg-submit" type="submit" className="btn-primary" style={{ width: "100%", height: 46 }} disabled={loading}>
        {loading ? "Creating account…" : "Create Account"}
      </button>

      <Divider />
      <GoogleButton loading={loading} setError={setError} />
    </form>
  );
}

function ForgotPasswordForm({ onBackToLogin }) {
  const [email, setEmail] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");

  const submit = async () => {
    setError(""); setSuccess("");
    if (!email) { setError("Please enter your email address."); return; }
    setLoading(true);
    try {
      const data = await apiForgotPassword(email);
      setSuccess(data?.message || "If an account exists, a reset link has been sent.");
    } catch (err) {
      setError(err.message || "Could not process request.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="auth-form">
      <p className="auth-label" style={{ textTransform: "none", fontSize: 13, letterSpacing: 0 }}>
        Enter your email and we'll send you a password reset link.
      </p>

      <Alert type="error">{error}</Alert>
      <Alert type="success">{success}</Alert>

      <Field label="Email address">
        <input
          className="auth-input"
          type="email"
          placeholder="you@example.com"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && submit()}
          disabled={loading}
          autoComplete="off"
        />
      </Field>

      <button className="btn-primary" style={{ width: "100%", height: 46 }} onClick={submit} disabled={loading}>
        {loading ? "Sending…" : "Send Reset Link"}
      </button>

      <button type="button" className="auth-link-btn" onClick={onBackToLogin}>
        ← Back to Sign In
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
    if (countdown <= 0) { navigate("/login", { replace: true }); return; }
    const t = setTimeout(() => setCountdown(c => c - 1), 1000);
    return () => clearTimeout(t);
  }, [countdown, navigate]);

  const submit = async () => {
    setError(""); setSuccess("");
    if (!password || !confirm) { setError("Please fill in both fields."); return; }
    if (password !== confirm) { setError("Passwords do not match."); return; }
    if (password.length < 6) { setError("Password must be at least 6 characters."); return; }
    setLoading(true);
    try {
      const { error: sbError } = await supabase.auth.updateUser({ password });
      if (sbError) throw sbError;
      await supabase.auth.signOut();
      setSuccess("Password updated! Redirecting to sign in…");
      setCountdown(3);
    } catch (err) {
      const msg = err?.message || "Could not reset password.";
      setError(msg.toLowerCase().includes("session") || msg.toLowerCase().includes("expired")
        ? "Your reset link has expired. Please request a new one."
        : msg);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="auth-form">
      <Alert type="error">{error}</Alert>
      <Alert type="success">{success}</Alert>

      {!success && (
        <>
          <Field label="New Password">
            <PasswordInput
              id="reset-password"
              placeholder="••••••••"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              disabled={loading}
              autoComplete="new-password"
            />
          </Field>

          <Field label="Confirm New Password">
            <PasswordInput
              id="reset-confirm"
              placeholder="••••••••"
              value={confirm}
              onChange={(e) => setConfirm(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && submit()}
              disabled={loading}
              autoComplete="new-password"
            />
          </Field>

          <button
            id="reset-submit"
            className="btn-primary"
            style={{ width: "100%", height: 46 }}
            onClick={submit}
            disabled={loading}
          >
            {loading ? "Updating…" : "Set New Password"}
          </button>
        </>
      )}

      <button type="button" className="auth-link-btn" onClick={onBackToLogin}>
        ← Back to Sign In
      </button>
    </div>
  );
}

// ── Main Login Page ───────────────────────────────────────────────────────────

export default function Login({ onLogin, sessionMessage }) {
  const location = useLocation();
  const navigate = useNavigate();

  const deriveTab = (pathname) => {
    if (pathname === "/reset-password") return "reset";
    if (pathname === "/forgot-password") return "forgot";
    if (pathname === "/register") return "register";
    return "login";
  };

  const [tab, setTab] = useState(deriveTab(location.pathname));

  useEffect(() => {
    const t = deriveTab(location.pathname);
    if (t !== tab) setTab(t);
  }, [location.pathname]);

  const goToLogin    = () => { setTab("login");  navigate("/login", { replace: true }); };
  const goToForgot   = () => { setTab("forgot"); navigate("/forgot-password", { replace: true }); };
  const goToRegister = () => { setTab("register"); navigate("/register", { replace: true }); };

  const showTabs = tab === "login" || tab === "register";

  return (
    <div className="auth-page">
      <ParticleBackground />

      <div className="auth-card">
        {/* Logo */}
        <div className="auth-logo">
          <div className="auth-logo-icon">◈</div>
          <h1 className="auth-logo-title">DataPulse</h1>
          <p className="auth-logo-sub">Secure Analytics Portal</p>
        </div>

        {/* Session warning */}
        {sessionMessage && tab === "login" && (
          <div className="auth-alert warning" style={{ marginBottom: 20 }}>
            <span>⏱</span>
            {sessionMessage}
          </div>
        )}

        {/* Tabs — only show on login / register */}
        {showTabs && (
          <div className="auth-tabs">
            <button id="tab-login" className={`auth-tab ${tab === "login" ? "active" : ""}`} onClick={goToLogin}>
              Sign In
            </button>
            <button id="tab-register" className={`auth-tab ${tab === "register" ? "active" : ""}`} onClick={goToRegister}>
              Register
            </button>
          </div>
        )}

        {/* Form panels */}
        {tab === "login"    && <LoginForm key="login" onLogin={onLogin} onForgot={goToForgot} />}
        {tab === "register" && <RegisterForm key="register" onLogin={onLogin} />}
        {tab === "forgot"   && <ForgotPasswordForm onBackToLogin={goToLogin} />}
        {tab === "reset"    && <ResetPasswordForm onBackToLogin={goToLogin} />}
      </div>
    </div>
  );
}
