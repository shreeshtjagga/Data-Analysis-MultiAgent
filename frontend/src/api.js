import { createClient } from '@supabase/supabase-js'
const supabaseUrl = import.meta.env.VITE_SUPABASE_URL
const supabaseAnonKey = import.meta.env.VITE_SUPABASE_ANON_KEY
if (!supabaseUrl || !supabaseAnonKey) {
  console.error('Missing Supabase URL or Anon Key in environment variables')
}
export const supabase = createClient(supabaseUrl, supabaseAnonKey)

const BASE = import.meta.env.VITE_API_BASE ? `${import.meta.env.VITE_API_BASE}/api` : "/api";


// ── Session Management ──────────────────────────────────────────────
// Session timeout configuration (in milliseconds)
const SESSION_TIMEOUT_MS = 60 * 60 * 1000;        // 1 hour — matches Supabase JWT default expiry
const INACTIVITY_TIMEOUT_MS = 30 * 60 * 1000;     // 30 minutes of no activity → auto-logout

let accessToken = null;
let _sessionSetAt = null;        // timestamp when token was set
let _lastActivityAt = Date.now(); // timestamp of last user interaction
let _onSessionExpired = null;    // callback set by App.jsx for auto-logout
let _inactivityTimer = null;     // interval for checking inactivity

/**
 * Register a callback that fires when the session expires (either by timeout
 * or inactivity). App.jsx sets this to trigger handleLogout().
 */
export function onSessionExpired(callback) {
  _onSessionExpired = callback;
}

/** Record user activity (called by App.jsx on mouse/keyboard/touch events). */
export function recordActivity() {
  _lastActivityAt = Date.now();
}

function _startInactivityMonitor() {
  _stopInactivityMonitor();
  _inactivityTimer = setInterval(() => {
    if (!accessToken) { _stopInactivityMonitor(); return; }

    const now = Date.now();
    // Check token age — force logout if the JWT itself is expired
    if (_sessionSetAt && (now - _sessionSetAt) >= SESSION_TIMEOUT_MS) {
      console.info('[Session] Token expired — logging out');
      _clearSessionState();
      if (_onSessionExpired) _onSessionExpired('session_expired');
      return;
    }
    // Check inactivity
    if ((now - _lastActivityAt) >= INACTIVITY_TIMEOUT_MS) {
      console.info('[Session] Inactivity timeout — logging out');
      _clearSessionState();
      if (_onSessionExpired) _onSessionExpired('inactivity');
      return;
    }
  }, 15_000); // check every 15 seconds
}

function _stopInactivityMonitor() {
  if (_inactivityTimer) { clearInterval(_inactivityTimer); _inactivityTimer = null; }
}

/** Internal: clear local state without side effects (avoids re-entrancy with clearToken). */
function _clearSessionState() {
  accessToken = null;
  _sessionSetAt = null;
  _stopInactivityMonitor();
}

export function setToken(token) {
  accessToken = token || null;
  if (token) {
    _sessionSetAt = Date.now();
    _lastActivityAt = Date.now();
    _startInactivityMonitor();
  } else {
    _sessionSetAt = null;
    _stopInactivityMonitor();
  }
}

export function getToken() {
  if (!accessToken) return null;
  // Quick check: if the token is stale, treat it as absent
  if (_sessionSetAt && (Date.now() - _sessionSetAt) >= SESSION_TIMEOUT_MS) {
    clearToken();
    return null;
  }
  return accessToken;
}

export function clearToken() {
  _clearSessionState();
  // Clear server-side refresh cookie
  fetch(`${BASE}/auth/logout`, { method: "POST", credentials: "include" }).catch(() => { });
  // Sign out from Supabase client (clears local storage session)
  try { supabase.auth.signOut().catch(() => {}); } catch (_) {}
}

// ── Token Refresh ───────────────────────────────────────────────────
async function refreshAccessToken() {
  try {
    const resp = await fetch(`${BASE}/auth/refresh`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
    });
    if (!resp.ok) return false;
    const data = await resp.json();
    if (data && data.access_token) {
      setToken(data.access_token);
      return true;
    }
  } catch (err) {
    return false;
  }
  return false;
}

// ── HTTP Helpers ────────────────────────────────────────────────────
function getStatusMessage(status) {
  const messages = {
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    409: "Conflict",
    422: "Unprocessable Entity",
    429: "Too Many Requests",
    500: "Server Error",
    502: "Bad Gateway",
    503: "Service Unavailable"
  };
  return messages[status] || `HTTP ${status}`;
}
function normalizeErrorDetail(body, fallback) {
  if (!body || typeof body !== 'object') return fallback;
  if (Array.isArray(body.detail)) {
    const errors = body.detail.map(err => {
      if (typeof err === 'object') {
        const field = err.loc ? err.loc[err.loc.length - 1] : 'Unknown field';
        return `${field}: ${err.msg}`;
      }
      return String(err);
    });
    return errors.join(' | ') || fallback;
  }
  if (typeof body.detail === 'string') {
    return body.detail;
  }
  if (typeof body.message === 'string') {
    return body.message;
  }
  return fallback;
}
async function apiFetch(path, options = {}) {
  const headers = { ...(options.headers || {}) };
  const token = getToken();
  if (token && options.withAuth !== false) {
    headers["Authorization"] = `Bearer ${token}`;
  }
  if (options.body && !(options.body instanceof FormData)) {
    headers["Content-Type"] = "application/json";
  }
  const response = await fetch(`${BASE}${path}`, {
    ...options,
    headers,
    credentials: options.credentials ?? "include",
  });
  if (response.status === 401 && !path.startsWith("/auth/login") && !path.startsWith("/auth/google") && !path.startsWith("/auth/refresh")) {
    const refreshed = await refreshAccessToken();
    if (refreshed) {
      const retryHeaders = { ...(options.headers || {}) };
      const token2 = getToken();
      if (token2 && options.withAuth !== false) {
        retryHeaders["Authorization"] = `Bearer ${token2}`;
      }
      if (options.body && !(options.body instanceof FormData)) {
        retryHeaders["Content-Type"] = "application/json";
      }
      const retryResp = await fetch(`${BASE}${path}`, {
        ...options,
        headers: retryHeaders,
        credentials: options.credentials ?? "include",
      });
      if (!retryResp.ok) {
        if (retryResp.status === 401) {
          clearToken();
          const authErr = new Error("Session expired — please log in again");
          authErr.status = 401;
          throw authErr;
        }
        let detail = getStatusMessage(retryResp.status);
        try {
          const body = await retryResp.json();
          detail = normalizeErrorDetail(body, detail);
        } catch (_) {
          console.warn("Failed to parse error response", _);
        }
        throw new Error(detail);
      }
      if (options.raw) return retryResp;
      return retryResp.json();
    }
    clearToken();
    const authErr = new Error("Session expired — please log in again");
    authErr.status = 401;
    throw authErr;
  }
  if (!response.ok) {
    let detail = `HTTP ${response.status}`;
    try {
      const body = await response.json();
      detail = normalizeErrorDetail(body, detail);
    } catch (_) {
      console.warn("Failed to parse error response", _);
    }
    throw new Error(detail);
  }
  if (options.raw) return response;
  return response.json();
}

// ── Public API Functions ────────────────────────────────────────────
export async function apiRegister(email, password, name = null) {
  return apiFetch("/auth/register", {
    method: "POST",
    withAuth: false,
    body: JSON.stringify({ email, password, name }),
  });
}
export async function apiLogin(email, password) {
  return apiFetch("/auth/login", {
    method: "POST",
    withAuth: false,
    body: JSON.stringify({ email, password }),
  });
}
export async function apiSyncSession(accessToken, refreshToken) {
  return apiFetch("/auth/sync-session", {
    method: "POST",
    withAuth: false,
    body: JSON.stringify({ access_token: accessToken, refresh_token: refreshToken }),
  });
}
export async function apiForgotPassword(email) {
  return apiFetch("/auth/forgot-password", {
    method: "POST",
    withAuth: false,
    body: JSON.stringify({ email }),
  });
}

export async function apiMe() {
  return apiFetch("/auth/me");
}
export async function apiAnalyze(file) {
  const form = new FormData();
  form.append("file", file);
  return apiFetch("/analyze", { method: "POST", body: form });
}
export async function apiChat(question, context = {}, history = []) {
  let safeContext = context;
  try {
    JSON.stringify(context);
  } catch (_) {
    safeContext = { ...(context || {}), charts: {} };
  }
  let chatBody = JSON.stringify({ question, context: safeContext, history });
  return apiFetch("/chat", {
    method: "POST",
    body: chatBody,
  });
}
export async function apiHistory(limit = 20) {
  return apiFetch(`/history?limit=${limit}`);
}
export async function apiHistoryAnalysis(analysisId) {
  return apiFetch(`/history/${analysisId}`);
}
export async function apiDeleteAnalysis(analysisId) {
  return apiFetch(`/history/${analysisId}`, { method: "DELETE" });
}
