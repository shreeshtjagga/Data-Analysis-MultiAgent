/*
 * api.js
 * ──────
 * Thin wrapper around fetch() that:
 *  • Prefixes every URL with /api  (Vite proxy → FastAPI)
 *  • Injects `Authorization: Bearer <access_token>` when an in-memory token exists
 *  • Uses an HttpOnly refresh cookie for refresh tokens and will auto-refresh
 *    the access token when it expires
 *  • Throws a structured error on non-2xx responses
 *
 * Token persistence: access token is kept in-memory (lost on hard reload);
 * refresh tokens are stored in an HttpOnly cookie by the backend.
 */

// We use "/api" for everything. 
// Locally, Vite proxies this to http://localhost:8000
// In Production, vercel.json proxies this to the Render backend.
const BASE = "/api";

function normalizeErrorDetail(body, fallback) {
  const raw = body?.detail ?? body?.message ?? fallback;

  if (typeof raw === "string") return raw;
  if (Array.isArray(raw)) {
    return raw
      .map((item) => (typeof item === "string" ? item : JSON.stringify(item)))
      .join("; ");
  }
  if (raw && typeof raw === "object") {
    if (typeof raw.message === "string" && raw.message.trim()) {
      const extra = Array.isArray(raw.errors) && raw.errors.length
        ? ` (${raw.errors
            .map((item) => {
              if (typeof item === "string") return item;
              try { return JSON.stringify(item); } catch (_) { return String(item); }
            })
            .join("; ")})`
        : "";
      return `${raw.message}${extra}`;
    }
    try {
      return JSON.stringify(raw);
    } catch (_) {
      return fallback;
    }
  }
  return String(raw ?? fallback);
}

// ── Token storage: in-memory access token + HttpOnly refresh cookie ───────
// Access token is held only in memory (lost on hard refresh). Refresh tokens
// are stored in an HttpOnly cookie set by the backend; we call /auth/refresh
// to obtain a new access token when needed.

let accessToken = null;

export function setToken(token) {
  accessToken = token || null;
}

export function getToken() {
  return accessToken;
}

export function clearToken() {
  if (!accessToken) return;   // already logged out — skip the logout request
  accessToken = null;
  // Best-effort: ask backend to clear the HttpOnly refresh cookie.
  // Fire-and-forget is intentional — we don't need to await this.
  fetch(`${BASE}/auth/logout`, { method: "POST", credentials: "include" }).catch(() => {});
}

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
    // ignore
  }
  return false;
}

// ── Core fetch helper ─────────────────────────────────────────────────────────

async function apiFetch(path, options = {}) {
  const headers = { ...(options.headers || {}) };

  // Attach JWT unless caller explicitly opts out
  const token = getToken();
  if (token && options.withAuth !== false) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  // Only set Content-Type for JSON bodies — let browser set it for FormData
  if (options.body && !(options.body instanceof FormData)) {
    headers["Content-Type"] = "application/json";
  }

  const response = await fetch(`${BASE}${path}`, {
    ...options,
    headers,
    credentials: options.credentials ?? "include",
  });

  // If authorization fails on a PROTECTED route, clear token and log out.
  // We ignore /auth/login and /auth/google so their specific error messages pass through.
  if (response.status === 401 && !path.startsWith("/auth/login") && !path.startsWith("/auth/google") && !path.startsWith("/auth/refresh")) {
    // Attempt one refresh if the access token expired
    const refreshed = await refreshAccessToken();
    if (refreshed) {
      // Retry original request once with new token
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
        let detail = `HTTP ${retryResp.status}`;
        try {
          const b = await retryResp.json();
          detail = b.detail || b.message || detail;
        } catch (_) {
          detail = String(detail);
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
      const rawDetail = body.detail || body.message || detail;
      detail = typeof rawDetail === 'object' ? JSON.stringify(rawDetail, null, 2) : rawDetail;
    } catch (_) {
      detail = String(detail);
    }
    throw new Error(detail);
  }

  // Return raw Response for file downloads; parse JSON otherwise
  if (options.raw) return response;
  return response.json();
}

// ── Auth ──────────────────────────────────────────────────────────────────────

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

export async function apiGoogleLogin(credential, clientId = null) {
  return apiFetch("/auth/google", {
    method: "POST",
    withAuth: false,
    body: JSON.stringify({ credential, client_id: clientId }),
  });
}

export async function apiForgotPassword(email) {
  return apiFetch("/auth/forgot-password", {
    method: "POST",
    withAuth: false,
    body: JSON.stringify({ email }),
  });
}

export async function apiResetPassword(token, newPassword) {
  return apiFetch("/auth/reset-password", {
    method: "POST",
    withAuth: false,
    body: JSON.stringify({ token, new_password: newPassword }),
  });
}

export async function apiMe() {
  return apiFetch("/auth/me");
}

// ── Analysis ──────────────────────────────────────────────────────────────────

export async function apiAnalyze(file) {
  const form = new FormData();
  form.append("file", file);
  return apiFetch("/analyze", { method: "POST", body: form });
}

export async function apiChat(question, context = {}) {
  return apiFetch("/chat", {
    method: "POST",
    body: JSON.stringify({ question, context }),
  });
}

// ── History ───────────────────────────────────────────────────────────────────

export async function apiHistory(limit = 20) {
  return apiFetch(`/history?limit=${limit}`);
}

export async function apiHistoryAnalysis(analysisId) {
  return apiFetch(`/history/${analysisId}`);
}

export async function apiDeleteAnalysis(analysisId) {
  return apiFetch(`/history/${analysisId}`, { method: "DELETE" });
}

// ── Health ────────────────────────────────────────────────────────────────────

export async function apiHealth() {
  return apiFetch("/health", { withAuth: false });
}
