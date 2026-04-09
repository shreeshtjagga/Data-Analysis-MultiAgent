/**
 * api.js
 * ──────
 * Thin wrapper around fetch() that:
 *  • Prefixes every URL with /api  (Vite proxy → FastAPI)
 *  • Injects the stored JWT as  Authorization: Bearer <token>
 *  • Throws a structured error on non-2xx responses
 *  • Clears token and surfaces 401 to caller (no forced hard redirect)
 */

// If deployed on Vercel, use the VITE_API_URL env variable (which points to Railway).
// Otherwise, fallback to "/api" for local Vite proxy development.
const BASE = import.meta.env.VITE_API_URL || "/api";

// ── Token storage (Local Storage for persistence across refreshes) ──────────

export function setToken(token) {
  if (token) {
    localStorage.setItem("datapulse_token", token);
  } else {
    localStorage.removeItem("datapulse_token");
  }
}

export function getToken() {
  return localStorage.getItem("datapulse_token");
}

export function clearToken() {
  localStorage.removeItem("datapulse_token");
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
  });

  // If authorization fails on a PROTECTED route, clear token and log out.
  // We ignore /auth/login and /auth/google so their specific error messages pass through.
  if (response.status === 401 && !path.startsWith("/auth/login") && !path.startsWith("/auth/google")) {
    clearToken();
    const authErr = new Error("Session expired — please log in again");
    authErr.status = 401;
    throw authErr;
  }

  if (!response.ok) {
    let detail = `HTTP ${response.status}`;
    try {
      const body = await response.json();
      detail = body.detail || body.message || detail;
    } catch (_) {}
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

export async function apiGoogleLogin(credential) {
  return apiFetch("/auth/google", {
    method: "POST",
    withAuth: false,
    body: JSON.stringify({ credential }),
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

export async function apiDeleteAnalysis(analysisId) {
  return apiFetch(`/history/${analysisId}`, { method: "DELETE" });
}

// ── Health ────────────────────────────────────────────────────────────────────

export async function apiHealth() {
  return apiFetch("/health", { withAuth: false });
}