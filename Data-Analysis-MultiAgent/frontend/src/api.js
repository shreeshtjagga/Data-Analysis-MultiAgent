/**
 * api.js
 * ──────
 * Thin wrapper around fetch() that:
 *  • Prefixes every URL with /api  (Vite proxy → FastAPI)
 *  • Injects the stored JWT as  Authorization: Bearer <token>
 *  • Throws a structured error on non-2xx responses
 *  • Auto-redirects to /login on 401 (expired token)
 */

const BASE = "/api";

// ── Token storage (in-memory only — never localStorage in this app) ──────────
let _token = null;

export function setToken(token) {
  _token = token;
}

export function getToken() {
  return _token;
}

export function clearToken() {
  _token = null;
}

// ── Core fetch helper ─────────────────────────────────────────────────────────

async function apiFetch(path, options = {}) {
  const headers = { ...(options.headers || {}) };

  // Attach JWT unless caller explicitly opts out
  if (_token && options.withAuth !== false) {
    headers["Authorization"] = `Bearer ${_token}`;
  }

  // Only set Content-Type for JSON bodies — let browser set it for FormData
  if (options.body && !(options.body instanceof FormData)) {
    headers["Content-Type"] = "application/json";
  }

  const response = await fetch(`${BASE}${path}`, {
    ...options,
    headers,
  });

  if (response.status === 401) {
    clearToken();
    window.location.href = "/login";
    throw new Error("Session expired — please log in again");
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

export async function apiRegister(email, password) {
  return apiFetch("/auth/register", {
    method: "POST",
    withAuth: false,
    body: JSON.stringify({ email, password }),
  });
}

export async function apiLogin(email, password) {
  return apiFetch("/auth/login", {
    method: "POST",
    withAuth: false,
    body: JSON.stringify({ email, password }),
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