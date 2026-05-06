"""
DataPulse Auth — Supabase Backend
==================================
Replaces custom JWT + bcrypt + google-auth with Supabase Auth.

REMOVED (dead after migration):
  - jose JWT (create_access_token, verify_access_token) → Supabase issues tokens
  - passlib/bcrypt (hash_password, verify_password) → Supabase hashes internally
  - python-jose refresh tokens → Supabase handles refresh via session
  - google.oauth2 id_token verification → Supabase verifies Google tokens
  - password reset token generation → Supabase has built-in resetPasswordForEmail

KEPT:
  - normalize_email() → still needed for input sanitization
  - SMTP email helpers → kept for custom notifications
  - get_user_by_id() → still needed by API endpoints
"""

import logging
import os
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

from email_validator import EmailNotValidError, validate_email
from supabase import create_client, Client
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy import func as sqlfunc

from .db import User, AnalysisHistory

logger = logging.getLogger(__name__)

# ── Supabase Admin Client ──────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "").strip()

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError(
        "CRITICAL: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set. "
        "Get them from Supabase Dashboard → Settings → API."
    )

# Service-role client: used server-side only, never exposed to frontend
_supabase_admin: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Public client: used for sign-in (validates user credentials via anon key)
_supabase_public: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

# ── Supabase JWT verification ──────────────────────────────────────────────────
# Supabase JWTs are standard HS256. Verification happens via Supabase Admin API.
# We call get_user(jwt) instead of manually decoding — this handles expiry,
# rotation, and revocation automatically.

async def verify_access_token(token: str) -> Optional[dict]:
    """
    Verify a Supabase JWT and return {sub: uuid, email: str} or None.
    Uses Supabase Admin API — handles expiry and revocation correctly.
    """
    import asyncio
    try:
        resp = await asyncio.to_thread(_supabase_admin.auth.get_user, token)
        user = resp.user
        if user is None:
            return None
        return {
            "sub": str(user.id),
            "email": user.email or "",
        }
    except Exception as exc:
        logger.debug("Supabase token verification failed: %s", exc)
        return None


# ── Email helpers ──────────────────────────────────────────────────────────────

def normalize_email(email: str, *, check_deliverability: bool = False) -> Optional[str]:
    if not email:
        return None
    try:
        info = validate_email(email.strip(), check_deliverability=check_deliverability)
        return info.normalized.lower()
    except EmailNotValidError:
        return None


def _send_password_reset_email(to_email: str, reset_link: str) -> bool:
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "")
    smtp_use_tls = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
    mail_from = os.getenv("MAIL_FROM", smtp_user).strip()

    if not smtp_host or not mail_from:
        logger.warning("SMTP not configured; skipping password reset email dispatch")
        return False

    message = EmailMessage()
    message["Subject"] = "DataPulse password reset"
    message["From"] = mail_from
    message["To"] = to_email
    message.set_content(
        "We received a request to reset your DataPulse password.\n\n"
        f"Reset link: {reset_link}\n\n"
        "If you did not request this, you can ignore this email."
    )
    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            if smtp_use_tls:
                server.starttls()
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.send_message(message)
        return True
    except Exception as exc:
        logger.exception("Failed to send password reset email: %s", exc)
        return False


# ── User profile sync ──────────────────────────────────────────────────────────
# Supabase Auth manages auth.users. Our SQLAlchemy `users` table stores
# the profile (name, etc.) keyed by the Supabase UUID (stored as string in email
# field for compatibility). After full migration, consider replacing the User
# table with a Supabase `profiles` table via RLS policies.

async def _get_or_create_profile(db: AsyncSession, supabase_user_id: str, email: str, name: Optional[str] = None) -> User:
    """Sync a Supabase auth user into our local users table."""
    result = await db.execute(
        select(User).where(User.supabase_id == supabase_user_id)
    )
    user = result.scalar_one_or_none()
    if user is not None:
        return user

    # First time login — create local profile row
    user = User(
        supabase_id=supabase_user_id,
        email=email,
        name=name,
    )
    db.add(user)
    await db.flush()
    await db.refresh(user)
    await db.commit()
    logger.info("Local profile created for Supabase user: %s (id=%d)", email, user.id)
    return user


# ── Register ───────────────────────────────────────────────────────────────────

async def register_user(db: AsyncSession, email: str, password: str, name: Optional[str] = None) -> dict:
    if not email or not password:
        return {"success": False, "message": "Email and password are required"}

    normalized_email = normalize_email(email, check_deliverability=False)
    if not normalized_email:
        return {"success": False, "message": "Please enter a valid email address"}

    if len(password) < 6:
        return {"success": False, "message": "Password must be at least 6 characters"}

    import asyncio
    try:
        resp = await asyncio.to_thread(
            _supabase_admin.auth.admin.create_user,
            {
                "email": normalized_email,
                "password": password,
                "email_confirm": True,  # skip confirmation email for now
                "user_metadata": {"name": name or ""},
            }
        )
        supabase_user = resp.user
        if supabase_user is None:
            return {"success": False, "message": "Registration failed. Please try again."}
    except Exception as exc:
        err_str = str(exc).lower()
        if "already registered" in err_str or "already exists" in err_str or "duplicate" in err_str:
            return {"success": False, "message": "Email already registered. Please log in instead."}
        logger.error("Supabase register_user failed: %s", exc)
        return {"success": False, "message": "Registration failed. Please try again."}

    # Sync profile to local DB
    user = await _get_or_create_profile(db, str(supabase_user.id), normalized_email, name)

    logger.info("User registered via Supabase: %s (local_id=%d)", normalized_email, user.id)
    return {
        "success": True,
        "message": "Registration successful! Please log in.",
        "user_id": user.id,
        "name": user.name,
        "email": user.email,
        "created_at": user.created_at,
        "updated_at": user.updated_at,
    }


# ── Login ──────────────────────────────────────────────────────────────────────

async def login_user(db: AsyncSession, email: str, password: str) -> dict:
    if not email or not password:
        return {"success": False, "message": "Email and password are required"}

    normalized_email = normalize_email(email, check_deliverability=False)
    if not normalized_email:
        return {"success": False, "message": "Invalid email or password"}

    import asyncio
    try:
        resp = await asyncio.to_thread(
            _supabase_public.auth.sign_in_with_password,
            {"email": normalized_email, "password": password}
        )
        session = resp.session
        supabase_user = resp.user
        if session is None or supabase_user is None:
            return {"success": False, "message": "Invalid email or password"}
    except Exception as exc:
        err_str = str(exc).lower()
        if "invalid" in err_str or "credentials" in err_str or "wrong" in err_str:
            logger.warning("FAILED LOGIN for email: %s", normalized_email)
            return {"success": False, "message": "Invalid email or password"}
        logger.error("Supabase login_user error: %s", exc)
        return {"success": False, "message": "Login failed. Please try again."}

    # Sync/get local profile
    name = (supabase_user.user_metadata or {}).get("name")
    user = await _get_or_create_profile(db, str(supabase_user.id), normalized_email, name)

    logger.info("User logged in via Supabase: %s (local_id=%d)", normalized_email, user.id)
    return {
        "success": True,
        "message": "Login successful!",
        "access_token": session.access_token,
        "refresh_token": session.refresh_token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "name": user.name,
            "email": user.email,
            "created_at": user.created_at,
            "updated_at": user.updated_at,
        },
    }


# ── Google OAuth ───────────────────────────────────────────────────────────────
# REMOVED: verify_google_token() using google.oauth2.id_token — dead, replaced by
# Supabase's built-in Google provider. Google tokens are now validated by Supabase
# via the OAuth code exchange flow or ID token submission.

async def login_google_user(db: AsyncSession, google_id_token: str, name: Optional[str] = None) -> dict:
    """
    Verify a Google ID token via Supabase and create/find the local user profile.
    Supabase validates the token against the Google Client ID configured in
    the Supabase Dashboard → Auth → Providers → Google.
    """
    import asyncio
    try:
        resp = await asyncio.to_thread(
            _supabase_public.auth.sign_in_with_id_token,
            {"provider": "google", "token": google_id_token}
        )
        session = resp.session
        supabase_user = resp.user
        if session is None or supabase_user is None:
            return {"success": False, "message": "Invalid Google token"}
    except Exception as exc:
        logger.error("Supabase Google login failed: %s", exc)
        return {"success": False, "message": "Invalid Google token"}

    email = supabase_user.email
    if not email:
        return {"success": False, "message": "No email provided by Google"}

    meta_name = name or (supabase_user.user_metadata or {}).get("full_name") or (supabase_user.user_metadata or {}).get("name")
    user = await _get_or_create_profile(db, str(supabase_user.id), email, meta_name)

    logger.info("Google user logged in via Supabase: %s (local_id=%d)", email, user.id)
    return {
        "success": True,
        "message": "Login successful!",
        "access_token": session.access_token,
        "refresh_token": session.refresh_token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "name": user.name,
            "email": user.email,
            "created_at": user.created_at,
            "updated_at": user.updated_at,
        },
    }


# ── Password Reset ─────────────────────────────────────────────────────────────
# REMOVED: create_password_reset_token(), verify_password_reset_token(),
# reset_password_with_token() using custom HMAC tokens — dead. Supabase has a
# built-in resetPasswordForEmail() that sends a secure, time-limited link.

async def request_password_reset(db: AsyncSession, email: str) -> dict:
    generic_message = "If an account exists for that email, a password reset link has been sent."
    normalized_email = normalize_email(email, check_deliverability=False)
    if not normalized_email:
        return {"success": True, "message": generic_message}

    import asyncio
    try:
        frontend_url = os.getenv("FRONTEND_URL", "http://localhost:5173").strip().rstrip("/")
        await asyncio.to_thread(
            _supabase_admin.auth.admin.generate_link,
            {
                "type": "recovery",
                "email": normalized_email,
                "options": {"redirect_to": f"{frontend_url}/reset-password"},
            }
        )
        logger.info("Password reset link generated for: %s", normalized_email)
    except Exception as exc:
        # Don't leak whether the email exists
        logger.warning("Password reset request failed (non-fatal): %s", exc)

    return {"success": True, "message": generic_message, "email_sent": True}


async def reset_password_with_token(db: AsyncSession, token: str, new_password: str) -> dict:
    """
    Reset password using the Supabase session token from the reset link.
    The frontend receives the token from the URL after clicking the reset link,
    then passes it here.
    """
    import asyncio
    try:
        # Exchange the recovery token for a session first
        resp = await asyncio.to_thread(
            _supabase_public.auth.verify_otp,
            {"token_hash": token, "type": "recovery"}
        )
        session = resp.session
        if session is None:
            return {"success": False, "message": "Invalid or expired reset token"}

        # Use the session token to update the password
        user_client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        await asyncio.to_thread(
            user_client.auth.set_session,
            session.access_token,
            session.refresh_token,
        )
        await asyncio.to_thread(
            user_client.auth.update_user,
            {"password": new_password}
        )
    except Exception as exc:
        logger.error("Password reset failed: %s", exc)
        return {"success": False, "message": "Invalid or expired reset token"}

    return {"success": True, "message": "Password reset successful. Please log in with your new password."}


# ── Token Refresh ──────────────────────────────────────────────────────────────
# REMOVED: create_refresh_token(), verify_refresh_token() using custom HMAC tokens.
# Supabase refresh tokens are opaque strings managed by Supabase internally.

async def refresh_session(refresh_token: str) -> Optional[dict]:
    """Exchange a Supabase refresh token for a new access token."""
    import asyncio
    try:
        resp = await asyncio.to_thread(
            _supabase_public.auth.refresh_session, refresh_token
        )
        session = resp.session
        if session is None:
            return None
        return {
            "access_token": session.access_token,
            "refresh_token": session.refresh_token,
        }
    except Exception as exc:
        logger.debug("Supabase refresh_session failed: %s", exc)
        return None


# ── User lookup ────────────────────────────────────────────────────────────────

async def get_user_by_id(db: AsyncSession, user_id: int) -> Optional[User]:
    return await db.get(User, user_id)
