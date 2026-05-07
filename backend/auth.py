   

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

                                                                                 
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "").strip()

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError(
        "CRITICAL: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set. "
        "Get them from Supabase Dashboard → Settings → API."
    )

                                                                       
_supabase_admin: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

                                                                           
_supabase_public: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

                                                                                 
                                                                                
                                                                           
                                         

async def verify_access_token(token: str) -> Optional[dict]:
       
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


                                                                                 
                                                                       
                                                                                
                                                                             
                                                          

async def _get_or_create_profile(db: AsyncSession, supabase_user_id: str, email: str, name: Optional[str] = None) -> User:
                                                               
    result = await db.execute(
        select(User).where(User.supabase_id == supabase_user_id)
    )
    user = result.scalar_one_or_none()
    if user is not None:
        return user

                                                 
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
                "email_confirm": True,                                   
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
                                             
        logger.warning("Password reset request failed (non-fatal): %s", exc)

    return {"success": True, "message": generic_message, "email_sent": True}


async def reset_password_with_token(db: AsyncSession, token: str, new_password: str) -> dict:
       
    import asyncio
    try:
                                                         
        resp = await asyncio.to_thread(
            _supabase_public.auth.verify_otp,
            {"token_hash": token, "type": "recovery"}
        )
        session = resp.session
        if session is None:
            return {"success": False, "message": "Invalid or expired reset token"}

                                                      
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


                                                                                 
                                                                            

async def refresh_session(refresh_token: str) -> Optional[dict]:
                                                                   
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


                                                                                 

async def get_user_by_id(db: AsyncSession, user_id: int) -> Optional[User]:
    return await db.get(User, user_id)
