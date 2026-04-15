

import logging
import os
import smtplib
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

from email_validator import EmailNotValidError, validate_email
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .db import User

logger = logging.getLogger(__name__)



JWT_SECRET = os.getenv("JWT_SECRET", "change_this_secret_in_production")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "1440"))  # 24 h

APP_ENV = os.getenv("APP_ENV", "production")
if APP_ENV == "production" and JWT_SECRET == "change_this_secret_in_production":
    raise RuntimeError("CRITICAL: Default JWT_SECRET is being used in production!")


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def normalize_email(email: str, *, check_deliverability: bool = False) -> Optional[str]:
    if not email:
        return None
    try:
        info = validate_email(email.strip(), check_deliverability=check_deliverability)
        return info.normalized.lower()
    except EmailNotValidError:
        return None




def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return pwd_context.verify(plain, hashed)
    except Exception as exc:
        logger.error("Password verification error: %s", exc)
        return False



def create_access_token(user_id: int, email: str) -> str:
    expire = datetime.now(tz=timezone.utc) + timedelta(minutes=JWT_EXPIRE_MINUTES)
    payload = {
        "sub": str(user_id),
        "email": email,
        "exp": expire,
        "iat": datetime.now(tz=timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def verify_access_token(token: str) -> Optional[dict]:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        user_id: str = payload.get("sub")
        email: str = payload.get("email")
        if user_id is None or email is None:
            return None
        return {"sub": user_id, "email": email}
    except JWTError as exc:
        logger.debug("JWT verification failed: %s", exc)
        return None


REFRESH_SECRET = os.getenv("REFRESH_SECRET", JWT_SECRET)
REFRESH_EXPIRE_DAYS = int(os.getenv("REFRESH_EXPIRE_DAYS", "7"))
PASSWORD_RESET_SECRET = os.getenv("PASSWORD_RESET_SECRET", JWT_SECRET)
PASSWORD_RESET_EXPIRE_MINUTES = int(os.getenv("PASSWORD_RESET_EXPIRE_MINUTES", "30"))


def create_refresh_token(user_id: int, email: str) -> str:
    expire = datetime.now(tz=timezone.utc) + timedelta(days=REFRESH_EXPIRE_DAYS)
    payload = {
        "sub": str(user_id),
        "email": email,
        "exp": expire,
        "iat": datetime.now(tz=timezone.utc),
        "typ": "refresh",
    }
    return jwt.encode(payload, REFRESH_SECRET, algorithm=JWT_ALGORITHM)


def verify_refresh_token(token: str) -> Optional[dict]:
    try:
        payload = jwt.decode(token, REFRESH_SECRET, algorithms=[JWT_ALGORITHM])
        # Ensure token type is refresh
        if payload.get("typ") != "refresh":
            return None
        user_id: str = payload.get("sub")
        email: str = payload.get("email")
        if user_id is None or email is None:
            return None
        return {"sub": user_id, "email": email}
    except JWTError as exc:
        logger.debug("Refresh token verification failed: %s", exc)
        return None


def create_password_reset_token(email: str) -> str:
    expire = datetime.now(tz=timezone.utc) + timedelta(minutes=PASSWORD_RESET_EXPIRE_MINUTES)
    payload = {
        "email": email,
        "exp": expire,
        "iat": datetime.now(tz=timezone.utc),
        "typ": "pwd_reset",
    }
    return jwt.encode(payload, PASSWORD_RESET_SECRET, algorithm=JWT_ALGORITHM)


def verify_password_reset_token(token: str) -> Optional[str]:
    try:
        payload = jwt.decode(token, PASSWORD_RESET_SECRET, algorithms=[JWT_ALGORITHM])
        if payload.get("typ") != "pwd_reset":
            return None
        email = payload.get("email")
        if not email:
            return None
        return str(email)
    except JWTError as exc:
        logger.debug("Password reset token verification failed: %s", exc)
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
        f"This link expires in {PASSWORD_RESET_EXPIRE_MINUTES} minutes.\n"
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



async def register_user(db: AsyncSession, email: str, password: str, name: Optional[str] = None) -> dict:
    if not email or not password:
        return {"success": False, "message": "Email and password are required"}

    normalized_email = normalize_email(email, check_deliverability=True)
    if not normalized_email:
        return {"success": False, "message": "Please enter a valid, deliverable email address"}

    if len(password) < 6:
        return {"success": False, "message": "Password must be at least 6 characters"}

    result = await db.execute(select(User).where(func.lower(User.email) == normalized_email))
    if result.scalar_one_or_none() is not None:
        return {"success": False, "message": "Email already registered. Please log in instead."}

    user = User(email=normalized_email, password_hash=hash_password(password), name=name)
    db.add(user)
    try:
        await db.flush()
        await db.refresh(user)
        await db.commit()
    except IntegrityError:
        await db.rollback()
        return {"success": False, "message": "Email already registered. Please log in instead."}

    logger.info("User registered: %s (id=%d)", normalized_email, user.id)
    return {
        "success": True,
        "message": "Registration successful! Please log in.",
        "user_id": user.id,
        "name": user.name,
        "email": user.email,
    }


async def login_user(db: AsyncSession, email: str, password: str) -> dict:
    if not email or not password:
        return {"success": False, "message": "Email and password are required"}

    normalized_email = normalize_email(email, check_deliverability=False)
    if not normalized_email:
        return {"success": False, "message": "Invalid email or password"}

    result = await db.execute(select(User).where(func.lower(User.email) == normalized_email))
    user: Optional[User] = result.scalar_one_or_none()

    if user is None or not verify_password(password, user.password_hash):
        logger.warning("Failed login attempt for: %s", normalized_email)
        return {"success": False, "message": "Invalid email or password"}

    token = create_access_token(user.id, user.email)
    logger.info("User logged in: %s (id=%d)", normalized_email, user.id)

    return {
        "success": True,
        "message": "Login successful!",
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "name": user.name,
            "email": user.email,
            "created_at": user.created_at,
            "updated_at": user.updated_at,
        },
    }


async def get_user_by_id(db: AsyncSession, user_id: int) -> Optional[User]:
    return await db.get(User, user_id)


async def get_user_by_email(db: AsyncSession, email: str) -> Optional[User]:
    normalized_email = normalize_email(email, check_deliverability=False)
    if not normalized_email:
        return None
    result = await db.execute(select(User).where(func.lower(User.email) == normalized_email))
    return result.scalar_one_or_none()


GOOGLE_CLIENT_ID = (
    os.getenv("GOOGLE_CLIENT_ID", "").strip()
    or os.getenv("FRONTEND_GOOGLE_CLIENT_ID", "").strip()
    or os.getenv("VITE_GOOGLE_CLIENT_ID", "").strip()
)


def verify_google_token(token: str) -> Optional[dict]:
    if not GOOGLE_CLIENT_ID:
        logger.error("Google token verification failed: GOOGLE_CLIENT_ID is unset")
        return None
    try:
        idinfo = id_token.verify_oauth2_token(token, google_requests.Request(), GOOGLE_CLIENT_ID)
        return idinfo
    except Exception as exc:
        logger.error("Google token verification failed: %s", exc)
        return None

async def login_google_user(db: AsyncSession, email: str, google_id: str, name: Optional[str] = None) -> dict:
    if not email:
        return {"success": False, "message": "Email is required"}

    normalized_email = normalize_email(email, check_deliverability=False)
    if not normalized_email:
        return {"success": False, "message": "Google account did not provide a valid email"}

    result = await db.execute(select(User).where(func.lower(User.email) == normalized_email))
    user: Optional[User] = result.scalar_one_or_none()

    if user is None:
        import secrets
        secure_random_pass = secrets.token_urlsafe(32)
        user = User(email=normalized_email, password_hash=hash_password(secure_random_pass), name=name)
        db.add(user)
        try:
            await db.flush()
            await db.refresh(user)
            await db.commit()
            logger.info("Google User registered: %s (id=%d)", normalized_email, user.id)
        except IntegrityError:
            await db.rollback()
            return {"success": False, "message": "Could not register Google user"}

    token = create_access_token(user.id, user.email)
    logger.info("Google User logged in: %s (id=%d)", normalized_email, user.id)

    return {
        "success": True,
        "message": "Login successful!",
        "access_token": token,
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
    normalized_email = normalize_email(email, check_deliverability=False)
    generic_message = "If an account exists for that email, a password reset link has been sent."

    if not normalized_email:
        return {"success": True, "message": generic_message}

    result = await db.execute(select(User).where(func.lower(User.email) == normalized_email))
    user: Optional[User] = result.scalar_one_or_none()
    if user is None:
        return {"success": True, "message": generic_message}

    token = create_password_reset_token(user.email)
    frontend_url = os.getenv("FRONTEND_URL", "http://localhost:5173").strip().rstrip("/")
    reset_link = f"{frontend_url}/reset-password?token={token}"

    sent = _send_password_reset_email(user.email, reset_link)
    app_env = os.getenv("APP_ENV", "production")
    expose_debug = os.getenv("EXPOSE_RESET_TOKEN_IN_DEV", "false").lower() == "true"
    debug_token = token if (app_env == "development" and (expose_debug or not sent)) else None

    return {
        "success": True,
        "message": generic_message,
        "debug_reset_token": debug_token,
        "email_sent": sent,
    }


async def reset_password_with_token(db: AsyncSession, token: str, new_password: str) -> dict:
    email = verify_password_reset_token(token)
    if not email:
        return {"success": False, "message": "Invalid or expired reset token"}

    normalized_email = normalize_email(email, check_deliverability=False)
    if not normalized_email:
        return {"success": False, "message": "Invalid reset token payload"}

    result = await db.execute(select(User).where(func.lower(User.email) == normalized_email))
    user: Optional[User] = result.scalar_one_or_none()
    if user is None:
        return {"success": False, "message": "Invalid or expired reset token"}

    user.password_hash = hash_password(new_password)
    try:
        await db.flush()
        await db.commit()
    except Exception as exc:
        await db.rollback()
        logger.exception("Failed to reset password for %s: %s", normalized_email, exc)
        return {"success": False, "message": "Could not reset password. Please try again."}

    return {"success": True, "message": "Password reset successful. Please log in with your new password."}
