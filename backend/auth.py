"""
auth.py
───────
Authentication helpers:
  • bcrypt password hashing / verification
  • JWT access-token creation and verification via python-jose
  • Async CRUD functions for user registration and login
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .db import User

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

JWT_SECRET = os.getenv("JWT_SECRET", "change_this_secret_in_production")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "1440"))  # 24 h

APP_ENV = os.getenv("APP_ENV", "production")
if APP_ENV == "production" and JWT_SECRET == "change_this_secret_in_production":
    raise RuntimeError("CRITICAL: Default JWT_SECRET is being used in production!")

# bcrypt context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ── Password helpers ──────────────────────────────────────────────────────────

def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return pwd_context.verify(plain, hashed)
    except Exception as exc:
        logger.error("Password verification error: %s", exc)
        return False


# ── JWT helpers ───────────────────────────────────────────────────────────────

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


# ── Refresh token helpers (HttpOnly cookie) ─────────────────────────────────
REFRESH_SECRET = os.getenv("REFRESH_SECRET", JWT_SECRET)
REFRESH_EXPIRE_DAYS = int(os.getenv("REFRESH_EXPIRE_DAYS", "7"))


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
        # ensure token type is refresh
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


# ── User CRUD ─────────────────────────────────────────────────────────────────

async def register_user(db: AsyncSession, email: str, password: str, name: Optional[str] = None) -> dict:
    if not email or not password:
        return {"success": False, "message": "Email and password are required"}

    if len(password) < 6:
        return {"success": False, "message": "Password must be at least 6 characters"}

    result = await db.execute(select(User).where(User.email == email))
    if result.scalar_one_or_none() is not None:
        return {"success": False, "message": "Email already registered. Please log in instead."}

    user = User(email=email, password_hash=hash_password(password), name=name)
    db.add(user)
    try:
        await db.flush()
        await db.refresh(user)
        await db.commit()
    except IntegrityError:
        await db.rollback()
        return {"success": False, "message": "Email already registered. Please log in instead."}

    logger.info("User registered: %s (id=%d)", email, user.id)
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

    result = await db.execute(select(User).where(User.email == email))
    user: Optional[User] = result.scalar_one_or_none()

    if user is None or not verify_password(password, user.password_hash):
        logger.warning("Failed login attempt for: %s", email)
        return {"success": False, "message": "Invalid email or password"}

    token = create_access_token(user.id, user.email)
    logger.info("User logged in: %s (id=%d)", email, user.id)

    # FIX: Return datetime objects directly (not .isoformat() strings)
    # Pydantic UserResponse expects datetime, not str
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
    result = await db.execute(select(User).where(User.email == email))
    return result.scalar_one_or_none()


GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")


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

    result = await db.execute(select(User).where(User.email == email))
    user: Optional[User] = result.scalar_one_or_none()

    if user is None:
        import secrets
        secure_random_pass = secrets.token_urlsafe(32)
        user = User(email=email, password_hash=hash_password(secure_random_pass), name=name)
        db.add(user)
        try:
            await db.flush()
            await db.refresh(user)
            await db.commit()
            logger.info("Google User registered: %s (id=%d)", email, user.id)
        except IntegrityError:
            await db.rollback()
            return {"success": False, "message": "Could not register Google user"}

    token = create_access_token(user.id, user.email)
    logger.info("Google User logged in: %s (id=%d)", email, user.id)

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
