"""
auth.py
───────
Authentication helpers:
  • bcrypt password hashing / verification  (replaces the old SHA-256 approach)
  • JWT access-token creation and verification via python-jose
  • Async CRUD functions for user registration and login
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

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

# bcrypt context — auto_deprecated keeps old hashes working while upgrading
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ── Password helpers ──────────────────────────────────────────────────────────

def hash_password(plain: str) -> str:
    """Return a bcrypt hash of *plain*."""
    return pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    """Return True if *plain* matches the stored bcrypt *hashed* value."""
    try:
        return pwd_context.verify(plain, hashed)
    except Exception as exc:
        logger.error("Password verification error: %s", exc)
        return False


# ── JWT helpers ───────────────────────────────────────────────────────────────

def create_access_token(user_id: int, email: str) -> str:
    """
    Issue a signed JWT containing the user's ID and email.

    Returns
    -------
    str — compact serialised JWT string
    """
    expire = datetime.now(tz=timezone.utc) + timedelta(minutes=JWT_EXPIRE_MINUTES)
    payload = {
        "sub": str(user_id),
        "email": email,
        "exp": expire,
        "iat": datetime.now(tz=timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def verify_access_token(token: str) -> Optional[dict]:
    """
    Decode and validate a JWT string.

    Returns
    -------
    dict with keys ``sub`` (user_id str) and ``email`` if valid,
    or ``None`` if expired / malformed.
    """
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


# ── User CRUD ─────────────────────────────────────────────────────────────────

async def register_user(db: AsyncSession, email: str, password: str) -> dict:
    """
    Create a new user.

    Parameters
    ----------
    db       : active AsyncSession from FastAPI dependency injection
    email    : must be unique
    password : plain-text; hashed with bcrypt before storage

    Returns
    -------
    dict with ``success``, ``message``, and on success ``user_id`` / ``email``
    """
    if not email or not password:
        return {"success": False, "message": "Email and password are required"}

    if len(password) < 6:
        return {"success": False, "message": "Password must be at least 6 characters"}

    # Duplicate check
    result = await db.execute(select(User).where(User.email == email))
    if result.scalar_one_or_none() is not None:
        return {"success": False, "message": "Email already registered. Please log in instead."}

    user = User(email=email, password_hash=hash_password(password))
    db.add(user)
    try:
        await db.flush()   # populate user.id without committing
        await db.refresh(user)
    except IntegrityError:
        await db.rollback()
        return {"success": False, "message": "Email already registered. Please log in instead."}

    logger.info("User registered: %s (id=%d)", email, user.id)
    return {
        "success": True,
        "message": "Registration successful! Please log in.",
        "user_id": user.id,
        "email": user.email,
    }


async def login_user(db: AsyncSession, email: str, password: str) -> dict:
    """
    Authenticate a user and return a signed JWT on success.

    Returns
    -------
    dict with ``success``, ``message``, ``access_token`` (str), and ``user`` (dict)
    """
    if not email or not password:
        return {"success": False, "message": "Email and password are required"}

    result = await db.execute(select(User).where(User.email == email))
    user: Optional[User] = result.scalar_one_or_none()

    if user is None or not verify_password(password, user.password_hash):
        logger.warning("Failed login attempt for: %s", email)
        return {"success": False, "message": "Invalid email or password"}

    token = create_access_token(user.id, user.email)
    logger.info("User logged in: %s (id=%d)", email, user.id)
    return {
        "success": True,
        "message": "Login successful!",
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "email": user.email,
            "created_at": user.created_at.isoformat(),
            "updated_at": user.updated_at.isoformat(),
        },
    }


async def get_user_by_id(db: AsyncSession, user_id: int) -> Optional[User]:
    """Fetch a User row by primary key."""
    return await db.get(User, user_id)


async def get_user_by_email(db: AsyncSession, email: str) -> Optional[User]:
    """Fetch a User row by email address."""
    result = await db.execute(select(User).where(User.email == email))
    return result.scalar_one_or_none()