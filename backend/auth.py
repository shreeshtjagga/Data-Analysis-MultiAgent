import logging
import os
from typing import Optional
from dotenv import load_dotenv
from email_validator import EmailNotValidError, validate_email
from supabase import create_client, Client
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from .db import User
import asyncio

load_dotenv()
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv('SUPABASE_URL', '').strip()
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY', '').strip()
SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY', '').strip()

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError(
        'CRITICAL: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set. '
        'Get them from Supabase Dashboard → Settings → API.'
    )

_supabase_admin: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
_supabase_public: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)


<<<<<<< HEAD
async def verify_access_token(token: str) -> Optional[dict]:
    """Validate a Supabase JWT and return {sub, email}, or None if invalid."""
    try:
        resp = await asyncio.to_thread(_supabase_admin.auth.get_user, token)
        user = resp.user
        if user is None:
            return None
        return {'sub': str(user.id), 'email': user.email or ''}
    except Exception as exc:
        logger.debug('Supabase token verification failed: %s', exc)
        return None
=======
JWT_SECRET = os.getenv("JWT_SECRET", "change_this_secret_in_production")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "1440"))  # 24 h

APP_ENV = os.getenv("APP_ENV", "production")
REGISTER_CHECK_DELIVERABILITY = APP_ENV != "development"
if APP_ENV == "production" and JWT_SECRET == "change_this_secret_in_production":
    raise RuntimeError("CRITICAL: Default JWT_SECRET is being used in production!")


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
>>>>>>> 353dae187ecc0b8c820894f8f192840c351ab417


def normalize_email(email: str, *, check_deliverability: bool = False) -> Optional[str]:
    if not email:
        return None
    try:
        info = validate_email(email.strip(), check_deliverability=check_deliverability)
        return info.normalized.lower()
    except EmailNotValidError:
        return None


async def _get_or_create_profile(
    db: AsyncSession,
    supabase_user_id: str,
    email: str,
    name: Optional[str] = None,
) -> User:
    """
    Fetch the local User row for the given Supabase UUID, creating it if absent.
    NOTE: Does NOT call db.commit() — the caller (or get_db dependency) owns commit.
    """
    result = await db.execute(select(User).where(User.supabase_id == supabase_user_id))
    user = result.scalar_one_or_none()
    if user is not None:
        return user

    # Also check by email (handles the case where account existed before supabase migration)
    result = await db.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()
    if user is not None:
        # Attach the supabase_id to the existing row
        if not user.supabase_id:
            user.supabase_id = supabase_user_id
            await db.flush()
        return user

    user = User(supabase_id=supabase_user_id, email=email, name=name or None)
    db.add(user)
    await db.flush()
    await db.refresh(user)
    logger.info('Local profile created for Supabase user: %s', email)
    return user


<<<<<<< HEAD
async def register_user(
    db: AsyncSession,
    email: str,
    password: str,
    name: Optional[str] = None,
) -> dict:
=======
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


def _frontend_base_url() -> str:
    configured = os.getenv("FRONTEND_URL", "").strip().rstrip("/")
    if configured:
        return configured

    cors_origins = os.getenv("CORS_ORIGINS", "")
    for origin in cors_origins.split(","):
        origin = origin.strip().rstrip("/")
        if origin:
            return origin

    return "http://localhost:5173"



async def register_user(db: AsyncSession, email: str, password: str, name: Optional[str] = None) -> dict:
>>>>>>> 353dae187ecc0b8c820894f8f192840c351ab417
    if not email or not password:
        return {'success': False, 'message': 'Email and password are required'}

<<<<<<< HEAD
    normalized_email = normalize_email(email, check_deliverability=False)
    if not normalized_email:
        return {'success': False, 'message': 'Please enter a valid email address'}
=======
    normalized_email = normalize_email(email, check_deliverability=REGISTER_CHECK_DELIVERABILITY)
    if not normalized_email:
        if REGISTER_CHECK_DELIVERABILITY:
            return {"success": False, "message": "Please enter a valid, deliverable email address"}
        return {"success": False, "message": "Please enter a valid email address"}
>>>>>>> 353dae187ecc0b8c820894f8f192840c351ab417

    if len(password) < 6:
        return {'success': False, 'message': 'Password must be at least 6 characters'}

    try:
<<<<<<< HEAD
        resp = await asyncio.to_thread(
            _supabase_admin.auth.admin.create_user,
            {
                'email': normalized_email,
                'password': password,
                'email_confirm': True,
                'user_metadata': {'name': name or ''},
            },
        )
        supabase_user = resp.user
        if supabase_user is None:
            return {'success': False, 'message': 'Registration failed. Please try again.'}
    except Exception as exc:
        err_str = str(exc).lower()
        if 'already registered' in err_str or 'already exists' in err_str or 'duplicate' in err_str:
            return {'success': False, 'message': 'Email already registered. Please log in instead.'}
        logger.error('Supabase register_user failed: %s', exc)
        return {'success': False, 'message': 'Registration failed. Please try again.'}
=======
        await db.flush()
        await db.refresh(user)
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        logger.exception("User registration failed for %s", normalized_email)
        detail = str(getattr(exc, "orig", exc)).lower()
        if "email" in detail and ("unique" in detail or "duplicate" in detail):
            return {"success": False, "message": "Email already registered. Please log in instead."}
        return {"success": False, "message": "Could not create account due to a database error. Please try again."}
>>>>>>> 353dae187ecc0b8c820894f8f192840c351ab417

    try:
        user = await _get_or_create_profile(db, str(supabase_user.id), normalized_email, name)
        await db.commit()
        await db.refresh(user)
        logger.info('User registered via Supabase: %s (local_id=%d)', normalized_email, user.id)
        return {
            'success': True,
            'message': 'Registration successful! Please log in.',
            'user_id': user.id,
            'name': user.name,
            'email': user.email,
            'created_at': user.created_at,
            'updated_at': user.updated_at,
        }
    except Exception as exc:
        await db.rollback()
        logger.error('Failed to create local profile after registration: %s', exc)
        # Supabase user was created — still return success so user can log in
        return {
            'success': True,
            'message': 'Registration successful! Please log in.',
            'user_id': None,
            'name': name,
            'email': normalized_email,
            'created_at': None,
            'updated_at': None,
        }


async def login_user(db: AsyncSession, email: str, password: str) -> dict:
    if not email or not password:
        return {'success': False, 'message': 'Email and password are required'}

    normalized_email = normalize_email(email, check_deliverability=False)
    if not normalized_email:
        return {'success': False, 'message': 'Invalid email or password'}

    try:
        # supabase-py v2: sign_in_with_password accepts a credentials dict
        resp = await asyncio.to_thread(
            _supabase_public.auth.sign_in_with_password,
            {'email': normalized_email, 'password': password},
        )
        session = resp.session
        supabase_user = resp.user
        if session is None or supabase_user is None:
            return {'success': False, 'message': 'Invalid email or password'}
    except Exception as exc:
        err_str = str(exc).lower()
        if any(k in err_str for k in ('invalid', 'credentials', 'wrong', 'not found', 'bad')):
            logger.warning('FAILED LOGIN for email: %s | %s', normalized_email, exc)
            return {'success': False, 'message': 'Invalid email or password'}
        if 'rate' in err_str or 'limit' in err_str or 'too many' in err_str:
            logger.warning('Rate limited login for: %s', normalized_email)
            return {'success': False, 'message': 'Too many login attempts. Please wait a moment and try again.'}
        if 'email' in err_str and 'confirm' in err_str:
            return {'success': False, 'message': 'Please verify your email address before logging in.'}
        logger.error('Supabase login_user error for %s: %s', normalized_email, exc)
        return {'success': False, 'message': 'Login failed. Please try again.'}

    name = (supabase_user.user_metadata or {}).get('name') or None

    try:
        user = await _get_or_create_profile(db, str(supabase_user.id), normalized_email, name)
        await db.commit()
        await db.refresh(user)
    except Exception as exc:
        await db.rollback()
        logger.error('Failed to sync local profile on login for %s: %s', normalized_email, exc)
        # Don't fail login just because local DB sync failed — return token anyway
        # The /auth/me and /auth/sync-session endpoints will retry profile creation
        return {
            'success': True,
            'message': 'Login successful!',
            'access_token': session.access_token,
            'refresh_token': session.refresh_token,
            'token_type': 'bearer',
            'user': {
                'id': 0,
                'name': name,
                'email': normalized_email,
                'created_at': None,
                'updated_at': None,
            },
        }

    logger.info('User logged in via Supabase: %s (local_id=%d)', normalized_email, user.id)
    return {
        'success': True,
        'message': 'Login successful!',
        'access_token': session.access_token,
        'refresh_token': session.refresh_token,
        'token_type': 'bearer',
        'user': {
            'id': user.id,
            'name': user.name,
            'email': user.email,
            'created_at': user.created_at,
            'updated_at': user.updated_at,
        },
    }


async def request_password_reset(db: AsyncSession, email: str) -> dict:
    generic_message = 'If an account exists for that email, a password reset link has been sent.'
    normalized_email = normalize_email(email, check_deliverability=False)
    if not normalized_email:
        return {'success': True, 'message': generic_message}

    try:
        frontend_url = os.getenv('FRONTEND_URL', 'http://localhost:5173').strip().rstrip('/')
        redirect_to = f'{frontend_url}/auth/callback'
        # reset_password_for_email triggers Supabase to send the actual recovery email.
        # The link in the email redirects to /auth/callback#access_token=...&type=recovery
        # which AuthCallback.jsx intercepts and routes to /reset-password.
        await asyncio.to_thread(
            _supabase_public.auth.reset_password_for_email,
            normalized_email,
            {'redirect_to': redirect_to},
        )
        logger.info('Password reset email dispatched by Supabase for: %s', normalized_email)
    except Exception as exc:
        logger.warning('Password reset request failed (non-fatal): %s', exc)

<<<<<<< HEAD
    return {'success': True, 'message': generic_message, 'email_sent': True}
=======
    token = create_password_reset_token(user.email)
    frontend_url = _frontend_base_url()
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
>>>>>>> 353dae187ecc0b8c820894f8f192840c351ab417


async def reset_password_with_token(db: AsyncSession, token: str, new_password: str) -> dict:
    """
    Legacy backend endpoint — kept for compatibility.
    The frontend now uses supabase.auth.updateUser() directly after the recovery
    session is established, so this endpoint is not actively called.
    """
    try:
        resp = await asyncio.to_thread(
            _supabase_public.auth.verify_otp,
            {'token_hash': token, 'type': 'recovery'},
        )
        session = resp.session
        if session is None:
            return {'success': False, 'message': 'Invalid or expired reset token'}
        user_client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        await asyncio.to_thread(user_client.auth.set_session, session.access_token, session.refresh_token)
        await asyncio.to_thread(user_client.auth.update_user, {'password': new_password})
    except Exception as exc:
        logger.error('Password reset failed: %s', exc)
        return {'success': False, 'message': 'Invalid or expired reset token'}
    return {'success': True, 'message': 'Password reset successful. Please log in with your new password.'}


async def refresh_session(refresh_token: str) -> Optional[dict]:
    try:
        resp = await asyncio.to_thread(_supabase_public.auth.refresh_session, refresh_token)
        session = resp.session
        if session is None:
            return None
        return {'access_token': session.access_token, 'refresh_token': session.refresh_token}
    except Exception as exc:
        logger.debug('Supabase refresh_session failed: %s', exc)
        return None


async def get_user_by_id(db: AsyncSession, user_id: int) -> Optional[User]:
    return await db.get(User, user_id)