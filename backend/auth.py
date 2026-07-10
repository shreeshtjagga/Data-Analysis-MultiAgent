import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from dotenv import load_dotenv
from email_validator import EmailNotValidError, validate_email
from passlib.context import CryptContext
from jose import JWTError, jwt
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from .db import User

load_dotenv()
logger = logging.getLogger(__name__)

JWT_SECRET = os.getenv('JWT_SECRET', 'datapulse_jwt_secret_change_in_prod_2024')
JWT_ALGORITHM = os.getenv('JWT_ALGORITHM', 'HS256')
JWT_EXPIRE_MINUTES = int(os.getenv('JWT_EXPIRE_MINUTES', '1440'))  # 24h
REFRESH_SECRET = os.getenv('REFRESH_SECRET', JWT_SECRET)
REFRESH_EXPIRE_DAYS = int(os.getenv('REFRESH_EXPIRE_DAYS', '7'))

pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')


def _supabase_sign_in(email: str, password: str) -> Optional[dict]:
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_ANON_KEY') or os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    if not url or not key:
        return None
    try:
        import requests
        auth_url = f"{url.rstrip('/')}/auth/v1/token?grant_type=password"
        resp = requests.post(
            auth_url,
            headers={'apikey': key, 'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'},
            json={'email': email, 'password': password},
            timeout=10,
        )
        if resp.status_code >= 400:
            return None
        data = resp.json()
        user = data.get('user') or {}
        if not user:
            return None
        user_id = user.get('id')
        user_email = (user.get('email') or email).lower()
        metadata = user.get('user_metadata') or {}
        name = metadata.get('name') or metadata.get('full_name') or user_email.split('@')[0]
        return {'supabase_id': str(user_id) if user_id else None, 'email': user_email, 'name': name}
    except Exception as exc:
        logger.debug('Supabase password fallback failed for %s: %s', email, exc)
        return None


def normalize_email(email: str, *, check_deliverability: bool = False) -> Optional[str]:
    if not email:
        return None
    try:
        info = validate_email(email.strip(), check_deliverability=check_deliverability)
        return info.normalized.lower()
    except EmailNotValidError:
        return None


def _hash_password(plain: str) -> str:
    return pwd_context.hash(plain)


def _verify_password(plain: str, hashed: str) -> bool:
    try:
        return pwd_context.verify(plain, hashed)
    except Exception:
        return False


def _create_access_token(user_id: int, email: str) -> str:
    expire = datetime.now(tz=timezone.utc) + timedelta(minutes=JWT_EXPIRE_MINUTES)
    payload = {'sub': str(user_id), 'email': email, 'exp': expire, 'iat': datetime.now(tz=timezone.utc)}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def _create_refresh_token(user_id: int, email: str) -> str:
    expire = datetime.now(tz=timezone.utc) + timedelta(days=REFRESH_EXPIRE_DAYS)
    payload = {'sub': str(user_id), 'email': email, 'exp': expire, 'iat': datetime.now(tz=timezone.utc), 'typ': 'refresh'}
    return jwt.encode(payload, REFRESH_SECRET, algorithm=JWT_ALGORITHM)


async def verify_access_token(token: str) -> Optional[dict]:
    """Validate a local JWT and return {sub, email}, or None if invalid."""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        user_id = payload.get('sub')
        email = payload.get('email')
        if user_id is None or email is None:
            return None
        return {'sub': str(user_id), 'email': email}
    except JWTError as exc:
        logger.debug('JWT verification failed: %s', exc)
        return None


async def register_user(db: AsyncSession, email: str, password: str, name: Optional[str] = None) -> dict:
    if not email or not password:
        return {'success': False, 'message': 'Email and password are required'}

    normalized_email = normalize_email(email, check_deliverability=False)
    if not normalized_email:
        return {'success': False, 'message': 'Please enter a valid email address'}

    if len(password) < 6:
        return {'success': False, 'message': 'Password must be at least 6 characters'}

    try:
        # Check if email already exists
        result = await db.execute(select(User).where(User.email == normalized_email))
        existing = result.scalar_one_or_none()
        if existing:
            if not existing.password_hash:
                existing.password_hash = _hash_password(password)
                if name and not existing.name:
                    existing.name = name
                await db.commit()
                await db.refresh(existing)
                logger.info('Added local password credentials for existing user: %s (id=%d)', normalized_email, existing.id)
                return {
                    'success': True,
                    'message': 'Registration successful! Please log in.',
                    'user_id': existing.id,
                    'name': existing.name,
                    'email': existing.email,
                    'created_at': existing.created_at,
                    'updated_at': existing.updated_at,
                }
            return {'success': False, 'message': 'Email already registered. Please log in instead.'}

        hashed = _hash_password(password)
        user = User(email=normalized_email, name=name or None, password_hash=hashed)
        db.add(user)
        await db.flush()
        await db.refresh(user)
        await db.commit()
        logger.info('User registered locally: %s (id=%d)', normalized_email, user.id)
        return {
            'success': True,
            'message': 'Registration successful! Please log in.',
            'user_id': user.id,
            'name': user.name,
            'email': user.email,
            'created_at': user.created_at,
            'updated_at': user.updated_at,
        }
    except IntegrityError:
        await db.rollback()
        return {'success': False, 'message': 'Email already registered. Please log in instead.'}
    except Exception as exc:
        await db.rollback()
        logger.error('register_user failed: %s', exc)
        return {'success': False, 'message': 'Registration failed. Please try again.'}


async def login_user(db: AsyncSession, email: str, password: str) -> dict:
    if not email or not password:
        return {'success': False, 'message': 'Email and password are required'}

    normalized_email = normalize_email(email, check_deliverability=False)
    if not normalized_email:
        return {'success': False, 'message': 'Invalid email or password'}

    try:
        result = await db.execute(select(User).where(User.email == normalized_email))
        user = result.scalar_one_or_none()
        if user is not None and user.password_hash and _verify_password(password, user.password_hash):
            access_token = _create_access_token(user.id, user.email)
            refresh_token = _create_refresh_token(user.id, user.email)
            logger.info('User logged in locally: %s (id=%d)', normalized_email, user.id)
            return {
                'success': True,
                'message': 'Login successful!',
                'access_token': access_token,
                'refresh_token': refresh_token,
                'token_type': 'bearer',
                'user': {
                    'id': user.id,
                    'name': user.name,
                    'email': user.email,
                    'created_at': user.created_at,
                    'updated_at': user.updated_at,
                },
            }

        supabase_user = _supabase_sign_in(normalized_email, password)
        if supabase_user is None:
            logger.warning('FAILED LOGIN for email: %s', normalized_email)
            return {'success': False, 'message': 'Invalid email or password'}

        if user is None:
            user = User(
                email=supabase_user['email'],
                name=supabase_user.get('name'),
                password_hash=_hash_password(password),
                supabase_id=supabase_user.get('supabase_id'),
            )
            db.add(user)
        else:
            user.password_hash = _hash_password(password)
            if supabase_user.get('supabase_id'):
                user.supabase_id = supabase_user['supabase_id']
            if supabase_user.get('name') and not user.name:
                user.name = supabase_user['name']
        await db.commit()
        await db.refresh(user)
        logger.info('Synced Supabase credentials locally: %s (id=%d)', normalized_email, user.id)

        access_token = _create_access_token(user.id, user.email)
        refresh_token = _create_refresh_token(user.id, user.email)
        logger.info('User logged in locally: %s (id=%d)', normalized_email, user.id)
        return {
            'success': True,
            'message': 'Login successful!',
            'access_token': access_token,
            'refresh_token': refresh_token,
            'token_type': 'bearer',
            'user': {
                'id': user.id,
                'name': user.name,
                'email': user.email,
                'created_at': user.created_at,
                'updated_at': user.updated_at,
            },
        }
    except Exception as exc:
        logger.error('login_user failed for %s: %s', normalized_email, exc)
        return {'success': False, 'message': 'Login failed. Please try again.'}


async def request_password_reset(db: AsyncSession, email: str) -> dict:
    return {'success': True, 'message': 'If an account exists for that email, a password reset link has been sent.', 'email_sent': False}


async def reset_password_with_token(db: AsyncSession, token: str, new_password: str) -> dict:
    return {'success': False, 'message': 'Password reset via token is not supported in local mode.'}


async def refresh_session(refresh_token: str) -> Optional[dict]:
    try:
        payload = jwt.decode(refresh_token, REFRESH_SECRET, algorithms=[JWT_ALGORITHM])
        if payload.get('typ') != 'refresh':
            return None
        user_id = payload.get('sub')
        email = payload.get('email')
        if not user_id or not email:
            return None
        new_access = _create_access_token(int(user_id), email)
        new_refresh = _create_refresh_token(int(user_id), email)
        return {'access_token': new_access, 'refresh_token': new_refresh}
    except JWTError as exc:
        logger.debug('Refresh token verification failed: %s', exc)
        return None


async def get_user_by_id(db: AsyncSession, user_id: int) -> Optional[User]:
    return await db.get(User, user_id)
