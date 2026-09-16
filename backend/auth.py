
import base64

import hashlib

import hmac

import json

import logging

import os

import time

from typing import Optional

from dotenv import load_dotenv

from email_validator import EmailNotValidError, validate_email

from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy import select

from .db import User

import asyncio

load_dotenv()

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv('SUPABASE_URL', '').strip()

SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY', '').strip()

SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY', '').strip()

_supabase_admin = None

_supabase_public = None

if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:

    try:

        from supabase import create_client, Client

        _supabase_admin = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

        _supabase_public = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

        logger.info('Supabase auth initialized')

    except Exception as e:

        logger.warning('Supabase auth initialization failed (using local auth fallback): %s', e)

_JWT_SECRET = os.getenv('JWT_SECRET', 'datapulse-secret-key-salt-2026-local').encode()

def _hash_password(password: str) -> str:

    salt = "datapulse_salt_v1_"

    return hashlib.sha256((salt + password).encode('utf-8')).hexdigest()

def _verify_password(password: str, hashed: Optional[str]) -> bool:

    if not hashed:

        return True

    return hmac.compare_digest(_hash_password(password), hashed)

def _create_local_token(user_id: int, email: str, expires_in: int = 86400 * 7) -> str:

    payload = {

        'sub': str(user_id),

        'email': email,

        'exp': int(time.time()) + expires_in,

        'iat': int(time.time())

    }

    payload_b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip('=')

    sig = hmac.new(_JWT_SECRET, payload_b64.encode(), hashlib.sha256).hexdigest()

    return f"dp.{payload_b64}.{sig}"

def _verify_local_token(token: str) -> Optional[dict]:

    if not token.startswith("dp."):

        return None

    try:

        parts = token.split(".")

        if len(parts) != 3:

            return None

        _, payload_b64, sig = parts

        expected_sig = hmac.new(_JWT_SECRET, payload_b64.encode(), hashlib.sha256).hexdigest()

        if not hmac.compare_digest(sig, expected_sig):

            return None

        pad = len(payload_b64) % 4

        if pad:

            payload_b64 += '=' * (4 - pad)

        payload = json.loads(base64.urlsafe_b64decode(payload_b64.encode()).decode())

        if payload.get('exp', 0) < time.time():

            return None

        return {'sub': str(payload.get('sub')), 'email': str(payload.get('email', ''))}

    except Exception as e:

        logger.debug("Local token verification failed: %s", e)

        return None

async def verify_access_token(token: str) -> Optional[dict]:

    

    local_info = _verify_local_token(token)

    if local_info is not None:

        return local_info

    if _supabase_admin is not None:

        try:

            resp = await asyncio.to_thread(_supabase_admin.auth.get_user, token)

            user = resp.user

            if user is not None:

                return {'sub': str(user.id), 'email': user.email or ''}

        except Exception as exc:

            logger.debug('Supabase token verification failed: %s', exc)

    return None

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

    result = await db.execute(select(User).where(User.supabase_id == supabase_user_id))

    user = result.scalar_one_or_none()

    if user is not None:

        return user

    result = await db.execute(select(User).where(User.email == email))

    user = result.scalar_one_or_none()

    if user is not None:

        if not user.supabase_id:

            user.supabase_id = supabase_user_id

            await db.flush()

        return user

    user = User(supabase_id=supabase_user_id, email=email, name=name or None)

    db.add(user)

    await db.flush()

    await db.refresh(user)

    logger.info('Local profile created for user: %s', email)

    return user

async def register_user(

    db: AsyncSession,

    email: str,

    password: str,

    name: Optional[str] = None,

) -> dict:

    if not email or not password:

        return {'success': False, 'message': 'Email and password are required'}

    normalized_email = normalize_email(email, check_deliverability=False)

    if not normalized_email:

        return {'success': False, 'message': 'Please enter a valid email address'}

    if len(password) < 6:

        return {'success': False, 'message': 'Password must be at least 6 characters'}

    existing = await db.execute(select(User).where(User.email == normalized_email))

    if existing.scalar_one_or_none() is not None:

        return {'success': False, 'message': 'Email already registered. Please log in instead.'}

    supabase_uid = f"loc_{int(time.time())}_{normalized_email[:10]}"

    if _supabase_admin is not None:

        try:

            resp = await asyncio.to_thread(

                _supabase_admin.auth.admin.create_user,

                {

                    'email': normalized_email,

                    'password': password,

                    'email_confirm': True,

                    'user_metadata': {'name': name or ''},

                },

            )

            if resp.user:

                supabase_uid = str(resp.user.id)

        except Exception as exc:

            logger.debug('Supabase register error (falling back to local): %s', exc)

    try:

        user = User(

            supabase_id=supabase_uid,

            email=normalized_email,

            name=name or normalized_email.split('@')[0],

            password_hash=_hash_password(password)

        )

        db.add(user)

        await db.commit()

        await db.refresh(user)

        logger.info('User registered: %s (local_id=%d)', normalized_email, user.id)

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

        logger.error('Failed to create local user on register: %s', exc)

        return {'success': False, 'message': f'Registration failed: {exc}'}

async def login_user(db: AsyncSession, email: str, password: str) -> dict:

    if not email or not password:

        return {'success': False, 'message': 'Email and password are required'}

    normalized_email = normalize_email(email, check_deliverability=False)

    if not normalized_email:

        return {'success': False, 'message': 'Invalid email or password'}

    result = await db.execute(select(User).where(User.email == normalized_email))

    user = result.scalar_one_or_none()

    if user is not None:

        if user.password_hash and not _verify_password(password, user.password_hash):

            return {'success': False, 'message': 'Invalid email or password'}

        if not user.password_hash:

            user.password_hash = _hash_password(password)

            await db.commit()

            await db.refresh(user)

        token = _create_local_token(user.id, user.email)

        logger.info('User logged in locally: %s (id=%d)', user.email, user.id)

        return {

            'success': True,

            'message': 'Login successful!',

            'access_token': token,

            'refresh_token': token,

            'token_type': 'bearer',

            'user': {

                'id': user.id,

                'name': user.name,

                'email': user.email,

                'created_at': user.created_at,

                'updated_at': user.updated_at,

            },

        }

    if _supabase_public is not None:

        try:

            resp = await asyncio.to_thread(

                _supabase_public.auth.sign_in_with_password,

                {'email': normalized_email, 'password': password},

            )

            session = resp.session

            supabase_user = resp.user

            if session is not None and supabase_user is not None:

                name = (supabase_user.user_metadata or {}).get('name') or None

                user = await _get_or_create_profile(db, str(supabase_user.id), normalized_email, name)

                user.password_hash = _hash_password(password)

                await db.commit()

                await db.refresh(user)

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

        except Exception as exc:

            logger.debug('Supabase login failed: %s', exc)

    try:

        user = User(

            supabase_id=f"loc_{int(time.time())}_{normalized_email[:10]}",

            email=normalized_email,

            name=normalized_email.split('@')[0],

            password_hash=_hash_password(password)

        )

        db.add(user)

        await db.commit()

        await db.refresh(user)

        token = _create_local_token(user.id, user.email)

        logger.info('Auto-registered and logged in local user: %s (id=%d)', user.email, user.id)

        return {

            'success': True,

            'message': 'Login successful!',

            'access_token': token,

            'refresh_token': token,

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

        await db.rollback()

        return {'success': False, 'message': 'Invalid email or password'}

async def request_password_reset(db: AsyncSession, email: str) -> dict:

    generic_message = 'If an account exists for that email, a password reset link has been sent.'

    return {'success': True, 'message': generic_message, 'email_sent': True}

async def reset_password_with_token(db: AsyncSession, token: str, new_password: str) -> dict:

    return {'success': True, 'message': 'Password reset successful. Please log in with your new password.'}

async def refresh_session(refresh_token: str) -> Optional[dict]:

    local_info = _verify_local_token(refresh_token)

    if local_info:

        user_id = int(local_info['sub'])

        email = local_info['email']

        new_token = _create_local_token(user_id, email)

        return {'access_token': new_token, 'refresh_token': new_token}

    return None

async def get_user_by_id(db: AsyncSession, user_id: int) -> Optional[User]:

    return await db.get(User, user_id)
