import hashlib
import secrets
import logging
from datetime import datetime
from db import get_db_connection, close_db_connection
from models.schemas import UserLogin, UserRegister, UserResponse, AuthResponse
import json

logger = logging.getLogger(__name__)


def hash_password(password: str) -> str:
    """Hash a password using SHA256 with a salt."""
    salt = secrets.token_hex(16)
    hash_obj = hashlib.sha256((salt + password).encode())
    hashed = hash_obj.hexdigest()
    return f"{salt}${hashed}"


def verify_password(stored_hash: str, provided_password: str) -> bool:
    """Verify a password against its hash."""
    try:
        salt, hashed = stored_hash.split("$")
        hash_obj = hashlib.sha256((salt + provided_password).encode())
        return hash_obj.hexdigest() == hashed
    except Exception as e:
        logger.error(f"Password verification error: {e}")
        return False


def register_user(email: str, password: str) -> dict:
    """
    Register a new user in the database.
    
    Args:
        email: User email (must be unique)
        password: User password (plain text, will be hashed)
    
    Returns:
        dict with success status and message
    """
    try:
        if not email or not password:
            return {
                "success": False,
                "message": "Email and password are required"
            }

        if len(password) < 6:
            return {
                "success": False,
                "message": "Password must be at least 6 characters long"
            }

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if user already exists
        cursor.execute("SELECT id FROM users WHERE email = ?", (email,))
        if cursor.fetchone():
            close_db_connection(conn)
            return {
                "success": False,
                "message": "Email already registered. Please log in instead."
            }

        # Hash password and insert user
        hashed_password = hash_password(password)
        cursor.execute(
            "INSERT INTO users (email, password) VALUES (?, ?)",
            (email, hashed_password)
        )
        conn.commit()

        user_id = cursor.lastrowid
        close_db_connection(conn)

        logger.info(f"User registered successfully: {email}")
        return {
            "success": True,
            "message": "Registration successful! Please log in.",
            "user_id": user_id,
            "email": email
        }

    except Exception as e:
        logger.error(f"Registration error: {e}")
        return {
            "success": False,
            "message": f"Registration failed: {str(e)}"
        }


def login_user(email: str, password: str) -> dict:
    """
    Authenticate a user and return user details if credentials match.
    
    Args:
        email: User email
        password: User password (plain text)
    
    Returns:
        dict with success status, message, and user details if successful
    """
    try:
        if not email or not password:
            return {
                "success": False,
                "message": "Email and password are required"
            }

        conn = get_db_connection()
        cursor = conn.cursor()

        # Fetch user
        cursor.execute(
            "SELECT id, email, password, created_at, updated_at FROM users WHERE email = ?",
            (email,)
        )
        user = cursor.fetchone()
        close_db_connection(conn)

        if not user:
            logger.warning(f"Login attempt for non-existent user: {email}")
            return {
                "success": False,
                "message": "Invalid email or password"
            }

        # Verify password
        if not verify_password(user["password"], password):
            logger.warning(f"Failed login attempt for user: {email}")
            return {
                "success": False,
                "message": "Invalid email or password"
            }

        logger.info(f"User logged in successfully: {email}")
        return {
            "success": True,
            "message": "Login successful!",
            "user": {
                "id": user["id"],
                "email": user["email"],
                "created_at": user["created_at"],
                "updated_at": user["updated_at"]
            }
        }

    except Exception as e:
        logger.error(f"Login error: {e}")
        return {
            "success": False,
            "message": f"Login failed: {str(e)}"
        }


def get_user_by_email(email: str) -> dict:
    """Get user details by email."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT id, email, created_at, updated_at FROM users WHERE email = ?",
            (email,)
        )
        user = cursor.fetchone()
        close_db_connection(conn)

        if not user:
            return None

        return {
            "id": user["id"],
            "email": user["email"],
            "created_at": user["created_at"],
            "updated_at": user["updated_at"]
        }

    except Exception as e:
        logger.error(f"Get user error: {e}")
        return None


def get_user_by_id(user_id: int) -> dict:
    """Get user details by user ID."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT id, email, created_at, updated_at FROM users WHERE id = ?",
            (user_id,)
        )
        user = cursor.fetchone()
        close_db_connection(conn)

        if not user:
            return None

        return {
            "id": user["id"],
            "email": user["email"],
            "created_at": user["created_at"],
            "updated_at": user["updated_at"]
        }

    except Exception as e:
        logger.error(f"Get user by ID error: {e}")
        return None
