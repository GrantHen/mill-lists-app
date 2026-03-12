"""
Authentication module for Mill Lists application.

Handles:
  - User registration with password policy enforcement
  - Password hashing via bcrypt
  - Login with credential validation
  - Server-side session management
  - Password reset token generation/validation
  - Rate limiting for auth endpoints
  - Activity logging
"""
import os
import re
import secrets
import time
import hashlib
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict

import bcrypt

from database import get_db, dict_from_row

log = logging.getLogger("auth")

# ─── Configuration ────────────────────────────────────────────────────────────

SESSION_DURATION_HOURS = 72         # 3 days
RESET_TOKEN_DURATION_HOURS = 1     # 1 hour
MAX_LOGIN_ATTEMPTS = 5             # per window
LOGIN_WINDOW_SECONDS = 300         # 5 minutes
COOKIE_NAME = "tifp_session"
SESSION_SECRET = os.environ.get("SESSION_SECRET", secrets.token_hex(32))

# Admin emails from environment variable (comma-separated)
ADMIN_EMAILS = [
    e.strip().lower()
    for e in os.environ.get("ADMIN_EMAILS", "").split(",")
    if e.strip()
]


# ─── Password Policy ─────────────────────────────────────────────────────────

def validate_password(password: str) -> tuple:
    """
    Validate password against policy.
    Returns (is_valid, error_message).
    """
    if len(password) < 10:
        return False, "Password must be at least 10 characters."
    if not re.search(r'[!@#$%^&*()_+\-=\[\]{};\':"\\|,.<>\/?`~]', password):
        return False, "Password must contain at least 1 special character."
    if not re.search(r'\d', password):
        return False, "Password must contain at least 1 number."
    return True, ""


def validate_email(email: str) -> tuple:
    """Validate email format. Returns (is_valid, error_message)."""
    if not email or not re.match(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$', email):
        return False, "Please enter a valid email address."
    return True, ""


# ─── Password Hashing ────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    """Hash password using bcrypt."""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt(12)).decode('utf-8')


def verify_password(password: str, password_hash: str) -> bool:
    """Verify password against bcrypt hash."""
    try:
        return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
    except Exception:
        return False


# ─── Rate Limiting (in-memory) ────────────────────────────────────────────────

_rate_limits = defaultdict(list)  # ip -> list of timestamps


def check_rate_limit(ip: str, max_attempts=MAX_LOGIN_ATTEMPTS,
                     window=LOGIN_WINDOW_SECONDS) -> tuple:
    """
    Check if IP is rate-limited.
    Returns (allowed, seconds_until_reset).
    """
    now = time.time()
    # Clean old entries
    _rate_limits[ip] = [t for t in _rate_limits[ip] if now - t < window]

    if len(_rate_limits[ip]) >= max_attempts:
        oldest = _rate_limits[ip][0]
        wait = int(window - (now - oldest))
        return False, max(wait, 1)

    return True, 0


def record_attempt(ip: str):
    """Record a login attempt for rate limiting."""
    _rate_limits[ip].append(time.time())


# ─── User Registration ───────────────────────────────────────────────────────

def register_user(email: str, password: str, display_name: str = None) -> dict:
    """
    Register a new user.
    Returns dict with success/error info.
    """
    email = email.strip().lower()

    # Validate email
    valid, err = validate_email(email)
    if not valid:
        return {"success": False, "error": err}

    # Validate password
    valid, err = validate_password(password)
    if not valid:
        return {"success": False, "error": err}

    # Check if email already exists
    conn = get_db()
    existing = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if existing:
        conn.close()
        return {"success": False, "error": "An account with this email already exists."}

    # Hash password and create user
    pw_hash = hash_password(password)

    # Check if this email should be auto-promoted to admin
    role = 'admin' if email in ADMIN_EMAILS else 'user'

    try:
        cursor = conn.execute("""
            INSERT INTO users (email, password_hash, display_name, role, is_active)
            VALUES (?, ?, ?, ?, 1)
        """, (email, pw_hash, display_name or email.split('@')[0], role))
        user_id = cursor.lastrowid
        conn.commit()

        # Log activity
        log_activity(conn, user_id, 'register', f"New account created (role={role})")
        conn.close()

        log.info(f"User registered: {email} (role={role})")
        return {"success": True, "user_id": user_id, "role": role}

    except Exception as e:
        conn.close()
        log.error(f"Registration error: {e}")
        return {"success": False, "error": "Registration failed. Please try again."}


# ─── Login ────────────────────────────────────────────────────────────────────

def login_user(email: str, password: str, ip: str = None,
               user_agent: str = None) -> dict:
    """
    Authenticate user and create session.
    Returns dict with session_id on success.
    """
    email = email.strip().lower()

    # Rate limit check
    if ip:
        allowed, wait = check_rate_limit(ip)
        if not allowed:
            return {"success": False,
                    "error": f"Too many login attempts. Try again in {wait} seconds."}

    conn = get_db()
    user = conn.execute(
        "SELECT * FROM users WHERE email = ?", (email,)
    ).fetchone()

    if not user:
        if ip:
            record_attempt(ip)
        conn.close()
        return {"success": False, "error": "Invalid email or password."}

    user = dict(user)

    # Check password
    if not verify_password(password, user['password_hash']):
        if ip:
            record_attempt(ip)
        log_activity(conn, user['id'], 'login_failed', f"Bad password from {ip}")
        conn.close()
        return {"success": False, "error": "Invalid email or password."}

    # Check account status
    if not user['is_active']:
        conn.close()
        return {"success": False, "error": "Account is deactivated. Contact an administrator."}

    if user['is_suspended']:
        conn.close()
        return {"success": False, "error": "Account is suspended. Contact an administrator."}

    # Create session
    session_id = secrets.token_urlsafe(48)
    expires = datetime.utcnow() + timedelta(hours=SESSION_DURATION_HOURS)

    conn.execute("""
        INSERT INTO sessions (id, user_id, expires_at, ip_address, user_agent)
        VALUES (?, ?, ?, ?, ?)
    """, (session_id, user['id'], expires.isoformat(), ip, user_agent))

    # Update last login
    conn.execute(
        "UPDATE users SET last_login_at = datetime('now') WHERE id = ?",
        (user['id'],)
    )

    log_activity(conn, user['id'], 'login', f"Logged in from {ip}")
    conn.commit()
    conn.close()

    log.info(f"User logged in: {email}")
    return {
        "success": True,
        "session_id": session_id,
        "user_id": user['id'],
        "email": user['email'],
        "display_name": user['display_name'],
        "role": user['role'],
    }


# ─── Session Management ──────────────────────────────────────────────────────

def get_session_user(session_id: str) -> dict:
    """
    Validate a session and return the user dict.
    Returns None if session is invalid or expired.
    """
    if not session_id:
        return None

    conn = get_db()
    row = conn.execute("""
        SELECT s.*, u.email, u.display_name, u.role, u.is_active, u.is_suspended
        FROM sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.id = ? AND s.expires_at > datetime('now')
    """, (session_id,)).fetchone()
    conn.close()

    if not row:
        return None

    user = dict(row)
    if not user['is_active'] or user['is_suspended']:
        return None

    return {
        "id": user['user_id'],
        "email": user['email'],
        "display_name": user['display_name'],
        "role": user['role'],
    }


def logout_user(session_id: str):
    """Destroy a session."""
    if not session_id:
        return
    conn = get_db()
    # Get user_id before deletion for logging
    sess = conn.execute("SELECT user_id FROM sessions WHERE id = ?", (session_id,)).fetchone()
    if sess:
        log_activity(conn, sess['user_id'], 'logout', '')
    conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
    conn.commit()
    conn.close()


def cleanup_expired_sessions():
    """Remove expired sessions from the database."""
    conn = get_db()
    conn.execute("DELETE FROM sessions WHERE expires_at < datetime('now')")
    conn.commit()
    conn.close()


# ─── Password Reset ──────────────────────────────────────────────────────────

def create_reset_token(email: str) -> dict:
    """
    Generate a password reset token for the given email.
    Always returns success (to prevent email enumeration).
    """
    email = email.strip().lower()
    conn = get_db()
    user = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()

    if not user:
        conn.close()
        # Don't reveal that the email doesn't exist
        return {"success": True, "message": "If that email exists, a reset link has been generated."}

    token = secrets.token_urlsafe(48)
    expires = datetime.utcnow() + timedelta(hours=RESET_TOKEN_DURATION_HOURS)

    # Invalidate old tokens
    conn.execute(
        "UPDATE password_reset_tokens SET used = 1 WHERE user_id = ? AND used = 0",
        (user['id'],)
    )

    conn.execute("""
        INSERT INTO password_reset_tokens (user_id, token, expires_at)
        VALUES (?, ?, ?)
    """, (user['id'], token, expires.isoformat()))

    log_activity(conn, user['id'], 'password_reset_requested', '')
    conn.commit()
    conn.close()

    # In production, you would email this token. For now, return it for the UI.
    return {
        "success": True,
        "token": token,
        "message": "If that email exists, a reset link has been generated.",
    }


def reset_password(token: str, new_password: str) -> dict:
    """Reset password using a valid token."""
    valid, err = validate_password(new_password)
    if not valid:
        return {"success": False, "error": err}

    conn = get_db()
    row = conn.execute("""
        SELECT * FROM password_reset_tokens
        WHERE token = ? AND used = 0 AND expires_at > datetime('now')
    """, (token,)).fetchone()

    if not row:
        conn.close()
        return {"success": False, "error": "Invalid or expired reset token."}

    row = dict(row)
    pw_hash = hash_password(new_password)

    conn.execute(
        "UPDATE users SET password_hash = ?, updated_at = datetime('now') WHERE id = ?",
        (pw_hash, row['user_id'])
    )
    conn.execute(
        "UPDATE password_reset_tokens SET used = 1 WHERE id = ?",
        (row['id'],)
    )

    # Invalidate all sessions for this user (force re-login)
    conn.execute("DELETE FROM sessions WHERE user_id = ?", (row['user_id'],))

    log_activity(conn, row['user_id'], 'password_reset', '')
    conn.commit()
    conn.close()

    return {"success": True}


# ─── Activity Logging ─────────────────────────────────────────────────────────

def log_activity(conn, user_id: int, action: str, details: str = "",
                 ip: str = None):
    """Log a user activity event."""
    try:
        conn.execute("""
            INSERT INTO activity_log (user_id, action, details, ip_address)
            VALUES (?, ?, ?, ?)
        """, (user_id, action, details, ip))
    except Exception as e:
        log.warning(f"Failed to log activity: {e}")
