"""
Admin module — user management and analytics APIs.

All functions assume authorization has already been checked by middleware.
"""
import logging
from database import get_db, dict_from_row, dicts_from_rows

log = logging.getLogger("admin")


# ─── User Management ──────────────────────────────────────────────────────────

def get_all_users(search: str = None, role: str = None,
                  status: str = None) -> list:
    """Get all users with optional filters."""
    conn = get_db()
    conditions = []
    params = []

    if search:
        conditions.append("(u.email LIKE ? OR u.display_name LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])
    if role:
        conditions.append("u.role = ?")
        params.append(role)
    if status == 'active':
        conditions.append("u.is_active = 1 AND u.is_suspended = 0")
    elif status == 'suspended':
        conditions.append("u.is_suspended = 1")
    elif status == 'inactive':
        conditions.append("u.is_active = 0")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    rows = conn.execute(f"""
        SELECT u.id, u.email, u.display_name, u.role,
               u.is_active, u.is_suspended, u.is_email_verified,
               u.created_at, u.last_login_at,
               (SELECT COUNT(*) FROM uploads WHERE user_id = u.id) as upload_count,
               (SELECT COUNT(*) FROM search_history WHERE user_id = u.id) as search_count,
               (SELECT COUNT(*) FROM activity_log WHERE user_id = u.id) as activity_count,
               (SELECT COUNT(*) FROM sessions WHERE user_id = u.id
                AND expires_at > datetime('now')) as active_sessions
        FROM users u
        {where}
        ORDER BY u.created_at DESC
    """, params).fetchall()

    conn.close()
    return dicts_from_rows(rows)


def get_user_detail(user_id: int) -> dict:
    """Get detailed info for a single user."""
    conn = get_db()

    user = conn.execute("""
        SELECT u.*,
               (SELECT COUNT(*) FROM uploads WHERE user_id = u.id) as upload_count,
               (SELECT COUNT(*) FROM search_history WHERE user_id = u.id) as search_count,
               (SELECT COUNT(*) FROM activity_log WHERE user_id = u.id) as activity_count
        FROM users u WHERE u.id = ?
    """, (user_id,)).fetchone()

    if not user:
        conn.close()
        return None

    user = dict(user)
    del user['password_hash']  # Never expose

    # Recent activity
    activity = conn.execute("""
        SELECT action, details, created_at
        FROM activity_log WHERE user_id = ?
        ORDER BY created_at DESC LIMIT 20
    """, (user_id,)).fetchall()
    user['recent_activity'] = dicts_from_rows(activity)

    conn.close()
    return user


def update_user(user_id: int, updates: dict, admin_id: int) -> dict:
    """Update user fields (admin action)."""
    allowed_fields = {'display_name', 'role', 'is_active', 'is_suspended'}
    conn = get_db()

    sets = []
    params = []
    for field, value in updates.items():
        if field in allowed_fields:
            sets.append(f"{field} = ?")
            params.append(value)

    if not sets:
        conn.close()
        return {"error": "No valid fields to update."}

    sets.append("updated_at = datetime('now')")
    params.append(user_id)

    conn.execute(f"UPDATE users SET {', '.join(sets)} WHERE id = ?", params)

    # Log the admin action
    from auth import log_activity
    changes = ', '.join(f"{k}={v}" for k, v in updates.items() if k in allowed_fields)
    log_activity(conn, admin_id, 'admin_update_user',
                 f"Updated user {user_id}: {changes}")

    # If suspending or deactivating, kill their sessions
    if updates.get('is_suspended') or not updates.get('is_active', True):
        conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))

    conn.commit()
    conn.close()
    return {"success": True}


# ─── Analytics ────────────────────────────────────────────────────────────────

def get_admin_analytics() -> dict:
    """Get comprehensive analytics for the admin dashboard."""
    conn = get_db()
    analytics = {}

    # User counts
    analytics['total_users'] = conn.execute(
        "SELECT COUNT(*) as c FROM users").fetchone()['c']
    analytics['active_users'] = conn.execute(
        "SELECT COUNT(*) as c FROM users WHERE is_active = 1 AND is_suspended = 0"
    ).fetchone()['c']
    analytics['admin_users'] = conn.execute(
        "SELECT COUNT(*) as c FROM users WHERE role = 'admin'").fetchone()['c']
    analytics['suspended_users'] = conn.execute(
        "SELECT COUNT(*) as c FROM users WHERE is_suspended = 1").fetchone()['c']

    # Recent registrations (last 30 days)
    analytics['new_users_30d'] = conn.execute("""
        SELECT COUNT(*) as c FROM users
        WHERE created_at > datetime('now', '-30 days')
    """).fetchone()['c']

    # Active sessions
    analytics['active_sessions'] = conn.execute(
        "SELECT COUNT(*) as c FROM sessions WHERE expires_at > datetime('now')"
    ).fetchone()['c']

    # Content stats
    analytics['total_uploads'] = conn.execute(
        "SELECT COUNT(*) as c FROM uploads").fetchone()['c']
    analytics['total_products'] = conn.execute(
        "SELECT COUNT(*) as c FROM products").fetchone()['c']
    analytics['total_mills'] = conn.execute(
        "SELECT COUNT(*) as c FROM mills").fetchone()['c']
    analytics['total_searches'] = conn.execute(
        "SELECT COUNT(*) as c FROM search_history").fetchone()['c']

    # Users by registration date (last 30 days, grouped by day)
    analytics['registrations_by_day'] = dicts_from_rows(conn.execute("""
        SELECT date(created_at) as day, COUNT(*) as count
        FROM users
        WHERE created_at > datetime('now', '-30 days')
        GROUP BY date(created_at)
        ORDER BY day
    """).fetchall())

    # Top users by activity
    analytics['top_users'] = dicts_from_rows(conn.execute("""
        SELECT u.id, u.email, u.display_name, u.role, u.last_login_at,
               (SELECT COUNT(*) FROM uploads WHERE user_id = u.id) as uploads,
               (SELECT COUNT(*) FROM search_history WHERE user_id = u.id) as searches,
               (SELECT COUNT(*) FROM activity_log WHERE user_id = u.id) as actions
        FROM users u
        ORDER BY actions DESC
        LIMIT 10
    """).fetchall())

    # Recent activity log (last 50 events)
    analytics['recent_activity'] = dicts_from_rows(conn.execute("""
        SELECT a.*, u.email, u.display_name
        FROM activity_log a
        LEFT JOIN users u ON u.id = a.user_id
        ORDER BY a.created_at DESC
        LIMIT 50
    """).fetchall())

    # Activity by action type
    analytics['activity_by_type'] = dicts_from_rows(conn.execute("""
        SELECT action, COUNT(*) as count
        FROM activity_log
        GROUP BY action
        ORDER BY count DESC
    """).fetchall())

    # Uploads per day (last 30 days)
    analytics['uploads_by_day'] = dicts_from_rows(conn.execute("""
        SELECT date(uploaded_at) as day, COUNT(*) as count
        FROM uploads
        WHERE uploaded_at > datetime('now', '-30 days')
        GROUP BY date(uploaded_at)
        ORDER BY day
    """).fetchall())

    conn.close()
    return analytics
