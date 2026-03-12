#!/usr/bin/env python3
"""
Management CLI for TIFP Mill Lists.

Usage:
    python3 manage.py promote-admin <email>
    python3 manage.py demote-admin <email>
    python3 manage.py create-admin <email> <password>
    python3 manage.py list-users
    python3 manage.py suspend-user <email>
    python3 manage.py activate-user <email>
    python3 manage.py reset-db
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from database import get_db, init_db
from auth import hash_password, validate_password, validate_email


def promote_admin(email: str):
    """Promote an existing user to admin role."""
    conn = get_db()
    user = conn.execute("SELECT id, role FROM users WHERE email = ?",
                        (email.lower(),)).fetchone()
    if not user:
        print(f"Error: No user found with email '{email}'")
        conn.close()
        return False

    if user['role'] == 'admin':
        print(f"User '{email}' is already an admin.")
        conn.close()
        return True

    conn.execute("UPDATE users SET role = 'admin', updated_at = datetime('now') WHERE id = ?",
                 (user['id'],))
    conn.commit()
    conn.close()
    print(f"Success: '{email}' promoted to admin.")
    return True


def demote_admin(email: str):
    """Demote an admin back to regular user."""
    conn = get_db()
    user = conn.execute("SELECT id, role FROM users WHERE email = ?",
                        (email.lower(),)).fetchone()
    if not user:
        print(f"Error: No user found with email '{email}'")
        conn.close()
        return False

    if user['role'] != 'admin':
        print(f"User '{email}' is not an admin.")
        conn.close()
        return True

    conn.execute("UPDATE users SET role = 'user', updated_at = datetime('now') WHERE id = ?",
                 (user['id'],))
    conn.commit()
    conn.close()
    print(f"Success: '{email}' demoted to regular user.")
    return True


def create_admin(email: str, password: str):
    """Create a new admin user directly."""
    email = email.strip().lower()

    valid, err = validate_email(email)
    if not valid:
        print(f"Error: {err}")
        return False

    valid, err = validate_password(password)
    if not valid:
        print(f"Error: {err}")
        return False

    conn = get_db()
    existing = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if existing:
        print(f"Error: User '{email}' already exists. Use 'promote-admin' instead.")
        conn.close()
        return False

    pw_hash = hash_password(password)
    conn.execute("""
        INSERT INTO users (email, password_hash, display_name, role, is_active)
        VALUES (?, ?, ?, 'admin', 1)
    """, (email, pw_hash, email.split('@')[0]))
    conn.commit()
    conn.close()
    print(f"Success: Admin user '{email}' created.")
    return True


def list_users():
    """List all users."""
    conn = get_db()
    rows = conn.execute("""
        SELECT id, email, display_name, role, is_active, is_suspended,
               created_at, last_login_at
        FROM users ORDER BY created_at
    """).fetchall()
    conn.close()

    if not rows:
        print("No users found.")
        return

    print(f"\n{'ID':>4}  {'Email':30s}  {'Role':6s}  {'Active':6s}  {'Created':20s}  {'Last Login':20s}")
    print("-" * 100)
    for r in rows:
        status = "yes" if r['is_active'] and not r['is_suspended'] else "no"
        if r['is_suspended']:
            status = "susp"
        print(f"{r['id']:>4}  {r['email']:30s}  {r['role']:6s}  {status:6s}  "
              f"{r['created_at'] or '':20s}  {r['last_login_at'] or 'never':20s}")
    print()


def suspend_user(email: str):
    """Suspend a user."""
    conn = get_db()
    user = conn.execute("SELECT id FROM users WHERE email = ?",
                        (email.lower(),)).fetchone()
    if not user:
        print(f"Error: No user found with email '{email}'")
        conn.close()
        return False

    conn.execute("UPDATE users SET is_suspended = 1, updated_at = datetime('now') WHERE id = ?",
                 (user['id'],))
    conn.execute("DELETE FROM sessions WHERE user_id = ?", (user['id'],))
    conn.commit()
    conn.close()
    print(f"Success: '{email}' suspended.")
    return True


def activate_user(email: str):
    """Reactivate a suspended user."""
    conn = get_db()
    user = conn.execute("SELECT id FROM users WHERE email = ?",
                        (email.lower(),)).fetchone()
    if not user:
        print(f"Error: No user found with email '{email}'")
        conn.close()
        return False

    conn.execute("""UPDATE users SET is_active = 1, is_suspended = 0,
                    updated_at = datetime('now') WHERE id = ?""",
                 (user['id'],))
    conn.commit()
    conn.close()
    print(f"Success: '{email}' activated.")
    return True


def reset_database():
    """Reset and reinitialize the database."""
    confirm = input("This will DELETE all data. Type 'yes' to confirm: ")
    if confirm.strip().lower() != 'yes':
        print("Aborted.")
        return

    from database import DB_PATH
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Deleted {DB_PATH}")
    init_db()
    print("Database reinitialized.")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == 'promote-admin':
        if len(sys.argv) < 3:
            print("Usage: python3 manage.py promote-admin <email>")
            sys.exit(1)
        promote_admin(sys.argv[2])

    elif command == 'demote-admin':
        if len(sys.argv) < 3:
            print("Usage: python3 manage.py demote-admin <email>")
            sys.exit(1)
        demote_admin(sys.argv[2])

    elif command == 'create-admin':
        if len(sys.argv) < 4:
            print("Usage: python3 manage.py create-admin <email> <password>")
            sys.exit(1)
        create_admin(sys.argv[2], sys.argv[3])

    elif command == 'list-users':
        list_users()

    elif command == 'suspend-user':
        if len(sys.argv) < 3:
            print("Usage: python3 manage.py suspend-user <email>")
            sys.exit(1)
        suspend_user(sys.argv[2])

    elif command == 'activate-user':
        if len(sys.argv) < 3:
            print("Usage: python3 manage.py activate-user <email>")
            sys.exit(1)
        activate_user(sys.argv[2])

    elif command == 'reset-db':
        reset_database()

    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
