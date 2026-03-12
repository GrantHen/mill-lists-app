"""
Mill Lists Application - Main Server with Multi-Tenant Group Support
Tornado-based web server with API routes for upload, parsing, search, CRUD,
authentication, admin management, and group/workspace management.
"""
import os
import sys
import json
import uuid
import shutil
import asyncio
import traceback
import functools
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import tornado.ioloop
import tornado.web
import tornado.httpserver

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from database import init_db, get_db, dict_from_row, dicts_from_rows, migrate_data_to_groups
from search_engine import search_products, get_search_suggestions, get_stats
from parsers.pipeline import parse_file
from lumber_normalizer import build_product_string
from auth import (
    register_user, login_user, logout_user, get_session_user,
    create_reset_token, reset_password, log_activity,
    check_rate_limit, record_attempt, cleanup_expired_sessions,
    COOKIE_NAME, SESSION_SECRET
)
from admin import get_all_users, get_user_detail, update_user, get_admin_analytics
from groups import (
    create_group, get_group, get_user_groups, update_group, delete_group,
    check_membership, get_group_members, update_member_role, remove_member,
    create_invitation, get_group_invitations, accept_invitation,
    revoke_invitation, get_user_pending_invitations, claim_invitations_for_user,
    get_group_analytics, get_group_activity,
    get_default_group, switch_active_group
)

log = logging.getLogger("server")

UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")

# Thread pool for blocking operations (parsing, DB)
executor = ThreadPoolExecutor(max_workers=4)


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTE PROTECTION DECORATORS
# ═══════════════════════════════════════════════════════════════════════════════

def require_auth(method):
    """Decorator: require a valid session. Sets self.current_user."""
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        session_id = self.get_cookie(COOKIE_NAME)
        user = get_session_user(session_id) if session_id else None
        if not user:
            if isinstance(self, PageHandler):
                self.redirect("/auth#login")
                return
            self.set_status(401)
            self.write(json.dumps({"error": "Authentication required."}))
            return
        self.current_user = user
        return method(self, *args, **kwargs)
    return wrapper


def require_admin(method):
    """Decorator: require admin role. Must be used after require_auth."""
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        session_id = self.get_cookie(COOKIE_NAME)
        user = get_session_user(session_id) if session_id else None
        if not user:
            if isinstance(self, PageHandler):
                self.redirect("/auth#login")
                return
            self.set_status(401)
            self.write(json.dumps({"error": "Authentication required."}))
            return
        if user.get('role') != 'admin':
            if isinstance(self, PageHandler):
                self.redirect("/app")
                return
            self.set_status(403)
            self.write(json.dumps({"error": "Admin access required."}))
            return
        self.current_user = user
        return method(self, *args, **kwargs)
    return wrapper


def require_group(method):
    """Decorator: require auth + active group. Sets self.active_group_id."""
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        # First require auth
        session_id = self.get_cookie(COOKIE_NAME)
        user = get_session_user(session_id) if session_id else None
        if not user:
            self.set_status(401)
            self.write(json.dumps({"error": "Authentication required."}))
            return
        self.current_user = user

        # Then get active group
        active_group_id = self.get_active_group()
        if active_group_id is None:
            self.set_status(400)
            self.write(json.dumps({
                "error": "No active group selected.",
                "code": "NO_GROUP"
            }))
            return

        self.active_group_id = active_group_id
        return method(self, *args, **kwargs)
    return wrapper


# ═══════════════════════════════════════════════════════════════════════════════
# BASE HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

class BaseHandler(tornado.web.RequestHandler):
    """Base handler with group context support."""
    ACTIVE_GROUP_COOKIE = "tifp_active_group"

    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")

    def write_json(self, data, status=200):
        self.set_status(status)
        self.write(json.dumps(data, default=str))

    def write_error(self, status_code, **kwargs):
        error_msg = kwargs.get("reason", "Internal server error")
        if "exc_info" in kwargs:
            error_msg = str(kwargs["exc_info"][1])
        self.write_json({"error": error_msg}, status_code)

    def get_client_ip(self):
        return self.request.headers.get("X-Forwarded-For",
               self.request.headers.get("X-Real-IP",
               self.request.remote_ip))

    def get_active_group(self):
        """Get active group ID from cookie, validate membership.
        Returns: int (group_id) or None
        """
        if not hasattr(self, 'current_user') or not self.current_user:
            return None

        group_id_str = self.get_cookie(self.ACTIVE_GROUP_COOKIE)
        if not group_id_str:
            return None

        try:
            group_id = int(group_id_str)
        except (ValueError, TypeError):
            return None

        user_id = self.current_user['id']
        is_admin = self.current_user.get('role') == 'admin'

        # Check membership (admins can bypass)
        is_member = check_membership(user_id, group_id)
        if not is_member and not is_admin:
            # Clear invalid cookie
            self.clear_cookie(self.ACTIVE_GROUP_COOKIE, path="/")
            return None

        return group_id


class PageHandler(tornado.web.RequestHandler):
    """Base handler for HTML pages."""
    def render_page(self, template):
        self.set_header("Content-Type", "text/html")
        with open(os.path.join(TEMPLATE_DIR, template), "r") as f:
            self.write(f.read())


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

class LandingHandler(PageHandler):
    def get(self):
        # If already logged in, redirect to app
        session_id = self.get_cookie(COOKIE_NAME)
        if session_id and get_session_user(session_id):
            self.redirect("/app")
            return
        self.render_page("landing.html")


class AuthPageHandler(PageHandler):
    def get(self):
        # If already logged in, redirect to app
        session_id = self.get_cookie(COOKIE_NAME)
        if session_id and get_session_user(session_id):
            self.redirect("/app")
            return
        self.render_page("auth.html")


class AppPageHandler(PageHandler):
    @require_auth
    def get(self):
        self.render_page("index.html")


class AdminPageHandler(PageHandler):
    @require_admin
    def get(self):
        self.render_page("admin.html")


class GroupsPageHandler(PageHandler):
    @require_auth
    def get(self):
        self.render_page("groups.html")


# ═══════════════════════════════════════════════════════════════════════════════
# AUTH API HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

class RegisterHandler(BaseHandler):
    def post(self):
        ip = self.get_client_ip()

        # Rate limit registration
        allowed, wait = check_rate_limit(ip, max_attempts=10, window=600)
        if not allowed:
            self.write_json(
                {"error": f"Too many attempts. Try again in {wait} seconds."},
                429)
            return

        try:
            data = json.loads(self.request.body)
        except Exception:
            self.write_json({"error": "Invalid request body."}, 400)
            return

        email = data.get("email", "")
        password = data.get("password", "")
        display_name = data.get("display_name")

        result = register_user(email, password, display_name)

        if result.get("success"):
            # Auto-login after registration
            login_result = login_user(email, password, ip=ip,
                                      user_agent=self.request.headers.get("User-Agent"))
            if login_result.get("success"):
                self.set_cookie(COOKIE_NAME, login_result["session_id"],
                                httponly=True, secure=False, path="/",
                                expires_days=3)

                # Create personal workspace for new user
                user_id = login_result["user_id"]
                user_display = login_result.get("display_name") or email.split('@')[0]
                try:
                    group = create_group(
                        name=f"{user_display}'s Workspace",
                        description="Personal workspace",
                        created_by=user_id
                    )
                    # Auto-set as active group
                    if group.get("id"):
                        self.set_cookie(
                            ACTIVE_GROUP_COOKIE,
                            str(group["id"]),
                            httponly=True, secure=False, path="/",
                            expires_days=30
                        )
                except Exception as e:
                    log.warning(f"Failed to create personal workspace for {email}: {e}")

                # Claim any pending invitations for this email
                claim_invitations_for_user(user_id, email)

                # Check for pending invitations
                pending = get_user_pending_invitations(email)

                self.write_json({
                    "success": True,
                    "user": {
                        "id": user_id,
                        "email": login_result["email"],
                        "display_name": login_result["display_name"],
                        "role": login_result["role"],
                    },
                    "pending_invitations": len(pending)
                })
            else:
                self.write_json({"success": True, "message": "Account created. Please log in."})
        else:
            self.write_json({"error": result.get("error", "Registration failed.")}, 400)


class LoginHandler(BaseHandler):
    def post(self):
        ip = self.get_client_ip()

        try:
            data = json.loads(self.request.body)
        except Exception:
            self.write_json({"error": "Invalid request body."}, 400)
            return

        email = data.get("email", "")
        password = data.get("password", "")

        result = login_user(email, password, ip=ip,
                            user_agent=self.request.headers.get("User-Agent"))

        if result.get("success"):
            self.set_cookie(COOKIE_NAME, result["session_id"],
                            httponly=True, secure=False, path="/",
                            expires_days=3)
            self.write_json({
                "success": True,
                "user": {
                    "id": result["user_id"],
                    "email": result["email"],
                    "display_name": result["display_name"],
                    "role": result["role"],
                }
            })
        else:
            status = 429 if "Too many" in result.get("error", "") else 401
            self.write_json({"error": result["error"]}, status)


class LogoutHandler(BaseHandler):
    def post(self):
        session_id = self.get_cookie(COOKIE_NAME)
        if session_id:
            logout_user(session_id)
        self.clear_cookie(COOKIE_NAME, path="/")
        self.clear_cookie(self.ACTIVE_GROUP_COOKIE, path="/")
        self.write_json({"success": True})


class MeHandler(BaseHandler):
    @require_auth
    def get(self):
        user_id = self.current_user['id']
        groups = get_user_groups(user_id)
        active_gid = self.get_active_group()

        self.write_json({
            "user": self.current_user,
            "groups": groups,
            "active_group_id": active_gid,
        })


class ForgotPasswordHandler(BaseHandler):
    def post(self):
        ip = self.get_client_ip()
        allowed, wait = check_rate_limit(ip, max_attempts=5, window=600)
        if not allowed:
            self.write_json(
                {"error": f"Too many attempts. Try again in {wait} seconds."},
                429)
            return

        try:
            data = json.loads(self.request.body)
        except Exception:
            self.write_json({"error": "Invalid request body."}, 400)
            return

        email = data.get("email", "")
        result = create_reset_token(email)
        # Always return the same message to prevent email enumeration
        response = {"success": True, "message": result.get("message", "")}
        # In dev mode, include the token so the UI can show it
        if result.get("token"):
            response["token"] = result["token"]
        self.write_json(response)


class ResetPasswordHandler(BaseHandler):
    def post(self):
        try:
            data = json.loads(self.request.body)
        except Exception:
            self.write_json({"error": "Invalid request body."}, 400)
            return

        token = data.get("token", "")
        new_password = data.get("password", "")

        result = reset_password(token, new_password)
        if result.get("success"):
            self.write_json({"success": True})
        else:
            self.write_json({"error": result.get("error", "Reset failed.")}, 400)


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP API HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

class GroupsListHandler(BaseHandler):
    @require_auth
    def get(self):
        """List user's groups."""
        user_id = self.current_user['id']
        groups = get_user_groups(user_id)
        self.write_json({"groups": groups})

    @require_auth
    def post(self):
        """Create a new group."""
        try:
            data = json.loads(self.request.body)
        except Exception:
            self.write_json({"error": "Invalid request body."}, 400)
            return

        name = data.get("name", "").strip()
        description = data.get("description", "").strip()

        if not name:
            self.write_json({"error": "Group name is required."}, 400)
            return

        user_id = self.current_user['id']
        result = create_group(name, description, user_id)

        if result.get("error"):
            self.write_json(result, 400)
        else:
            self.write_json(result, 201)


class GroupDetailHandler(BaseHandler):
    @require_auth
    def get(self, group_id):
        """Get group details."""
        group_id = int(group_id)
        user_id = self.current_user['id']
        is_admin = self.current_user.get('role') == 'admin'

        # Check membership
        is_member = check_membership(user_id, group_id)
        if not is_member and not is_admin:
            self.write_json({"error": "Access denied."}, 403)
            return

        group = get_group(group_id)
        if not group:
            self.write_json({"error": "Group not found."}, 404)
            return

        self.write_json({"group": group})

    @require_auth
    def put(self, group_id):
        """Update group (owner only)."""
        group_id = int(group_id)
        user_id = self.current_user['id']
        is_admin = self.current_user.get('role') == 'admin'

        group = get_group(group_id)
        if not group:
            self.write_json({"error": "Group not found."}, 404)
            return

        # Check ownership
        is_owner = group['owner_id'] == user_id
        if not is_owner and not is_admin:
            self.write_json({"error": "Only owner can update group."}, 403)
            return

        try:
            data = json.loads(self.request.body)
        except Exception:
            self.write_json({"error": "Invalid request body."}, 400)
            return

        result = update_group(group_id, data)
        if result.get("error"):
            self.write_json(result, 400)
        else:
            self.write_json(result)

    @require_auth
    def delete(self, group_id):
        """Delete group (owner only)."""
        group_id = int(group_id)
        user_id = self.current_user['id']
        is_admin = self.current_user.get('role') == 'admin'

        group = get_group(group_id)
        if not group:
            self.write_json({"error": "Group not found."}, 404)
            return

        # Check ownership
        is_owner = group['owner_id'] == user_id
        if not is_owner and not is_admin:
            self.write_json({"error": "Only owner can delete group."}, 403)
            return

        result = delete_group(group_id)
        if result.get("error"):
            self.write_json(result, 400)
        else:
            self.write_json(result)


class GroupMembersHandler(BaseHandler):
    @require_auth
    def get(self, group_id):
        """List group members."""
        group_id = int(group_id)
        user_id = self.current_user['id']
        is_admin = self.current_user.get('role') == 'admin'

        # Check membership
        is_member = check_membership(user_id, group_id)
        if not is_member and not is_admin:
            self.write_json({"error": "Access denied."}, 403)
            return

        members = get_group_members(group_id)
        self.write_json({"members": members})


class GroupMemberHandler(BaseHandler):
    @require_auth
    def put(self, group_id, member_id):
        """Update member role (admin only within group)."""
        group_id = int(group_id)
        member_id = int(member_id)
        user_id = self.current_user['id']
        is_admin = self.current_user.get('role') == 'admin'

        group = get_group(group_id)
        if not group:
            self.write_json({"error": "Group not found."}, 404)
            return

        # Check ownership/admin
        is_owner = group['owner_id'] == user_id
        if not is_owner and not is_admin:
            self.write_json({"error": "Only owner can manage members."}, 403)
            return

        try:
            data = json.loads(self.request.body)
        except Exception:
            self.write_json({"error": "Invalid request body."}, 400)
            return

        role = data.get("role", "member")
        result = update_member_role(group_id, member_id, role)
        if result.get("error"):
            self.write_json(result, 400)
        else:
            self.write_json(result)

    @require_auth
    def delete(self, group_id, member_id):
        """Remove member from group (owner only)."""
        group_id = int(group_id)
        member_id = int(member_id)
        user_id = self.current_user['id']
        is_admin = self.current_user.get('role') == 'admin'

        group = get_group(group_id)
        if not group:
            self.write_json({"error": "Group not found."}, 404)
            return

        # Check ownership
        is_owner = group['owner_id'] == user_id
        if not is_owner and not is_admin:
            self.write_json({"error": "Only owner can remove members."}, 403)
            return

        result = remove_member(group_id, member_id)
        if result.get("error"):
            self.write_json(result, 400)
        else:
            self.write_json(result)


class GroupInvitationsHandler(BaseHandler):
    @require_auth
    def get(self, group_id):
        """List group invitations (admin/owner only)."""
        group_id = int(group_id)
        user_id = self.current_user['id']
        is_admin = self.current_user.get('role') == 'admin'

        group = get_group(group_id)
        if not group:
            self.write_json({"error": "Group not found."}, 404)
            return

        # Check ownership
        is_owner = group['owner_id'] == user_id
        if not is_owner and not is_admin:
            self.write_json({"error": "Only owner can view invitations."}, 403)
            return

        invitations = get_group_invitations(group_id)
        self.write_json({"invitations": invitations})

    @require_auth
    def post(self, group_id):
        """Create invitation to group."""
        group_id = int(group_id)
        user_id = self.current_user['id']
        is_admin = self.current_user.get('role') == 'admin'

        group = get_group(group_id)
        if not group:
            self.write_json({"error": "Group not found."}, 404)
            return

        # Check ownership
        is_owner = group['owner_id'] == user_id
        if not is_owner and not is_admin:
            self.write_json({"error": "Only owner can invite members."}, 403)
            return

        try:
            data = json.loads(self.request.body)
        except Exception:
            self.write_json({"error": "Invalid request body."}, 400)
            return

        email = data.get("email", "").strip()
        role = data.get("role", "member")

        if not email:
            self.write_json({"error": "Email is required."}, 400)
            return

        result = create_invitation(group_id, email, role, user_id)
        if result.get("error"):
            self.write_json(result, 400)
        else:
            self.write_json(result, 201)


class GroupInvitationHandler(BaseHandler):
    @require_auth
    def delete(self, group_id, invitation_id):
        """Revoke invitation (owner only)."""
        group_id = int(group_id)
        invitation_id = int(invitation_id)
        user_id = self.current_user['id']
        is_admin = self.current_user.get('role') == 'admin'

        group = get_group(group_id)
        if not group:
            self.write_json({"error": "Group not found."}, 404)
            return

        # Check ownership
        is_owner = group['owner_id'] == user_id
        if not is_owner and not is_admin:
            self.write_json({"error": "Only owner can revoke invitations."}, 403)
            return

        result = revoke_invitation(invitation_id)
        if result.get("error"):
            self.write_json(result, 400)
        else:
            self.write_json(result)


class AcceptInvitationHandler(BaseHandler):
    @require_auth
    def post(self):
        """Accept an invitation."""
        try:
            data = json.loads(self.request.body)
        except Exception:
            self.write_json({"error": "Invalid request body."}, 400)
            return

        token = data.get("token", "").strip()
        if not token:
            self.write_json({"error": "Invitation token is required."}, 400)
            return

        user_id = self.current_user['id']
        result = accept_invitation(token, user_id)
        if result.get("error"):
            self.write_json(result, 400)
        else:
            self.write_json(result)


class PendingInvitationsHandler(BaseHandler):
    @require_auth
    def get(self):
        """Get pending invitations for current user."""
        email = self.current_user['email']
        invitations = get_user_pending_invitations(email)
        self.write_json({"invitations": invitations})


class GroupAnalyticsHandler(BaseHandler):
    @require_auth
    def get(self, group_id):
        """Get group analytics (members only)."""
        group_id = int(group_id)
        user_id = self.current_user['id']
        is_admin = self.current_user.get('role') == 'admin'

        # Check membership
        is_member = check_membership(user_id, group_id)
        if not is_member and not is_admin:
            self.write_json({"error": "Access denied."}, 403)
            return

        analytics = get_group_analytics(group_id)
        self.write_json(analytics)


class SwitchGroupHandler(BaseHandler):
    @require_auth
    def post(self):
        """Switch active group."""
        try:
            data = json.loads(self.request.body)
        except Exception:
            self.write_json({"error": "Invalid request body."}, 400)
            return

        group_id = data.get("group_id")
        if not group_id:
            self.write_json({"error": "group_id is required."}, 400)
            return

        try:
            group_id = int(group_id)
        except (ValueError, TypeError):
            self.write_json({"error": "Invalid group_id."}, 400)
            return

        user_id = self.current_user['id']
        is_admin = self.current_user.get('role') == 'admin'

        # Check membership
        is_member = check_membership(user_id, group_id)
        if not is_member and not is_admin:
            self.write_json({"error": "Not a member of this group."}, 403)
            return

        # Set cookie
        self.set_cookie(self.ACTIVE_GROUP_COOKIE, str(group_id),
                       httponly=True, path="/")

        result = switch_active_group(user_id, group_id)
        self.write_json(result)


# ═══════════════════════════════════════════════════════════════════════════════
# ADMIN API HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

class AdminUsersHandler(BaseHandler):
    @require_admin
    def get(self):
        search = self.get_argument("search", None)
        role = self.get_argument("role", None)
        status = self.get_argument("status", None)
        users = get_all_users(search=search, role=role, status=status)
        self.write_json({"users": users})


class AdminUserHandler(BaseHandler):
    @require_admin
    def get(self, user_id):
        user = get_user_detail(int(user_id))
        if not user:
            self.write_json({"error": "User not found."}, 404)
            return
        self.write_json({"user": user})

    @require_admin
    def put(self, user_id):
        try:
            data = json.loads(self.request.body)
        except Exception:
            self.write_json({"error": "Invalid request body."}, 400)
            return

        # Prevent admin from changing their own role
        if int(user_id) == self.current_user['id'] and 'role' in data:
            self.write_json({"error": "Cannot change your own role."}, 400)
            return

        result = update_user(int(user_id), data, self.current_user['id'])
        if result.get("error"):
            self.write_json(result, 400)
        else:
            self.write_json(result)


class AdminAnalyticsHandler(BaseHandler):
    @require_admin
    def get(self):
        analytics = get_admin_analytics()
        self.write_json(analytics)


class AdminGroupsHandler(BaseHandler):
    @require_admin
    def get(self):
        """Get all groups with stats (admin only)."""
        conn = get_db()
        groups = conn.execute("""
            SELECT g.*,
                   (SELECT COUNT(*) FROM group_memberships WHERE group_id = g.id) as member_count,
                   (SELECT COUNT(*) FROM uploads WHERE group_id = g.id) as upload_count,
                   (SELECT COUNT(*) FROM products WHERE group_id = g.id) as product_count
            FROM groups g
            ORDER BY g.created_at DESC
        """).fetchall()
        conn.close()

        self.write_json({"groups": dicts_from_rows(groups)})


# ═══════════════════════════════════════════════════════════════════════════════
# UPLOAD HANDLERS (group-scoped)
# ═══════════════════════════════════════════════════════════════════════════════

class UploadHandler(BaseHandler):
    @require_group
    async def post(self):
        """Handle file upload with automatic parsing (group-scoped)."""
        try:
            files = self.request.files.get("files", [])
            if not files:
                self.write_json({"error": "No files uploaded"}, 400)
                return

            user_id = self.current_user['id']
            group_id = self.active_group_id
            results = []

            for file_info in files:
                result = await asyncio.get_event_loop().run_in_executor(
                    executor, self._process_upload, file_info, user_id, group_id
                )
                results.append(result)

            # Log upload activity
            conn = get_db()
            log_activity(conn, user_id, 'upload',
                         f"Uploaded {len(files)} file(s) to group {group_id}")
            conn.commit()
            conn.close()

            self.write_json({"uploads": results})
        except Exception as e:
            self.write_json({"error": str(e)}, 500)

    def _process_upload(self, file_info, user_id, group_id):
        """Process a single file upload with group scoping."""
        original_name = file_info["filename"]
        ext = os.path.splitext(original_name)[1].lower()
        unique_name = f"{uuid.uuid4().hex}{ext}"
        file_path = os.path.join(UPLOAD_DIR, unique_name)

        # Save file
        with open(file_path, "wb") as f:
            f.write(file_info["body"])

        file_size = len(file_info["body"])

        # Determine file type
        if ext in ('.pdf',):
            file_type = 'pdf'
        elif ext in ('.xlsx', '.xls'):
            file_type = 'excel'
        elif ext in ('.csv',):
            file_type = 'csv'
        else:
            file_type = 'unknown'

        # Create upload record with group_id
        conn = get_db()
        cursor = conn.execute("""
            INSERT INTO uploads (filename, original_filename, file_path, file_type, file_size, status, user_id, group_id)
            VALUES (?, ?, ?, ?, ?, 'processing', ?, ?)
        """, (unique_name, original_name, file_path, file_type, file_size, user_id, group_id))
        upload_id = cursor.lastrowid
        conn.commit()

        # Parse the file through the pipeline
        try:
            parse_result = parse_file(file_path, original_name, method="auto")

            if parse_result.success:
                # Detect or create mill (scoped to group)
                mill_name = parse_result.mill_name
                mill_id = None
                if mill_name:
                    mill_id = self._get_or_create_mill(conn, mill_name, parse_result, group_id)

                # Insert parsed rows with group_id
                for row in parse_result.rows:
                    row_mill_name = mill_name or 'Unknown Mill'
                    conn.execute("""
                        INSERT INTO products (
                            upload_id, mill_id, mill_name, species, product,
                            product_normalized, thickness, grade, description,
                            quantity, quantity_numeric, uom, price, price_numeric,
                            length, width, surface, treatment, color, cut_type,
                            notes, confidence, raw_text, source_row, group_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        upload_id, mill_id, row_mill_name,
                        row.species, row.product, row.product_normalized,
                        row.thickness, row.grade, row.description,
                        row.quantity, row.quantity_numeric, row.uom,
                        row.price, row.price_numeric,
                        row.length, row.width, row.surface,
                        row.treatment, row.color, row.cut_type,
                        row.notes, row.confidence, row.raw_text, row.source_row,
                        group_id
                    ))

                # Update upload status
                conn.execute("""
                    UPDATE uploads SET
                        status = 'parsed',
                        mill_id = ?,
                        mill_name_detected = ?,
                        parsing_method = ?,
                        parsing_confidence = ?,
                        row_count = ?,
                        parsed_at = datetime('now')
                    WHERE id = ?
                """, (mill_id, mill_name, parse_result.parsing_method,
                      parse_result.confidence, len(parse_result.rows), upload_id))
            else:
                conn.execute("""
                    UPDATE uploads SET
                        status = 'failed',
                        parsing_method = ?,
                        error_message = ?
                    WHERE id = ?
                """, (parse_result.parsing_method,
                      '; '.join(parse_result.errors), upload_id))

            conn.commit()
            conn.close()

            upload_row = get_db().execute("SELECT * FROM uploads WHERE id = ?", (upload_id,)).fetchone()

            return {
                "upload_id": upload_id,
                "filename": original_name,
                "status": "parsed" if parse_result.success else "failed",
                "mill_name": parse_result.mill_name,
                "row_count": len(parse_result.rows),
                "parsing_method": parse_result.parsing_method,
                "confidence": parse_result.confidence,
                "errors": parse_result.errors,
                "warnings": parse_result.warnings,
            }

        except Exception as e:
            conn.execute("""
                UPDATE uploads SET status = 'failed', error_message = ? WHERE id = ?
            """, (str(e), upload_id))
            conn.commit()
            conn.close()
            return {
                "upload_id": upload_id,
                "filename": original_name,
                "status": "failed",
                "error": str(e),
            }

    def _get_or_create_mill(self, conn, name, parse_result, group_id):
        """Get existing mill or create new one (scoped to group)."""
        name = name.strip()
        if len(name) > 100:
            name = name[:100]

        # Look up mill within group scope
        existing = conn.execute(
            "SELECT id FROM mills WHERE group_id = ? AND name = ?",
            (group_id, name)
        ).fetchone()
        if existing:
            return existing['id']

        cursor = conn.execute("""
            INSERT INTO mills (name, location, phone, email, contact_name, group_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (name, parse_result.mill_location, parse_result.mill_phone,
              parse_result.mill_email, parse_result.mill_contact, group_id))
        return cursor.lastrowid


class UploadReParseHandler(BaseHandler):
    @require_group
    async def post(self, upload_id):
        """Re-parse an upload, optionally with AI (group-scoped)."""
        try:
            body = json.loads(self.request.body) if self.request.body else {}
            method = body.get("method", "auto")

            result = await asyncio.get_event_loop().run_in_executor(
                executor, self._reparse, int(upload_id), self.active_group_id
            )
            self.write_json(result)
        except Exception as e:
            self.write_json({"error": str(e)}, 500)

    def _reparse(self, upload_id, group_id):
        """Re-parse implementation with group validation."""
        conn = get_db()
        upload = conn.execute("SELECT * FROM uploads WHERE id = ? AND group_id = ?",
                             (upload_id, group_id)).fetchone()
        if not upload:
            return {"error": "Upload not found or access denied"}

        # Delete existing parsed rows
        conn.execute("DELETE FROM products WHERE upload_id = ?", (upload_id,))
        conn.commit()

        file_path = upload['file_path']
        original_filename = upload['original_filename'] or ''

        # Use pipeline for all methods
        parse_result = parse_file(file_path, original_filename, method="auto")

        # Re-insert rows
        mill_name = parse_result.mill_name or upload['mill_name_detected'] or 'Unknown Mill'
        mill_id = upload['mill_id']

        for row in parse_result.rows:
            conn.execute("""
                INSERT INTO products (
                    upload_id, mill_id, mill_name, species, product,
                    product_normalized, thickness, grade, description,
                    quantity, quantity_numeric, uom, price, price_numeric,
                    length, width, surface, treatment, color, cut_type,
                    notes, confidence, raw_text, source_row, group_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                upload_id, mill_id, mill_name,
                row.species, row.product, row.product_normalized,
                row.thickness, row.grade, row.description,
                row.quantity, row.quantity_numeric, row.uom,
                row.price, row.price_numeric,
                row.length, row.width, row.surface,
                row.treatment, row.color, row.cut_type,
                row.notes, row.confidence, row.raw_text, row.source_row,
                group_id
            ))

        conn.execute("""
            UPDATE uploads SET
                status = ?, parsing_method = ?, parsing_confidence = ?,
                row_count = ?, parsed_at = datetime('now'), error_message = ?
            WHERE id = ?
        """, (
            'parsed' if parse_result.success else 'failed',
            parse_result.parsing_method, parse_result.confidence,
            len(parse_result.rows),
            '; '.join(parse_result.errors) if parse_result.errors else None,
            upload_id
        ))
        conn.commit()
        conn.close()

        return {
            "success": parse_result.success,
            "row_count": len(parse_result.rows),
            "method": parse_result.parsing_method,
            "confidence": parse_result.confidence,
            "errors": parse_result.errors,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SEARCH HANDLERS (group-scoped)
# ═══════════════════════════════════════════════════════════════════════════════

class SearchHandler(BaseHandler):
    @require_group
    async def get(self):
        query = self.get_argument("q", "")
        limit = int(self.get_argument("limit", 100))
        offset = int(self.get_argument("offset", 0))

        if not query:
            self.write_json({"error": "Query parameter 'q' is required"}, 400)
            return

        # Log search with group context
        user_id = self.current_user['id']
        group_id = self.active_group_id
        conn = get_db()
        conn.execute("INSERT INTO search_history (query, user_id, group_id) VALUES (?, ?, ?)",
                     (query, user_id, group_id))
        conn.commit()
        conn.close()

        result = await asyncio.get_event_loop().run_in_executor(
            executor, search_products, query, limit, offset
        )

        # Filter results to active group
        if 'products' in result:
            conn = get_db()
            filtered_products = []
            for product in result['products']:
                # Verify product belongs to group
                check = conn.execute(
                    "SELECT id FROM products WHERE id = ? AND group_id = ?",
                    (product['id'], group_id)
                ).fetchone()
                if check:
                    filtered_products.append(product)
            conn.close()
            result['products'] = filtered_products
            result['total'] = len(filtered_products)

        # Update search history with result count
        if 'total' in result:
            conn = get_db()
            conn.execute("""
                UPDATE search_history SET result_count = ?
                WHERE user_id = ? AND group_id = ? ORDER BY searched_at DESC LIMIT 1
            """, (result['total'], user_id, group_id))
            conn.commit()
            conn.close()

        self.write_json(result)


class SuggestHandler(BaseHandler):
    @require_group
    async def get(self):
        query = self.get_argument("q", "")
        if not query:
            self.write_json([])
            return
        suggestions = await asyncio.get_event_loop().run_in_executor(
            executor, get_search_suggestions, query
        )
        self.write_json(suggestions)


# ═══════════════════════════════════════════════════════════════════════════════
# PRODUCT CRUD HANDLERS (group-scoped)
# ═══════════════════════════════════════════════════════════════════════════════

class ProductsHandler(BaseHandler):
    @require_group
    def get(self):
        """List products with filtering, sorting, pagination (group-scoped)."""
        conn = get_db()
        group_id = self.active_group_id

        # Query params
        upload_id = self.get_argument("upload_id", None)
        mill_name = self.get_argument("mill", None)
        species = self.get_argument("species", None)
        thickness = self.get_argument("thickness", None)
        grade = self.get_argument("grade", None)
        flagged = self.get_argument("flagged", None)
        unreviewed = self.get_argument("unreviewed", None)
        sort = self.get_argument("sort", "id")
        order = self.get_argument("order", "asc")
        limit = int(self.get_argument("limit", 100))
        offset = int(self.get_argument("offset", 0))

        conditions = ["group_id = ?"]
        params = [group_id]

        if upload_id:
            conditions.append("upload_id = ?")
            params.append(int(upload_id))
        if mill_name:
            conditions.append("mill_name LIKE ?")
            params.append(f"%{mill_name}%")
        if species:
            conditions.append("species LIKE ?")
            params.append(f"%{species}%")
        if thickness:
            conditions.append("thickness = ?")
            params.append(thickness)
        if grade:
            conditions.append("grade LIKE ?")
            params.append(f"%{grade}%")
        if flagged == '1':
            conditions.append("is_flagged = 1")
        if unreviewed == '1':
            conditions.append("is_reviewed = 0")

        where = f"WHERE {' AND '.join(conditions)}"

        # Validate sort column
        valid_sorts = ['id', 'mill_name', 'species', 'thickness', 'grade',
                       'quantity_numeric', 'confidence', 'created_at']
        if sort not in valid_sorts:
            sort = 'id'
        order = 'DESC' if order.lower() == 'desc' else 'ASC'

        total = conn.execute(
            f"SELECT COUNT(*) as c FROM products {where}", params
        ).fetchone()['c']

        rows = conn.execute(
            f"SELECT * FROM products {where} ORDER BY {sort} {order} LIMIT ? OFFSET ?",
            params + [limit, offset]
        ).fetchall()

        conn.close()

        return self.write_json({
            "total": total,
            "offset": offset,
            "limit": limit,
            "products": dicts_from_rows(rows),
        })


class ProductHandler(BaseHandler):
    @require_group
    def get(self, product_id):
        conn = get_db()
        row = conn.execute("""
            SELECT * FROM products WHERE id = ? AND group_id = ?
        """, (int(product_id), self.active_group_id)).fetchone()
        conn.close()
        if not row:
            self.write_json({"error": "Product not found"}, 404)
            return
        self.write_json(dict_from_row(row))

    @require_group
    def put(self, product_id):
        """Update a product (manual correction, group-scoped)."""
        conn = get_db()
        product = conn.execute("""
            SELECT * FROM products WHERE id = ? AND group_id = ?
        """, (int(product_id), self.active_group_id)).fetchone()

        if not product:
            self.write_json({"error": "Product not found"}, 404)
            return

        data = json.loads(self.request.body)

        fields = ['species', 'product', 'product_normalized', 'thickness', 'grade',
                   'description', 'quantity', 'quantity_numeric', 'price', 'price_numeric',
                   'length', 'width', 'surface', 'treatment', 'color', 'cut_type',
                   'notes', 'mill_name', 'is_reviewed', 'is_flagged', 'confidence']

        updates = []
        params = []
        for f in fields:
            if f in data:
                updates.append(f"{f} = ?")
                params.append(data[f])

        if not updates:
            self.write_json({"error": "No fields to update"}, 400)
            return

        updates.append("updated_at = datetime('now')")

        # Rebuild normalized product
        if any(f in data for f in ['species', 'thickness', 'grade', 'color',
                                    'cut_type', 'surface', 'treatment', 'length']):
            current = dict_from_row(product)
            merged = {**current, **data}
            normalized = build_product_string(merged)
            updates.append("product_normalized = ?")
            params.append(normalized)

        params.append(int(product_id))
        conn.execute(f"UPDATE products SET {', '.join(updates)} WHERE id = ?", params)
        conn.commit()

        row = conn.execute("SELECT * FROM products WHERE id = ?",
                           (int(product_id),)).fetchone()
        conn.close()
        self.write_json(dict_from_row(row))

    @require_group
    def delete(self, product_id):
        conn = get_db()
        product = conn.execute("""
            SELECT * FROM products WHERE id = ? AND group_id = ?
        """, (int(product_id), self.active_group_id)).fetchone()

        if not product:
            self.write_json({"error": "Product not found"}, 404)
            return

        conn.execute("DELETE FROM products WHERE id = ?", (int(product_id),))
        conn.commit()
        conn.close()
        self.write_json({"success": True})


class ProductsBulkHandler(BaseHandler):
    @require_group
    def post(self):
        """Bulk operations on products (group-scoped)."""
        data = json.loads(self.request.body)
        action = data.get("action")
        ids = data.get("ids", [])

        if not ids:
            self.write_json({"error": "No product IDs provided"}, 400)
            return

        conn = get_db()
        group_id = self.active_group_id

        # Verify all IDs belong to group
        placeholders = ','.join('?' * len(ids))
        count = conn.execute(
            f"SELECT COUNT(*) as c FROM products WHERE id IN ({placeholders}) AND group_id = ?",
            ids + [group_id]
        ).fetchone()['c']

        if count != len(ids):
            self.write_json({"error": "Some products do not belong to this group"}, 403)
            return

        if action == "review":
            conn.execute(f"UPDATE products SET is_reviewed = 1, updated_at = datetime('now') WHERE id IN ({placeholders})", ids)
        elif action == "flag":
            conn.execute(f"UPDATE products SET is_flagged = 1, updated_at = datetime('now') WHERE id IN ({placeholders})", ids)
        elif action == "unflag":
            conn.execute(f"UPDATE products SET is_flagged = 0, updated_at = datetime('now') WHERE id IN ({placeholders})", ids)
        elif action == "delete":
            conn.execute(f"DELETE FROM products WHERE id IN ({placeholders})", ids)
        elif action == "set_mill":
            mill_name = data.get("mill_name")
            if mill_name:
                conn.execute(f"UPDATE products SET mill_name = ?, updated_at = datetime('now') WHERE id IN ({placeholders})", [mill_name] + ids)
        else:
            self.write_json({"error": f"Unknown action: {action}"}, 400)
            return

        conn.commit()
        conn.close()
        self.write_json({"success": True, "affected": len(ids)})


# ═══════════════════════════════════════════════════════════════════════════════
# UPLOAD MANAGEMENT (group-scoped)
# ═══════════════════════════════════════════════════════════════════════════════

class UploadsHandler(BaseHandler):
    @require_group
    def get(self):
        conn = get_db()
        rows = conn.execute("""
            SELECT * FROM uploads WHERE group_id = ? ORDER BY uploaded_at DESC
        """, (self.active_group_id,)).fetchall()
        conn.close()
        self.write_json({"uploads": dicts_from_rows(rows)})


class UploadDetailHandler(BaseHandler):
    @require_group
    def get(self, upload_id):
        conn = get_db()
        upload = conn.execute("""
            SELECT * FROM uploads WHERE id = ? AND group_id = ?
        """, (int(upload_id), self.active_group_id)).fetchone()
        if not upload:
            self.write_json({"error": "Upload not found"}, 404)
            return
        products = conn.execute(
            "SELECT * FROM products WHERE upload_id = ? ORDER BY source_row",
            (int(upload_id),)
        ).fetchall()
        conn.close()
        self.write_json({
            "upload": dict_from_row(upload),
            "products": dicts_from_rows(products),
        })

    @require_group
    def put(self, upload_id):
        """Update upload metadata (group-scoped)."""
        data = json.loads(self.request.body)
        conn = get_db()
        upload = conn.execute("""
            SELECT * FROM uploads WHERE id = ? AND group_id = ?
        """, (int(upload_id), self.active_group_id)).fetchone()

        if not upload:
            self.write_json({"error": "Upload not found"}, 404)
            return

        if 'mill_name' in data:
            mill_name = data['mill_name']
            conn.execute("UPDATE uploads SET mill_name_detected = ? WHERE id = ?",
                         (mill_name, int(upload_id)))
            conn.execute("UPDATE products SET mill_name = ? WHERE upload_id = ?",
                         (mill_name, int(upload_id)))

        if 'status' in data:
            conn.execute("UPDATE uploads SET status = ? WHERE id = ?",
                         (data['status'], int(upload_id)))

        conn.commit()
        upload = conn.execute("SELECT * FROM uploads WHERE id = ?",
                              (int(upload_id),)).fetchone()
        conn.close()
        self.write_json(dict_from_row(upload))

    @require_group
    def delete(self, upload_id):
        conn = get_db()
        upload = conn.execute("""
            SELECT * FROM uploads WHERE id = ? AND group_id = ?
        """, (int(upload_id), self.active_group_id)).fetchone()
        if upload:
            conn.execute("DELETE FROM products WHERE upload_id = ?", (int(upload_id),))
            try:
                os.remove(upload['file_path'])
            except OSError:
                pass
            conn.execute("DELETE FROM uploads WHERE id = ?", (int(upload_id),))
            conn.commit()
        conn.close()
        self.write_json({"success": True})


# ═══════════════════════════════════════════════════════════════════════════════
# MILLS, FILTERS, STATS (group-scoped)
# ═══════════════════════════════════════════════════════════════════════════════

class MillsHandler(BaseHandler):
    @require_group
    def get(self):
        conn = get_db()
        rows = conn.execute("""
            SELECT m.*, COUNT(p.id) as product_count
            FROM mills m
            LEFT JOIN products p ON p.mill_id = m.id
            WHERE m.group_id = ?
            GROUP BY m.id
            ORDER BY m.name
        """, (self.active_group_id,)).fetchall()
        conn.close()
        self.write_json({"mills": dicts_from_rows(rows)})


class FiltersHandler(BaseHandler):
    @require_group
    def get(self):
        conn = get_db()
        group_id = self.active_group_id

        species = [r['species'] for r in conn.execute(
            "SELECT DISTINCT species FROM products WHERE group_id = ? AND species IS NOT NULL ORDER BY species",
            (group_id,)
        ).fetchall()]
        thicknesses = [r['thickness'] for r in conn.execute(
            "SELECT DISTINCT thickness FROM products WHERE group_id = ? AND thickness IS NOT NULL ORDER BY thickness",
            (group_id,)
        ).fetchall()]
        grades = [r['grade'] for r in conn.execute(
            "SELECT DISTINCT grade FROM products WHERE group_id = ? AND grade IS NOT NULL ORDER BY grade",
            (group_id,)
        ).fetchall()]
        mills = [r['mill_name'] for r in conn.execute(
            "SELECT DISTINCT mill_name FROM products WHERE group_id = ? AND mill_name IS NOT NULL ORDER BY mill_name",
            (group_id,)
        ).fetchall()]
        conn.close()
        self.write_json({
            "species": species,
            "thicknesses": thicknesses,
            "grades": grades,
            "mills": mills,
        })


class StatsHandler(BaseHandler):
    @require_group
    def get(self):
        # Get stats for active group
        group_id = self.active_group_id
        conn = get_db()

        stats = {
            "total_products": conn.execute(
                "SELECT COUNT(*) as c FROM products WHERE group_id = ?", (group_id,)
            ).fetchone()['c'],
            "total_uploads": conn.execute(
                "SELECT COUNT(*) as c FROM uploads WHERE group_id = ?", (group_id,)
            ).fetchone()['c'],
            "total_mills": conn.execute(
                "SELECT COUNT(*) as c FROM mills WHERE group_id = ?", (group_id,)
            ).fetchone()['c'],
            "flagged_products": conn.execute(
                "SELECT COUNT(*) as c FROM products WHERE group_id = ? AND is_flagged = 1", (group_id,)
            ).fetchone()['c'],
            "unreviewed_products": conn.execute(
                "SELECT COUNT(*) as c FROM products WHERE group_id = ? AND is_reviewed = 0", (group_id,)
            ).fetchone()['c'],
        }
        conn.close()
        self.write_json(stats)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG (admin-only for write, auth for read)
# ═══════════════════════════════════════════════════════════════════════════════

class ConfigHandler(BaseHandler):
    CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

    @require_auth
    def get(self):
        """Return current config (key presence only, never the value)."""
        key = os.environ.get("OPENAI_API_KEY", "")
        if not key and os.path.exists(self.CONFIG_PATH):
            try:
                with open(self.CONFIG_PATH) as f:
                    key = json.load(f).get("OPENAI_API_KEY", "")
            except Exception:
                pass
        self.write_json({
            "openai_key_set": bool(key),
            "openai_key_preview": (key[:8] + "..." if key else ""),
        })

    @require_admin
    def post(self):
        """Save OpenAI API key to config.json (admin only)."""
        data = json.loads(self.request.body)
        key = data.get("openai_api_key", "").strip()
        if not key:
            self.write_json({"error": "No API key provided"}, 400)
            return
        cfg = {}
        if os.path.exists(self.CONFIG_PATH):
            try:
                with open(self.CONFIG_PATH) as f:
                    cfg = json.load(f)
            except Exception:
                pass
        cfg["OPENAI_API_KEY"] = key
        with open(self.CONFIG_PATH, "w") as f:
            json.dump(cfg, f)
        os.environ["OPENAI_API_KEY"] = key

        # Log
        conn = get_db()
        log_activity(conn, self.current_user['id'], 'config_update',
                     'Updated OpenAI API key')
        conn.commit()
        conn.close()

        self.write_json({"success": True, "preview": key[:8] + "..."})


# ═══════════════════════════════════════════════════════════════════════════════
# APPLICATION SETUP
# ═══════════════════════════════════════════════════════════════════════════════

def make_app():
    return tornado.web.Application([
        # ── Page routes ──────────────────────────────────────────────────
        (r"/", LandingHandler),
        (r"/auth", AuthPageHandler),
        (r"/app", AppPageHandler),
        (r"/admin", AdminPageHandler),
        (r"/groups", GroupsPageHandler),

        # ── Auth API ─────────────────────────────────────────────────────
        (r"/api/auth/register", RegisterHandler),
        (r"/api/auth/login", LoginHandler),
        (r"/api/auth/logout", LogoutHandler),
        (r"/api/auth/me", MeHandler),
        (r"/api/auth/forgot-password", ForgotPasswordHandler),
        (r"/api/auth/reset-password", ResetPasswordHandler),

        # ── Group API ────────────────────────────────────────────────────
        (r"/api/groups", GroupsListHandler),
        (r"/api/groups/(\d+)", GroupDetailHandler),
        (r"/api/groups/(\d+)/members", GroupMembersHandler),
        (r"/api/groups/(\d+)/members/(\d+)", GroupMemberHandler),
        (r"/api/groups/(\d+)/invitations", GroupInvitationsHandler),
        (r"/api/groups/(\d+)/invitations/(\d+)", GroupInvitationHandler),
        (r"/api/groups/(\d+)/analytics", GroupAnalyticsHandler),
        (r"/api/groups/switch", SwitchGroupHandler),
        (r"/api/invitations/accept", AcceptInvitationHandler),
        (r"/api/invitations/pending", PendingInvitationsHandler),

        # ── Admin API ────────────────────────────────────────────────────
        (r"/api/admin/users", AdminUsersHandler),
        (r"/api/admin/users/(\d+)", AdminUserHandler),
        (r"/api/admin/analytics", AdminAnalyticsHandler),
        (r"/api/admin/groups", AdminGroupsHandler),

        # ── Core API (group-scoped) ──────────────────────────────────────
        (r"/api/upload", UploadHandler),
        (r"/api/uploads", UploadsHandler),
        (r"/api/uploads/(\d+)", UploadDetailHandler),
        (r"/api/uploads/(\d+)/reparse", UploadReParseHandler),
        (r"/api/search", SearchHandler),
        (r"/api/suggest", SuggestHandler),
        (r"/api/products", ProductsHandler),
        (r"/api/products/bulk", ProductsBulkHandler),
        (r"/api/products/(\d+)", ProductHandler),
        (r"/api/mills", MillsHandler),
        (r"/api/filters", FiltersHandler),
        (r"/api/stats", StatsHandler),
        (r"/api/config", ConfigHandler),

        # ── Static files ─────────────────────────────────────────────────
        (r"/static/(.*)", tornado.web.StaticFileHandler, {"path": STATIC_DIR}),
    ],
        debug=True,
        cookie_secret=SESSION_SECRET,
        template_path=TEMPLATE_DIR,
        static_path=STATIC_DIR,
    )


def setup_session_cleanup():
    """Periodically clean up expired sessions."""
    callback = tornado.ioloop.PeriodicCallback(
        cleanup_expired_sessions, 3600 * 1000  # every hour
    )
    callback.start()


def main():
    # Ensure directories exist
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    os.makedirs(STATIC_DIR, exist_ok=True)
    os.makedirs(os.path.join(os.path.dirname(__file__), "data"), exist_ok=True)

    # Initialize database
    init_db()
    migrate_data_to_groups()

    # Start server
    app = make_app()
    port = int(os.environ.get("PORT", 8888))
    app.listen(port)

    # Start session cleanup task
    setup_session_cleanup()

    print(f"\n{'='*60}")
    print(f"  TIFP Mill Lists - Trading Intelligence Tool")
    print(f"  Running at http://localhost:{port}")
    print(f"  Landing:  http://localhost:{port}/")
    print(f"  Auth:     http://localhost:{port}/auth")
    print(f"  App:      http://localhost:{port}/app")
    print(f"  Groups:   http://localhost:{port}/groups")
    print(f"  Admin:    http://localhost:{port}/admin")
    print(f"{'='*60}\n")
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
