"""
Multi-tenant group/workspace management module for lumber trading app.

Provides CRUD operations for groups, membership management, invitations,
and group-scoped analytics and activity tracking.
"""

import hashlib
import secrets
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

from database import get_db, dict_from_row, dicts_from_rows
from auth import log_activity


# ============================================================================
# SLUG GENERATION
# ============================================================================

def _slugify(name: str) -> str:
    """
    Generate URL-safe slug from group name.

    Lowercase, replace spaces/special chars with hyphens, collapse multiples.
    """
    slug = name.lower().strip()
    # Replace spaces and special characters with hyphens
    slug = re.sub(r'[^\w\s-]', '', slug)
    slug = re.sub(r'[\s_]+', '-', slug)
    # Collapse multiple hyphens
    slug = re.sub(r'-+', '-', slug)
    # Strip leading/trailing hyphens
    slug = slug.strip('-')
    return slug


def _ensure_unique_slug(base_slug: str) -> str:
    """Ensure slug is unique by appending random suffix if needed."""
    conn = get_db()
    cursor = conn.cursor()

    # Check if base slug already exists
    cursor.execute("SELECT id FROM groups WHERE slug = ? AND is_active = 1", (base_slug,))
    if not cursor.fetchone():
        return base_slug

    # Append random suffix
    for _ in range(10):
        slug = f"{base_slug}-{secrets.token_hex(3)}"
        cursor.execute("SELECT id FROM groups WHERE slug = ? AND is_active = 1", (slug,))
        if not cursor.fetchone():
            return slug

    raise ValueError("Could not generate unique slug")


# ============================================================================
# GROUP CRUD
# ============================================================================

def create_group(
    name: str,
    description: str = "",
    created_by_user_id: int = None
) -> Dict[str, Any]:
    """
    Create a new group and add creator as owner.

    Args:
        name: Group name (2-100 chars)
        description: Group description
        created_by_user_id: User ID of creator

    Returns:
        Dict with group info and created status, or error dict
    """
    # Validate name
    if not name or len(name) < 2 or len(name) > 100:
        return {"error": "Group name must be 2-100 characters"}

    try:
        conn = get_db()
        cursor = conn.cursor()

        # Generate unique slug
        base_slug = _slugify(name)
        slug = _ensure_unique_slug(base_slug)

        # Create group
        now = datetime.utcnow().isoformat()
        cursor.execute("""
            INSERT INTO groups (name, description, slug, created_by, is_active, created_at, updated_at)
            VALUES (?, ?, ?, ?, 1, ?, ?)
        """, (name, description, slug, created_by_user_id, now, now))

        group_id = cursor.lastrowid
        conn.commit()

        # Add creator as owner
        cursor.execute("""
            INSERT INTO group_memberships (group_id, user_id, role, joined_at)
            VALUES (?, ?, 'owner', ?)
        """, (group_id, created_by_user_id, now))
        conn.commit()

        # Log activity
        log_activity(
            conn, created_by_user_id, "create_group",
            f"Created group '{name}' (id={group_id})"
        )

        return {
            "success": True,
            "id": group_id,
            "name": name,
            "description": description,
            "slug": slug,
            "created_by": created_by_user_id,
            "created_at": now
        }
    except Exception as e:
        return {"error": str(e)}


def get_group(group_id: int) -> Optional[Dict[str, Any]]:
    """
    Get group details including member count.

    Args:
        group_id: Group ID

    Returns:
        Group dict or None if not found/inactive
    """
    try:
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, name, description, slug, created_by, is_active, created_at, updated_at
            FROM groups
            WHERE id = ? AND is_active = 1
        """, (group_id,))

        row = cursor.fetchone()
        if not row:
            return None

        group = dict_from_row(row)

        # Get member count
        cursor.execute("""
            SELECT COUNT(*) as count FROM group_memberships WHERE group_id = ?
        """, (group_id,))
        count_row = cursor.fetchone()
        group["member_count"] = count_row["count"] if count_row else 0

        return group
    except Exception:
        return None


def get_user_groups(user_id: int) -> List[Dict[str, Any]]:
    """
    Get all groups a user belongs to with their role in each.

    Args:
        user_id: User ID

    Returns:
        List of group dicts with member role
    """
    try:
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT g.id, g.name, g.description, g.slug, g.created_by,
                   g.is_active, g.created_at, g.updated_at,
                   gm.role, gm.joined_at
            FROM groups g
            JOIN group_memberships gm ON g.id = gm.group_id
            WHERE gm.user_id = ? AND g.is_active = 1
            ORDER BY g.name
        """, (user_id,))

        rows = cursor.fetchall()
        groups = []
        for row in rows:
            group = dict_from_row(row)
            groups.append(group)

        return groups
    except Exception:
        return []


def update_group(
    group_id: int,
    updates_dict: Dict[str, Any],
    user_id: int
) -> Dict[str, Any]:
    """
    Update group name/description. Only owner can update.

    Args:
        group_id: Group ID
        updates_dict: Dict with 'name' and/or 'description' keys
        user_id: User attempting update

    Returns:
        Success or error dict
    """
    try:
        # Check ownership
        membership = check_membership(user_id, group_id)
        if not membership or membership["role"] != "owner":
            return {"error": "Only group owner can update group"}

        conn = get_db()
        cursor = conn.cursor()

        # Get current group
        group = get_group(group_id)
        if not group:
            return {"error": "Group not found"}

        # Prepare updates
        updates = {}
        if "name" in updates_dict:
            name = updates_dict["name"]
            if len(name) < 2 or len(name) > 100:
                return {"error": "Group name must be 2-100 characters"}
            updates["name"] = name

        if "description" in updates_dict:
            updates["description"] = updates_dict["description"]

        if not updates:
            return {"success": True, "message": "No changes"}

        # Update group
        updates["updated_at"] = datetime.utcnow().isoformat()
        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values()) + [group_id]

        cursor.execute(f"""
            UPDATE groups SET {set_clause} WHERE id = ?
        """, values)
        conn.commit()

        # Log activity
        log_activity(
            conn, user_id, "update_group",
            f"Updated group '{group['name']}' (id={group_id})"
        )

        return {"success": True, "updates": updates}
    except Exception as e:
        return {"error": str(e)}


def delete_group(group_id: int, user_id: int) -> Dict[str, Any]:
    """
    Soft-delete group (set is_active=0). Only owner can delete.

    Args:
        group_id: Group ID
        user_id: User attempting deletion

    Returns:
        Success or error dict
    """
    try:
        # Check ownership
        membership = check_membership(user_id, group_id)
        if not membership or membership["role"] != "owner":
            return {"error": "Only group owner can delete group"}

        conn = get_db()
        cursor = conn.cursor()

        group = get_group(group_id)
        if not group:
            return {"error": "Group not found"}

        # Soft delete
        now = datetime.utcnow().isoformat()
        cursor.execute("""
            UPDATE groups SET is_active = 0, updated_at = ? WHERE id = ?
        """, (now, group_id))
        conn.commit()

        # Log activity
        log_activity(
            conn, user_id, "delete_group",
            f"Deleted group '{group['name']}' (id={group_id})"
        )

        return {"success": True, "message": "Group deleted"}
    except Exception as e:
        return {"error": str(e)}


# ============================================================================
# MEMBERSHIP MANAGEMENT
# ============================================================================

def check_membership(user_id: int, group_id: int) -> Optional[Dict[str, Any]]:
    """
    Check if user is a member of group. Returns membership info or None.

    Args:
        user_id: User ID
        group_id: Group ID

    Returns:
        Membership dict with role, or None
    """
    try:
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, group_id, user_id, role, joined_at
            FROM group_memberships
            WHERE user_id = ? AND group_id = ?
        """, (user_id, group_id))

        row = cursor.fetchone()
        return dict_from_row(row) if row else None
    except Exception:
        return None


def require_membership(user_id: int, group_id: int) -> Dict[str, Any]:
    """
    Check membership or return error dict. Useful for middleware/auth checks.

    Args:
        user_id: User ID
        group_id: Group ID

    Returns:
        Membership dict or error dict
    """
    membership = check_membership(user_id, group_id)
    if membership:
        return membership
    return {"error": "User is not a member of this group"}


def get_group_members(group_id: int) -> List[Dict[str, Any]]:
    """
    Get all members of a group with user info.

    Args:
        group_id: Group ID

    Returns:
        List of member dicts with user info and role
    """
    try:
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT gm.id, gm.user_id, u.email, u.display_name,
                   gm.role, gm.joined_at
            FROM group_memberships gm
            LEFT JOIN users u ON gm.user_id = u.id
            WHERE gm.group_id = ?
            ORDER BY gm.role DESC, u.display_name ASC
        """, (group_id,))

        rows = cursor.fetchall()
        return dicts_from_rows(rows) if rows else []
    except Exception:
        return []


def update_member_role(
    group_id: int,
    target_user_id: int,
    new_role: str,
    acting_user_id: int
) -> Dict[str, Any]:
    """
    Update a member's role. Only owner can change roles.

    Restrictions:
    - Acting user must be owner
    - Can't change own role
    - Can't have multiple owners (transfer ownership required)
    - Valid roles: owner, admin, member

    Args:
        group_id: Group ID
        target_user_id: User being updated
        new_role: New role (owner, admin, member)
        acting_user_id: User making the change

    Returns:
        Success or error dict
    """
    try:
        # Validate new_role
        if new_role not in ["owner", "admin", "member"]:
            return {"error": "Invalid role"}

        # Check acting user is owner
        acting_membership = check_membership(acting_user_id, group_id)
        if not acting_membership or acting_membership["role"] != "owner":
            return {"error": "Only group owner can change member roles"}

        # Can't change own role
        if acting_user_id == target_user_id:
            return {"error": "You cannot change your own role"}

        # Get target membership
        target_membership = check_membership(target_user_id, group_id)
        if not target_membership:
            return {"error": "Target user is not a member"}

        # Can't have multiple owners - check if target is owner and new role differs
        if target_membership["role"] == "owner" and new_role != "owner":
            return {"error": "Cannot remove owner role - transfer ownership first"}

        # If making someone owner, check no other owners exist (already checked above)
        if new_role == "owner" and target_membership["role"] != "owner":
            # Check if owner already exists
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) as count FROM group_memberships
                WHERE group_id = ? AND role = 'owner'
            """, (group_id,))
            row = cursor.fetchone()
            if row and row["count"] > 0:
                return {"error": "Group already has an owner - transfer ownership instead"}

        # Update role
        conn = get_db()
        cursor = conn.cursor()
        now = datetime.utcnow().isoformat()

        cursor.execute("""
            UPDATE group_memberships SET role = ? WHERE user_id = ? AND group_id = ?
        """, (new_role, target_user_id, group_id))
        conn.commit()

        # Log activity
        log_activity(
            conn, acting_user_id, "update_member_role",
            f"Changed user {target_user_id} role to {new_role} in group {group_id}"
        )

        return {
            "success": True,
            "user_id": target_user_id,
            "group_id": group_id,
            "new_role": new_role
        }
    except Exception as e:
        return {"error": str(e)}


def remove_member(
    group_id: int,
    target_user_id: int,
    acting_user_id: int
) -> Dict[str, Any]:
    """
    Remove a member from group. Owner or admin can remove, but with restrictions.

    Restrictions:
    - Acting user must be owner or admin
    - Can't remove owner
    - Owner can't remove themselves
    - Kills user's sessions for that group context

    Args:
        group_id: Group ID
        target_user_id: User being removed
        acting_user_id: User making the removal

    Returns:
        Success or error dict
    """
    try:
        # Check acting user is owner or admin
        acting_membership = check_membership(acting_user_id, group_id)
        if not acting_membership or acting_membership["role"] not in ["owner", "admin"]:
            return {"error": "Only owner or admin can remove members"}

        # Get target membership
        target_membership = check_membership(target_user_id, group_id)
        if not target_membership:
            return {"error": "Target user is not a member"}

        # Can't remove owner
        if target_membership["role"] == "owner":
            return {"error": "Cannot remove group owner"}

        # Owner can't remove themselves
        if acting_user_id == target_user_id and acting_membership["role"] == "owner":
            return {"error": "Owner cannot remove themselves"}

        # Remove membership
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute("""
            DELETE FROM group_memberships WHERE user_id = ? AND group_id = ?
        """, (target_user_id, group_id))
        conn.commit()

        # Log activity
        log_activity(
            conn, acting_user_id, "remove_member",
            f"Removed user {target_user_id} from group {group_id}"
        )

        # TODO: Kill user's sessions for this group context
        # This would be implemented in a session management module

        return {"success": True, "message": "Member removed"}
    except Exception as e:
        return {"error": str(e)}


# ============================================================================
# INVITATIONS
# ============================================================================

def create_invitation(
    group_id: int,
    email: str,
    invited_by_user_id: int,
    role: str = "member"
) -> Dict[str, Any]:
    """
    Create an invitation to join group. Stores SHA-256 hash of token.

    Restrictions:
    - Only owner/admin can invite
    - Can't invite someone already in group
    - Can't invite if pending invite already exists
    - Expires in 7 days

    Args:
        group_id: Group ID
        email: Email address to invite
        invited_by_user_id: User ID of inviter
        role: Initial role for accepted member (default: member)

    Returns:
        Dict with plain token (for invite link) and invitation info, or error dict
    """
    try:
        # Check inviter is owner or admin
        inviter_membership = check_membership(invited_by_user_id, group_id)
        if not inviter_membership or inviter_membership["role"] not in ["owner", "admin"]:
            return {"error": "Only owner or admin can invite members"}

        if role not in ["owner", "admin", "member"]:
            return {"error": "Invalid role"}

        conn = get_db()
        cursor = conn.cursor()

        # Check email not already in group
        cursor.execute("""
            SELECT gm.id FROM group_memberships gm
            JOIN users u ON gm.user_id = u.id
            WHERE gm.group_id = ? AND u.email = ?
        """, (group_id, email))

        if cursor.fetchone():
            return {"error": "User is already a member of this group"}

        # Check no pending invite exists
        cursor.execute("""
            SELECT id FROM group_invitations
            WHERE group_id = ? AND email = ? AND status = 'pending'
        """, (group_id, email))

        if cursor.fetchone():
            return {"error": "Pending invitation already exists for this email"}

        # Generate token and hash
        token = secrets.token_urlsafe(48)
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        now = datetime.utcnow().isoformat()
        expires_at = (datetime.utcnow() + timedelta(days=7)).isoformat()

        # Create invitation
        cursor.execute("""
            INSERT INTO group_invitations
            (group_id, email, token_hash, invited_by, role, status, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
        """, (group_id, email, token_hash, invited_by_user_id, role, now, expires_at))

        invitation_id = cursor.lastrowid
        conn.commit()

        # Log activity
        log_activity(
            conn, invited_by_user_id, "create_invitation",
            f"Invited {email} to group {group_id}"
        )

        return {
            "success": True,
            "id": invitation_id,
            "group_id": group_id,
            "email": email,
            "token": token,  # Plain token for invite link
            "role": role,
            "expires_at": expires_at
        }
    except Exception as e:
        return {"error": str(e)}


def get_group_invitations(group_id: int) -> List[Dict[str, Any]]:
    """
    Get all invitations for a group.

    Args:
        group_id: Group ID

    Returns:
        List of invitation dicts
    """
    try:
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, group_id, email, invited_by, role, status,
                   created_at, expires_at, accepted_at, revoked_at
            FROM group_invitations
            WHERE group_id = ?
            ORDER BY created_at DESC
        """, (group_id,))

        rows = cursor.fetchall()
        return dicts_from_rows(rows) if rows else []
    except Exception:
        return []


def accept_invitation(token: str, user_id: int) -> Dict[str, Any]:
    """
    Accept an invitation. Validates token, expiry, and user email.

    Args:
        token: Plain token from invite link
        user_id: User ID accepting invitation

    Returns:
        Success or error dict
    """
    try:
        conn = get_db()
        cursor = conn.cursor()

        # Get user email
        cursor.execute("SELECT email FROM users WHERE id = ?", (user_id,))
        user_row = cursor.fetchone()
        if not user_row:
            return {"error": "User not found"}

        user_email = user_row["email"]

        # Hash provided token
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        # Find invitation
        cursor.execute("""
            SELECT id, group_id, email, role, status, expires_at
            FROM group_invitations
            WHERE token_hash = ?
        """, (token_hash,))

        invitation = cursor.fetchone()
        if not invitation:
            return {"error": "Invalid invitation token"}

        invitation = dict_from_row(invitation)

        # Check status
        if invitation["status"] != "pending":
            return {"error": f"Invitation is {invitation['status']}"}

        # Check expiry
        expires_at = datetime.fromisoformat(invitation["expires_at"])
        if datetime.utcnow() > expires_at:
            # Mark as expired
            cursor.execute("""
                UPDATE group_invitations SET status = 'expired' WHERE id = ?
            """, (invitation["id"],))
            conn.commit()
            return {"error": "Invitation has expired"}

        # Check email matches
        if user_email.lower() != invitation["email"].lower():
            return {"error": "Email does not match invitation"}

        # Create membership
        now = datetime.utcnow().isoformat()
        cursor.execute("""
            INSERT INTO group_memberships (group_id, user_id, role, joined_at)
            VALUES (?, ?, ?, ?)
        """, (invitation["group_id"], user_id, invitation["role"], now))

        # Mark invitation as accepted
        cursor.execute("""
            UPDATE group_invitations SET status = 'accepted', accepted_at = ? WHERE id = ?
        """, (now, invitation["id"]))

        conn.commit()

        # Log activity
        log_activity(
            conn, user_id, "accept_invitation",
            f"Accepted invitation to group {invitation['group_id']}"
        )

        return {
            "success": True,
            "group_id": invitation["group_id"],
            "role": invitation["role"],
            "message": "Invitation accepted"
        }
    except Exception as e:
        return {"error": str(e)}


def revoke_invitation(
    invitation_id: int,
    group_id: int,
    user_id: int
) -> Dict[str, Any]:
    """
    Revoke an invitation. Only owner/admin can revoke.

    Args:
        invitation_id: Invitation ID
        group_id: Group ID
        user_id: User revoking invitation

    Returns:
        Success or error dict
    """
    try:
        # Check user is owner or admin
        membership = check_membership(user_id, group_id)
        if not membership or membership["role"] not in ["owner", "admin"]:
            return {"error": "Only owner or admin can revoke invitations"}

        conn = get_db()
        cursor = conn.cursor()

        # Check invitation exists and belongs to group
        cursor.execute("""
            SELECT id FROM group_invitations WHERE id = ? AND group_id = ?
        """, (invitation_id, group_id))

        if not cursor.fetchone():
            return {"error": "Invitation not found"}

        now = datetime.utcnow().isoformat()
        cursor.execute("""
            UPDATE group_invitations SET status = 'revoked', revoked_at = ? WHERE id = ?
        """, (now, invitation_id))
        conn.commit()

        # Log activity
        log_activity(
            conn, user_id, "revoke_invitation",
            f"Revoked invitation {invitation_id} in group {group_id}"
        )

        return {"success": True, "message": "Invitation revoked"}
    except Exception as e:
        return {"error": str(e)}


def get_user_pending_invitations(email: str) -> List[Dict[str, Any]]:
    """
    Get pending invitations for an email address.

    Useful for newly registered users to see pending invites.

    Args:
        email: Email address

    Returns:
        List of invitation dicts with group info
    """
    try:
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT gi.id, gi.group_id, g.name as group_name, g.slug,
                   gi.email, gi.role, gi.status, gi.created_at, gi.expires_at
            FROM group_invitations gi
            JOIN groups g ON gi.group_id = g.id
            WHERE gi.email = ? AND gi.status = 'pending' AND g.is_active = 1
            ORDER BY gi.created_at DESC
        """, (email,))

        rows = cursor.fetchall()
        return dicts_from_rows(rows) if rows else []
    except Exception:
        return []


def claim_invitations_for_user(user_id: int, email: str) -> List[Dict[str, Any]]:
    """
    Get pending invitations for a user's email after registration.

    Returns list of invitations so user can choose to accept them.
    Does NOT auto-accept.

    Args:
        user_id: User ID
        email: User's email address

    Returns:
        List of pending invitation dicts
    """
    return get_user_pending_invitations(email)


# ============================================================================
# GROUP ANALYTICS
# ============================================================================

def get_group_analytics(group_id: int) -> Dict[str, Any]:
    """
    Get comprehensive analytics for a group.

    Includes:
    - member_count
    - upload_count
    - product_count
    - mill_count
    - search_count
    - uploads_by_day (last 30d)
    - recent_activity (last 20 events)
    - top_members_by_uploads
    - top_members_by_searches

    Args:
        group_id: Group ID

    Returns:
        Analytics dict
    """
    try:
        conn = get_db()
        cursor = conn.cursor()

        # Member count
        cursor.execute("""
            SELECT COUNT(*) as count FROM group_memberships WHERE group_id = ?
        """, (group_id,))
        member_count = cursor.fetchone()["count"]

        # Upload count
        cursor.execute("""
            SELECT COUNT(*) as count FROM uploads WHERE group_id = ?
        """, (group_id,))
        upload_count = cursor.fetchone()["count"]

        # Product count
        cursor.execute("""
            SELECT COUNT(*) as count FROM products WHERE group_id = ?
        """, (group_id,))
        product_count = cursor.fetchone()["count"]

        # Mill count
        cursor.execute("""
            SELECT COUNT(*) as count FROM mills WHERE group_id = ?
        """, (group_id,))
        mill_count = cursor.fetchone()["count"]

        # Search count
        cursor.execute("""
            SELECT COUNT(*) as count FROM search_history WHERE group_id = ?
        """, (group_id,))
        search_count = cursor.fetchone()["count"]

        # Uploads by day (last 30 days)
        thirty_days_ago = (datetime.utcnow() - timedelta(days=30)).isoformat()
        cursor.execute("""
            SELECT DATE(created_at) as date, COUNT(*) as count
            FROM uploads
            WHERE group_id = ? AND created_at > ?
            GROUP BY DATE(created_at)
            ORDER BY date DESC
        """, (group_id, thirty_days_ago))

        uploads_by_day = dicts_from_rows(cursor.fetchall())

        # Recent activity
        cursor.execute("""
            SELECT id, user_id, action, details, created_at
            FROM activity_log
            WHERE group_id = ?
            ORDER BY created_at DESC
            LIMIT 20
        """, (group_id,))

        recent_activity = dicts_from_rows(cursor.fetchall())

        # Top members by uploads
        cursor.execute("""
            SELECT u.id, u.email, u.display_name, COUNT(up.id) as upload_count
            FROM users u
            LEFT JOIN uploads up ON u.id = up.user_id AND up.group_id = ?
            WHERE u.id IN (
                SELECT user_id FROM group_memberships WHERE group_id = ?
            )
            GROUP BY u.id
            ORDER BY upload_count DESC
            LIMIT 10
        """, (group_id, group_id))

        top_members_by_uploads = dicts_from_rows(cursor.fetchall())

        # Top members by searches
        cursor.execute("""
            SELECT u.id, u.email, u.display_name, COUNT(sh.id) as search_count
            FROM users u
            LEFT JOIN search_history sh ON u.id = sh.user_id AND sh.group_id = ?
            WHERE u.id IN (
                SELECT user_id FROM group_memberships WHERE group_id = ?
            )
            GROUP BY u.id
            ORDER BY search_count DESC
            LIMIT 10
        """, (group_id, group_id))

        top_members_by_searches = dicts_from_rows(cursor.fetchall())

        return {
            "group_id": group_id,
            "member_count": member_count,
            "upload_count": upload_count,
            "product_count": product_count,
            "mill_count": mill_count,
            "search_count": search_count,
            "uploads_by_day": uploads_by_day,
            "recent_activity": recent_activity,
            "top_members_by_uploads": top_members_by_uploads,
            "top_members_by_searches": top_members_by_searches
        }
    except Exception as e:
        return {"error": str(e)}


def get_group_activity(group_id: int, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Get recent activity log entries for a group.

    Args:
        group_id: Group ID
        limit: Max number of entries (default: 50)

    Returns:
        List of activity log dicts
    """
    try:
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, user_id, action, details, created_at
            FROM activity_log
            WHERE group_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """, (group_id, limit))

        rows = cursor.fetchall()
        return dicts_from_rows(rows) if rows else []
    except Exception:
        return []


# ============================================================================
# ACTIVE GROUP MANAGEMENT
# ============================================================================

def get_default_group(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Get the first group a user belongs to (for initial login).

    Args:
        user_id: User ID

    Returns:
        Group dict or None
    """
    try:
        groups = get_user_groups(user_id)
        return groups[0] if groups else None
    except Exception:
        return None


def switch_active_group(user_id: int, group_id: int) -> Dict[str, Any]:
    """
    Switch user's active group. Validates membership.

    Args:
        user_id: User ID
        group_id: Group ID to switch to

    Returns:
        Group dict or error dict
    """
    try:
        # Check membership
        membership = check_membership(user_id, group_id)
        if not membership:
            return {"error": "User is not a member of this group"}

        # Get group
        group = get_group(group_id)
        if not group:
            return {"error": "Group not found"}

        return {
            "success": True,
            "group": group,
            "role": membership["role"]
        }
    except Exception as e:
        return {"error": str(e)}
