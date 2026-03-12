"""
Database module for Mill Lists application.
Uses SQLite with FTS5 for full-text search.
Includes auth tables, group/workspace tables, and activity tracking.
"""
import sqlite3
import os
import logging
from datetime import datetime

log = logging.getLogger("database")

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "milllists.db")


def get_db():
    """Get a database connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Initialize database schema."""
    conn = get_db()
    c = conn.cursor()

    # ── Auth: Users ───────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            display_name TEXT,
            role TEXT DEFAULT 'user' CHECK(role IN ('user', 'admin')),
            is_active INTEGER DEFAULT 1,
            is_suspended INTEGER DEFAULT 0,
            is_email_verified INTEGER DEFAULT 0,
            email_verification_token TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            last_login_at TEXT
        )
    """)

    # ── Auth: Sessions ────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            created_at TEXT DEFAULT (datetime('now')),
            expires_at TEXT NOT NULL,
            ip_address TEXT,
            user_agent TEXT
        )
    """)

    # ── Auth: Password Reset Tokens ───────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS password_reset_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            token TEXT UNIQUE NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            expires_at TEXT NOT NULL,
            used INTEGER DEFAULT 0
        )
    """)

    # ── Groups / Workspaces ──────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT,
            slug TEXT UNIQUE NOT NULL,
            created_by INTEGER NOT NULL REFERENCES users(id),
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS group_memberships (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            role TEXT NOT NULL DEFAULT 'member'
                CHECK(role IN ('owner', 'admin', 'member')),
            joined_at TEXT DEFAULT (datetime('now')),
            UNIQUE(group_id, user_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS group_invitations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
            email TEXT NOT NULL,
            token_hash TEXT UNIQUE NOT NULL,
            invited_by INTEGER NOT NULL REFERENCES users(id),
            role TEXT DEFAULT 'member' CHECK(role IN ('admin', 'member')),
            status TEXT DEFAULT 'pending'
                CHECK(status IN ('pending', 'accepted', 'expired', 'revoked')),
            created_at TEXT DEFAULT (datetime('now')),
            expires_at TEXT NOT NULL,
            accepted_at TEXT,
            revoked_at TEXT
        )
    """)

    # ── Analytics: Activity Log (group-aware) ─────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
            group_id INTEGER REFERENCES groups(id) ON DELETE SET NULL,
            action TEXT NOT NULL,
            details TEXT,
            ip_address TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

    # ── Mills (group-scoped) ─────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS mills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER REFERENCES groups(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            location TEXT,
            phone TEXT,
            email TEXT,
            contact_name TEXT,
            notes TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(group_id, name)
        )
    """)

    # ── Uploads (group-scoped) ───────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER REFERENCES groups(id) ON DELETE CASCADE,
            filename TEXT NOT NULL,
            original_filename TEXT NOT NULL,
            file_path TEXT NOT NULL,
            file_type TEXT NOT NULL,
            file_size INTEGER,
            mill_id INTEGER REFERENCES mills(id),
            mill_name_detected TEXT,
            user_id INTEGER REFERENCES users(id),
            status TEXT DEFAULT 'pending',
            parsing_method TEXT,
            parsing_confidence REAL,
            error_message TEXT,
            row_count INTEGER DEFAULT 0,
            uploaded_at TEXT DEFAULT (datetime('now')),
            parsed_at TEXT,
            reviewed_at TEXT
        )
    """)

    # ── Products (group-scoped via upload, explicit group_id for queries) ─
    c.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER REFERENCES groups(id) ON DELETE CASCADE,
            upload_id INTEGER NOT NULL REFERENCES uploads(id) ON DELETE CASCADE,
            mill_id INTEGER REFERENCES mills(id),
            mill_name TEXT,
            species TEXT,
            product TEXT NOT NULL,
            product_normalized TEXT,
            thickness TEXT,
            grade TEXT,
            description TEXT,
            quantity TEXT,
            quantity_numeric REAL,
            uom TEXT DEFAULT 'BF',
            price TEXT,
            price_numeric REAL,
            length TEXT,
            width TEXT,
            surface TEXT,
            treatment TEXT,
            color TEXT,
            cut_type TEXT,
            notes TEXT,
            confidence REAL DEFAULT 1.0,
            is_reviewed INTEGER DEFAULT 0,
            is_flagged INTEGER DEFAULT 0,
            raw_text TEXT,
            source_row INTEGER,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)

    # ── FTS5 Index ────────────────────────────────────────────────────────
    c.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS products_fts USING fts5(
            product,
            product_normalized,
            species,
            grade,
            description,
            thickness,
            length,
            mill_name,
            notes,
            treatment,
            color,
            cut_type,
            content='products',
            content_rowid='id',
            tokenize='porter unicode61'
        )
    """)

    # FTS triggers
    c.execute("""
        CREATE TRIGGER IF NOT EXISTS products_ai AFTER INSERT ON products BEGIN
            INSERT INTO products_fts(rowid, product, product_normalized, species, grade, description, thickness, length, mill_name, notes, treatment, color, cut_type)
            VALUES (new.id, new.product, new.product_normalized, new.species, new.grade, new.description, new.thickness, new.length, new.mill_name, new.notes, new.treatment, new.color, new.cut_type);
        END
    """)
    c.execute("""
        CREATE TRIGGER IF NOT EXISTS products_ad AFTER DELETE ON products BEGIN
            INSERT INTO products_fts(products_fts, rowid, product, product_normalized, species, grade, description, thickness, length, mill_name, notes, treatment, color, cut_type)
            VALUES ('delete', old.id, old.product, old.product_normalized, old.species, old.grade, old.description, old.thickness, old.length, old.mill_name, old.notes, old.treatment, old.color, old.cut_type);
        END
    """)
    c.execute("""
        CREATE TRIGGER IF NOT EXISTS products_au AFTER UPDATE ON products BEGIN
            INSERT INTO products_fts(products_fts, rowid, product, product_normalized, species, grade, description, thickness, length, mill_name, notes, treatment, color, cut_type)
            VALUES ('delete', old.id, old.product, old.product_normalized, old.species, old.grade, old.description, old.thickness, old.length, old.mill_name, old.notes, old.treatment, old.color, old.cut_type);
            INSERT INTO products_fts(rowid, product, product_normalized, species, grade, description, thickness, length, mill_name, notes, treatment, color, cut_type)
            VALUES (new.id, new.product, new.product_normalized, new.species, new.grade, new.description, new.thickness, new.length, new.mill_name, new.notes, new.treatment, new.color, new.cut_type);
        END
    """)

    # ── Other tables ──────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS parsing_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER REFERENCES groups(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            mill_id INTEGER REFERENCES mills(id),
            file_type TEXT,
            column_mapping TEXT,
            parsing_rules TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS search_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER REFERENCES groups(id) ON DELETE CASCADE,
            query TEXT NOT NULL,
            result_count INTEGER,
            user_id INTEGER REFERENCES users(id),
            searched_at TEXT DEFAULT (datetime('now'))
        )
    """)

    # ── Indexes ───────────────────────────────────────────────────────────
    c.execute("CREATE INDEX IF NOT EXISTS idx_products_mill ON products(mill_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_products_upload ON products(upload_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_products_species ON products(species)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_products_thickness ON products(thickness)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_products_grade ON products(grade)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_products_group ON products(group_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_uploads_status ON uploads(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_uploads_group ON uploads(group_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_mills_group ON mills(group_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_activity_user ON activity_log(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_activity_group ON activity_log(group_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_activity_action ON activity_log(action)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_activity_created ON activity_log(created_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_memberships_user ON group_memberships(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_memberships_group ON group_memberships(group_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_invitations_group ON group_invitations(group_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_invitations_email ON group_invitations(email)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_invitations_status ON group_invitations(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_search_group ON search_history(group_id)")

    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")

    # Run any pending migrations
    migrate_db()


def migrate_db():
    """
    Run incremental schema migrations.
    Safe to call on every startup — skips already-applied changes.

    Uses ALTER TABLE ADD COLUMN for additive-only changes (safe in SQLite).
    Never uses ALTER TABLE RENAME (causes FK cascade issues in SQLite).
    """
    conn = get_db()
    c = conn.cursor()

    # ── Guard: ensure users table has the full auth schema ────────────────
    users_cols = {row[1] for row in c.execute("PRAGMA table_info(users)").fetchall()}
    if 'password_hash' not in users_cols:
        conn.close()
        raise RuntimeError(
            "Database schema is outdated (missing auth columns). "
            "Delete data/milllists.db and restart to rebuild the schema. "
            "Uploaded files in uploads/ are not affected."
        )

    # ── Migration: add group_id to existing tables if missing ────────────
    # These are safe ADD COLUMN migrations for databases created before
    # the group feature was added.

    def _has_column(table, col):
        cols = {row[1] for row in c.execute(f"PRAGMA table_info({table})").fetchall()}
        return col in cols

    migrated = False

    # uploads.group_id
    if _has_column('uploads', 'filename') and not _has_column('uploads', 'group_id'):
        c.execute("ALTER TABLE uploads ADD COLUMN group_id INTEGER REFERENCES groups(id)")
        log.info("Migration: added group_id to uploads")
        migrated = True

    # products.group_id
    if _has_column('products', 'product') and not _has_column('products', 'group_id'):
        c.execute("ALTER TABLE products ADD COLUMN group_id INTEGER REFERENCES groups(id)")
        log.info("Migration: added group_id to products")
        migrated = True

    # mills.group_id  (also drop the old UNIQUE on name-only)
    if _has_column('mills', 'name') and not _has_column('mills', 'group_id'):
        c.execute("ALTER TABLE mills ADD COLUMN group_id INTEGER REFERENCES groups(id)")
        log.info("Migration: added group_id to mills")
        migrated = True

    # activity_log.group_id
    if _has_column('activity_log', 'action') and not _has_column('activity_log', 'group_id'):
        c.execute("ALTER TABLE activity_log ADD COLUMN group_id INTEGER REFERENCES groups(id)")
        log.info("Migration: added group_id to activity_log")
        migrated = True

    # search_history.group_id
    if _has_column('search_history', 'query') and not _has_column('search_history', 'group_id'):
        c.execute("ALTER TABLE search_history ADD COLUMN group_id INTEGER REFERENCES groups(id)")
        log.info("Migration: added group_id to search_history")
        migrated = True

    # parsing_templates.group_id
    if _has_column('parsing_templates', 'name') and not _has_column('parsing_templates', 'group_id'):
        c.execute("ALTER TABLE parsing_templates ADD COLUMN group_id INTEGER REFERENCES groups(id)")
        log.info("Migration: added group_id to parsing_templates")
        migrated = True

    # ── Ensure group tables exist (for DBs created before groups feature) ─
    tables = {row[0] for row in c.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}

    if 'groups' not in tables:
        c.execute("""
            CREATE TABLE groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                slug TEXT UNIQUE NOT NULL,
                created_by INTEGER NOT NULL REFERENCES users(id),
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
        log.info("Migration: created groups table")
        migrated = True

    if 'group_memberships' not in tables:
        c.execute("""
            CREATE TABLE group_memberships (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                role TEXT NOT NULL DEFAULT 'member'
                    CHECK(role IN ('owner', 'admin', 'member')),
                joined_at TEXT DEFAULT (datetime('now')),
                UNIQUE(group_id, user_id)
            )
        """)
        log.info("Migration: created group_memberships table")
        migrated = True

    if 'group_invitations' not in tables:
        c.execute("""
            CREATE TABLE group_invitations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
                email TEXT NOT NULL,
                token_hash TEXT UNIQUE NOT NULL,
                invited_by INTEGER NOT NULL REFERENCES users(id),
                role TEXT DEFAULT 'member' CHECK(role IN ('admin', 'member')),
                status TEXT DEFAULT 'pending'
                    CHECK(status IN ('pending', 'accepted', 'expired', 'revoked')),
                created_at TEXT DEFAULT (datetime('now')),
                expires_at TEXT NOT NULL,
                accepted_at TEXT,
                revoked_at TEXT
            )
        """)
        log.info("Migration: created group_invitations table")
        migrated = True

    if migrated:
        # Create indexes for new columns/tables
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_products_group ON products(group_id)",
            "CREATE INDEX IF NOT EXISTS idx_uploads_group ON uploads(group_id)",
            "CREATE INDEX IF NOT EXISTS idx_mills_group ON mills(group_id)",
            "CREATE INDEX IF NOT EXISTS idx_activity_group ON activity_log(group_id)",
            "CREATE INDEX IF NOT EXISTS idx_memberships_user ON group_memberships(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_memberships_group ON group_memberships(group_id)",
            "CREATE INDEX IF NOT EXISTS idx_invitations_group ON group_invitations(group_id)",
            "CREATE INDEX IF NOT EXISTS idx_invitations_email ON group_invitations(email)",
            "CREATE INDEX IF NOT EXISTS idx_invitations_status ON group_invitations(status)",
            "CREATE INDEX IF NOT EXISTS idx_search_group ON search_history(group_id)",
        ]:
            try:
                c.execute(idx_sql)
            except Exception:
                pass  # Index may already exist

    conn.commit()
    conn.close()

    if migrated:
        log.info("Database migrations completed successfully.")


def migrate_data_to_groups():
    """
    One-time data migration: for every existing user without a group,
    create a personal 'My Workspace' group and assign their orphaned
    (group_id IS NULL) data to it.

    Safe to call on every startup — only acts on data not yet migrated.
    """
    conn = get_db()
    c = conn.cursor()

    # Find users who have no group memberships at all
    orphan_users = c.execute("""
        SELECT u.id, u.email, u.display_name
        FROM users u
        WHERE u.id NOT IN (SELECT DISTINCT user_id FROM group_memberships)
    """).fetchall()

    if not orphan_users:
        conn.close()
        return

    import re

    for user in orphan_users:
        uid = user[0]
        email = user[1]
        display_name = user[2] or email.split('@')[0]

        group_name = f"{display_name}'s Workspace"
        # Create a unique slug from the email
        slug = re.sub(r'[^a-z0-9]+', '-', email.lower().split('@')[0]).strip('-')
        # Ensure slug uniqueness
        existing = c.execute(
            "SELECT id FROM groups WHERE slug = ?", (slug,)
        ).fetchone()
        if existing:
            slug = f"{slug}-{uid}"

        # Create group
        c.execute("""
            INSERT INTO groups (name, description, slug, created_by)
            VALUES (?, 'Auto-created personal workspace', ?, ?)
        """, (group_name, slug, uid))
        gid = c.lastrowid

        # Add user as owner
        c.execute("""
            INSERT INTO group_memberships (group_id, user_id, role)
            VALUES (?, ?, 'owner')
        """, (gid, uid))

        # Assign orphaned uploads (user_id matches, group_id is NULL)
        c.execute(
            "UPDATE uploads SET group_id = ? WHERE user_id = ? AND group_id IS NULL",
            (gid, uid)
        )

        # Assign orphaned products via their uploads
        c.execute("""
            UPDATE products SET group_id = ?
            WHERE group_id IS NULL
            AND upload_id IN (SELECT id FROM uploads WHERE user_id = ?)
        """, (gid, uid))

        # Assign orphaned mills (they may have been created by uploads)
        c.execute(
            "UPDATE mills SET group_id = ? WHERE group_id IS NULL",
            (gid,)
        )

        # Assign orphaned activity
        c.execute(
            "UPDATE activity_log SET group_id = ? WHERE user_id = ? AND group_id IS NULL",
            (gid, uid)
        )

        # Assign orphaned search history
        c.execute(
            "UPDATE search_history SET group_id = ? WHERE user_id = ? AND group_id IS NULL",
            (gid, uid)
        )

        log.info(f"Migration: created workspace '{group_name}' for user {email} (group_id={gid})")

    conn.commit()
    conn.close()
    log.info(f"Data migration: created personal workspaces for {len(orphan_users)} user(s).")


def dict_from_row(row):
    """Convert sqlite3.Row to dict."""
    if row is None:
        return None
    return dict(row)


def dicts_from_rows(rows):
    """Convert list of sqlite3.Row to list of dicts."""
    return [dict(r) for r in rows]
