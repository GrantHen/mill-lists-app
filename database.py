"""
Database module for Mill Lists application.
Uses SQLite with FTS5 for full-text search.
Includes auth tables: users, sessions, password resets, activity log.
"""
import sqlite3
import os
from datetime import datetime

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

    # ── Analytics: Activity Log ───────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
            action TEXT NOT NULL,
            details TEXT,
            ip_address TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

    # ── Mills ─────────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS mills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            location TEXT,
            phone TEXT,
            email TEXT,
            contact_name TEXT,
            notes TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)

    # ── Uploads ───────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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

    # ── Products ──────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    c.execute("CREATE INDEX IF NOT EXISTS idx_uploads_status ON uploads(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_activity_user ON activity_log(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_activity_action ON activity_log(action)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_activity_created ON activity_log(created_at)")

    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")

    # Run any pending migrations
    migrate_db()


def migrate_db():
    """
    Run incremental schema migrations.
    Safe to call on every startup — skips already-applied changes.

    IMPORTANT: Never use ALTER TABLE ... RENAME on tables referenced by
    foreign keys in other tables. SQLite will update all FK references
    to the new name, causing FK violations when the old table is dropped.
    Instead, use writable_schema or a full rebuild.
    """
    import logging
    log = logging.getLogger("database")

    conn = get_db()
    c = conn.cursor()

    # ── Guard: ensure users table has the full auth schema ────────────────
    # If the users table is missing password_hash, it's a legacy minimal table.
    # We rebuild the entire DB to avoid SQLite FK rename cascade issues.
    users_cols = {row[1] for row in c.execute("PRAGMA table_info(users)").fetchall()}
    if 'password_hash' not in users_cols:
        log.warning("Detected legacy users table — requires full DB rebuild.")
        log.warning(
            "Please delete data/milllists.db and restart the server. "
            "Your uploaded files in uploads/ are preserved."
        )
        # Raise a clear error so the operator knows what to do
        conn.close()
        raise RuntimeError(
            "Database schema is outdated (missing auth columns). "
            "Delete data/milllists.db and restart to rebuild the schema. "
            "Uploaded files in uploads/ are not affected."
        )

    conn.close()


def dict_from_row(row):
    """Convert sqlite3.Row to dict."""
    if row is None:
        return None
    return dict(row)


def dicts_from_rows(rows):
    """Convert list of sqlite3.Row to list of dicts."""
    return [dict(r) for r in rows]
