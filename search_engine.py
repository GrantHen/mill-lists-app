"""
Search engine for mill stock list products.
Combines SQLite FTS5, fuzzy matching, and lumber-specific synonym expansion.
"""
import re
import sqlite3
from difflib import SequenceMatcher
from database import get_db, dicts_from_rows

# Lumber search synonyms - maps query terms to expanded search terms
SEARCH_SYNONYMS = {
    # Dimensions (nominal to standard)
    "2x4": ["2x4", "2 x 4", "8/4"],
    "2x6": ["2x6", "2 x 6"],
    "2x8": ["2x8", "2 x 8"],
    "1x4": ["1x4", "1 x 4", "4/4"],
    "1x6": ["1x6", "1 x 6"],

    # Thickness aliases
    "1 inch": ["4/4", "1\"", "1 inch"],
    "2 inch": ["8/4", "2\"", "2 inch"],
    "inch and a quarter": ["5/4"],
    "inch and a half": ["6/4"],

    # Grade aliases
    "select": ["select", "s&b", "sel & btr", "sel&btr", "select & better"],
    "common": ["common", "1c", "#1c", "1 com", "#1 common"],
    "#1": ["#1", "1c", "#1c", "#1 common", "1 common"],
    "#2": ["#2", "2c", "#2c", "#2 common", "2 common", "2a"],
    "fas": ["fas", "fas/sel"],
    "rustic": ["rustic", "wormy", "ambrosia", "character"],
    "prime": ["prime", "superprime", "premium"],

    # Species aliases
    "pine": ["pine", "syp", "southern yellow pine", "white pine"],
    "oak": ["oak", "red oak", "white oak"],
    "maple": ["maple", "hard maple", "soft maple"],
    "birch": ["birch", "white birch", "yellow birch"],
    "elm": ["elm", "red elm", "grey elm"],
    "cedar": ["cedar", "aromatic cedar", "aromatic red cedar"],
    "treated": ["treated", "pressure treated", "pt"],

    # Cut types
    "rift": ["rift", "rift & quartered", "rift & quarter", "r&q", "quartered"],
    "quartersawn": ["quartered", "quarter sawn", "quarter", "rift & quartered"],
    "plain": ["plain sawn", "plain", "flat sawn"],

    # Conditions
    "kiln dried": ["kiln dried", "kd", "kiln"],
    "air dried": ["air dried", "ad"],
    "steamed": ["steamed"],
    "wormy": ["wormy", "ambrosia", "whnd"],
}


def search_products(query: str, limit: int = 100, offset: int = 0) -> dict:
    """
    Search products using a multi-strategy approach:
    1. FTS5 full-text search with synonym expansion
    2. Fuzzy matching on remaining results
    3. Rank by relevance

    Returns dict with results, total count, and search metadata.
    """
    conn = get_db()

    # Expand the query with synonyms
    expanded_terms = _expand_query(query)

    results = []
    seen_ids = set()

    # Strategy 1: FTS5 search
    fts_results = _fts_search(conn, expanded_terms, limit * 2)
    for row in fts_results:
        row_dict = dict(row)
        rid = row_dict['id']
        if rid not in seen_ids:
            row_dict['match_type'] = 'exact'
            row_dict['relevance'] = row_dict.get('rank', 0)
            results.append(row_dict)
            seen_ids.add(rid)

    # Strategy 2: LIKE-based search for partial matches
    like_results = _like_search(conn, query, expanded_terms, limit * 2)
    for row in like_results:
        row_dict = dict(row)
        rid = row_dict['id']
        if rid not in seen_ids:
            row_dict['match_type'] = 'partial'
            row_dict['relevance'] = 0.5
            results.append(row_dict)
            seen_ids.add(rid)

    # Strategy 3: Fuzzy matching on product descriptions
    if len(results) < limit:
        fuzzy_results = _fuzzy_search(conn, query, limit * 2, seen_ids)
        for row in fuzzy_results:
            if row['id'] not in seen_ids:
                row['match_type'] = 'fuzzy'
                results.append(row)
                seen_ids.add(row['id'])

    # Sort by relevance
    results.sort(key=lambda r: r.get('relevance', 0), reverse=True)

    # Log search
    try:
        conn.execute(
            "INSERT INTO search_history (query, result_count) VALUES (?, ?)",
            (query, len(results))
        )
        conn.commit()
    except Exception:
        pass

    total = len(results)
    paged = results[offset:offset + limit]

    conn.close()

    return {
        "query": query,
        "expanded_terms": expanded_terms,
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": paged,
    }


def _expand_query(query: str) -> list:
    """Expand a search query with lumber industry synonyms."""
    terms = [query.lower().strip()]
    query_lower = query.lower().strip()

    # Check each synonym group
    for key, synonyms in SEARCH_SYNONYMS.items():
        if key in query_lower or any(s in query_lower for s in synonyms):
            terms.extend(synonyms)

    # Also split query into words and check each
    words = query_lower.split()
    for word in words:
        word = word.strip('#')
        if word in SEARCH_SYNONYMS:
            terms.extend(SEARCH_SYNONYMS[word])

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for t in terms:
        if t not in seen:
            seen.add(t)
            unique.append(t)

    return unique


def _fts_search(conn, terms: list, limit: int) -> list:
    """Full-text search using SQLite FTS5."""
    results = []

    for term in terms[:10]:  # Limit to prevent too many queries
        # Clean the term for FTS5
        fts_term = _clean_fts_query(term)
        if not fts_term:
            continue

        try:
            rows = conn.execute("""
                SELECT p.*, pf.rank
                FROM products_fts pf
                JOIN products p ON p.id = pf.rowid
                WHERE products_fts MATCH ?
                ORDER BY pf.rank
                LIMIT ?
            """, (fts_term, limit)).fetchall()

            results.extend(rows)
        except sqlite3.OperationalError:
            # FTS query syntax error, try simpler approach
            try:
                simple_term = re.sub(r'[^\w\s]', '', term)
                if simple_term.strip():
                    rows = conn.execute("""
                        SELECT p.*, pf.rank
                        FROM products_fts pf
                        JOIN products p ON p.id = pf.rowid
                        WHERE products_fts MATCH ?
                        ORDER BY pf.rank
                        LIMIT ?
                    """, (f'"{simple_term}"', limit)).fetchall()
                    results.extend(rows)
            except sqlite3.OperationalError:
                pass

    return results


def _clean_fts_query(term: str) -> str:
    """Clean a search term for FTS5 query syntax."""
    # Remove special characters that break FTS5
    cleaned = re.sub(r'[^\w\s/&#+]', ' ', term)
    cleaned = cleaned.strip()
    if not cleaned:
        return None

    # If term has spaces, wrap in quotes for phrase search
    if ' ' in cleaned:
        return f'"{cleaned}"'

    return cleaned


def _like_search(conn, original_query: str, terms: list, limit: int) -> list:
    """LIKE-based search for partial matches."""
    results = []

    # Build search patterns
    patterns = set()
    patterns.add(f'%{original_query}%')
    for term in terms[:5]:
        patterns.add(f'%{term}%')

    # Also search individual important words
    words = original_query.lower().split()
    important_words = [w for w in words if len(w) > 2 and w not in ('the', 'and', 'for', 'all', 'with', 'any')]

    for pattern in patterns:
        try:
            rows = conn.execute("""
                SELECT * FROM products
                WHERE product LIKE ? OR product_normalized LIKE ?
                OR species LIKE ? OR grade LIKE ? OR description LIKE ?
                LIMIT ?
            """, (pattern, pattern, pattern, pattern, pattern, limit)).fetchall()
            results.extend(rows)
        except Exception:
            pass

    # Multi-word search: all important words must match somewhere
    if len(important_words) >= 2:
        conditions = []
        params = []
        for word in important_words:
            conditions.append("""(
                product LIKE ? OR product_normalized LIKE ?
                OR species LIKE ? OR grade LIKE ? OR description LIKE ?
                OR thickness LIKE ? OR mill_name LIKE ?
            )""")
            pat = f'%{word}%'
            params.extend([pat] * 7)

        if conditions:
            sql = f"SELECT * FROM products WHERE {' AND '.join(conditions)} LIMIT ?"
            params.append(limit)
            try:
                rows = conn.execute(sql, params).fetchall()
                results.extend(rows)
            except Exception:
                pass

    return results


def _fuzzy_search(conn, query: str, limit: int, exclude_ids: set) -> list:
    """Fuzzy search using Python's difflib for similarity matching."""
    results = []

    # Get a sample of products to compare against
    try:
        all_products = conn.execute("""
            SELECT * FROM products
            WHERE id NOT IN ({})
            LIMIT 5000
        """.format(','.join(str(i) for i in exclude_ids) if exclude_ids else '0')).fetchall()
    except Exception:
        return results

    query_lower = query.lower()

    for row in all_products:
        row_dict = dict(row)
        # Calculate similarity against multiple fields
        best_score = 0

        for field in ['product', 'product_normalized', 'species', 'grade', 'description']:
            val = row_dict.get(field)
            if val:
                score = SequenceMatcher(None, query_lower, val.lower()).ratio()
                best_score = max(best_score, score)

                # Also check if any query word appears
                for word in query_lower.split():
                    if len(word) > 2 and word in val.lower():
                        best_score = max(best_score, 0.6)

        if best_score >= 0.4:
            row_dict['relevance'] = best_score
            results.append(row_dict)

    # Sort by score and take top results
    results.sort(key=lambda r: r['relevance'], reverse=True)
    return results[:limit]


def get_search_suggestions(query: str, limit: int = 10) -> list:
    """Get search suggestions based on existing data."""
    conn = get_db()
    suggestions = set()

    # Get matching species
    rows = conn.execute(
        "SELECT DISTINCT species FROM products WHERE species LIKE ? LIMIT ?",
        (f'%{query}%', limit)
    ).fetchall()
    for r in rows:
        if r['species']:
            suggestions.add(r['species'])

    # Get matching grades
    rows = conn.execute(
        "SELECT DISTINCT grade FROM products WHERE grade LIKE ? LIMIT ?",
        (f'%{query}%', limit)
    ).fetchall()
    for r in rows:
        if r['grade']:
            suggestions.add(r['grade'])

    # Get matching thicknesses
    rows = conn.execute(
        "SELECT DISTINCT thickness FROM products WHERE thickness LIKE ? LIMIT ?",
        (f'%{query}%', limit)
    ).fetchall()
    for r in rows:
        if r['thickness']:
            suggestions.add(r['thickness'])

    conn.close()
    return sorted(list(suggestions))[:limit]


def get_stats() -> dict:
    """Get database statistics for the dashboard."""
    conn = get_db()
    stats = {}

    stats['total_products'] = conn.execute("SELECT COUNT(*) as c FROM products").fetchone()['c']
    stats['total_uploads'] = conn.execute("SELECT COUNT(*) as c FROM uploads").fetchone()['c']
    stats['total_mills'] = conn.execute("SELECT COUNT(DISTINCT mill_name) as c FROM products WHERE mill_name IS NOT NULL").fetchone()['c']
    stats['total_species'] = conn.execute("SELECT COUNT(DISTINCT species) as c FROM products WHERE species IS NOT NULL").fetchone()['c']

    # Recent uploads
    stats['recent_uploads'] = dicts_from_rows(conn.execute("""
        SELECT * FROM uploads ORDER BY uploaded_at DESC LIMIT 5
    """).fetchall())

    # Top species
    stats['top_species'] = dicts_from_rows(conn.execute("""
        SELECT species, COUNT(*) as count FROM products
        WHERE species IS NOT NULL
        GROUP BY species ORDER BY count DESC LIMIT 10
    """).fetchall())

    # Top mills
    stats['top_mills'] = dicts_from_rows(conn.execute("""
        SELECT mill_name, COUNT(*) as count FROM products
        WHERE mill_name IS NOT NULL
        GROUP BY mill_name ORDER BY count DESC LIMIT 10
    """).fetchall())

    # Low confidence rows
    stats['low_confidence_count'] = conn.execute(
        "SELECT COUNT(*) as c FROM products WHERE confidence < 0.5"
    ).fetchone()['c']

    # Unreviewed count
    stats['unreviewed_count'] = conn.execute(
        "SELECT COUNT(*) as c FROM products WHERE is_reviewed = 0"
    ).fetchone()['c']

    conn.close()
    return stats
