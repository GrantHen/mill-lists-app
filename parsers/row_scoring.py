"""
Row-level scoring and validation — Layers B + D of the parsing pipeline.

Layer B: Determines if a candidate text fragment looks like an inventory row.
Layer D: Validates a fully-parsed ParsedRow before database insertion.

Each row gets a numeric score 0.0–1.0:
  >= 0.70  → auto-accept
  0.40–0.69 → accept but flag for review
  < 0.40  → reject (do not insert)
"""
import re
from .base import ParsedRow

# ─── Thresholds ───────────────────────────────────────────────────────────────

ACCEPT_THRESHOLD = 0.70    # auto-accept, mark as reviewed
REVIEW_THRESHOLD = 0.40    # insert but flag for manual review
# Below REVIEW_THRESHOLD → reject entirely

# ─── Positive signal patterns ─────────────────────────────────────────────────

_THICKNESS_RE = re.compile(r'\b(\d{1,2})/(\d)\b')
_GRADE_RE = re.compile(
    r'\b(?:FAS|F1F|FAS/1F|FAS/SEL|S[&/]B|SEL|S&BTR|SELECT|'
    r'#?[1-3][A-C]?\s*(?:COM|COMMON)?|1C|2C|2A|3A|'
    r'PRIME|PREMIUM|RUSTIC|FAB|D\s*&\s*BTR|'
    r'SEL\s*&\s*BTR|1\s*COM|2\s*COM)\b',
    re.I
)
_QTY_BF_RE = re.compile(r'(?:[\d,]+)\s*[\'"]?\s*(?:BF|BD\s*FT|BOARD\s*F)|\b[\d,]{3,}[\'"]', re.I)
_QTY_PATTERN = re.compile(r'\b[\d,]+\s*(?:BF|BD\s*FT)\b|\b[\d,]{3,}[\'"]', re.I)
_TL_RE = re.compile(r'\bT/?L\b', re.I)
_LENGTH_RE = re.compile(r"\b\d{1,2}['\-–]\d{0,2}['\"]?", re.I)
_PRICE_RE = re.compile(r'\$\s*[\d,.]+|\b\d+\.\d{2}\b')
_DIMENSION_RE = re.compile(r'\b\d+["\']?\s*(?:&\s*W|[xX×]\s*\d)', re.I)

# Species keywords (lowercase)
_SPECIES_WORDS = frozenset([
    'ash', 'aspen', 'basswood', 'beech', 'birch', 'cedar', 'cherry',
    'coffeenut', 'cottonwood', 'cypress', 'elm', 'hackberry', 'hemlock',
    'hickory', 'locust', 'maple', 'oak', 'pine', 'poplar',
    'sassafras', 'sycamore', 'walnut', 'alder', 'gum',
])

# ─── Negative signal patterns ─────────────────────────────────────────────────

_EMAIL_RE = re.compile(r'[\w.+-]+@[\w-]+\.[\w.-]+')
_URL_RE = re.compile(r'(?:https?://|www\.)\S+|\w+\.(?:com|net|org|io)\b', re.I)
_PHONE_RE = re.compile(r'\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}')
_ADDRESS_RE = re.compile(
    r'^\d{1,5}\s+[A-Za-z]+.*(?:St|Street|Rd|Road|Ave|Avenue|Blvd|Dr|Drive|Hwy|Lane|Way)\b', re.I
)
_CITYSTATEZIP_RE = re.compile(r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,?\s+[A-Z]{2}\s+\d{5}')
_CONTACT_RE = re.compile(r'\b(?:contact|email|phone|fax|cell|office|visit|call\s+us)\b', re.I)
_DISCLAIMER_RE = re.compile(r'\b(?:subject\s+to|without\s+notice|fsc|pefc|certified)\b', re.I)
_BRANDING_RE = re.compile(r'\b(?:instagram|linkedin|facebook|twitter|youtube)\b', re.I)


# ─── Layer B: Raw text candidacy scoring ──────────────────────────────────────

def score_candidate_text(text: str) -> float:
    """
    Score raw text to decide if it's worth trying to parse as an inventory row.
    Returns 0.0–1.0.  Used BEFORE field extraction.
    """
    if not text or len(text.strip()) < 3:
        return 0.0

    score = 0.0
    t = text.strip()

    # ── Positive signals ──
    if _THICKNESS_RE.search(t):
        score += 0.30
    if _GRADE_RE.search(t):
        score += 0.25
    if _QTY_BF_RE.search(t) or _TL_RE.search(t):
        score += 0.20
    if _has_species_word(t):
        score += 0.15
    if _LENGTH_RE.search(t):
        score += 0.05
    if _PRICE_RE.search(t):
        score += 0.10
    if _DIMENSION_RE.search(t):
        score += 0.05

    # ── Negative signals ──
    if _EMAIL_RE.search(t):
        score -= 0.60
    if _URL_RE.search(t):
        score -= 0.50
    if _PHONE_RE.search(t):
        score -= 0.50
    if _ADDRESS_RE.search(t):
        score -= 0.50
    if _CITYSTATEZIP_RE.search(t):
        score -= 0.50
    if _CONTACT_RE.search(t):
        score -= 0.30
    if _DISCLAIMER_RE.search(t):
        score -= 0.30
    if _BRANDING_RE.search(t):
        score -= 0.40

    # Short text with no positive signals
    if len(t) < 8 and score <= 0:
        score -= 0.20

    return max(0.0, min(1.0, score))


def is_candidate_row(text: str, min_score: float = 0.20) -> bool:
    """Quick gate: should we even attempt to parse this text as inventory?"""
    return score_candidate_text(text) >= min_score


# ─── Layer D: Parsed row validation & final scoring ──────────────────────────

def score_parsed_row(row: ParsedRow) -> float:
    """
    Score a fully-parsed row.  This is the FINAL quality score that determines
    whether the row is accepted, flagged for review, or rejected.

    Returns 0.0–1.0, also sets row.confidence.
    """
    score = 0.0

    # ── Positive signals from parsed fields ──
    if row.species:
        score += 0.25
    if row.thickness:
        # Valid quarter-format thickness is a very strong signal
        if re.match(r'^\d{1,2}/\d$', row.thickness):
            score += 0.30
        else:
            score += 0.15
    if row.grade:
        score += 0.20
    if row.quantity_numeric and row.quantity_numeric > 0:
        score += 0.15
    elif row.quantity and row.quantity.strip():
        q = row.quantity.strip().upper()
        if q in ('T/L', 'TL', 'CALL'):
            score += 0.10
        elif re.search(r'\d', q):
            score += 0.10
    if row.price or row.price_numeric:
        score += 0.05
    if row.length:
        score += 0.05
    if row.color:
        score += 0.03
    if row.surface:
        score += 0.03
    if row.cut_type:
        score += 0.03

    # ── Negative signals from raw text ──
    raw = row.raw_text or ''
    if _EMAIL_RE.search(raw):
        score -= 0.50
    if _URL_RE.search(raw):
        score -= 0.40
    if _PHONE_RE.search(raw):
        score -= 0.40
    if _ADDRESS_RE.search(raw):
        score -= 0.40
    if _CITYSTATEZIP_RE.search(raw):
        score -= 0.40

    # ── Structural penalties ──
    # Row with no species AND no thickness AND no grade → very suspicious
    if not row.species and not row.thickness and not row.grade:
        score -= 0.30

    # Row with no thickness AND no grade → weak
    if not row.thickness and not row.grade:
        score -= 0.15

    # Row that is only a quantity + color/modifier with no product info
    # e.g. "White 6,000'" or "Unselected 2,450'"
    if not row.thickness and not row.grade and row.raw_text:
        raw_stripped = re.sub(r'[\d,]+[\'"]?\s*', '', row.raw_text).strip()
        if len(raw_stripped) < 15 and not _THICKNESS_RE.search(row.raw_text):
            score -= 0.20

    score = max(0.0, min(1.0, score))
    row.confidence = round(score, 3)
    return score


def classify_row(row: ParsedRow) -> str:
    """
    Classify a scored row as 'accept', 'review', or 'reject'.
    Call score_parsed_row() first to set row.confidence.
    """
    if row.confidence >= ACCEPT_THRESHOLD:
        return 'accept'
    elif row.confidence >= REVIEW_THRESHOLD:
        return 'review'
    else:
        return 'reject'


def validate_row(row: ParsedRow) -> list:
    """
    Run structural validation checks. Returns list of warning strings.
    These don't change the score but provide diagnostic info.
    """
    warnings = []

    if not row.species:
        warnings.append("Missing species")
    if not row.thickness:
        warnings.append("Missing thickness")
    if not row.grade:
        warnings.append("Missing grade")
    if not row.quantity and not row.quantity_numeric:
        warnings.append("Missing quantity")

    # Check for obvious data pollution
    if row.grade and len(row.grade) > 40:
        warnings.append("Grade field suspiciously long")
    if row.species and re.search(r'\d', row.species):
        warnings.append("Species contains digits")
    if row.thickness and not re.match(r'^\d{1,2}/\d{1,2}$', row.thickness):
        warnings.append(f"Non-standard thickness format: {row.thickness}")

    return warnings


# ─── Batch filtering ──────────────────────────────────────────────────────────

def filter_rows(rows: list, include_review: bool = True) -> tuple:
    """
    Score and filter a list of ParsedRows.

    Returns:
        (accepted, review, rejected) — three lists of ParsedRow
    """
    accepted = []
    review = []
    rejected = []

    for row in rows:
        score_parsed_row(row)
        classification = classify_row(row)

        if classification == 'accept':
            accepted.append(row)
        elif classification == 'review':
            if include_review:
                row.notes = (row.notes or '') + ' [needs review]'
                review.append(row)
            else:
                rejected.append(row)
        else:
            rejected.append(row)

    return accepted, review, rejected


# ─── Deduplication ────────────────────────────────────────────────────────────

def deduplicate_rows(rows: list) -> list:
    """
    Remove exact-duplicate rows based on key fields.
    Keeps the higher-confidence version.
    """
    seen = {}
    for row in rows:
        key = (
            (row.species or '').lower(),
            (row.thickness or '').lower(),
            (row.grade or '').lower(),
            row.quantity_numeric,
            (row.color or '').lower(),
        )
        if key in seen:
            if row.confidence > seen[key].confidence:
                seen[key] = row
        else:
            seen[key] = row

    return list(seen.values())


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _has_species_word(text: str) -> bool:
    lower = text.lower()
    return any(sw in lower for sw in _SPECIES_WORDS)
