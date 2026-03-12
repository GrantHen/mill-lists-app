"""
Document-level text cleaning — Layer A of the parsing pipeline.

Strips headers, footers, contact blocks, branding, URLs, emails,
phone numbers, addresses, disclaimers, and other non-inventory noise
BEFORE any row-level parsing begins.
"""
import re

# ─── Compiled patterns (built once, used many times) ──────────────────────────

# Contact / address / branding patterns that are NEVER inventory
_JUNK_LINE_PATTERNS = [
    # Email
    re.compile(r'[\w.+-]+@[\w-]+\.[\w.-]+'),
    # URL / website
    re.compile(r'(?:https?://|www\.)\S+', re.I),
    re.compile(r'\w+\.(com|net|org|io|biz)\b', re.I),
    # Phone: (555) 123-4567, 555-123-4567, 555.123.4567
    re.compile(r'(?:(?:ph|phone|fax|cell|office|tel)[:\s]*)?'
               r'\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}', re.I),
    # Street address: "123 Main St", "P.O. Box 12"
    re.compile(r'^\d{1,5}\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*'
               r'\s+(?:St|Street|Rd|Road|Ave|Avenue|Blvd|Dr|Drive|Hwy|Highway|Lane|Ln|Way|Ct|Court)\b', re.I),
    re.compile(r'^P\.?\s*O\.?\s*Box\s+\d', re.I),
    # City, State ZIP
    re.compile(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,?\s+[A-Z]{2}\s+\d{5}'),
    # Disclaimers & legal
    re.compile(r'all\s+(?:quantities|lumber|inventory)\s+(?:subject|are\s+subject)', re.I),
    re.compile(r'prices?\s+(?:subject|are\s+subject|may\s+change)', re.I),
    re.compile(r'(?:fsc|pefc|sfi)[\s®]*(?:certified|cert|chain|coc|available)', re.I),
    re.compile(r'certified\s+(?:chain|forest|wood)', re.I),
    # Branding / contact-us
    re.compile(r'(?:visit\s+us|check\s+(?:us|out)|follow\s+us)', re.I),
    re.compile(r'(?:contact\s+us|reach\s+out|ask\s+about|please\s+(?:call|reach|contact))', re.I),
    re.compile(r'(?:instagram|linkedin|facebook|twitter|youtube)[\s:@]', re.I),
    # Email-signature style
    re.compile(r'^(?:from|to|sent|cc|subject|date)\s*:', re.I),
    # Pure date lines
    re.compile(r'^\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}$'),
    # Page numbers
    re.compile(r'^page\s+\d+\s*(of\s+\d+)?$', re.I),
    # Empty-ish: only punctuation/whitespace
    re.compile(r'^[\s\-_=*#.,:;|]+$'),
    # "Services offered", "We are interested", etc.
    re.compile(r'^(?:services?\s+offered|we\s+are\s+interested|to\s+purchase)', re.I),
    # Header labels that aren't inventory
    re.compile(r'^(?:stock\s+list|inventory\s+list|kiln\s+dried\s+(?:stock|inventory)|'
               r'net\s+tally|kd\s+stock\s*(?:list)?|hardwood\s+(?:stock|inventory))\s*$', re.I),
]

# Lines that are ALWAYS headers/footers when they appear at doc top/bottom
_HEADER_FOOTER_PATTERNS = [
    re.compile(r'^\*+\s*specials?\s*\*+', re.I),
    re.compile(r'^(?:county|state)\s+(?:highway|road|route)\s+', re.I),
]


def clean_text(raw_text: str) -> tuple:
    """
    Clean extracted document text, returning (cleaned_lines, metadata).

    Returns:
        cleaned_lines: list of str — only lines that might be inventory
        metadata: dict — extracted metadata (mill_name candidates, date, etc.)
    """
    if not raw_text:
        return [], {}

    lines = raw_text.split('\n')
    cleaned = []
    metadata = {
        'mill_name_candidates': [],
        'stock_date': None,
        'emails': [],
        'phones': [],
        'locations': [],
    }

    for i, raw_line in enumerate(lines):
        line = raw_line.strip()
        if not line:
            continue

        # ── Extract metadata from early lines ──
        if i < 25:
            _extract_metadata(line, i, metadata)

        # ── Test against junk patterns ──
        if _is_junk_line(line):
            continue

        # ── Additional heuristic checks ──
        # Very short lines with no lumber signals
        if len(line) < 4:
            continue

        # Lines that are ALL CAPS with no digits and no lumber content
        # (likely branding/title lines after the first few lines)
        if (i > 3 and line == line.upper() and not re.search(r'\d', line)
                and len(line) > 30 and not _has_species_word(line)):
            continue

        cleaned.append(line)

    return cleaned, metadata


def _is_junk_line(line: str) -> bool:
    """Test a single line against all junk patterns."""
    for pat in _JUNK_LINE_PATTERNS:
        if pat.search(line):
            return True
    for pat in _HEADER_FOOTER_PATTERNS:
        if pat.search(line):
            return True
    return False


def is_noise_text(text: str) -> bool:
    """
    Quick check if a text fragment is noise (for use by row parsers).
    More targeted than full-line junk detection.
    """
    if not text:
        return True
    t = text.strip()
    if len(t) < 2:
        return True
    # Email
    if re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', t):
        return True
    # URL
    if re.search(r'(?:https?://|www\.)\S+', t, re.I):
        return True
    if re.match(r'\w+\.(com|net|org)\b', t, re.I):
        return True
    # Phone
    if re.search(r'\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}', t):
        return True
    # Address
    if re.match(r'^\d{1,5}\s+[A-Z][a-z]+.*(?:St|Rd|Ave|Blvd|Dr|Hwy|Lane|Way)\b', t, re.I):
        return True
    # City/State/ZIP
    if re.match(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,?\s+[A-Z]{2}\s+\d{5}', t):
        return True
    return False


# ─── Metadata extraction ──────────────────────────────────────────────────────

def _extract_metadata(line: str, line_idx: int, metadata: dict):
    """Extract mill name, date, contact info from header-area lines."""
    # Date
    if not metadata['stock_date']:
        m = re.search(r'(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}|\w+\s+\d{1,2},?\s*\d{4})', line)
        if m:
            metadata['stock_date'] = m.group(1)

    # Email
    m = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', line)
    if m:
        metadata['emails'].append(m.group())

    # Phone
    m = re.search(r'\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}', line)
    if m:
        metadata['phones'].append(m.group())

    # Location (City, ST ZIP)
    m = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,?\s+[A-Z]{2}\s+\d{5})', line)
    if m:
        metadata['locations'].append(m.group(1))

    # Mill name candidates — prioritize lines with industry keywords
    _MILL_KEYWORDS = re.compile(
        r'(?:lumber|sawmill|hardwood|wood\s*products|forest\s*products|timber|'
        r'co\.|c0\.|company|inc\.|llc|corp|hardwoods)',  # c0. = OCR for co.
        re.I
    )
    if _MILL_KEYWORDS.search(line) and not re.search(r'[\w.+-]+@', line):
        # Strip URLs and phone numbers from mill name
        name = re.sub(r'(?:https?://|www\.)\S+', '', line).strip()
        name = re.sub(r'\w+\.(com|net|org)\b\S*', '', name, flags=re.I).strip()
        name = re.sub(r'\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}', '', name).strip()
        name = re.sub(r'[\s,]+$', '', name).strip()
        if name and len(name) > 3:
            metadata['mill_name_candidates'].append((line_idx, name))

    # Also try to extract a company name from a URL domain
    url_m = re.search(r'(?:https?://|www\.)([\w-]+)\.(?:com|net|org)', line, re.I)
    if url_m:
        domain = url_m.group(1)
        # Convert camelCase or run-together to spaced
        domain_spaced = re.sub(r'([a-z])([A-Z])', r'\1 \2', domain)
        domain_spaced = domain_spaced.replace('-', ' ').strip()
        if len(domain_spaced) > 3 and domain_spaced.lower() not in ('google', 'facebook', 'instagram'):
            metadata['mill_name_candidates'].append((line_idx + 100, domain_spaced.title()))


def detect_mill_name(metadata: dict, filename: str = "") -> str:
    """
    Determine the best mill name from metadata candidates + filename.

    Priority:
      1. Lines with lumber industry keywords (sorted by position, earlier = better)
      2. Filename-based detection (strip date/extension patterns)
      3. First non-junk short line from the document header
    """
    candidates = metadata.get('mill_name_candidates', [])

    # Filter out bad candidates
    good = []
    for idx, name in candidates:
        # Skip if it's just a species list
        if _is_all_species(name):
            continue
        # Skip very long lines (probably not a company name)
        if len(name) > 80:
            continue
        # Skip if it starts with a digit (address)
        if re.match(r'^\d', name):
            continue
        good.append((idx, name))

    if good:
        # Return the earliest (topmost) good candidate
        good.sort(key=lambda x: x[0])
        return good[0][1]

    # Filename fallback
    if filename:
        name = _mill_name_from_filename(filename)
        if name:
            return name

    return None


def _mill_name_from_filename(filename: str) -> str:
    """Try to extract a mill name from the original filename."""
    # Strip extension
    name = re.sub(r'\.[a-z]+$', '', filename, flags=re.I)
    # Strip common suffixes: dates, "stock list", "inventory", etc.
    name = re.sub(r'[\s_-]*(?:stock\s*(?:list|sheet)|send\s*out\s*inventory|'
                  r'inventory|kd|send\s*out|cam|stocklist)\b.*$', '', name, flags=re.I)
    # Strip trailing dates in various formats
    name = re.sub(r'[\s_-]*\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\s*$', '', name)
    name = re.sub(r'[\s_-]*(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s*\d{2,4}\s*$',
                  '', name, flags=re.I)
    name = re.sub(r'[\s_-]*\d{6,8}\s*$', '', name)
    # Split CamelCase into separate words
    name = re.sub(r'([a-z])([A-Z])', r'\1 \2', name)
    # Replace underscores/dashes with spaces
    name = re.sub(r'[_-]+', ' ', name).strip()
    # Remove common non-name suffixes
    name = re.sub(r'\b(Send Out|SendOut|Inventory|Stock)\b', '', name, flags=re.I).strip()
    name = re.sub(r'\s{2,}', ' ', name).strip()
    # Skip if it's a UUID-like hex string
    if re.match(r'^[0-9a-f]{20,}$', name.replace(' ', '')):
        return None
    if name and len(name) > 3:
        return name.strip()
    return None


# ─── Helpers ──────────────────────────────────────────────────────────────────

_SPECIES_WORDS = frozenset([
    'ash', 'aspen', 'basswood', 'beech', 'birch', 'cedar', 'cherry',
    'coffeenut', 'cottonwood', 'cypress', 'elm', 'hackberry', 'hemlock',
    'hickory', 'locust', 'maple', 'oak', 'pine', 'poplar',
    'sassafras', 'sycamore', 'walnut', 'alder', 'gum',
])


def _has_species_word(text: str) -> bool:
    """Check if text contains any lumber species keyword."""
    lower = text.lower()
    return any(sw in lower for sw in _SPECIES_WORDS)


def _is_all_species(text: str) -> bool:
    """Check if a line is ONLY species names (multi-species header)."""
    words = re.split(r'\s{2,}', text.strip())
    if not words:
        return False
    species_count = sum(1 for w in words if _has_species_word(w))
    return species_count >= len(words) * 0.8
