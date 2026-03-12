"""
PDF parser for mill stock lists.

Strategies (tried in order, best result wins):
  1. Table extraction with species-header detection (Tigerton)
  2. Column-aware word extraction (Devereaux, Granite Valley)
  3. Line-by-line text parsing (Brenneman, CC Cook OCR)
  4. OCR fallback for scanned/image PDFs

All strategies feed through the row-scoring pipeline — junk rows
are filtered AFTER extraction, not during.
"""
import re
import os
import subprocess
import tempfile
import logging
import pdfplumber
from .base import ParseResult, ParsedRow
from .cleaning import clean_text, detect_mill_name, is_noise_text
from .row_scoring import score_candidate_text, is_candidate_row
from lumber_normalizer import (
    normalize_thickness, normalize_grade, normalize_species,
    build_product_string, SPECIES_MAP
)

log = logging.getLogger("pdf_parser")


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def parse_pdf(file_path: str) -> ParseResult:
    """
    Parse a PDF mill stock list.  Tries multiple strategies and returns
    the one with the most high-quality rows.
    """
    result = ParseResult(parsing_method="pdf_text")

    try:
        # ── Raw extraction ────────────────────────────────────────────────
        all_text = ""
        all_words = []
        all_tables = []
        is_scanned = False

        with pdfplumber.open(file_path) as pdf:
            if len(pdf.pages) == 0:
                is_scanned = True
            else:
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    all_text += text + "\n"

                    words = page.extract_words()
                    for w in words:
                        all_words.append({
                            'x0': w['x0'],
                            'top': w['top'] + (page.page_number - 1) * 10000,
                            'text': w['text'],
                            'page': page.page_number,
                        })

                    tables = page.extract_tables()
                    if tables:
                        all_tables.extend(tables)

                if len(all_text.strip()) < 50:
                    is_scanned = True

        # ── OCR fallback ──────────────────────────────────────────────────
        if is_scanned:
            all_text = _ocr_pdf(file_path)
            if all_text.strip():
                result.parsing_method = "pdf_ocr"
                result.warnings.append("Scanned PDF — used OCR extraction.")
            else:
                result.success = False
                result.errors.append("OCR failed. Try AI parsing.")
                return result

        result.raw_text = all_text

        # ── Document-level cleanup + metadata extraction ──────────────────
        cleaned_lines, metadata = clean_text(all_text)
        result._metadata = metadata  # stash for pipeline.py mill-name detection

        # Extract metadata
        result.stock_date = metadata.get('stock_date')
        if metadata.get('emails'):
            result.mill_email = metadata['emails'][0]
        if metadata.get('phones'):
            result.mill_phone = metadata['phones'][0]
        if metadata.get('locations'):
            result.mill_location = metadata['locations'][0]

        # Mill name detection
        mill_name = detect_mill_name(metadata, os.path.basename(file_path))
        result.mill_name = mill_name

        # ── Try all strategies, pick best ─────────────────────────────────
        strategies = []

        # Strategy 1: Table parsing (best for Tigerton, structured tables)
        if all_tables and not is_scanned:
            table_rows = _parse_species_header_tables(all_tables)
            if table_rows and len(table_rows) >= 5:
                strategies.append(("pdf_table", table_rows, 0.88))

        # Strategy 2: Column-aware word extraction (Devereaux, Granite Valley)
        if all_words and not is_scanned:
            col_rows = _parse_by_word_columns(all_words)
            if col_rows and len(col_rows) >= 5:
                strategies.append(("pdf_columns", col_rows, 0.82))

        # Strategy 3: Line-by-line text parsing
        text_rows = _parse_text_lines(cleaned_lines)
        if text_rows and len(text_rows) >= 3:
            strategies.append(("pdf_text", text_rows, 0.72))

        # ── Pick best strategy by count of rows with good scores ──────────
        if not strategies:
            result.success = False
            result.errors.append("Could not auto-parse structure. Try AI parsing.")
            return result

        best_method, best_rows, base_conf = _pick_best_strategy(strategies)
        result.rows = best_rows
        result.success = True
        result.parsing_method = best_method if not is_scanned else "pdf_ocr"
        result.confidence = base_conf

    except Exception as e:
        log.error(f"PDF parsing error: {e}", exc_info=True)
        result.success = False
        result.errors.append(f"PDF error: {str(e)}")

    return result


def _pick_best_strategy(strategies: list) -> tuple:
    """
    Pick the strategy that produces the most high-quality rows.
    Each entry: (method_name, rows, base_confidence).
    """
    from .row_scoring import score_parsed_row, REVIEW_THRESHOLD

    best = None
    best_good_count = -1

    for method, rows, base_conf in strategies:
        # Score all rows
        good = 0
        for r in rows:
            score_parsed_row(r)
            if r.confidence >= REVIEW_THRESHOLD:
                good += 1

        if good > best_good_count:
            best_good_count = good
            best = (method, rows, base_conf)

    return best


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 1: Table parsing with species-in-header
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_species_header_tables(all_tables: list) -> list:
    """Parse pdfplumber tables where species appears in header rows."""
    parsed_rows = []
    row_num = 0

    for table in all_tables:
        if not table or len(table) < 2:
            continue

        header_row = table[0]
        if not header_row:
            continue

        # Multi-species side-by-side table (Tigerton 12+ cols)
        if len(header_row) >= 10:
            multi_rows = _parse_multi_species_table(table, row_num)
            parsed_rows.extend(multi_rows)
            row_num += len(multi_rows)
            continue

        # Single-species table
        species = _extract_species_from_header(header_row)
        if not species:
            for cell in header_row:
                if cell and _is_species_header(str(cell)):
                    species = normalize_species(str(cell).rstrip(':'))
                    break

        col_idx = _detect_table_columns(header_row)

        for data_row in table[1:]:
            if not data_row or all(c is None or str(c).strip() == '' for c in data_row):
                continue
            row = _parse_structured_table_row(data_row, col_idx, species, row_num)
            if row:
                parsed_rows.append(row)
                row_num += 1

    return parsed_rows


def _parse_multi_species_table(table: list, row_num_start: int) -> list:
    """Parse Tigerton's multi-species grid table."""
    parsed_rows = []
    row_num = row_num_start

    if not table:
        return []

    header = table[0]
    n_cols = len(header)

    # Find column groups — each starts with "SPECIES: Grade: ..."
    groups = []
    i = 0
    while i < n_cols:
        cell = str(header[i] or '').strip()
        if cell:
            sp_match = re.match(r'^([A-Za-z ]+?)[\s:]+(?:Grade|grade)', cell)
            if sp_match:
                species_name = normalize_species(sp_match.group(1).strip())
                col_map = {'thickness': i, 'grade': None, 'description': None,
                           'qty': None, 'length': None}
                for j in range(i + 1, min(i + 8, n_cols)):
                    hdr = str(header[j] or '').strip().lower()
                    if 'grade' in hdr:
                        col_map['grade'] = j
                    elif 'desc' in hdr:
                        col_map['description'] = j
                    elif any(k in hdr for k in ('bft', 'bf', 'qty', 'quantity')):
                        col_map['qty'] = j
                    elif 'length' in hdr or 'board' in hdr:
                        col_map['length'] = j
                groups.append((species_name, col_map))
        i += 1

    for data_row in table[1:]:
        if not data_row:
            continue
        for species, col_map in groups:
            row = _parse_structured_table_row(data_row, col_map, species, row_num)
            if row:
                parsed_rows.append(row)
                row_num += 1

    return parsed_rows


def _extract_species_from_header(header_row: list) -> str:
    for cell in header_row:
        if not cell:
            continue
        text = str(cell).strip()
        m = re.match(r'^([A-Za-z][A-Za-z\s]+?)[\s:]+(?:Grade|grade|GRADE)', text)
        if m:
            return normalize_species(m.group(1).strip())
        if _is_species_header(text):
            return normalize_species(text.rstrip(':'))
    return None


def _detect_table_columns(header_row: list) -> dict:
    col_map = {}
    for i, cell in enumerate(header_row):
        if not cell:
            continue
        h = str(cell).strip().lower()
        if any(k in h for k in ('thick', 'size')):
            col_map['thickness'] = i
        elif 'grade' in h:
            col_map['grade'] = i
        elif any(k in h for k in ('desc', 'color', 'surface', 'finish')):
            col_map['description'] = i
        elif any(k in h for k in ('bft', 'bf', 'qty', 'quantity', 'board feet')):
            col_map['qty'] = i
        elif 'length' in h or 'board' in h:
            col_map['length'] = i
        elif 'price' in h:
            col_map['price'] = i

    # Defaults
    if 'thickness' not in col_map and header_row:
        col_map['thickness'] = 0
    if 'grade' not in col_map and len(header_row) > 1:
        col_map['grade'] = 1
    if 'description' not in col_map and len(header_row) > 2:
        col_map['description'] = 2
    if 'qty' not in col_map and len(header_row) > 3:
        col_map['qty'] = 3
    if 'length' not in col_map and len(header_row) > 4:
        col_map['length'] = 4

    return col_map


def _parse_structured_table_row(data_row: list, col_map: dict,
                                 species: str, row_num: int) -> ParsedRow:
    """Parse a single structured table row."""
    def get(key, default=None):
        idx = col_map.get(key)
        if idx is not None and idx < len(data_row):
            v = data_row[idx]
            return str(v).strip() if v is not None else default
        return default

    thickness = get('thickness')
    grade = get('grade')
    description = get('description')
    qty_str = get('qty')
    length = get('length')
    price = get('price')

    if not any([thickness, grade, description, qty_str]):
        return None

    # Skip header-repeater rows
    if thickness and re.search(r'(grade|species|description|thick|qty)', thickness, re.I):
        return None

    # Noise check
    raw = ' | '.join(str(c) for c in data_row if c is not None)
    if is_noise_text(raw):
        return None

    # Parse thickness
    thickness_norm = None
    if thickness:
        m = re.match(r'^(\d+/\d+)', thickness.strip())
        if m:
            thickness_norm = normalize_thickness(m.group(1))

    # Parse quantity
    qty_numeric = None
    qty_display = qty_str or ""
    if qty_str:
        if re.match(r'^\d?T/?L$', qty_str.strip(), re.I):
            qty_display = "T/L"
        elif re.match(r'^call', qty_str.strip(), re.I):
            qty_display = "Call"
        else:
            qty_clean = re.sub(r'[^\d.]', '', qty_str)
            if qty_clean:
                try:
                    qty_numeric = float(qty_clean)
                    qty_display = f"{int(qty_numeric):,} BF"
                except ValueError:
                    pass

    grade_norm = normalize_grade(grade) if grade else None
    description_clean = description.strip() if description else None

    product_parts = [p for p in [thickness_norm, grade_norm, description_clean] if p]
    product = ' '.join(product_parts) if product_parts else ""
    if not product:
        return None

    row = ParsedRow(
        species=species,
        thickness=thickness_norm,
        grade=grade_norm,
        description=description_clean,
        quantity=qty_display if qty_display else None,
        quantity_numeric=qty_numeric,
        length=length.strip() if length and length.strip() else None,
        price=price,
        product=product,
        source_row=row_num,
        raw_text=raw,
        confidence=0.88,
    )
    row.product_normalized = build_product_string(row.to_dict())
    return row


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 2: Column-aware word extraction
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_by_word_columns(all_words: list) -> list:
    """
    Group words by y-position, detect column boundaries via x-clustering,
    then parse each column independently.
    """
    if not all_words:
        return []

    # Group words into rows by y proximity
    rows_by_y = {}
    for w in all_words:
        y_bucket = round(w['top'] / 6) * 6
        if y_bucket not in rows_by_y:
            rows_by_y[y_bucket] = []
        rows_by_y[y_bucket].append(w)

    # Detect column boundaries
    x_starts = []
    for y, words in rows_by_y.items():
        sorted_words = sorted(words, key=lambda w: w['x0'])
        if sorted_words:
            x_starts.append(sorted_words[0]['x0'])

    if not x_starts:
        return []

    col_boundaries = _cluster_x_positions(x_starts)
    if len(col_boundaries) < 2:
        return []

    # Parse rows, assigning words to columns
    parsed_rows = []
    species_by_col = {i: None for i in range(len(col_boundaries))}
    row_num = 0

    for y in sorted(rows_by_y.keys()):
        words = sorted(rows_by_y[y], key=lambda w: w['x0'])

        # Group words into columns
        col_words = {i: [] for i in range(len(col_boundaries))}
        for w in words:
            col_idx = _assign_column(w['x0'], col_boundaries)
            col_words[col_idx].append(w['text'])

        col_texts = {i: ' '.join(col_words[i]).strip() for i in range(len(col_boundaries))}

        # Check if all columns are species headers
        all_species = all(
            _is_species_header(t) or not t
            for t in col_texts.values()
        )
        if all_species and any(t and _is_species_header(t) for t in col_texts.values()):
            for i, t in col_texts.items():
                if t and _is_species_header(t):
                    species_by_col[i] = normalize_species(t.rstrip(':'))
            continue

        # Process each column
        for col_idx in range(len(col_boundaries)):
            col_text = col_texts.get(col_idx, '').strip()
            if not col_text:
                continue

            # Species header
            if _is_species_header(col_text):
                species_by_col[col_idx] = normalize_species(col_text.rstrip(':'))
                continue

            # Noise filter
            if is_noise_text(col_text):
                continue

            # Candidacy check — skip text that doesn't look like inventory
            if not is_candidate_row(col_text, min_score=0.15):
                continue

            parsed = _parse_product_line(col_text, species_by_col.get(col_idx), row_num)
            if parsed:
                parsed_rows.append(parsed)
                row_num += 1

    return parsed_rows


def _cluster_x_positions(x_vals: list, tolerance: float = 30.0) -> list:
    if not x_vals:
        return []
    sorted_x = sorted(set(round(x / tolerance) * tolerance for x in x_vals))
    clusters = []
    for x in sorted_x:
        if not clusters or x - clusters[-1] > tolerance * 1.5:
            clusters.append(x)
    return clusters


def _assign_column(x: float, boundaries: list) -> int:
    if len(boundaries) == 1:
        return 0
    for i in range(len(boundaries) - 1):
        midpoint = (boundaries[i] + boundaries[i + 1]) / 2
        if x < midpoint:
            return i
    return len(boundaries) - 1


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 3: Line-by-line text parsing
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_text_lines(lines: list) -> list:
    """
    Parse mill stock list from cleaned text lines.
    Species appear as section headers; product rows follow beneath.
    """
    rows = []
    current_species = None
    row_num = 0

    for raw_line in lines:
        raw_line = raw_line.strip()
        if not raw_line:
            continue

        # Multi-species header line
        species_parts = re.split(r'\s{3,}', raw_line)
        if all(_is_species_header(p.strip()) or not p.strip() for p in species_parts):
            found_sp = False
            for p in species_parts:
                if p.strip() and _is_species_header(p.strip()):
                    current_species = normalize_species(p.strip().rstrip(':').strip('*- '))
                    found_sp = True
                    break
            if found_sp:
                continue

        # Single species header
        if _is_species_header(raw_line):
            current_species = normalize_species(raw_line.rstrip(':').strip('*- '))
            continue

        # Try multi-column split
        segments = _split_multicolumn_line(raw_line)
        for segment in segments:
            segment = segment.strip()
            if not segment:
                continue
            if _is_species_header(segment):
                current_species = normalize_species(segment.rstrip(':').strip('*- '))
                continue

            # Candidacy check
            if not is_candidate_row(segment, min_score=0.15):
                continue

            parsed = _parse_product_line(segment, current_species, row_num)
            if parsed:
                rows.append(parsed)
                row_num += 1

    return rows


def _split_multicolumn_line(line: str) -> list:
    """Split lines that contain multiple product columns merged together."""
    thickness_matches = list(re.finditer(r'\b(\d+/\d)\b', line))
    if len(thickness_matches) <= 1:
        return [line]

    segments = re.split(r'\s{3,}', line)
    valid = [s.strip() for s in segments if s.strip() and len(s.strip()) > 3]
    if len(valid) >= 2:
        return valid

    return [line]


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLE PRODUCT LINE PARSER
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_product_line(line: str, species: str, row_num: int) -> ParsedRow:
    """
    Parse a single product line from text. Handles multiple formats:
      A: "4,300' 4/4 #1C"             qty-first
      B: "T/L 4/4 FAS/SEL"            T/L prefix
      C: "4/4 S/B White 6,000'"       thickness-first, qty at end
      D: "1 T/L 5/4 FAS/SEL"          count + T/L
    """
    if not line or len(line) < 3:
        return None

    # Quick noise rejection
    if is_noise_text(line):
        return None

    row = ParsedRow(species=species, raw_text=line, source_row=row_num)

    # ── Format A: qty' thickness grade ──
    m = re.match(r'^([\d,]+)[\'"]?\s+(\d+/\d+)\s+(.*)', line)
    if m:
        qty_str = m.group(1).replace(',', '')
        row.thickness = normalize_thickness(m.group(2))
        try:
            row.quantity_numeric = float(qty_str)
            row.quantity = f"{int(row.quantity_numeric):,} BF"
        except ValueError:
            row.quantity = qty_str
        _parse_grade_description(m.group(3).strip(), row)
        row.product = line
        row.product_normalized = build_product_string(row.to_dict())
        return row

    # ── Format D: count T/L thickness grade ──
    m = re.match(r'^(\d+)\s+T/?L\s+(\d+/\d+)\s+(.*)', line, re.I)
    if m:
        row.quantity = f"{m.group(1)} T/L"
        row.thickness = normalize_thickness(m.group(2))
        _parse_grade_description(m.group(3).strip(), row)
        row.product = line
        row.product_normalized = build_product_string(row.to_dict())
        return row

    # ── Format B: T/L thickness grade ──
    m = re.match(r'^(T/?L|Call)\s+(\d+/\d+)\s+(.*)', line, re.I)
    if m:
        row.quantity = m.group(1).upper().replace('TL', 'T/L')
        row.thickness = normalize_thickness(m.group(2))
        _parse_grade_description(m.group(3).strip(), row)
        row.product = line
        row.product_normalized = build_product_string(row.to_dict())
        return row

    # ── Format C: thickness grade [qty at end] ──
    m = re.match(r'^(\d+/\d+)\s+(.*)', line)
    if m:
        row.thickness = normalize_thickness(m.group(1))
        remainder = m.group(2).strip()

        # Qty at end
        qty_end = re.search(r'([\d,]+)[\'"]?\s*$', remainder)
        if qty_end:
            qty_str = qty_end.group(1).replace(',', '')
            try:
                v = float(qty_str)
                if v > 10:
                    row.quantity_numeric = v
                    row.quantity = f"{int(v):,} BF"
                    remainder = remainder[:qty_end.start()].strip()
            except ValueError:
                pass

        _parse_grade_description(remainder, row)
        row.product = line
        row.product_normalized = build_product_string(row.to_dict())
        return row

    # ── Fallback: has a thickness fraction somewhere ──
    m = re.search(r'\b(\d+/\d)\b', line)
    if m:
        row.thickness = normalize_thickness(m.group(1))
        row.product = line
        row.description = line
        row.product_normalized = build_product_string(row.to_dict())
        return row

    # ── No thickness found — only accept if has grade + qty-like pattern ──
    if (re.search(r'\b(?:FAS|S[&/]B|#?\d[AC]?|SEL|PRIME|RUSTIC|FAB)\b', line, re.I)
            and re.search(r'\d', line)):
        row.product = line
        row.description = line
        row.product_normalized = build_product_string(row.to_dict())
        return row

    # Reject everything else — prefer missing data over junk
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# GRADE / DESCRIPTION PARSER
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_grade_description(text: str, row: ParsedRow):
    """Parse grade, color, surface, cut type, treatment from remainder string."""
    if not text:
        return

    original = text.strip()
    row.description = original

    # Length patterns
    length_m = re.search(
        r"(\d+['\-–]\d*['\"]?(?:\s*(?:&|and|-)\s*\d+['\"]?)?(?:\s*only)?)\s*$",
        text, re.I
    )
    if length_m:
        row.length = length_m.group(1).strip()
        text = text[:length_m.start()].strip()

    # Surface
    surf_m = re.search(r'\b(RGH|Rough|S2S|S4S|15/16|13/16|1\s*3/16|1\s*5/16)\b', text, re.I)
    if surf_m:
        row.surface = surf_m.group(1).strip()

    # Color
    color_m = re.search(
        r'\b(White|Brown|Red|Natural|Sap|Heart(?:wood)?|Calico|Wheat|'
        r'Northern|Unselected|Rocky\s*Mountain)\b', text, re.I
    )
    if color_m:
        row.color = color_m.group(1).strip().title()

    # Cut type
    cut_m = re.search(
        r'\b(Rift\s*&?\s*Quarter(?:ed)?|Rift|Quarter(?:ed)?|Plain\s*Sawn|'
        r'Flitch|Live\s*Sawn|Circle[\s-]Sawn)\b', text, re.I
    )
    if cut_m:
        row.cut_type = cut_m.group(1).strip().title()

    # Treatment
    treat_m = re.search(
        r'\b(Steamed|Unsteamed|Stained|Spalted|Wormy|Ambrosia|Checked|'
        r'Striped|WHND|Stick\s*Shadow|Mineral|CND)\b', text, re.I
    )
    if treat_m:
        row.treatment = treat_m.group(1).strip()

    # Grade — strip identified components
    grade_text = text
    for val in [row.length, row.color, row.cut_type, row.treatment]:
        if val:
            grade_text = re.sub(re.escape(val), '', grade_text, flags=re.I)

    grade_text = re.sub(r'\b(RGH|Rough|S2S|S4S|15/16|13/16)\b', '', grade_text, flags=re.I)
    grade_text = re.sub(r"\d+['\-–]\s*\d*['\"]?(?:\s*only)?", '', grade_text)
    grade_text = re.sub(r'\b\d{2,6}\b', '', grade_text)
    grade_text = re.sub(r'[,"\s]+', ' ', grade_text).strip().strip('-').strip()

    if grade_text:
        row.grade = normalize_grade(grade_text)


# ═══════════════════════════════════════════════════════════════════════════════
# SPECIES DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def _is_species_header(text: str) -> bool:
    """Check if a line is a lumber species name/header."""
    text_clean = text.strip().upper().rstrip(':').strip('*- ')
    if not text_clean:
        return False

    if text_clean.lower().rstrip(':') in SPECIES_MAP:
        return True

    known = [
        'ASH', 'ASPEN', 'BASSWOOD', 'BEECH', 'BIRCH', 'CEDAR', 'CHERRY',
        'COFFEENUT', 'COTTONWOOD', 'CYPRESS', 'ELM', 'HACKBERRY', 'HEMLOCK',
        'HICKORY', 'HONEY LOCUST', 'MAPLE', 'OAK', 'PINE', 'POPLAR',
        'SASSAFRAS', 'SYCAMORE', 'WALNUT', 'ALDER', 'SAP GUM',
        'HARD MAPLE', 'SOFT MAPLE', 'RED OAK', 'WHITE OAK', 'RED ELM',
        'GREY ELM', 'WHITE BIRCH', 'YELLOW BIRCH', 'WHITE PINE', 'WHITE ASH',
        'BLACK ASH', 'SOFT MAPLE - RED LEAF', 'WALNUT - STEAMED',
        'RED OAK PLAIN SAWN', 'RED OAK RIFT & QUARTER',
        'WHITE OAK PLAIN SAWN', 'WHITE OAK RIFT & QUARTER',
        'WHITE OAK RIFTED & QUARTERED', 'WHITE OAK - QUARTER AND RIFT',
        'RQ WALNUT',
    ]
    for kw in known:
        if text_clean == kw or text_clean.startswith(kw + ' ') or text_clean.startswith(kw + ':'):
            return True

    # ALL_CAPS, no digits, <=5 words, contains species keyword
    if (text_clean == text_clean.upper()
            and len(text_clean) < 50
            and not re.search(r'\d', text_clean)
            and len(text_clean.split()) <= 5):
        species_words = {'ASH', 'ASPEN', 'BASSWOOD', 'BEECH', 'BIRCH', 'CEDAR', 'CHERRY',
                         'COFFEENUT', 'ELM', 'HACKBERRY', 'HEMLOCK', 'HICKORY', 'LOCUST',
                         'MAPLE', 'OAK', 'PINE', 'POPLAR', 'SASSAFRAS', 'SYCAMORE',
                         'WALNUT', 'ALDER', 'GUM', 'COTTONWOOD'}
        words = set(text_clean.split())
        if words & species_words:
            return True

    return False


# ═══════════════════════════════════════════════════════════════════════════════
# OCR
# ═══════════════════════════════════════════════════════════════════════════════

def _ocr_pdf(file_path: str) -> str:
    """OCR a scanned/image-based PDF using pdf2image + pytesseract."""
    try:
        from pdf2image import convert_from_path
        import pytesseract
        images = convert_from_path(file_path, dpi=300)
        all_text = ""
        for img in images:
            page_text = pytesseract.image_to_string(img, config='--psm 6')
            all_text += page_text + "\n"
        return all_text
    except ImportError:
        try:
            from pdf2image import convert_from_path
            images = convert_from_path(file_path, dpi=300)
            all_text = ""
            for img in images:
                with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
                    img.save(tmp.name, 'PNG')
                    result = subprocess.run(
                        ['tesseract', tmp.name, '-', '--psm', '6'],
                        capture_output=True, text=True, timeout=60
                    )
                    all_text += result.stdout + "\n"
                    os.unlink(tmp.name)
            return all_text
        except Exception:
            return ""
    except Exception:
        return ""
