"""
Parsing pipeline orchestrator.

This is the single entry point for all file parsing. It coordinates:

  1. File type detection
  2. Raw text/table extraction
  3. Document-level cleanup (Layer A)
  4. Strategy selection + row extraction (Layers B + C)
  5. Row scoring & validation (Layer D)
  6. Deduplication & filtering
  7. Mill name detection
  8. Final result assembly

Every row that reaches the database has been scored and validated.
"""
import os
import re
import logging
from .base import ParseResult, ParsedRow
from .cleaning import clean_text, detect_mill_name, is_noise_text
from .row_scoring import (
    score_parsed_row, classify_row, filter_rows,
    deduplicate_rows, validate_row,
    ACCEPT_THRESHOLD, REVIEW_THRESHOLD
)

log = logging.getLogger("pipeline")


def parse_file(file_path: str, original_filename: str = "",
               method: str = "auto") -> ParseResult:
    """
    Main entry point — parse any supported file.

    Args:
        file_path: absolute path to the uploaded file
        original_filename: the user's original filename (for mill-name hints)
        method: 'auto' | 'ai' | 'pdf' | 'excel'

    Returns:
        ParseResult with scored, validated, filtered rows.
    """
    result = ParseResult()
    ext = os.path.splitext(file_path)[1].lower()

    # ── Step 1: File type detection ──────────────────────────────────────────
    file_type = _detect_file_type(ext)
    log.info(f"Parsing {original_filename} (type={file_type}, method={method})")

    # ── Step 2+3+4: Extract + parse via strategy ────────────────────────────
    try:
        if method == 'ai':
            raw_result = _extract_via_ai(file_path, file_type)
        elif file_type == 'pdf':
            raw_result = _extract_pdf(file_path)
        elif file_type in ('excel', 'csv'):
            raw_result = _extract_excel(file_path)
        else:
            raw_result = _extract_pdf(file_path)
            if not raw_result.success:
                raw_result = _extract_excel(file_path)
    except Exception as e:
        log.error(f"Extraction failed: {e}", exc_info=True)
        result.success = False
        result.errors.append(f"Extraction error: {str(e)}")
        return result

    # Transfer metadata from raw extraction
    result.parsing_method = raw_result.parsing_method
    result.raw_text = raw_result.raw_text
    result.warnings = list(raw_result.warnings)
    result.errors = list(raw_result.errors)

    if not raw_result.rows:
        result.success = False
        if not result.errors:
            result.errors.append("No rows extracted. Try AI parsing or manual entry.")
        return result

    # ── Step 5: Score every row ──────────────────────────────────────────────
    for row in raw_result.rows:
        score_parsed_row(row)

    # ── Step 6: Filter — accept / review / reject ────────────────────────────
    accepted, review, rejected = filter_rows(raw_result.rows, include_review=True)

    log.info(f"Scoring: {len(accepted)} accepted, {len(review)} review, "
             f"{len(rejected)} rejected out of {len(raw_result.rows)} raw rows")

    # Flag review rows
    for row in review:
        row.notes = (row.notes or '').replace(' [needs review]', '')
        # Don't double-tag

    # ── Step 7: Deduplicate ──────────────────────────────────────────────────
    combined = accepted + review
    combined = deduplicate_rows(combined)

    # ── Step 8: Mill name detection ──────────────────────────────────────────
    mill_name = raw_result.mill_name
    if not mill_name or _is_bad_mill_name(mill_name):
        # Try to detect from metadata collected during cleaning
        metadata = getattr(raw_result, '_metadata', {})
        detected = detect_mill_name(metadata, original_filename)
        if detected and not _is_bad_mill_name(detected):
            mill_name = detected
        elif original_filename:
            from .cleaning import _mill_name_from_filename
            fn_name = _mill_name_from_filename(original_filename)
            # Only use filename if it looks like a real name (not a UUID)
            if fn_name and not re.match(r'^[0-9a-f\-]{20,}$', fn_name):
                mill_name = fn_name

    result.mill_name = mill_name
    result.mill_location = raw_result.mill_location
    result.mill_phone = raw_result.mill_phone
    result.mill_email = raw_result.mill_email
    result.mill_contact = raw_result.mill_contact
    result.stock_date = raw_result.stock_date

    # ── Step 9: Assemble final result ────────────────────────────────────────
    result.rows = combined
    result.success = len(combined) > 0
    if combined:
        avg_conf = sum(r.confidence for r in combined) / len(combined)
        result.confidence = round(avg_conf, 3)
    else:
        result.confidence = 0.0

    # Summary warnings
    if rejected:
        result.warnings.append(
            f"{len(rejected)} rows rejected as non-inventory "
            f"(contact info, addresses, junk text, etc.)")
    if review:
        result.warnings.append(
            f"{len(review)} rows flagged for manual review (low confidence)")

    log.info(f"Final: {len(result.rows)} rows, confidence={result.confidence:.2f}")
    return result


# ─── File type detection ──────────────────────────────────────────────────────

def _detect_file_type(ext: str) -> str:
    if ext in ('.pdf',):
        return 'pdf'
    elif ext in ('.xlsx', '.xls'):
        return 'excel'
    elif ext in ('.csv',):
        return 'csv'
    return 'unknown'


# ─── Strategy dispatchers ────────────────────────────────────────────────────

def _extract_pdf(file_path: str) -> ParseResult:
    """Extract rows from a PDF, applying cleaning layer."""
    from .pdf_parser import parse_pdf
    result = parse_pdf(file_path)
    return result


def _extract_excel(file_path: str) -> ParseResult:
    """Extract rows from an Excel/CSV file."""
    from .excel_parser import parse_excel
    result = parse_excel(file_path)
    return result


def _extract_via_ai(file_path: str, file_type: str) -> ParseResult:
    """Extract using AI parsing with strict validation."""
    from .ai_parser import parse_with_ai
    import pdfplumber

    text = ""
    if file_type == 'pdf':
        try:
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    text += (page.extract_text() or "") + "\n"
        except Exception:
            pass

        # If scanned, try OCR
        if len(text.strip()) < 50:
            from .pdf_parser import _ocr_pdf
            text = _ocr_pdf(file_path)
    else:
        import pandas as pd
        try:
            if file_type == 'excel':
                df = pd.read_excel(file_path)
            else:
                df = pd.read_csv(file_path)
            text = df.to_string()
        except Exception as e:
            result = ParseResult()
            result.errors.append(f"Could not read file for AI parsing: {e}")
            return result

    return parse_with_ai(text, file_type)


# ─── Mill name validation ────────────────────────────────────────────────────

def _is_bad_mill_name(name: str) -> bool:
    """Check if a detected mill name is obviously wrong."""
    if not name:
        return True
    n = name.strip().lower()
    # URL
    if re.search(r'\.(com|net|org|io)\b', n):
        return True
    # Email
    if '@' in n:
        return True
    # Too short
    if len(n) < 4:
        return True
    # Is just species names
    from .cleaning import _is_all_species, _SPECIES_WORDS
    words = n.split()
    species_count = sum(1 for w in words if w.lower() in _SPECIES_WORDS)
    if species_count >= len(words) * 0.7 and len(words) >= 2:
        return True
    # Contains "Email:" or "phone:" etc.
    if re.search(r'\b(email|phone|fax|http|www)\b', n, re.I):
        return True
    return False
