"""
AI-powered parser using OpenAI GPT-4o for difficult-to-parse mill lists.

Improvements over previous version:
  - Strict JSON schema enforcement
  - Retry logic with exponential backoff (up to 3 attempts)
  - Post-processing validation layer — AI output is cleaned deterministically
  - Row-level scoring applied to AI-extracted rows
  - API key reloaded on every call (not cached at module level)
  - Malformed/partial JSON recovery
"""
import json
import os
import re
import time
import logging
import urllib.request
import urllib.error
from .base import ParseResult, ParsedRow
from .cleaning import is_noise_text
from .row_scoring import score_parsed_row, classify_row
from lumber_normalizer import (
    normalize_thickness, normalize_grade, normalize_species,
    build_product_string
)

log = logging.getLogger("ai_parser")

MAX_RETRIES = 3
RETRY_DELAYS = [2, 5, 10]  # seconds
MAX_INPUT_CHARS = 15000

# ─── API Key ──────────────────────────────────────────────────────────────────

def get_openai_key() -> str:
    """Get OpenAI API key, checking env var first, then config.json."""
    key = os.environ.get("OPENAI_API_KEY", "")
    if not key:
        cfg_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.json")
        if os.path.exists(cfg_path):
            try:
                with open(cfg_path) as f:
                    key = json.load(f).get("OPENAI_API_KEY", "")
            except Exception:
                pass
    return key


# ─── System prompt — focused on inventory-only extraction ─────────────────────

SYSTEM_PROMPT = """You are a lumber industry data extraction expert. Your ONLY job is to extract INVENTORY/PRODUCT rows from mill stock lists into structured JSON.

CRITICAL RULES:
1. Extract ONLY actual inventory/product rows — items that represent lumber for sale.
2. DO NOT extract: contact info, addresses, phone numbers, email addresses, website text, legal disclaimers, FSC certifications, branding text, headers, footers, or any non-product information.
3. Species names (ASH, CHERRY, HARD MAPLE, RED OAK, etc.) appear as SECTION HEADERS. Apply the current species to all products listed under it until the next species header.
4. Each product row typically has: thickness (4/4, 5/4, 6/4, 8/4), grade (FAS, S&B, #1C, #2A, etc.), and quantity (board feet or T/L).

Return ONLY valid JSON matching this exact schema:

{
  "mill_name": "string or null — the company name only, NOT a URL or email",
  "stock_date": "string or null",
  "products": [
    {
      "species": "string — wood species from section header",
      "thickness": "string — quarter format like 4/4, 5/4, 8/4",
      "grade": "string — lumber grade (FAS, Select & Better, #1 Common, etc.)",
      "quantity": "string — board feet amount, T/L, or Call",
      "quantity_numeric": "number or null",
      "price": "string or null",
      "length": "string or null",
      "width": "string or null",
      "surface": "string or null — RGH, S2S, etc.",
      "color": "string or null — White, Brown, Red, Heart, Sap, etc.",
      "cut_type": "string or null — Rift, Quartered, Plain Sawn, etc.",
      "treatment": "string or null — Steamed, Stained, Wormy, etc.",
      "notes": "string or null — product-specific notes ONLY",
      "raw_text": "the original text of this product line"
    }
  ]
}

IMPORTANT: Extract EVERY product line. Do not skip or summarize. Do not include non-product rows."""


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def parse_with_ai(text: str, file_type: str = "pdf") -> ParseResult:
    """
    Parse mill stock list text using OpenAI GPT-4o.
    Includes retry logic and post-processing validation.
    """
    result = ParseResult(parsing_method="ai_openai")

    # Reload key every time (user may have just saved it)
    api_key = get_openai_key()
    if not api_key:
        result.success = False
        result.errors.append(
            "OpenAI API key not configured. Go to Settings to add your key.")
        return result

    # Truncate very long text
    if len(text) > MAX_INPUT_CHARS:
        text = text[:MAX_INPUT_CHARS] + "\n... [truncated]"
        result.warnings.append(
            f"Input truncated to {MAX_INPUT_CHARS} chars for AI processing.")

    # ── Call API with retry ──────────────────────────────────────────────────
    response = None
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            response = _call_openai(text, api_key)
            if response:
                break
        except Exception as e:
            last_error = str(e)
            log.warning(f"AI attempt {attempt+1}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAYS[attempt])

    if not response:
        result.success = False
        result.errors.append(
            f"AI parsing failed after {MAX_RETRIES} attempts. "
            f"Last error: {last_error or 'Empty response'}")
        return result

    # ── Parse JSON response ──────────────────────────────────────────────────
    data = _extract_json(response)
    if not data:
        result.success = False
        result.errors.append("Could not parse JSON from AI response.")
        result.warnings.append(f"Raw response preview: {response[:300]}")
        return result

    # Validate top-level schema
    if not isinstance(data, dict):
        result.success = False
        result.errors.append("AI response is not a JSON object.")
        return result

    # ── Extract mill info ────────────────────────────────────────────────────
    mill_name = data.get("mill_name")
    if mill_name and not _is_valid_mill_name(mill_name):
        mill_name = None
    result.mill_name = mill_name
    result.stock_date = data.get("stock_date")

    # ── Extract and validate products ────────────────────────────────────────
    products = data.get("products", [])
    if not isinstance(products, list):
        result.success = False
        result.errors.append("AI response 'products' is not a list.")
        return result

    for i, p in enumerate(products):
        if not isinstance(p, dict):
            continue

        row = _build_row_from_ai(p, i)
        if row is None:
            continue

        # Post-processing: score the row
        score_parsed_row(row)
        classification = classify_row(row)

        # Only keep rows that pass validation
        if classification != 'reject':
            result.rows.append(row)
        else:
            log.debug(f"AI row rejected (conf={row.confidence:.2f}): {row.raw_text}")

    result.success = len(result.rows) > 0
    if result.rows:
        avg_conf = sum(r.confidence for r in result.rows) / len(result.rows)
        result.confidence = round(avg_conf, 3)
    else:
        result.confidence = 0.0
        if products:
            result.warnings.append(
                f"AI extracted {len(products)} rows but all were rejected "
                f"by validation (likely non-inventory content).")

    log.info(f"AI parse: {len(products)} raw → {len(result.rows)} validated")
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# ROW BUILDER — deterministic post-processing of AI output
# ═══════════════════════════════════════════════════════════════════════════════

def _build_row_from_ai(p: dict, idx: int) -> ParsedRow:
    """
    Build a ParsedRow from an AI-extracted product dict.
    Applies normalization and basic sanity checks.
    """
    raw_text = str(p.get("raw_text", ""))

    # Quick noise rejection
    if is_noise_text(raw_text):
        return None

    # Extract fields with normalization
    species = _clean_field(p.get("species"))
    thickness = _clean_field(p.get("thickness"))
    grade = _clean_field(p.get("grade"))
    quantity = _clean_field(p.get("quantity"))
    qty_numeric = p.get("quantity_numeric")

    # Normalize
    species = normalize_species(species) if species else None
    thickness = normalize_thickness(thickness) if thickness else None
    grade = normalize_grade(grade) if grade else None

    # Quantity numeric validation
    if qty_numeric is not None:
        try:
            qty_numeric = float(qty_numeric)
            if qty_numeric <= 0:
                qty_numeric = None
        except (ValueError, TypeError):
            qty_numeric = None

    # Must have at least one meaningful inventory field
    if not any([species, thickness, grade, quantity, qty_numeric]):
        return None

    row = ParsedRow(
        species=species,
        product=raw_text or "",
        thickness=thickness,
        grade=grade,
        quantity=quantity,
        quantity_numeric=qty_numeric,
        price=_clean_field(p.get("price")),
        length=_clean_field(p.get("length")),
        width=_clean_field(p.get("width")),
        surface=_clean_field(p.get("surface")),
        color=_clean_field(p.get("color")),
        cut_type=_clean_field(p.get("cut_type")),
        treatment=_clean_field(p.get("treatment")),
        notes=_clean_field(p.get("notes")),
        description=_clean_field(p.get("description")),
        raw_text=raw_text,
        source_row=idx,
    )
    row.product_normalized = build_product_string(row.to_dict())
    return row


def _clean_field(val) -> str:
    """Clean a single field value from AI output."""
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ('null', 'none', 'n/a', 'na', '-', ''):
        return None
    return s


def _is_valid_mill_name(name: str) -> bool:
    """Check if AI-returned mill name is plausible."""
    if not name:
        return False
    n = name.strip()
    if len(n) < 3:
        return False
    # Reject URLs, emails
    if re.search(r'[@.](?:com|net|org|io)\b', n, re.I):
        return False
    if re.search(r'https?://', n, re.I):
        return False
    return True


# ═══════════════════════════════════════════════════════════════════════════════
# OPENAI API CALL
# ═══════════════════════════════════════════════════════════════════════════════

def _call_openai(text: str, api_key: str) -> str:
    """Call OpenAI API with structured output enforcement."""
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    payload = json.dumps({
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Parse this mill stock list. "
             f"Return ONLY the JSON with inventory rows:\n\n{text}"}
        ],
        "temperature": 0.05,
        "max_tokens": 8000,
        "response_format": {"type": "json_object"}
    }).encode('utf-8')

    req = urllib.request.Request(url, data=payload, headers=headers)

    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            return data["choices"][0]["message"]["content"]
    except urllib.error.HTTPError as e:
        body = e.read().decode() if hasattr(e, 'read') else ''
        raise Exception(f"OpenAI API error {e.code}: {body[:200]}")
    except urllib.error.URLError as e:
        raise Exception(f"Network error: {str(e)}")


# ═══════════════════════════════════════════════════════════════════════════════
# JSON EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_json(text: str) -> dict:
    """Extract JSON from AI response, handling edge cases."""
    if not text:
        return None

    # Direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Code block
    m = re.search(r'```(?:json)?\s*\n?(.*?)\n?\s*```', text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    # Find outermost JSON object
    start = text.find('{')
    if start >= 0:
        # Find matching closing brace
        depth = 0
        for i in range(start, len(text)):
            if text[i] == '{':
                depth += 1
            elif text[i] == '}':
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[start:i+1])
                    except json.JSONDecodeError:
                        break

    # Last resort: try to fix common JSON issues
    cleaned = text.strip()
    # Remove trailing comma before closing brace/bracket
    cleaned = re.sub(r',\s*([}\]])', r'\1', cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    return None
