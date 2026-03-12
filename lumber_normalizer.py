"""
Lumber industry product normalizer.
Handles synonyms, abbreviations, and standard product terminology
used across different mill stock lists.
"""
import re

# Thickness mappings (quarter system to standard)
THICKNESS_MAP = {
    "3/4": "3/4",
    "4/4": "4/4", "1\"": "4/4", "1 inch": "4/4",
    "5/4": "5/4", "1-1/4\"": "5/4",
    "6/4": "6/4", "1-1/2\"": "6/4",
    "7/4": "7/4",
    "8/4": "8/4", "2\"": "8/4", "2 inch": "8/4",
    "9/4": "9/4",
    "10/4": "10/4", "2-1/2\"": "10/4",
    "12/4": "12/4", "3\"": "12/4",
    "13/16": "13/16",
    "15/16": "15/16",
    "5/8": "5/8",
}

# Grade abbreviation mappings
GRADE_MAP = {
    "fas": "FAS",
    "f.a.s.": "FAS",
    "fas/sel": "FAS/SEL",
    "s&b": "Select & Better",
    "s & b": "Select & Better",
    "sel & btr": "Select & Better",
    "sel&btr": "Select & Better",
    "sel & better": "Select & Better",
    "select & btr": "Select & Better",
    "select & btr.": "Select & Better",
    "select & better": "Select & Better",
    "s&btr": "Select & Better",
    "#1c": "#1 Common",
    "1c": "#1 Common",
    "#1 com": "#1 Common",
    "1 com": "#1 Common",
    "1 common": "#1 Common",
    "#1 common": "#1 Common",
    "1com": "#1 Common",
    "1com.": "#1 Common",
    "#2c": "#2 Common",
    "2c": "#2 Common",
    "2 com": "#2 Common",
    "2 common": "#2 Common",
    "#2 common": "#2 Common",
    "2com": "#2 Common",
    "2a": "#2A Common",
    "2a com": "#2A Common",
    "2a common": "#2A Common",
    "#2a": "#2A Common",
    "#2a common": "#2A Common",
    "2a/3a": "#2A/3A Common",
    "3 com": "#3 Common",
    "3 common": "#3 Common",
    "#3 common": "#3 Common",
    "3com": "#3 Common",
    "3a com": "#3A Common",
    "#1c&btr": "#1 Common & Better",
    "1c&btr": "#1 Common & Better",
    "#1 common & btr": "#1 Common & Better",
    "1 common & btr": "#1 Common & Better",
    "#1c 1&2 white": "#1C 1&2 White",
    "1/btr": "#1 & Better",
    "1&btr": "#1 & Better",
    "rustic": "Rustic",
    "prime": "Prime",
    "superprime": "Super Prime",
    "premium": "Premium",
    "fab": "FAB",
    "fab mineral": "FAB Mineral",
    "fab stained": "FAB Stained",
    "d&btr": "D & Better",
    "prem furniture": "Premium Furniture",
    "s1f&btr": "S1F & Better",
    "1c s1f&btr": "#1C S1F & Better",
    "red 1f&btr": "Red 1F & Better",
    "#2a/3a": "#2A/3A",
    "rgh": "Rough",
}

# Species synonyms
SPECIES_MAP = {
    "ash": "Ash",
    "white ash": "White Ash",
    "aspen": "Aspen",
    "basswood": "Basswood",
    "beech": "Beech",
    "birch": "Birch",
    "white birch": "White Birch",
    "yellow birch": "Yellow Birch",
    "catalpa": "Catalpa",
    "cedar": "Cedar",
    "aromatic cedar": "Aromatic Cedar",
    "aromatic red cedar": "Aromatic Red Cedar",
    "cedar aromatic": "Aromatic Cedar",
    "cedar, aromatic red": "Aromatic Red Cedar",
    "cherry": "Cherry",
    "coffeenut": "Coffeenut",
    "cottonwood": "Cottonwood",
    "cypress": "Cypress",
    "elm": "Elm",
    "grey elm": "Grey Elm",
    "red elm": "Red Elm",
    "hackberry": "Hackberry",
    "hard maple": "Hard Maple",
    "hemlock": "Hemlock",
    "hickory": "Hickory",
    "honey locust": "Honey Locust",
    "maple": "Maple",
    "soft maple": "Soft Maple",
    "soft maple red leaf": "Soft Maple",
    "soft maple - red leaf": "Soft Maple",
    "oak": "Oak",
    "red oak": "Red Oak",
    "white oak": "White Oak",
    "white oak rift & quarter": "White Oak Rift & Quarter",
    "white oak rifted & quartered": "White Oak Rift & Quarter",
    "white oak rift & quartered": "White Oak Rift & Quarter",
    "white oak plain sawn": "White Oak Plain Sawn",
    "red oak plain sawn": "Red Oak Plain Sawn",
    "red oak rift & quarter": "Red Oak Rift & Quarter",
    "pine": "Pine",
    "white pine": "White Pine",
    "syp": "Southern Yellow Pine",
    "southern yellow pine": "Southern Yellow Pine",
    "treated pine": "Treated Pine",
    "poplar": "Poplar",
    "sassafras": "Sassafras",
    "sap gum": "Sap Gum",
    "sycamore": "Sycamore",
    "walnut": "Walnut",
    "rq walnut": "RQ Walnut",
    "alder": "Alder",
    "black ash": "Black Ash",
}

# Surface / description abbreviations
SURFACE_MAP = {
    "rgh": "Rough",
    "s2s": "S2S (Surfaced Two Sides)",
    "s2s 15/16": "S2S 15/16",
    "s4s": "S4S (Surfaced Four Sides)",
    "s25": "S2S",
    "sap s2s": "SAP S2S",
    "brn s2s": "Brown S2S",
}

# Cut type mappings
CUT_MAP = {
    "rift": "Rift",
    "quartered": "Quartered",
    "quarter": "Quartered",
    "qtr": "Quartered",
    "rift & quartered": "Rift & Quartered",
    "rift & quarter": "Rift & Quartered",
    "r&q": "Rift & Quartered",
    "plain sawn": "Plain Sawn",
    "flitch": "Flitch Sawn",
    "flitch sawn": "Flitch Sawn",
    "circle-sawn": "Circle Sawn",
    "live sawn": "Live Sawn",
}

# Color mappings
COLOR_MAP = {
    "white": "White",
    "brown": "Brown",
    "red": "Red",
    "natural": "Natural",
    "sap": "Sap",
    "heart": "Heart",
    "heartwood": "Heartwood",
    "sapwood": "Sapwood",
    "wheat": "Wheat",
    "northern": "Northern",
    "calico": "Calico",
    "unselected": "Unselected",
}

# Treatment / condition
TREATMENT_MAP = {
    "steamed": "Steamed",
    "unsteamed": "Unsteamed",
    "stained": "Stained",
    "spalted": "Spalted",
    "wormy": "Wormy",
    "ambrosia": "Ambrosia/Wormy",
    "whnd": "Wormy",
    "whnd(ambrosia)": "Ambrosia/Wormy",
    "checked": "Checked",
    "striped": "Striped",
    "stick shadow": "Stick Shadow",
    "light stick shadow": "Light Stick Shadow",
}


def normalize_thickness(raw):
    """Normalize thickness to standard quarter format."""
    if not raw:
        return None
    raw = str(raw).strip().lower()
    if raw in THICKNESS_MAP:
        return THICKNESS_MAP[raw]
    # Try to match fraction pattern
    m = re.match(r'(\d+)/(\d+)', raw)
    if m:
        return f"{m.group(1)}/{m.group(2)}"
    return raw.upper()


def normalize_grade(raw):
    """Normalize grade abbreviations."""
    if not raw:
        return None
    raw_lower = str(raw).strip().lower()
    if raw_lower in GRADE_MAP:
        return GRADE_MAP[raw_lower]
    return str(raw).strip()


def normalize_species(raw):
    """Normalize species names."""
    if not raw:
        return None
    raw_lower = str(raw).strip().lower().rstrip(':')
    if raw_lower in SPECIES_MAP:
        return SPECIES_MAP[raw_lower]
    # Try partial matching
    for key, val in SPECIES_MAP.items():
        if key in raw_lower or raw_lower in key:
            return val
    return str(raw).strip().title()


def normalize_quantity(raw):
    """Extract numeric quantity from various formats."""
    if not raw:
        return None, None
    raw = str(raw).strip().replace(",", "").replace("'", "")
    # Try to extract number
    m = re.search(r'([\d.]+)', raw)
    if m:
        try:
            return float(m.group(1)), raw
        except ValueError:
            pass
    if raw.upper() in ("CALL", "T/L", "TL"):
        return None, raw.upper()
    return None, raw


def build_product_string(row):
    """Build a normalized product description string from parsed fields."""
    parts = []
    if row.get('thickness'):
        parts.append(row['thickness'])
    if row.get('species'):
        parts.append(row['species'])
    if row.get('grade'):
        parts.append(row['grade'])
    if row.get('color'):
        parts.append(row['color'])
    if row.get('cut_type'):
        parts.append(row['cut_type'])
    if row.get('surface'):
        parts.append(row['surface'])
    if row.get('treatment'):
        parts.append(row['treatment'])
    if row.get('length'):
        parts.append(row['length'])
    return " ".join(parts) if parts else row.get('product', '')


def extract_thickness_from_text(text):
    """Extract thickness from a product description string."""
    if not text:
        return None
    m = re.search(r'\b(\d{1,2}/\d)\b', text)
    if m:
        return normalize_thickness(m.group(1))
    return None


def extract_quantity_from_text(text):
    """Extract board feet quantity from text like '4,300' 4/4 #1C'."""
    if not text:
        return None, None
    # Match patterns like "4,300'" or "4300'" or just "4300"
    m = re.match(r"^([\d,]+)'?\s", text)
    if m:
        val = m.group(1).replace(",", "")
        try:
            return float(val), f"{val} BF"
        except ValueError:
            pass
    return None, None
