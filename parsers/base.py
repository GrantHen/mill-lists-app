"""
Base classes and utilities for parsing mill stock lists.
"""
from dataclasses import dataclass, field, asdict
from typing import Optional, List


@dataclass
class ParsedRow:
    """A single parsed product/inventory row."""
    species: Optional[str] = None
    product: str = ""
    product_normalized: Optional[str] = None
    thickness: Optional[str] = None
    grade: Optional[str] = None
    description: Optional[str] = None
    quantity: Optional[str] = None
    quantity_numeric: Optional[float] = None
    uom: str = "BF"
    price: Optional[str] = None
    price_numeric: Optional[float] = None
    length: Optional[str] = None
    width: Optional[str] = None
    surface: Optional[str] = None
    treatment: Optional[str] = None
    color: Optional[str] = None
    cut_type: Optional[str] = None
    notes: Optional[str] = None
    confidence: float = 1.0
    raw_text: Optional[str] = None
    source_row: Optional[int] = None

    def to_dict(self):
        return asdict(self)


@dataclass
class ParseResult:
    """Result of parsing a mill stock list file."""
    success: bool = False
    rows: List[ParsedRow] = field(default_factory=list)
    mill_name: Optional[str] = None
    mill_location: Optional[str] = None
    mill_phone: Optional[str] = None
    mill_email: Optional[str] = None
    mill_contact: Optional[str] = None
    stock_date: Optional[str] = None
    parsing_method: str = "unknown"
    confidence: float = 0.0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    raw_text: Optional[str] = None

    def to_dict(self):
        return {
            "success": self.success,
            "rows": [r.to_dict() for r in self.rows],
            "mill_name": self.mill_name,
            "mill_location": self.mill_location,
            "mill_phone": self.mill_phone,
            "mill_email": self.mill_email,
            "mill_contact": self.mill_contact,
            "stock_date": self.stock_date,
            "parsing_method": self.parsing_method,
            "confidence": self.confidence,
            "errors": self.errors,
            "warnings": self.warnings,
            "row_count": len(self.rows),
        }
