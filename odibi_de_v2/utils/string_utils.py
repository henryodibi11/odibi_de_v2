"""
string_utils.py

Reusable string utilities for text normalization, column name formatting,
and general-purpose transformations for data engineering workflows.
"""

import re
from typing import Optional, List


# ---------------------------
# Core Normalization
# ---------------------------

def normalize_string(value: Optional[str], case: str = "lower") -> str:
    """
    Strip whitespace and convert to lower/upper/title case.

    Args:
        value: Input string (None-safe).
        case: "lower", "upper", "title".

    Returns:
        Normalized string.
    """
    if not value:
        return ""
    value = value.strip()
    match case.lower():
        case "lower": return value.lower()
        case "upper": return value.upper()
        case "title": return value.title()
        case _: raise ValueError(f"Unsupported case: {case}")


def remove_extra_whitespace(text: str) -> str:
    """Collapse multiple spaces into one and strip edges."""
    return re.sub(r"\s+", " ", (text or "").strip())


def is_null_or_blank(text: Optional[str]) -> bool:
    """True if text is None, empty, or only whitespace."""
    return not text or text.strip() == ""


# ---------------------------
# Column Name Cleaning
# ---------------------------

def clean_column_name(name: str) -> str:
    """Convert to snake_case, replacing non-alphanumerics with underscores."""
    return re.sub(r"[^\w]+", "_", (name or "")).strip("_").lower()


def standardize_column_names(columns: List[str]) -> List[str]:
    """Apply `clean_column_name` to a list of column names."""
    return [clean_column_name(c) for c in columns]


# ---------------------------
# Case Conversions
# ---------------------------

def to_snake_case(text: str) -> str:
    """CamelCase → snake_case."""
    text = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", text)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    return text.lower()


def to_kebab_case(text: str) -> str:
    """Any string → kebab-case."""
    return re.sub(r"[^\w]+", "-", (text or "").strip()).lower()


def to_camel_case(text: str) -> str:
    """Any string → camelCase."""
    words = re.split(r"[\W_]+", (text or "").strip())
    return words[0].lower() + "".join(w.title() for w in words[1:])


def to_pascal_case(text: str) -> str:
    """Any string → PascalCase."""
    words = re.split(r"[\W_]+", (text or "").strip())
    return "".join(w.title() for w in words)


def slugify(text: str) -> str:
    """Safe identifier: lowercase, underscores only."""
    return re.sub(r"[^\w]+", "_", (text or "").strip()).strip("_").lower()


# ---------------------------
# Unified Transformer
# ---------------------------

def transform_column_name(name: str, style: str) -> str:
    """
    Transform a column name to a specific style.

    Supported styles:
      - "snake"   → snake_case
      - "camel"   → camelCase
      - "pascal"  → PascalCase
      - "kebab"   → kebab-case
      - "slug"    → slug_format
      - "lower"   → lowercase
      - "upper"   → UPPERCASE

    Example:
        >>> transform_column_name("Total Sales ($)", "snake")
        'total_sales'
        >>> transform_column_name("Total Sales ($)", "camel")
        'totalSales'
        >>> transform_column_name("Total Sales ($)", "pascal")
        'TotalSales'
    """
    style = style.lower()
    if style == "snake": return clean_column_name(name)
    if style == "camel": return to_camel_case(name)
    if style == "pascal": return to_pascal_case(name)
    if style == "kebab": return to_kebab_case(name)
    if style == "slug":  return slugify(name)
    if style == "lower": return (name or "").lower()
    if style == "upper": return (name or "").upper()
    raise ValueError(f"Unsupported case style: {style}")
