"""
Reference range text parser utility

Parses text-based reference ranges (e.g., "0.00 - 4.00") into numeric LLN/ULN values
"""
import re
from typing import Optional, Tuple


def parse_reference_range_text(range_text: Optional[str]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Parse reference range text to extract LLN and ULN values
    
    Handles patterns like:
    - "0.00 - 4.00" → (0.00, 4.00, None)
    - "1.005-1.025" → (1.005, 1.025, None)
    - "< or = 4.00" → (None, 4.00, None)
    - "> or = 60" → (60, None, None)
    - "0.00 - 4.00 ng/mL" → (0.00, 4.00, "ng/mL")
    
    Args:
        range_text: Text like "0.00 - 4.00", "< or = 4.00", "1.005-1.025"
    
    Returns:
        Tuple of (low_value, high_value, unit) as floats/string
        Returns (None, None, None) if cannot parse
    """
    if not range_text:
        return None, None, None
    
    # Normalize text - remove extra whitespace
    text = ' '.join(range_text.strip().split())
    
    # Skip empty strings
    if not text:
        return None, None, None
    
    # Skip qualitative values (these don't have numeric ranges)
    qualitative_values = [
        'neg', 'negative', 'positive', 'none', 'yellow', 'clear', 'cloudy',
        'yelllow', 'sm amt', 'small', 'large', 'trace', 'moderate'
    ]
    text_lower = text.lower()
    if text_lower in qualitative_values:
        return None, None, None
    
    # Pattern 1: "X.XX - Y.YY" or "X.XX-Y.YY" (with or without spaces around dash)
    # Also handles "X.XX - Y.YY unit" format
    match = re.match(r'^([\d.]+)\s*-\s*([\d.]+)(?:\s+(\w+(?:\s*/\s*\w+)?))?$', text, re.IGNORECASE)
    if match:
        try:
            low = float(match.group(1))
            high = float(match.group(2))
            unit = match.group(3) if match.group(3) else None
            return low, high, unit
        except ValueError:
            pass
    
    # Pattern 2: "< or = X.XX" or "<= X.XX" or "< or= X.XX" (upper limit only)
    match = re.match(r'^<[=\s]*or[=\s]*=\s*([\d.]+)(?:\s+(\w+(?:\s*/\s*\w+)?))?$', text, re.IGNORECASE)
    if match:
        try:
            high = float(match.group(1))
            unit = match.group(2) if match.group(2) else None
            return None, high, unit
        except ValueError:
            pass
    
    # Pattern 3: "> or = X.XX" or ">= X.XX" or "> or= X.XX" (lower limit only)
    match = re.match(r'^>[=\s]*or[=\s]*=\s*([\d.]+)(?:\s+(\w+(?:\s*/\s*\w+)?))?$', text, re.IGNORECASE)
    if match:
        try:
            low = float(match.group(1))
            unit = match.group(2) if match.group(2) else None
            return low, None, unit
        except ValueError:
            pass
    
    # Pattern 4: "X.XX - Y.YY unit" (with unit at end, more specific)
    match = re.match(r'^([\d.]+)\s*-\s*([\d.]+)\s+([a-zA-Z]+(?:\s*/\s*[a-zA-Z]+)?)$', text)
    if match:
        try:
            low = float(match.group(1))
            high = float(match.group(2))
            unit = match.group(3)
            return low, high, unit
        except ValueError:
            pass
    
    # Pattern 5: "<= X.XX" (simpler upper limit)
    match = re.match(r'^<=\s*([\d.]+)(?:\s+(\w+(?:\s*/\s*\w+)?))?$', text, re.IGNORECASE)
    if match:
        try:
            high = float(match.group(1))
            unit = match.group(2) if match.group(2) else None
            return None, high, unit
        except ValueError:
            pass
    
    # Pattern 6: ">= X.XX" (simpler lower limit)
    match = re.match(r'^>=\s*([\d.]+)(?:\s+(\w+(?:\s*/\s*\w+)?))?$', text, re.IGNORECASE)
    if match:
        try:
            low = float(match.group(1))
            unit = match.group(2) if match.group(2) else None
            return low, None, unit
        except ValueError:
            pass
    
    # Pattern 7: Handle ranges with parentheses like "sm amt (.5-1mg/dL)" or "<=3 RBC"
    # Extract numeric part from parentheses
    match = re.search(r'\(([\d.]+)\s*-\s*([\d.]+)(\w+)?\)', text, re.IGNORECASE)
    if match:
        try:
            low = float(match.group(1))
            high = float(match.group(2))
            unit = match.group(3) if match.group(3) else None
            return low, high, unit
        except ValueError:
            pass
    
    # Pattern 8: Handle "<=X" or "<=X unit" or "<=X unit/unit" (no spaces, with or without equals)
    match = re.match(r'^<[=\s]*(\d+(?:\.\d+)?)(?:\s+(\w+(?:\s*/\s*\w+)?))?$', text, re.IGNORECASE)
    if match:
        try:
            high = float(match.group(1))
            unit = match.group(2) if match.group(2) else None
            return None, high, unit
        except ValueError:
            pass
    
    # Pattern 9: Handle ">X" or ">X unit" (no equals, lower limit only)
    match = re.match(r'^>[=\s]*(\d+(?:\.\d+)?)(?:\s+(\w+(?:\s*/\s*\w+)?))?$', text, re.IGNORECASE)
    if match:
        try:
            low = float(match.group(1))
            unit = match.group(2) if match.group(2) else None
            return low, None, unit
        except ValueError:
            pass
    
    # Pattern 10: Handle "<=X unit" where unit is attached (e.g., "<=3RBC" or "<=150mg/d")
    match = re.match(r'^<[=\s]*(\d+(?:\.\d+)?)([a-zA-Z]+(?:\s*/\s*[a-zA-Z]+)?)$', text, re.IGNORECASE)
    if match:
        try:
            high = float(match.group(1))
            unit = match.group(2)
            return None, high, unit
        except ValueError:
            pass
    
    # Pattern 11: Handle ranges without spaces like "4.5-8" (already covered by Pattern 1, but ensure it works)
    # This should be caught by Pattern 1, but adding explicit check for integers
    match = re.match(r'^(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)$', text)
    if match:
        try:
            low = float(match.group(1))
            high = float(match.group(2))
            return low, high, None
        except ValueError:
            pass
    
    # Could not parse - return None values
    return None, None, None

