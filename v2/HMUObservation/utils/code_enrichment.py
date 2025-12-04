"""
Code enrichment utility for observations

Enriches missing LOINC codes using data-driven lookup table and provides normalized text
"""
from typing import Optional, Tuple, List, Dict, Any

from .code_enrichment_mappings import LAB_TEST_LOINC_MAPPING

# Note: LAB_TEST_LOINC_MAPPING is now imported from code_enrichment_mappings.py
# This keeps the mapping data separate from the enrichment logic for better maintainability


def enrich_observation_code(
    code_text: Optional[str], 
    code_coding_array: Optional[List[Dict[str, Any]]],
    enable_enrichment: bool = True,
    enrichment_mode: str = "hybrid"
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Enrich observation code when coding array is missing
    
    Args:
        code_text: The code.text value from the observation
        code_coding_array: The code.coding array (may be None or empty)
        enable_enrichment: Feature flag to enable/disable enrichment
        enrichment_mode: "loinc_only", "synthetic_only", or "hybrid"
    
    Returns: Tuple of (list of code dictionaries, normalized_text)
        - codes_to_add: List of code dictionaries to add to the coding array
        - normalized_text: Well-formatted canonical name for the observation
    """
    codes_to_add: List[Dict[str, Any]] = []
    normalized_text: Optional[str] = None
    
    # If enrichment is disabled, return empty
    if not enable_enrichment:
        return codes_to_add, normalized_text
    
    # If coding array exists and is not empty, use it (no enrichment needed)
    if code_coding_array and len(code_coding_array) > 0:
        # Still need to get normalized_text from lookup if available
        normalized_text_for_lookup = code_text.lower().strip() if code_text else None
        if normalized_text_for_lookup:
            loinc_match = LAB_TEST_LOINC_MAPPING.get(normalized_text_for_lookup)
            if not loinc_match:
                # Try fuzzy matching (handle punctuation variations)
                cleaned_text = normalized_text_for_lookup.replace(',', '').replace('-', ' ').replace('_', ' ')
                loinc_match = LAB_TEST_LOINC_MAPPING.get(cleaned_text.strip())
            if loinc_match and loinc_match.get("normalized_text"):
                normalized_text = loinc_match["normalized_text"]
        return codes_to_add, normalized_text  # No enrichment needed, but return normalized_text if found
    
    # If no coding but text exists, try to enrich
    if not code_text:
        return codes_to_add, None  # Nothing to enrich
    
    # Clean code_text: remove tabs and newlines (often contain component data)
    # Take only first line or first 200 chars to avoid extremely long synthetic codes
    cleaned_code_text = code_text.strip()
    if '\t' in cleaned_code_text or '\n' in cleaned_code_text:
        # If contains tabs/newlines, take only the first part (before first tab/newline)
        cleaned_code_text = cleaned_code_text.split('\t')[0].split('\n')[0].strip()
    # Limit to 200 chars for synthetic code generation
    if len(cleaned_code_text) > 200:
        cleaned_code_text = cleaned_code_text[:200]
    
    # Normalize text for lookup (handle variations)
    normalized_text_for_lookup = cleaned_code_text.lower().strip()
    
    # Try exact match first
    loinc_match = LAB_TEST_LOINC_MAPPING.get(normalized_text_for_lookup)
    
    # If no exact match, try fuzzy matching (handle punctuation variations)
    if not loinc_match:
        cleaned_text = normalized_text_for_lookup.replace(',', '').replace('-', ' ').replace('_', ' ')
        loinc_match = LAB_TEST_LOINC_MAPPING.get(cleaned_text.strip())
    
    if loinc_match and loinc_match.get("code"):
        # Found LOINC match - use normalized_text from lookup table
        normalized_text = loinc_match.get("normalized_text", code_text.strip())
        
        # Only add LOINC code if mode allows it
        if enrichment_mode in ["loinc_only", "hybrid"]:
            code_dict = {
                "code": loinc_match["code"],
                "system": loinc_match["system"],
                "display": loinc_match["display"],
                "text": code_text
            }
            codes_to_add.append(code_dict)
            # DEBUG: Log enrichment (only occasionally to avoid spam)
            import random
            if random.random() < 0.001:  # Log 0.1% of enrichments
                print(f"🔍 DEBUG enrich_observation_code - Added LOINC code for '{code_text}': {code_dict}")
    
    # Fallback: Create synthetic code if no LOINC match and mode allows
    if not codes_to_add and enrichment_mode in ["synthetic_only", "hybrid"]:
        synthetic_code = normalized_text_for_lookup.replace(' ', '_').replace(',', '').replace('-', '_').replace('/', '_')
        
        # Truncate to 255 characters to fit Redshift VARCHAR(255) constraint
        # Use a hash suffix for truncated codes to maintain uniqueness
        if len(synthetic_code) > 255:
            import hashlib
            # Truncate to 250 chars and add 5-char hash suffix for uniqueness
            hash_suffix = hashlib.md5(synthetic_code.encode()).hexdigest()[:5]
            synthetic_code = synthetic_code[:250] + hash_suffix
        
        # For synthetic codes, use a cleaned version of original text as normalized
        normalized_text = code_text.strip() if not normalized_text else normalized_text
        code_dict = {
            "code": synthetic_code,
            "system": "http://thirdopinion.io/CodeSystem/observation-text",
            "display": code_text,
            "text": code_text
        }
        codes_to_add.append(code_dict)
        # DEBUG: Log synthetic code creation
        import random
        if random.random() < 0.001:  # Log 0.1% of enrichments
            print(f"🔍 DEBUG enrich_observation_code - Created synthetic code for '{code_text}': {code_dict}")
    
    return codes_to_add, normalized_text


def get_normalized_text_for_code(code_code: str, code_system: str, code_text: Optional[str] = None) -> Optional[str]:
    """
    Get normalized text for an existing code by looking up in mapping table
    
    Args:
        code_code: The LOINC code (e.g., "2857-1")
        code_system: The code system (e.g., "http://loinc.org")
        code_text: Optional original text for fallback lookup
    
    Returns:
        Normalized text if found, None otherwise
    """
    # Only lookup for LOINC codes
    if code_system != "http://loinc.org":
        # Try to find by text if provided
        if code_text:
            normalized_text_for_lookup = code_text.lower().strip()
            loinc_match = LAB_TEST_LOINC_MAPPING.get(normalized_text_for_lookup)
            if not loinc_match:
                cleaned_text = normalized_text_for_lookup.replace(',', '').replace('-', ' ').replace('_', ' ')
                loinc_match = LAB_TEST_LOINC_MAPPING.get(cleaned_text.strip())
            if loinc_match and loinc_match.get("normalized_text"):
                return loinc_match["normalized_text"]
        return None
    
    # Lookup by code - find all entries with this code and return the normalized_text
    for text_key, mapping in LAB_TEST_LOINC_MAPPING.items():
        if mapping.get("code") == code_code and mapping.get("system") == code_system:
            return mapping.get("normalized_text")
    
    # If not found by code, try by text if provided
    if code_text:
        normalized_text_for_lookup = code_text.lower().strip()
        loinc_match = LAB_TEST_LOINC_MAPPING.get(normalized_text_for_lookup)
        if not loinc_match:
            cleaned_text = normalized_text_for_lookup.replace(',', '').replace('-', ' ').replace('_', ' ')
            loinc_match = LAB_TEST_LOINC_MAPPING.get(cleaned_text.strip())
        if loinc_match and loinc_match.get("normalized_text"):
            return loinc_match["normalized_text"]
    
    return None
