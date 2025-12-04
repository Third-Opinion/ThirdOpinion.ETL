"""
Medication code enrichment utility for medications

Enriches missing RxNorm codes using:
1. Redshift lookup table (cached results)
2. RxNav REST API (free, 20 req/sec limit)
3. Amazon Comprehend Medical (paid, better accuracy)

Following Medication_Code_Enrichment_Strategy.md
"""
import logging
import re
import time
from typing import Optional, Tuple, Dict, Any
from urllib.parse import quote
from collections import deque
from threading import Lock

logger = logging.getLogger(__name__)

# Optional imports for API integrations
try:
    import requests
except ImportError:
    logger.warning("requests library not available - RxNav API enrichment will be disabled")
    requests = None

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    logger.warning("boto3 not available - Comprehend Medical enrichment will be disabled")
    boto3 = None


def normalize_medication_name(name: Optional[str]) -> Optional[str]:
    """Normalize medication name for lookup"""
    if not name:
        return None
    normalized = name.strip().upper()
    # Remove common prefixes/suffixes
    normalized = re.sub(r'\b(COMPOUNDED|COMPOUND)\b', '', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\(SR\)|SR\b', '', normalized)
    # Remove extra whitespace
    normalized = " ".join(normalized.split())
    return normalized


# Rate limiter for RxNav API (20 requests per second)
class RxNavRateLimiter:
    """Thread-safe rate limiter for RxNav API (20 req/sec)"""
    def __init__(self, max_requests: int = 20, time_window: float = 1.0):
        self.max_requests = max_requests
        self.time_window = time_window
        self.request_times = deque()
        self.lock = Lock()
    
    def wait_if_needed(self):
        """Wait if we've exceeded the rate limit"""
        with self.lock:
            now = time.time()
            # Remove requests older than time window
            while self.request_times and self.request_times[0] < now - self.time_window:
                self.request_times.popleft()
            
            # If we're at the limit, wait
            if len(self.request_times) >= self.max_requests:
                sleep_time = self.time_window - (now - self.request_times[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    # Clean up again after sleep
                    now = time.time()
                    while self.request_times and self.request_times[0] < now - self.time_window:
                        self.request_times.popleft()
            
            # Record this request
            self.request_times.append(time.time())


# Global rate limiter instance (created when needed)
_rxnav_rate_limiter = None

def get_rxnav_rate_limiter():
    """Get or create the global RxNav rate limiter"""
    global _rxnav_rate_limiter
    if _rxnav_rate_limiter is None and requests:
        _rxnav_rate_limiter = RxNavRateLimiter(max_requests=20, time_window=1.0)
    return _rxnav_rate_limiter


def lookup_rxnorm_code(medication_name: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Lookup RxNorm code using RxNav API with rate limiting
    
    Returns: (rxnorm_code, rxnorm_system, display_name)
    """
    if not medication_name or not requests:
        return None, None, None
    
    # Enforce rate limit (20 requests per second)
    rate_limiter = get_rxnav_rate_limiter()
    if rate_limiter:
        rate_limiter.wait_if_needed()
    
    normalized = normalize_medication_name(medication_name)
    base_url = "https://rxnav.nlm.nih.gov/REST"
    
    try:
        # Try exact match first
        url = f"{base_url}/drugs.json?name={quote(normalized)}"
        # Increased timeout for network connector scenarios
        response = requests.get(url, timeout=30, verify=True)
        
        if response.status_code == 200:
            data = response.json()
            if 'drugGroup' in data and 'conceptGroup' in data['drugGroup']:
                concepts = data['drugGroup']['conceptGroup']
                for group in concepts:
                    if 'conceptProperties' in group:
                        # Get first RxNorm code
                        concept = group['conceptProperties'][0]
                        rxcui = concept.get('rxcui')
                        if rxcui:
                            return rxcui, "http://www.nlm.nih.gov/research/umls/rxnorm", concept.get('name', medication_name)
        
        # Wait before second API call (rate limiting)
        if rate_limiter:
            rate_limiter.wait_if_needed()
        
        # Fall back to approximate match
        url = f"{base_url}/approximateTerm.json?term={quote(normalized)}&maxEntries=1"
        # Increased timeout for network connector scenarios
        response = requests.get(url, timeout=30, verify=True)
        
        if response.status_code == 200:
            data = response.json()
            if 'approximateGroup' in data and 'candidate' in data['approximateGroup']:
                candidates = data['approximateGroup']['candidate']
                if candidates and len(candidates) > 0:
                    candidate = candidates[0]
                    rxcui = candidate.get('rxcui')
                    # Handle score as string or int
                    score = candidate.get('score', 0)
                    try:
                        score = int(score) if isinstance(score, str) else (score or 0)
                    except (ValueError, TypeError):
                        score = 0
                    if rxcui and score >= 80:  # Confidence threshold
                        return rxcui, "http://www.nlm.nih.gov/research/umls/rxnorm", candidate.get('name', medication_name)
        
    except requests.exceptions.HTTPError as e:
        if e.response and e.response.status_code == 429:  # Too Many Requests
            logger.warning(f"Rate limit exceeded for {medication_name}, waiting...")
            time.sleep(1)
        else:
            logger.warning(f"HTTP error looking up {medication_name}: {str(e)}")
    except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout, requests.exceptions.Timeout) as e:
        # Network connectivity issue - provide detailed diagnostics
        error_msg = str(e)
        error_type = type(e).__name__
        
        # Log once per job run to avoid log spam
        if not hasattr(lookup_rxnorm_code, '_network_error_logged'):
            logger.warning("=" * 80)
            logger.warning("⚠️  NETWORK CONNECTIVITY ISSUE - RxNav API")
            logger.warning("=" * 80)
            logger.warning(f"Error Type: {error_type}")
            logger.warning(f"Error Message: {error_msg[:500]}")
            logger.warning("")
            logger.warning("Troubleshooting Steps:")
            logger.warning("1. Verify network connector is added to Glue job configuration:")
            logger.warning("   - Check 'Connections' section in Glue job")
            logger.warning("   - Network connector should be listed alongside Redshift connection")
            logger.warning("2. Check Security Groups:")
            logger.warning("   - Security group must allow outbound HTTPS (port 443)")
            logger.warning("   - Security group must allow outbound DNS (port 53)")
            logger.warning("3. Check Network ACLs:")
            logger.warning("   - Network ACLs must allow outbound HTTPS and DNS")
            logger.warning("4. Test DNS Resolution:")
            logger.warning("   - Verify DNS resolution works: nslookup rxnav.nlm.nih.gov")
            logger.warning("5. Network Connector Configuration:")
            logger.warning("   - Ensure network connector has internet access enabled")
            logger.warning("   - Verify network connector is in the same VPC as Glue job")
            logger.warning("=" * 80)
            lookup_rxnorm_code._network_error_logged = True
    except Exception as e:
        logger.warning(f"Error looking up {medication_name}: {str(e)}")
    
    return None, None, None


def enrich_with_comprehend_medical(medication_name: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[float]]:
    """
    Enrich medication using Amazon Comprehend Medical
    
    Returns: (rxnorm_code, rxnorm_system, display_name, confidence)
    """
    if not medication_name or not boto3:
        return None, None, None, None
    
    try:
        comprehend_medical = boto3.client('comprehendmedical')
        response = comprehend_medical.detect_entities_v2(Text=medication_name)
        
        # Find medication entities
        for entity in response.get('Entities', []):
            if entity.get('Type') == 'MEDICATION':
                # Get RxNorm concepts
                rxnorm_concepts = entity.get('RxNormConcepts', [])
                if rxnorm_concepts:
                    # Get highest confidence concept
                    best_concept = max(rxnorm_concepts, key=lambda x: x.get('Score', 0))
                    rxnorm_code = best_concept.get('Code')
                    confidence = best_concept.get('Score', 0)
                    
                    if rxnorm_code and confidence >= 0.7:  # Confidence threshold
                        return (
                            rxnorm_code,
                            "http://www.nlm.nih.gov/research/umls/rxnorm",
                            entity.get('Text', medication_name),
                            confidence
                        )
    except ClientError as e:
        logger.warning(f"Comprehend Medical error for {medication_name}: {str(e)}")
    except Exception as e:
        logger.warning(f"Unexpected error with Comprehend Medical: {str(e)}")
    
    return None, None, None, None


def enrich_medication_code(
    medication_name: Optional[str],
    existing_code: Optional[str] = None,
    existing_system: Optional[str] = None,
    enable_enrichment: bool = True,
    enrichment_mode: str = "hybrid",
    lookup_cache: Optional[Dict[str, Dict[str, Any]]] = None
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[float]]:
    """
    Enrich medication code when missing
    
    Args:
        medication_name: Medication name (primary_text) to enrich
        existing_code: Existing primary_code (if any)
        existing_system: Existing primary_system (if any)
        enable_enrichment: Feature flag to enable/disable enrichment
        enrichment_mode: "rxnav_only", "comprehend_only", or "hybrid"
        lookup_cache: Optional dictionary cache for lookup results (normalized_name -> enrichment_result)
    
    Returns: Tuple of (rxnorm_code, rxnorm_system, display_name, source, confidence)
        - rxnorm_code: RxNorm code (e.g., "314076")
        - rxnorm_system: Coding system URI (e.g., "http://www.nlm.nih.gov/research/umls/rxnorm")
        - display_name: Display name for the medication
        - source: Enrichment source ("rxnav", "comprehend_medical", "lookup_cache", or None)
        - confidence: Confidence score (0.0-1.0) or None
    """
    # If enrichment is disabled, return existing or None
    if not enable_enrichment:
        return existing_code, existing_system, medication_name, None, None
    
    # If already has RxNorm code, no need to enrich
    if existing_code and existing_system == "http://www.nlm.nih.gov/research/umls/rxnorm":
        return existing_code, existing_system, medication_name, None, None
    
    if not medication_name:
        return None, None, None, None, None
    
    normalized = normalize_medication_name(medication_name)
    
    # Tier 1: Check lookup cache if provided
    if lookup_cache and normalized:
        cached_result = lookup_cache.get(normalized)
        if cached_result:
            return (
                cached_result.get('rxnorm_code'),
                cached_result.get('rxnorm_system'),
                cached_result.get('display_name', medication_name),
                'lookup_cache',
                cached_result.get('confidence', 1.0)
            )
    
    # Tier 2: Try RxNav API (free)
    if enrichment_mode in ["rxnav_only", "hybrid"]:
        rxnorm_code, rxnorm_system, display_name = lookup_rxnorm_code(medication_name)
        if rxnorm_code:
            return rxnorm_code, rxnorm_system, display_name, 'rxnav', 0.9
    
    # Tier 3: Try Comprehend Medical (paid, better for complex cases)
    if enrichment_mode in ["comprehend_only", "hybrid"]:
        rxnorm_code, rxnorm_system, display_name, confidence = enrich_with_comprehend_medical(medication_name)
        if rxnorm_code and confidence and confidence >= 0.7:
            return rxnorm_code, rxnorm_system, display_name, 'comprehend_medical', confidence
    
    # No match found
    return None, None, None, None, None


def get_lookup_table_schema() -> str:
    """Get the SQL schema for medication code lookup table"""
    return """
    CREATE TABLE IF NOT EXISTS public.medication_code_lookup (
        normalized_name VARCHAR(500) NOT NULL PRIMARY KEY,
        rxnorm_code VARCHAR(50),
        rxnorm_system VARCHAR(200) DEFAULT 'http://www.nlm.nih.gov/research/umls/rxnorm',
        medication_name VARCHAR(500),
        confidence_score DECIMAL(3,2),
        enrichment_source VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    DISTSTYLE KEY
    DISTKEY (normalized_name)
    SORTKEY (normalized_name);
    """


