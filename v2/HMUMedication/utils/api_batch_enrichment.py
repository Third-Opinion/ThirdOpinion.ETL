"""
Batch API enrichment for medications

Handles batch processing of API enrichment requests with rate limiting,
error handling, and result caching.
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

# Import enrichment utilities
try:
    from utils.enrichment import enrich_medication_code, normalize_medication_name
except ImportError:
    logger.warning("Could not import enrichment utilities - batch enrichment will be disabled")
    enrich_medication_code = None
    normalize_medication_name = None


def test_network_connectivity() -> bool:
    """
    Test network connectivity to external APIs
    
    Returns:
        True if connectivity test passes, False otherwise
    """
    import socket
    try:
        import requests
    except ImportError:
        logger.warning("requests module not available for connectivity test")
        return False
    
    try:
        # Test DNS resolution
        try:
            ip_address = socket.gethostbyname('rxnav.nlm.nih.gov')
            logger.info(f"✅ DNS resolution successful for rxnav.nlm.nih.gov -> {ip_address}")
        except socket.gaierror as e:
            logger.warning(f"❌ DNS resolution failed: {e}")
            return False
        
        # Test TCP connection to port 443 (HTTPS)
        try:
            logger.info("🔍 Testing TCP connection to rxnav.nlm.nih.gov:443...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            result = sock.connect_ex((ip_address, 443))
            sock.close()
            
            if result == 0:
                logger.info("✅ TCP connection to port 443 successful")
            else:
                logger.warning(f"❌ TCP connection to port 443 failed (error code: {result})")
                logger.warning("   This indicates a security group or network ACL is blocking outbound HTTPS")
                logger.warning("   Check security group outbound rules for HTTPS (port 443)")
                return False
        except Exception as e:
            logger.warning(f"❌ TCP connection test failed: {type(e).__name__}: {e}")
            return False
        
        # Test HTTPS connectivity (full TLS handshake)
        try:
            logger.info("🔍 Testing HTTPS connectivity (TLS handshake)...")
            response = requests.get('https://rxnav.nlm.nih.gov', timeout=30, verify=True)
            if response.status_code in [200, 301, 302]:
                logger.info("✅ HTTPS connectivity test passed")
                return True
            else:
                logger.warning(f"⚠️  HTTPS connectivity test returned status {response.status_code}")
                return False
        except requests.exceptions.ConnectionError as e:
            error_msg = str(e)
            logger.warning(f"❌ HTTPS connectivity test failed: ConnectionError")
            logger.warning(f"   Error: {error_msg[:300]}")
            if "Network is unreachable" in error_msg or "Errno 101" in error_msg:
                logger.warning("")
                logger.warning("=" * 80)
                logger.warning("🔧 DIAGNOSIS: Network Connector Configuration Issue")
                logger.warning("=" * 80)
                logger.warning("DNS resolution works ✅, but HTTPS connection fails ❌")
                logger.warning("")
                logger.warning("This indicates:")
                logger.warning("1. ✅ DNS is working (can resolve hostnames)")
                logger.warning("2. ❌ Outbound HTTPS (port 443) is blocked")
                logger.warning("")
                logger.warning("SOLUTION:")
                logger.warning("1. Check Security Group attached to Network Connector:")
                logger.warning("   - Must have OUTBOUND rule: HTTPS (443) to 0.0.0.0/0")
                logger.warning("   - Must have OUTBOUND rule: All Traffic to 0.0.0.0/0 (if using that)")
                logger.warning("")
                logger.warning("2. Check Network ACLs:")
                logger.warning("   - Must allow OUTBOUND HTTPS (443) TCP")
                logger.warning("")
                logger.warning("3. Verify Network Connector:")
                logger.warning("   - Internet access is enabled")
                logger.warning("   - Security group is attached")
                logger.warning("   - Subnets have route to internet gateway")
                logger.warning("=" * 80)
            return False
        except Exception as e:
            logger.warning(f"❌ HTTPS connectivity test failed: {type(e).__name__}: {e}")
            return False
            
    except Exception as e:
        logger.warning(f"❌ Network connectivity test failed: {type(e).__name__}: {e}")
        return False


def enrich_medications_batch(
    medications: List[Dict[str, Any]],
    enrichment_mode: str = "hybrid",
    lookup_cache: Optional[Dict[str, Dict[str, Any]]] = None,
    max_batch_size: int = 1000,
    enable_enrichment: bool = True
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Enrich a batch of medications via API calls with rate limiting
    
    Args:
        medications: List of medication dictionaries with keys:
            - medication_name: Name of medication
            - normalized_name: Normalized name (optional, will be computed if missing)
            - existing_code: Existing code (optional)
            - existing_system: Existing system (optional)
        enrichment_mode: "rxnav_only", "comprehend_only", or "hybrid"
        lookup_cache: Optional dictionary cache to check before API calls
        max_batch_size: Maximum number of medications to process
        enable_enrichment: Whether enrichment is enabled
    
    Returns:
        Tuple of:
            - enrichment_results: List of enrichment result dictionaries
            - updated_lookup_cache: Updated lookup cache with new results
    """
    if not enable_enrichment:
        logger.info("Batch enrichment is disabled")
        return [], lookup_cache or {}
    
    if enrich_medication_code is None:
        logger.warning("Enrichment utilities not available - skipping batch enrichment")
        return [], lookup_cache or {}
    
    if not medications:
        return [], lookup_cache or {}
    
    # Test network connectivity once at the start
    if not hasattr(enrich_medications_batch, '_connectivity_tested'):
        logger.info("🔍 Testing network connectivity...")
        connectivity_ok = test_network_connectivity()
        enrich_medications_batch._connectivity_tested = True
        if not connectivity_ok:
            logger.warning("⚠️  Network connectivity test failed - API enrichment may not work")
            logger.warning("   Troubleshooting:")
            logger.warning("   1. Verify network connector is added to Glue job 'Connections'")
            logger.warning("   2. Check Security Groups allow outbound HTTPS (443) and DNS (53)")
            logger.warning("   3. Check Network ACLs allow outbound traffic")
            logger.warning("   4. Verify network connector has internet access enabled")
        else:
            logger.info("✅ Network connectivity test passed - proceeding with API enrichment")
    
    # Limit batch size
    medications_to_process = medications[:max_batch_size]
    if len(medications) > max_batch_size:
        logger.info(f"Limiting batch size to {max_batch_size} medications (requested {len(medications)})")
    
    logger.info(f"🚀 Starting batch API enrichment for {len(medications_to_process)} medications...")
    logger.info(f"   Enrichment mode: {enrichment_mode}")
    
    enrichment_results = []
    updated_cache = lookup_cache.copy() if lookup_cache else {}
    successful_enrichments = 0
    failed_enrichments = 0
    skipped_enrichments = 0
    
    for idx, med in enumerate(medications_to_process, 1):
        medication_name = med.get('medication_name')
        if not medication_name:
            skipped_enrichments += 1
            continue
        
        # Normalize medication name if not provided
        normalized_name = med.get('normalized_name')
        if not normalized_name and normalize_medication_name:
            normalized_name = normalize_medication_name(medication_name)
        
        # Skip if already in cache (shouldn't happen, but double-check)
        if normalized_name and updated_cache.get(normalized_name):
            skipped_enrichments += 1
            continue
        
        try:
            # Call enrichment API
            rxnorm_code, rxnorm_system, display_name, source, confidence = enrich_medication_code(
                medication_name=medication_name,
                existing_code=med.get('existing_code'),
                existing_system=med.get('existing_system'),
                enable_enrichment=True,
                enrichment_mode=enrichment_mode,
                lookup_cache=updated_cache  # Use updated cache to avoid redundant calls
            )
            
            if rxnorm_code:
                successful_enrichments += 1
                
                # Prepare enrichment result
                enrichment_result = {
                    'normalized_name': normalized_name or medication_name.upper().strip(),
                    'rxnorm_code': rxnorm_code,
                    'rxnorm_system': rxnorm_system or "http://www.nlm.nih.gov/research/umls/rxnorm",
                    'medication_name': display_name or medication_name,
                    'confidence_score': float(confidence) if confidence else 1.0,
                    'enrichment_source': source or 'unknown'
                }
                enrichment_results.append(enrichment_result)
                
                # Update cache for immediate use
                if normalized_name:
                    updated_cache[normalized_name] = {
                        'rxnorm_code': rxnorm_code,
                        'rxnorm_system': rxnorm_system,
                        'display_name': display_name,
                        'confidence': confidence
                    }
                
                # Log progress every 50 medications
                if successful_enrichments % 50 == 0:
                    logger.info(f"   Progress: {successful_enrichments}/{len(medications_to_process)} enriched ({idx} processed)")
            else:
                failed_enrichments += 1
                # Log failures every 100
                if failed_enrichments % 100 == 0:
                    logger.warning(f"   {failed_enrichments} medications failed enrichment so far")
                
        except Exception as e:
            failed_enrichments += 1
            error_msg = str(e)
            # Don't log network errors for every medication (too verbose)
            if "Network is unreachable" in error_msg or "ConnectionError" in str(type(e).__name__):
                # Only log first network error
                if not hasattr(enrich_medications_batch, '_network_error_logged'):
                    logger.warning(f"⚠️  Network connectivity issue detected: {error_msg[:200]}")
                    logger.warning("   API enrichment skipped due to network restrictions. "
                                  "Glue job requires VPC with NAT Gateway for external API access.")
                    enrich_medications_batch._network_error_logged = True
            else:
                logger.warning(f"Error enriching medication '{medication_name}': {error_msg[:200]}")
            # Continue processing other medications even if one fails
    
    logger.info(f"✅ Batch enrichment completed:")
    logger.info(f"   ✅ Successful: {successful_enrichments}")
    logger.info(f"   ❌ Failed: {failed_enrichments}")
    logger.info(f"   ⏭️  Skipped: {skipped_enrichments}")
    logger.info(f"   📊 Total processed: {len(medications_to_process)}")
    
    return enrichment_results, updated_cache


def prepare_medications_for_batch_enrichment(
    medication_rows: List[Any],
    normalize_func: Optional[Any] = None
) -> List[Dict[str, Any]]:
    """
    Prepare medication rows from Spark DataFrame for batch enrichment
    
    Args:
        medication_rows: List of Spark Row objects or dictionaries
        normalize_func: Optional normalization function
    
    Returns:
        List of medication dictionaries ready for batch enrichment
    """
    unique_medications = []
    seen_normalized = set()
    
    normalize = normalize_func or normalize_medication_name
    
    for row in medication_rows:
        # Handle Spark Row objects - they support bracket notation: row['column_name']
        try:
            # Spark Row objects support bracket notation directly (works like dict)
            medication_name = row['primary_text']
            existing_code = row['primary_code']
            existing_system = row['primary_system']
            
            # Filter out None/empty medication names
            if not medication_name:
                continue
        except (AttributeError, KeyError, TypeError) as e:
            logger.warning(f"Error accessing row fields: {type(e).__name__}: {e} - skipping row")
            continue
        
        # Normalize medication name
        normalized = normalize(medication_name) if normalize else medication_name.upper().strip()
        
        # Skip duplicates
        if normalized and normalized in seen_normalized:
            continue
        
        seen_normalized.add(normalized)
        
        unique_medications.append({
            'medication_name': medication_name,
            'normalized_name': normalized,
            'existing_code': existing_code,
            'existing_system': existing_system
        })
    
    return unique_medications

