"""
Test script for medication enrichment API

Tests the enrichment API with real data from Redshift
"""
import sys
import logging
from typing import Dict, Any

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Test data from Redshift
TEST_MEDICATIONS = [
    {
        'medication_name': 'OXANDROLONE 25MG',
        'normalized_name': 'OXANDROLONE 25MG',
        'existing_code': None,
        'existing_system': None
    },
    {
        'medication_name': 'DECA 200mg/ML',
        'normalized_name': 'DECA 200MG/ML',
        'existing_code': None,
        'existing_system': None
    },
    {
        'medication_name': 'TADALAFIL 12MG CAPSULE',
        'normalized_name': 'TADALAFIL 12MG CAPSULE',
        'existing_code': None,
        'existing_system': None
    },
    {
        'medication_name': 'Sildenafil 110 compounded capsules',
        'normalized_name': 'SILDENAFIL 110 COMPOUNDED CAPSULES',
        'existing_code': None,
        'existing_system': None
    },
    {
        'medication_name': 'SEMAGLUTIDE 5MG/2ML',
        'normalized_name': 'SEMAGLUTIDE 5MG/2ML',
        'existing_code': None,
        'existing_system': None
    }
]


def test_enrichment_api():
    """Test the enrichment API with real medication data"""
    logger.info("=" * 80)
    logger.info("🧪 TESTING MEDICATION ENRICHMENT API")
    logger.info("=" * 80)
    
    try:
        # Import enrichment utilities
        from utils.enrichment import enrich_medication_code, normalize_medication_name
        from utils.api_batch_enrichment import enrich_medications_batch
        
        logger.info(f"Testing with {len(TEST_MEDICATIONS)} medications")
        logger.info("")
        
        # Test 1: Normalize medication names
        logger.info("Test 1: Normalizing medication names...")
        for med in TEST_MEDICATIONS[:3]:
            normalized = normalize_medication_name(med['medication_name'])
            logger.info(f"  '{med['medication_name']}' -> '{normalized}'")
        logger.info("")
        
        # Test 2: Single medication enrichment
        logger.info("Test 2: Single medication enrichment...")
        test_med = TEST_MEDICATIONS[0]
        rxnorm_code, rxnorm_system, display_name, source, confidence = enrich_medication_code(
            medication_name=test_med['medication_name'],
            existing_code=None,
            existing_system=None,
            enable_enrichment=True,
            enrichment_mode="hybrid",
            lookup_cache={}
        )
        logger.info(f"  Medication: {test_med['medication_name']}")
        logger.info(f"  RxNorm Code: {rxnorm_code}")
        logger.info(f"  System: {rxnorm_system}")
        logger.info(f"  Display: {display_name}")
        logger.info(f"  Source: {source}")
        logger.info(f"  Confidence: {confidence}")
        logger.info("")
        
        # Test 3: Batch enrichment (small batch)
        logger.info("Test 3: Batch enrichment (3 medications)...")
        batch_results, updated_cache = enrich_medications_batch(
            medications=TEST_MEDICATIONS[:3],
            enrichment_mode="hybrid",
            lookup_cache={},
            max_batch_size=10,
            enable_enrichment=True
        )
        logger.info(f"  Batch enriched: {len(batch_results)} medications")
        for result in batch_results:
            logger.info(f"    - {result['medication_name']}: {result['rxnorm_code']} ({result['enrichment_source']})")
        logger.info("")
        
        logger.info("=" * 80)
        logger.info("✅ ALL TESTS COMPLETED")
        logger.info("=" * 80)
        
    except ImportError as e:
        logger.error(f"❌ Import error: {e}")
        logger.error("Make sure you're running from the v2/HMUMedication directory")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    test_enrichment_api()



