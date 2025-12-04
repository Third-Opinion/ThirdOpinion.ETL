"""
Local script to enrich medications via API calls

This script runs locally (with internet access) to:
1. Read medications from Redshift that need enrichment
2. Call RxNav/Comprehend Medical APIs to enrich them
3. Write enrichment results back to the lookup table in Redshift

Run this script separately from your local machine, then run the Glue ETL job
to use the enriched lookup table.

Usage:
    python enrich_medications_locally.py [--limit 1000] [--mode hybrid]
"""
import sys
import os
import argparse
import logging
from typing import List, Dict, Any
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    logger.error("boto3 not installed. Install with: pip install boto3")
    sys.exit(1)

try:
    # Import enrichment utilities
    from utils.enrichment import enrich_medication_code, normalize_medication_name
    from utils.api_batch_enrichment import enrich_medications_batch
except ImportError as e:
    logger.error(f"Could not import enrichment utilities: {e}")
    logger.error("Make sure you're running from the v2/HMUMedication directory")
    sys.exit(1)


def get_redshift_config():
    """
    Get Redshift configuration - supports both cluster and serverless
    
    Returns:
        dict with configuration
    """
    # Cluster-based connection (using Secrets Manager) - default for this environment
    cluster_id = os.getenv('REDSHIFT_CLUSTER_ID', 'prod-redshift-main-ue2')
    database = os.getenv('REDSHIFT_DATABASE', 'dev')
    secret_arn = os.getenv('REDSHIFT_SECRET_ARN', 'arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4')
    region = os.getenv('AWS_REGION', 'us-east-2')
    
    # Check if cluster connection should be used (default)
    if cluster_id and secret_arn:
        return {
            'type': 'cluster',
            'cluster_id': cluster_id,
            'database': database,
            'secret_arn': secret_arn,
            'region': region
        }
    
    # Fall back to serverless (if cluster not configured)
    workgroup = os.getenv('REDSHIFT_WORKGROUP')
    if workgroup:
        return {
            'type': 'serverless',
            'workgroup': workgroup,
            'database': database,
            'region': region
        }
    
    logger.error("Redshift configuration not found!")
    logger.info("Set environment variables:")
    logger.info("  - REDSHIFT_CLUSTER_ID and REDSHIFT_SECRET_ARN (for cluster) - DEFAULT")
    logger.info("  - OR REDSHIFT_WORKGROUP (for serverless)")
    return None


def execute_redshift_query(sql: str):
    """
    Execute a query using Redshift Data API (supports both cluster and serverless)
    
    Returns:
        List of dictionaries (rows)
    """
    config = get_redshift_config()
    if not config:
        return []
    
    import time
    client = boto3.client('redshift-data', region_name=config['region'])
    
    try:
        # Prepare execute_statement parameters based on connection type
        execute_params = {
            'Database': config['database'],
            'Sql': sql
        }
        
        if config['type'] == 'cluster':
            execute_params['ClusterIdentifier'] = config['cluster_id']
            execute_params['SecretArn'] = config['secret_arn']
        else:
            execute_params['WorkgroupName'] = config['workgroup']
        
        response = client.execute_statement(**execute_params)
        query_id = response['Id']
        
        # Wait for completion
        while True:
            status = client.describe_statement(Id=query_id)
            if status['Status'] == 'FINISHED':
                break
            elif status['Status'] in ['FAILED', 'ABORTED']:
                error = status.get('Error', 'Unknown error')
                logger.error(f"Query failed: {error}")
                return []
            time.sleep(1)
        
        # Get results
        result = client.get_statement_result(Id=query_id)
        
        # Convert to list of dicts
        columns = [col['name'] for col in result['ColumnMetadata']]
        rows = []
        for record in result['Records']:
            row = {}
            for i, col in enumerate(columns):
                value = record[i]
                if value:
                    # Extract the actual value from the dict structure
                    row[col] = list(value.values())[0] if isinstance(value, dict) else value
                else:
                    row[col] = None
            rows.append(row)
        
        return rows
        
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []


def get_medications_from_redshift(limit: int = 1000):
    """
    Read medications from Redshift that need enrichment
    
    Args:
        limit: Maximum number of unique medications to retrieve
    
    Returns:
        List of medication dictionaries
    """
    config = get_redshift_config()
    if not config:
        return []
    
    logger.info(f"Connecting to Redshift ({config['type']})...")
    
    # Query medications that need enrichment
    query = f"""
    SELECT DISTINCT
        primary_text as medication_name,
        primary_code,
        primary_system
    FROM public.medications
    WHERE (primary_code IS NULL OR primary_system IS NULL 
           OR primary_system != 'http://www.nlm.nih.gov/research/umls/rxnorm')
      AND primary_text IS NOT NULL
      AND primary_text != ''
    LIMIT {limit}
    """
    
    rows = execute_redshift_query(query)
    
    medications = []
    seen_normalized = set()
    
    for row in rows:
        medication_name = row.get('medication_name')
        if not medication_name:
            continue
        
        normalized = normalize_medication_name(medication_name)
        if normalized and normalized not in seen_normalized:
            seen_normalized.add(normalized)
            medications.append({
                'medication_name': medication_name,
                'normalized_name': normalized,
                'existing_code': row.get('primary_code'),
                'existing_system': row.get('primary_system')
            })
    
    logger.info(f"Retrieved {len(medications)} unique medications from Redshift")
    return medications


def write_enrichment_results_to_redshift(enrichment_results: List[Dict[str, Any]]):
    """
    Write enrichment results to Redshift lookup table
    
    Args:
        enrichment_results: List of enrichment result dictionaries
    """
    if not enrichment_results:
        logger.info("No enrichment results to write")
        return
    
    config = get_redshift_config()
    logger.info(f"Writing {len(enrichment_results)} enrichment results to Redshift lookup table...")
    
    # Delete existing entries first (UPSERT behavior)
    normalized_names = [r['normalized_name'].replace("'", "''") for r in enrichment_results]
    
    # Split into chunks to avoid SQL statement size limits
    chunk_size = 100
    total_written = 0
    
    import time
    client = boto3.client('redshift-data', region_name=config['region'])
    
    for i in range(0, len(enrichment_results), chunk_size):
        chunk = enrichment_results[i:i+chunk_size]
        chunk_names = normalized_names[i:i+chunk_size]
        
        # Build delete and insert SQL
        names_str = "', '".join(chunk_names)
        delete_sql = f"DELETE FROM public.medication_code_lookup WHERE normalized_name IN ('{names_str}');"
        
        # Build insert SQL with proper escaping
        values = []
        for result in chunk:
            normalized = result['normalized_name'].replace("'", "''")
            rxnorm_code = result.get('rxnorm_code', '').replace("'", "''") if result.get('rxnorm_code') else 'NULL'
            rxnorm_system = (result.get('rxnorm_system') or 'http://www.nlm.nih.gov/research/umls/rxnorm').replace("'", "''")
            medication_name = (result.get('medication_name') or '').replace("'", "''")
            confidence = float(result.get('confidence_score', 1.0))
            source = (result.get('enrichment_source', 'unknown') or 'unknown').replace("'", "''")
            
            # Handle NULL values properly
            rxnorm_code_val = f"'{rxnorm_code}'" if rxnorm_code != 'NULL' else 'NULL'
            values.append(f"('{normalized}', {rxnorm_code_val}, '{rxnorm_system}', '{medication_name}', {confidence}, '{source}')")
        
        values_str = ",\n    ".join(values)
        insert_sql = f"""
        INSERT INTO public.medication_code_lookup 
        (normalized_name, rxnorm_code, rxnorm_system, medication_name, confidence_score, enrichment_source)
        VALUES
            {values_str};
        """
        
        # Execute combined SQL
        combined_sql = delete_sql + "\n" + insert_sql
        
        try:
            # Prepare execute_statement parameters based on connection type
            execute_params = {
                'Database': config['database'],
                'Sql': combined_sql
            }
            
            if config['type'] == 'cluster':
                execute_params['ClusterIdentifier'] = config['cluster_id']
                execute_params['SecretArn'] = config['secret_arn']
            else:
                execute_params['WorkgroupName'] = config['workgroup']
            
            response = client.execute_statement(**execute_params)
            query_id = response['Id']
            
            # Wait for completion
            while True:
                status = client.describe_statement(Id=query_id)
                if status['Status'] == 'FINISHED':
                    total_written += len(chunk)
                    break
                elif status['Status'] in ['FAILED', 'ABORTED']:
                    error = status.get('Error', 'Unknown error')
                    logger.error(f"Query failed: {error}")
                    logger.debug(f"Failed SQL: {combined_sql[:500]}...")
                    return
                time.sleep(1)
        except Exception as e:
            logger.error(f"Error writing chunk to Redshift: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return
    
    logger.info(f"✅ Successfully wrote {total_written} enrichment results to lookup table")


def main():
    """Main enrichment process"""
    parser = argparse.ArgumentParser(description='Enrich medications via API calls')
    parser.add_argument('--limit', type=int, default=1000,
                       help='Maximum number of medications to enrich (default: 1000)')
    parser.add_argument('--mode', type=str, default='hybrid',
                       choices=['hybrid', 'rxnav_only', 'comprehend_only'],
                       help='Enrichment mode (default: hybrid)')
    
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("🚀 MEDICATION ENRICHMENT - LOCAL API PROCESSING")
    logger.info("=" * 80)
    logger.info(f"Enrichment mode: {args.mode}")
    logger.info(f"Limit: {args.limit} medications")
    logger.info("")
    
    start_time = datetime.now()
    
    # Step 1: Read medications from Redshift
    logger.info("📥 STEP 1: READING MEDICATIONS FROM REDSHIFT")
    medications = get_medications_from_redshift(limit=args.limit)
    
    if not medications:
        logger.warning("No medications found that need enrichment")
        return
    
    logger.info(f"Found {len(medications)} unique medications to enrich")
    logger.info("")
    
    # Step 2: Enrich via API
    logger.info("🔍 STEP 2: ENRICHING MEDICATIONS VIA API")
    enrichment_results, _ = enrich_medications_batch(
        medications=medications,
        enrichment_mode=args.mode,
        lookup_cache={},  # Start fresh - will check Redshift lookup table separately
        max_batch_size=args.limit,
        enable_enrichment=True
    )
    
    logger.info(f"✅ Enriched {len(enrichment_results)} medications")
    logger.info("")
    
    # Step 3: Write results to Redshift
    if enrichment_results:
        logger.info("💾 STEP 3: WRITING RESULTS TO REDSHIFT")
        write_enrichment_results_to_redshift(enrichment_results)
    
    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info("")
    logger.info("=" * 80)
    logger.info("🎉 ENRICHMENT COMPLETED")
    logger.info("=" * 80)
    logger.info(f"⏱️  Duration: {duration}")
    logger.info(f"📊 Medications enriched: {len(enrichment_results)}/{len(medications)}")
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. Run the Glue ETL job to use the enriched lookup table")
    logger.info("  2. Re-run this script to enrich more medications")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()

