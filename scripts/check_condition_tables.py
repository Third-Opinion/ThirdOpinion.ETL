#!/usr/bin/env python3
"""
Check which condition tables exist in Redshift and their row counts
"""

import boto3
import time
import sys

# Redshift Data API client
client = boto3.client('redshift-data', region_name='us-east-1')

def execute_query(sql):
    """Execute a query and return results"""
    try:
        # Submit query
        response = client.execute_statement(
            WorkgroupName='to-prd-redshift-serverless',
            Database='dev',
            Sql=sql
        )

        query_id = response['Id']
        print(f"Query submitted: {query_id}")

        # Wait for query to complete
        while True:
            status_response = client.describe_statement(Id=query_id)
            status = status_response['Status']

            if status == 'FINISHED':
                break
            elif status in ['FAILED', 'ABORTED']:
                error = status_response.get('Error', 'Unknown error')
                print(f"Query failed: {error}")
                return None

            time.sleep(1)

        # Get results
        result = client.get_statement_result(Id=query_id)
        return result

    except Exception as e:
        print(f"Error executing query: {e}")
        return None

def main():
    print("=" * 80)
    print("Checking Condition Tables in Redshift")
    print("=" * 80)

    # List all condition tables
    print("\nStep 1: Listing all condition* tables...")
    tables_query = """
    SELECT table_name,
           pg_size_pretty(pg_total_relation_size('public.' || table_name)) as size
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name LIKE 'condition%'
    ORDER BY table_name;
    """

    result = execute_query(tables_query)
    if result:
        print("\nTables found:")
        for record in result['Records']:
            table_name = record[0].get('stringValue', 'N/A')
            size = record[1].get('stringValue', 'N/A')
            print(f"  - {table_name} ({size})")

    # Get row counts for each expected table
    expected_tables = [
        'conditions',
        'condition_categories',
        'condition_notes',
        'condition_body_sites',
        'condition_stages',
        'condition_codes',
        'condition_evidence',
        'condition_extensions'
    ]

    print("\nStep 2: Checking row counts...")
    for table in expected_tables:
        count_query = f"SELECT COUNT(*) FROM public.{table};"
        result = execute_query(count_query)

        if result and result['Records']:
            count = result['Records'][0][0].get('longValue', 0)
            print(f"  {table}: {count:,} rows")
        else:
            print(f"  {table}: Table does not exist or query failed")

    print("\n" + "=" * 80)
    print("Done!")
    print("=" * 80)

if __name__ == "__main__":
    main()
