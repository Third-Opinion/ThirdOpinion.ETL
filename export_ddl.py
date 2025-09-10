#!/usr/bin/env python3

import subprocess
import json
import sys
import os

def run_aws_command(cmd):
    """Run AWS CLI command and return JSON result"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {cmd}")
        print(f"Error: {e.stderr}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON decode error for command: {cmd}")
        print(f"Output: {result.stdout}")
        return None

def get_table_list():
    """Get list of tables using MCP"""
    print("📋 Getting list of tables...")
    
    # Use the MCP to get tables
    cmd = '''python3 -c "
import sys
sys.path.append('.')
from mcp_awslabs_redshift_mcp_server import list_tables
result = list_tables('prod-redshift-main-ue2', 'dev', 'public')
print(json.dumps(result))
"'''
    
    # For now, use the hardcoded list we know works
    tables = [
        "care_plan_care_teams", "care_plan_categories", "care_plan_goals", "care_plan_identifiers", "care_plans",
        "condition_body_sites", "condition_categories", "condition_codes", "condition_evidence", "condition_extensions", 
        "condition_notes", "condition_stages", "conditions",
        "diagnostic_report_based_on", "diagnostic_report_categories", "diagnostic_report_media", 
        "diagnostic_report_performers", "diagnostic_report_presented_forms", "diagnostic_report_results", "diagnostic_reports",
        "document_reference_authors", "document_reference_categories", "document_reference_content", 
        "document_reference_identifiers", "document_references",
        "encounter_hospitalization", "encounter_identifiers", "encounter_locations", "encounter_participants", 
        "encounter_reasons", "encounter_types", "encounters",
        "medication_dispense_auth_prescriptions", "medication_dispense_dosage_instructions", "medication_dispense_identifiers", 
        "medication_dispense_performers", "medication_dispenses",
        "medication_identifiers", "medication_request_categories", "medication_request_dosage_instructions", 
        "medication_request_identifiers", "medication_request_notes", "medication_requests", "medications",
        "observation_categories", "observation_components", "observation_derived_from", "observation_interpretations", 
        "observation_members", "observation_notes", "observation_performers", "observation_reference_ranges", "observations",
        "patient_addresses", "patient_communications", "patient_contacts", "patient_links", "patient_names", 
        "patient_practitioners", "patient_telecoms", "patients",
        "practitioner_addresses", "practitioner_names", "practitioner_telecoms", "practitioners",
        "procedure_code_codings", "procedure_identifiers", "procedures"
    ]
    
    print(f"📊 Found {len(tables)} tables to export")
    return tables

def get_table_columns(cluster_id, database, schema, table):
    """Get column information for a table using MCP"""
    query = f"""
    SELECT 
        a.attname as column_name,
        pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,
        a.attnotnull as not_null,
        a.attnum as ordinal_position
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid
    WHERE n.nspname = '{schema}'
      AND c.relname = '{table}'
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum;
    """
    
    # Execute query using AWS CLI
    cmd = f'''aws redshift-data execute-statement --cluster-identifier {cluster_id} --database {database} --sql "{query}" --profile to-prd-admin --query "Id" --output text'''
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error executing query: {result.stderr}")
        return None
    
    query_id = result.stdout.strip()
    
    # Wait for completion
    while True:
        status_cmd = f"aws redshift-data describe-statement --id {query_id} --profile to-prd-admin --query 'Status' --output text"
        status_result = subprocess.run(status_cmd, shell=True, capture_output=True, text=True)
        if status_result.returncode != 0:
            print(f"Error checking status: {status_result.stderr}")
            return None
        
        status = status_result.stdout.strip()
        if status == "FINISHED":
            break
        elif status in ["FAILED", "ABORTED"]:
            print(f"Query failed with status: {status}")
            return None
        else:
            import time
            time.sleep(1)
    
    # Get results
    result_cmd = f"aws redshift-data get-statement-result --id {query_id} --profile to-prd-admin"
    result = subprocess.run(result_cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error getting results: {result.stderr}")
        return None
    
    try:
        data = json.loads(result.stdout)
        columns = []
        for row in data.get('Records', []):
            col_name = row[0]['stringValue']
            data_type = row[1]['stringValue']
            not_null = row[2]['booleanValue']
            ordinal = row[3]['longValue']
            columns.append((col_name, data_type, not_null, ordinal))
        return columns
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        print(f"Error parsing results: {e}")
        return None

def generate_ddl(schema, table, columns):
    """Generate DDL for a table"""
    ddl = f"CREATE TABLE {schema}.{table} (\n"
    
    column_defs = []
    for col_name, data_type, not_null, ordinal in columns:
        not_null_str = " NOT NULL" if not_null else ""
        column_defs.append(f"    {col_name} {data_type}{not_null_str}")
    
    ddl += ",\n".join(column_defs)
    ddl += "\n);"
    
    return ddl

def main():
    # Configuration
    cluster_id = "prod-redshift-main-ue2"
    database = "dev"
    schema = "public"
    output_dir = "ddl_exports"
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    print("🚀 Starting DDL export for all tables in dev database...")
    print(f"📁 Output directory: {output_dir}")
    print("")
    
    # Get table list
    tables = get_table_list()
    
    # Export DDL for each table
    count = 0
    total = len(tables)
    
    for table in tables:
        count += 1
        print(f"[{count}/{total}] Exporting DDL for: {table}")
        
        # Get column information
        columns = get_table_columns(cluster_id, database, schema, table)
        
        if columns:
            # Generate DDL
            ddl = generate_ddl(schema, table, columns)
            
            # Write to file
            output_file = os.path.join(output_dir, f"{table}.ddl")
            with open(output_file, 'w') as f:
                f.write(ddl)
            
            print(f"✅ Exported: {output_file}")
        else:
            print(f"❌ Failed to get columns for: {table}")
    
    print("")
    print("🎉 DDL export completed!")
    print(f"📁 All DDL files saved to: {output_dir}/")
    print(f"📊 Total tables processed: {total}")

if __name__ == "__main__":
    main()
