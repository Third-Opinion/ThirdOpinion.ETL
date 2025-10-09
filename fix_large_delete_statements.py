#!/usr/bin/env python3
"""Fix large DELETE statement issue in all Glue job files"""

import glob

old_pattern = '''        # Step 4: Build preactions for selective deletion
        selective_preactions = preactions
        if entities_to_delete:
            # Create DELETE statements for specific entity IDs
            entity_ids_str = "', '".join(entities_to_delete)
            delete_clause = f"DELETE FROM public.{table_name} WHERE {id_column} IN ('{entity_ids_str}');"

            if selective_preactions:
                selective_preactions = delete_clause + " " + selective_preactions
            else:
                selective_preactions = delete_clause

            logger.info(f"Will delete {len(entities_to_delete)} existing entities before inserting updated versions")'''

new_pattern = '''        # Step 4: Build preactions for selective deletion
        selective_preactions = preactions
        if entities_to_delete:
            # For large deletions, use a different strategy to avoid "Statement is too large" error
            # Redshift has a 16MB limit on SQL statements
            num_entities = len(entities_to_delete)

            # If we have too many entities (>10000), just truncate the table and re-insert everything
            # This is more efficient than trying to batch large DELETE statements
            if num_entities > 10000:
                logger.info(f"Large deletion ({num_entities} entities) - will truncate table and re-insert all data")
                delete_clause = f"TRUNCATE TABLE public.{table_name};"
                # Convert ALL incoming data back to dynamic frame for full re-insert
                filtered_df = df  # Use all data, not just filtered
                to_process_count = total_records
            else:
                # For smaller deletions, use IN clause
                entity_ids_str = "', '".join(entities_to_delete)
                delete_clause = f"DELETE FROM public.{table_name} WHERE {id_column} IN ('{entity_ids_str}');"

            if selective_preactions:
                selective_preactions = delete_clause + " " + selective_preactions
            else:
                selective_preactions = delete_clause

            logger.info(f"Will delete {num_entities} existing entities before inserting updated versions")'''

files = glob.glob("HMU*/HMU*.py")

for file_path in files:
    if file_path == "HMUCondition/HMUCondition.py":
        print(f"✓ Skipping {file_path} (already fixed)")
        continue

    with open(file_path, 'r') as f:
        content = f.read()

    if old_pattern in content:
        content = content.replace(old_pattern, new_pattern)
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"✓ Fixed {file_path}")
    else:
        print(f"✗ Pattern not found in {file_path}")

print("\nDone!")
