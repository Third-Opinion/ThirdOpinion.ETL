#!/usr/bin/env python3
"""
Add deduplication logic to filter_dataframe_by_version functions in all HMU*.py files
"""

import re
import sys

files_to_fix = [
    'HMUAllergyIntolerance/HMUAllergyIntolerance.py',
    'HMUCarePlan/HMUCarePlan.py',
    'HMUCondition/HMUCondition.py',
    'HMUDiagnosticReport/HMUDiagnosticReport.py',
    'HMUDocumentReference/HMUDocumentReference.py',
    'HMUEncounter/HMUEncounter.py',
    'HMUMedication/HMUMedication.py',
    'HMUMedicationDispense/HMUMedicationDispense.py',
    'HMUMedicationRequest/HMUMedicationRequest.py',
    'HMUObservation/HMUObservation.py',
    'HMUPractitioner/HMUPractitioner.py',
    'HMUProcedure/HMUProcedure.py',
]

# The old pattern to find and replace
old_pattern = r'''(def filter_dataframe_by_version\(df, existing_versions, id_column\):
    """.*?"""
    logger\.info\("Filtering data based on version comparison\.\.\."\)

    )if not existing_versions:
        # No existing data, all records are new
        total_count = df\.count\(\)
        logger\.info\(f"No existing versions found - treating all \{total_count\} records as new"\)
        return df, total_count, 0

    # Add a column to mark records that need processing'''

# The new pattern with deduplication
new_pattern = r'''\1# Step 1: Deduplicate incoming data - keep only latest version per entity
    from pyspark.sql.window import Window

    window_spec = Window.partitionBy(id_column).orderBy(F.col("meta_last_updated").desc())
    df_latest = df.withColumn("row_num", F.row_number().over(window_spec)) \\
                  .filter(F.col("row_num") == 1) \\
                  .drop("row_num")

    incoming_count = df.count()
    deduplicated_count = df_latest.count()

    if incoming_count > deduplicated_count:
        logger.info(f"Deduplicated incoming data: {incoming_count} → {deduplicated_count} records (kept latest per entity)")

    if not existing_versions:
        # No existing data, all records are new
        logger.info(f"No existing versions found - treating all {deduplicated_count} records as new")
        return df_latest, deduplicated_count, 0

    # Step 2: Compare with existing versions
    # Add a column to mark records that need processing'''

for filepath in files_to_fix:
    print(f"Processing {filepath}...")

    try:
        with open(filepath, 'r') as f:
            content = f.read()

        # Check if already has deduplication
        if 'Window.partitionBy(id_column).orderBy(F.col("meta_last_updated")' in content:
            print(f"  ✓ Already has deduplication logic, skipping")
            continue

        # Apply the replacement
        new_content, count = re.subn(old_pattern, new_pattern, content, flags=re.DOTALL)

        if count > 0:
            # Also need to update the df references to df_latest
            new_content = new_content.replace('df_with_flag = df.withColumn(', 'df_with_flag = df_latest.withColumn(')
            new_content = new_content.replace('total_count = df.count()', 'total_count = df_latest.count()')  # Update other df references if any

            with open(filepath, 'w') as f:
                f.write(new_content)
            print(f"  ✓ Added deduplication logic ({count} replacement(s))")
        else:
            print(f"  ✗ Pattern not found - may need manual update")

    except Exception as e:
        print(f"  ✗ Error: {e}")

print("\nDone!")
