#!/usr/bin/env python3
"""
Add meta_last_updated to child table transforms in HMU files

This script updates child table transform functions to include meta_last_updated
from the parent entity's meta.lastUpdated field.
"""

import re
import sys

# Files that have child tables and need fixing
files_to_fix = {
    'HMUPractitioner/HMUPractitioner.py': {
        'entity_id': 'practitioner_id',
        'transforms': ['transform_practitioner_names', 'transform_practitioner_telecoms', 'transform_practitioner_addresses']
    },
    'HMUDocumentReference/HMUDocumentReference.py': {
        'entity_id': 'document_reference_id',
        'transforms': ['transform_document_identifiers', 'transform_document_authors', 'transform_document_context']
    },
    'HMUCondition/HMUCondition.py': {
        'entity_id': 'condition_id',
        'transforms': [
            'transform_condition_categories',
            'transform_condition_codes',
            'transform_condition_body_sites',
            'transform_condition_stages',
            'transform_condition_evidence',
            'transform_condition_notes'
        ]
    },
    'HMUEncounter/HMUEncounter.py': {
        'entity_id': 'encounter_id',
        'transforms': [
            'transform_encounter_types',
            'transform_encounter_diagnoses',
            'transform_encounter_participants',
            'transform_encounter_locations'
        ]
    },
    'HMUMedicationDispense/HMUMedicationDispense.py': {
        'entity_id': 'medication_dispense_id',
        'transforms': [
            'transform_dispense_identifiers',
            'transform_dispense_performers',
            'transform_dispense_dosage_instructions'
        ]
    },
}

def add_meta_last_updated_to_transform(content, transform_name, entity_id):
    """Add meta_last_updated extraction to a transform function"""

    # Find the transform function
    pattern = rf'(def {transform_name}\(df\):.*?)(    {entity_id}_df = df\.select\(\s+F\.col\("id"\)\.alias\("{entity_id}"\),)'

    def replacement(match):
        func_start = match.group(1)
        select_start = match.group(2)

        # Add meta_last_updated extraction before the entity_id select
        return f'''{func_start}{select_start}
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),'''

    new_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    if new_content != content:
        print(f"  ✓ Added meta_last_updated to {transform_name}")
        return new_content, True

    # Try alternate pattern for direct select without intermediate DF
    pattern2 = rf'(def {transform_name}\(df\):.*?)(    \w+_final = \w+_df\.select\(\s+F\.col\("{entity_id}"\),)'

    new_content = re.sub(pattern2, lambda m: f'''{m.group(1)}{m.group(2)}
        F.col("meta_last_updated"),''', content, flags=re.DOTALL)

    if new_content != content:
        print(f"  ✓ Added meta_last_updated to {transform_name} final select")
        return new_content, True

    return content, False

def add_meta_to_flat_df(content, entity_id):
    """Add meta_last_updated to flat DataFrame selections"""

    # Pattern to find flat_df selections for child tables
    pattern = rf'(\w+_flat_df = \w+_df\.select\(\s+F\.col\("{entity_id}"\)\.cast\(StringType\(\)\)\.alias\("{entity_id}"\),)'

    def replacement(match):
        return f'''{match.group(1)}
            F.col("meta_last_updated").cast(TimestampType()).alias("meta_last_updated"),'''

    new_content = re.sub(pattern, replacement, content)

    if new_content != content:
        print(f"  ✓ Added meta_last_updated to flat_df selections")
        return new_content, True

    return content, False

def add_meta_to_resolve_choice(content):
    """Add meta_last_updated to resolveChoice specs"""

    # Pattern to find resolveChoice with specs that don't have meta_last_updated
    pattern = r'(\.resolveChoice\(\s+specs=\[\s+\("[^"]+", "cast:[^"]+"\),)(?!\s+\("meta_last_updated")'

    def replacement(match):
        return f'''{match.group(1)}
                ("meta_last_updated", "cast:timestamp"),'''

    new_content = re.sub(pattern, replacement, content)

    if new_content != content:
        count = len(re.findall(pattern, content))
        print(f"  ✓ Added meta_last_updated to {count} resolveChoice spec(s)")
        return new_content, True

    return content, False

def add_meta_to_create_table(content):
    """Add meta_last_updated to CREATE TABLE statements"""

    # Pattern to find CREATE TABLE with first column but no meta_last_updated
    pattern = r'(CREATE TABLE IF NOT EXISTS public\.\w+ \(\s+\w+ [^,]+,)(?!\s+meta_last_updated)'

    def replacement(match):
        return f'''{match.group(1)}
        meta_last_updated TIMESTAMP,'''

    new_content = re.sub(pattern, replacement, content)

    if new_content != content:
        count = len(re.findall(pattern, content))
        print(f"  ✓ Added meta_last_updated to {count} CREATE TABLE statement(s)")
        return new_content, True

    return content, False

print("Fixing child tables in HMU files...")
print("=" * 60)

for filepath, config in files_to_fix.items():
    print(f"\n📁 {filepath}")

    try:
        with open(filepath, 'r') as f:
            content = f.read()

        original_content = content
        changed = False

        # Fix each transform function
        for transform in config['transforms']:
            content, modified = add_meta_last_updated_to_transform(content, transform, config['entity_id'])
            if modified:
                changed = True

        # Fix flat_df selections
        content, modified = add_meta_to_flat_df(content, config['entity_id'])
        if modified:
            changed = True

        # Fix resolveChoice
        content, modified = add_meta_to_resolve_choice(content)
        if modified:
            changed = True

        # Fix CREATE TABLE
        content, modified = add_meta_to_create_table(content)
        if modified:
            changed = True

        if changed:
            with open(filepath, 'w') as f:
                f.write(content)
            print(f"  ✅ File updated successfully")
        else:
            print(f"  ⚠️  No changes made (may already be fixed or needs manual review)")

    except Exception as e:
        print(f"  ❌ Error: {e}")

print("\n" + "=" * 60)
print("✅ Done!")
