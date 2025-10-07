#!/usr/bin/env python3
"""
Fix SQL syntax issues in views - specifically missing commas before section breaks
"""

import os
import re
import sys

def fix_sql_file(filepath):
    """Fix missing commas in SQL file"""
    with open(filepath, 'r') as f:
        content = f.read()

    original_content = content

    # Pattern to find lines that should have a trailing comma but don't
    # Look for: field_name  -- comment\n followed by section divider
    pattern = r'(\s+\w+[\w\.\(\)]*)\s*(--[^\n]*)\n(\s*-- =+)'

    def check_and_add_comma(match):
        field_part = match.group(1)
        comment_part = match.group(2)
        divider = match.group(3)

        # If the field doesn't end with a comma, add one
        if not field_part.rstrip().endswith(','):
            return f"{field_part}," + comment_part + '\n' + divider
        else:
            return match.group(0)

    # Apply the fix
    content = re.sub(pattern, check_and_add_comma, content)

    # Also fix pattern where there's an empty line between field and divider
    pattern2 = r'(\s+\w+[\w\.\(\)]*)\s*(--[^\n,]*)\n\n(\s*-- =+)'

    def check_and_add_comma2(match):
        field_part = match.group(1)
        comment_part = match.group(2)
        divider = match.group(3)

        # If the field doesn't end with a comma, add one
        if not field_part.rstrip().endswith(','):
            return f"{field_part}," + comment_part + '\n\n' + divider
        else:
            return match.group(0)

    content = re.sub(pattern2, check_and_add_comma2, content)

    if content != original_content:
        print(f"Fixed: {os.path.basename(filepath)}")
        with open(filepath, 'w') as f:
            f.write(content)
        return True
    return False

def main():
    views_dir = '/Users/ken/code/ThirdOpinion/ThirdOpinion.ETL/views'
    fixed_count = 0

    for filename in sorted(os.listdir(views_dir)):
        if filename.endswith('.sql'):
            filepath = os.path.join(views_dir, filename)
            if fix_sql_file(filepath):
                fixed_count += 1

    print(f"\nTotal files fixed: {fixed_count}")

if __name__ == "__main__":
    main()