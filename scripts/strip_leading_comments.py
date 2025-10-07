#!/usr/bin/env python3
"""
Create versions of SQL views without leading comments for deployment
"""

import os
import re

def strip_leading_comments(content):
    """Remove all comments before CREATE VIEW statement"""
    lines = content.split('\n')
    create_index = -1

    for i, line in enumerate(lines):
        if line.strip().startswith('CREATE VIEW'):
            create_index = i
            break

    if create_index == -1:
        return content  # No CREATE VIEW found, return as-is

    # Return from CREATE VIEW onward
    return '\n'.join(lines[create_index:])

def main():
    views_dir = '/Users/ken/code/ThirdOpinion/ThirdOpinion.ETL/views'

    # Create a deployment directory
    deploy_dir = '/Users/ken/code/ThirdOpinion/ThirdOpinion.ETL/views_deploy'
    os.makedirs(deploy_dir, exist_ok=True)

    for filename in sorted(os.listdir(views_dir)):
        if filename.endswith('.sql') and not filename.startswith('test_'):
            input_path = os.path.join(views_dir, filename)
            output_path = os.path.join(deploy_dir, filename)

            with open(input_path, 'r') as f:
                content = f.read()

            stripped = strip_leading_comments(content)

            with open(output_path, 'w') as f:
                f.write(stripped)

            print(f"Created deployment version: {filename}")

    print(f"\nDeployment versions created in: {deploy_dir}")

if __name__ == "__main__":
    main()