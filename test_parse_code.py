#!/usr/bin/env python3
"""Test the parse_code_string function with actual sample data"""

import re

def parse_code_string(code_str):
    """Parse string representation of code object to extract coding array"""
    if code_str is None or code_str == '' or code_str == 'null':
        return None

    try:
        # Handle case where it's already a proper dict/struct
        if isinstance(code_str, dict):
            return code_str

        # Parse string representation
        if not isinstance(code_str, str):
            return None

        # Extract the text field first (if present)
        text_match = re.search(r'text=([^,}]+?)(?:,|$|})', code_str)
        text_value = text_match.group(1).strip() if text_match else None
        if text_value == 'null' or text_value == '_text=null':
            text_value = None

        # Extract all coding entries
        codings = []

        # First try to extract the coding array content
        # Look for coding=[ ... ] pattern
        coding_array_match = re.search(r'coding=\[(.*?)\](?:,\s*text=|$)', code_str, re.DOTALL)

        print(f"Text value: {text_value}")
        print(f"Coding array match: {coding_array_match is not None}")

        if coding_array_match:
            coding_array_str = coding_array_match.group(1)
            print(f"Coding array string: {coding_array_str[:200]}")

            # Split by '}, {' to separate individual coding objects
            coding_parts = re.split(r'\},\s*\{', coding_array_str)
            print(f"Number of coding parts: {len(coding_parts)}")

            for i, part in enumerate(coding_parts):
                # Clean up the part (remove leading/trailing braces if present)
                part = part.strip().strip('{}')
                print(f"  Part {i}: {part[:100]}")

                # Extract system, code, and display from each part
                system_match = re.search(r'system=([^,}]+)', part)
                code_match = re.search(r'code=([^,}]+)', part)
                display_match = re.search(r'display=([^,}]+)', part)

                system = system_match.group(1).strip() if system_match else None
                code = code_match.group(1).strip() if code_match else None
                display = display_match.group(1).strip() if display_match else None

                print(f"    System: {system}")
                print(f"    Code: {code}")
                print(f"    Display: {display}")

                # Clean up values and add to codings list
                if system and system != 'null' and code and code != 'null':
                    # Remove any quotes or extra characters
                    system = system.strip(',').strip()
                    code = code.strip(',').strip()
                    display = display.strip(',').strip() if display and display != 'null' else None

                    codings.append({
                        'system': system,
                        'code': code,
                        'display': display
                    })
        else:
            print("Coding array pattern not found, trying fallback")

        print(f"Total codings extracted: {len(codings)}")

        if codings:
            return {
                'text': text_value,
                'coding': codings
            }
        else:
            return None

    except Exception as e:
        print(f"Failed to parse code string: {str(e)}")
        return None


# Test with sample data from the CSV
sample_code = "{id=null, extension=null, coding=[{id=null, extension=null, system=http://snomed.info/sct, _system=null, version=null, _version=null, code=399068003, _code=null, display=Malignant neoplasm of prostate, _display=null, userSelected=null, _userSelected=null}, {id=null, extension=null, system=http://hl7.org/fhir/sid/icd-10-cm, _system=null, version=null, _version=null, code=C61, _code=null, display=Malignant neoplasm of prostate, _display=null, userSelected=null, _userSelected=null}], text=Malignant neoplasm of prostate, _text=null}"

print("=" * 80)
print("Testing parse_code_string with sample data")
print("=" * 80)
result = parse_code_string(sample_code)
print("\n" + "=" * 80)
print("Final result:")
print(result)
print("=" * 80)
