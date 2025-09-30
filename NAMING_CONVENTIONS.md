# Glue Job Field Naming Conventions

Based on HMUPatient.py template, all Glue jobs should follow these naming conventions:

## General Rules
1. **Use snake_case** for all field aliases and new fields (not camelCase)
2. **Lowercase all field names** in aliases
3. **Use underscores to separate words** in multi-word fields

## Standard Field Transformations

### ID Fields
- `id` → `{resource}_id` (e.g., `patient_id`, `practitioner_id`, `observation_id`)
- Reference IDs: `{resource}_id` format (e.g., `organization_id`, `practitioner_id`)

### Date/Time Fields
- `birthDate` → `birth_date`
- `deceasedDateTime` → `deceased_date`
- `period.start` → `period_start`
- `period.end` → `period_end`
- Timestamps: Use `_at` suffix for timestamps (e.g., `created_at`, `updated_at`)
- Dates: Use `_date` suffix for date fields

### Boolean Fields
- Keep simple names without prefixes (e.g., `active`, `deceased`, `preferred`)
- Use present tense

### Metadata Fields
- `meta.versionId` → `meta_version_id`
- `meta.lastUpdated` → `meta_last_updated`
- `meta.source` → `meta_source`
- `meta.security` → `meta_security`
- `meta.tag` → `meta_tag`

### CodeableConcept Fields Pattern
For FHIR CodeableConcept fields, use this pattern:
- `{field}_code` - for the code value
- `{field}_display` - for the display text
- `{field}_system` - for the system URL

Example:
- `maritalStatus` → `marital_status_code`, `marital_status_display`, `marital_status_system`
- `language` → `language_code`, `language_display`, `language_system`

### Nested/Complex Fields
- Flatten nested structures with underscore separation
- `name.family` → `family_name`
- `name.given` → `given_names` (plural when array)
- `telecom.system` → `telecom_system`
- `telecom.value` → `telecom_value`
- `address.line` → `address_line`
- `address.postalCode` → `postal_code`

### Contact/Related Person Fields
- Prefix with context: `contact_name_family`, `contact_telecom_value`

### JSON/Complex Data
- Store as JSON strings for complex nested data
- Field name should indicate if it contains JSON (e.g., `extensions`, `given_names`)

## Resource Type Field
- Always include `resourcetype` field with the FHIR resource type

## Standard Audit Fields
Every main table should include:
- `created_at` - Timestamp when record was created
- `updated_at` - Timestamp when record was last updated

## Consistency Requirements
1. Same field across different resources must use identical naming
2. Similar transformations must follow the same pattern
3. Reference fields must be consistent (always `{resource}_id`)
4. Date handling must be uniform across all jobs

## Examples to Follow
✅ Correct:
- `patient_id`, `birth_date`, `marital_status_code`
- `telecom_system`, `period_start`, `created_at`

❌ Incorrect:
- `patientId`, `birthDate`, `maritalStatusCode`
- `telecomSystem`, `periodStart`, `createdAt`