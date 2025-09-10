# FHIR Database Schema Analysis Summary

## Overview
This document provides a comprehensive analysis of the existing FHIR database schema structure to guide the implementation of the remaining 8 materialized views for the fact tables.

**Database Scale**: Production database contains **36.9M+ total records** across 50 active tables (22 tables with zero records excluded).

## Database Structure Analysis

### 1. Directory Structure
The `ddl_exports/` directory contains 68 DDL files representing a fully normalized FHIR database schema with the following pattern:
- **Main entity tables**: Core FHIR resource tables (e.g., `conditions.ddl`, `observations.ddl`)
- **Related tables**: Normalized child tables for complex fields (e.g., `condition_codes.ddl`, `observation_components.ddl`)

### 2. Core FHIR Entities Analysis

**Production Data Volume Summary:**
- **Total Main FHIR Entity Records**: 36,558,056
- **Total Related Table Records**: 110,397,542
- **Active Tables**: 50 (out of 72 total tables)

#### Main FHIR Entity Record Counts:
| Entity | Records | Implementation Priority |
|--------|---------|------------------------|
| **observations** | 14,852,844 | HIGH - Most complex, highest volume |
| **medication_requests** | 9,975,848 | HIGH - High volume, medium complexity |
| **document_references** | 4,494,496 | MEDIUM - High volume, medium complexity |
| **conditions** | 3,160,649 | HIGH - High volume, high complexity |
| **diagnostic_reports** | 1,772,356 | MEDIUM - Medium volume, high complexity |
| **encounters** | 1,144,809 | ✅ DONE - Already implemented |
| **procedures** | 610,881 | LOW - Low volume, low complexity |
| **patients** | 350,854 | ✅ DONE - Already implemented |
| **medication_dispenses** | 232,007 | LOW - Low volume, medium complexity |
| **practitioners** | 7,333 | LOW - Low volume, low complexity |
| **medications** | 6,801 | LOW - Low volume, low complexity |
| **care_plans** | 171 | LOW - Very low volume, medium complexity |

#### 2.1 Care Plans
**Main Table**: `care_plans` (171 records)
- **Primary Key**: `care_plan_id VARCHAR(255) NOT NULL`
- **Foreign Keys**: `patient_id VARCHAR(255) NOT NULL`
- **Related Tables**:
  - `care_plan_care_teams` - Team assignments (161 records)
  - `care_plan_categories` - Categorization codes (171 records)
  - `care_plan_goals` - Associated goals (181 records)
  - `care_plan_identifiers` - External identifiers (171 records)
- **Key Fields**: `status`, `intent`, `title`, standard FHIR metadata
- **Data Volume**: Very low volume entity suitable for comprehensive materialized view

#### 2.2 Conditions
**Main Table**: `conditions` (3,160,649 records)
- **Primary Key**: `condition_id VARCHAR(255) NOT NULL`
- **Foreign Keys**: `patient_id VARCHAR(255) NOT NULL`, `encounter_id VARCHAR(255)`
- **Related Tables**:
  - `condition_codes` - ICD-10/SNOMED codes (5,881,524 records)
  - `condition_categories` - Category classifications (3,160,758 records)
  - `condition_body_sites` - Anatomical locations (1,715 records)
  - `condition_evidence` - Supporting evidence (0 records - exclude)
  - `condition_extensions` - Custom extensions (8,812,114 records)
  - `condition_notes` - Clinical notes (428,457 records)
  - `condition_stages` - Disease staging (0 records - exclude)
- **Key Fields**: Clinical/verification status, severity, onset/abatement timing, extensive FHIR metadata
- **Data Volume**: High volume entity with complex related data requiring careful aggregation strategy

#### 2.3 Diagnostic Reports
**Main Table**: `diagnostic_reports` (1,772,356 records)
- **Primary Key**: `diagnostic_report_id VARCHAR(65535)`
- **Foreign Keys**: `patient_id VARCHAR(65535)`, `encounter_id VARCHAR(65535)`
- **Related Tables**:
  - `diagnostic_report_based_on` - Referenced requests (1,158,657 records)
  - `diagnostic_report_categories` - Report classifications (1,800,137 records)
  - `diagnostic_report_media` - Attached media (1,354,111 records)
  - `diagnostic_report_performers` - Report creators (1,568,826 records)
  - `diagnostic_report_presented_forms` - Document attachments (52,274 records)
  - `diagnostic_report_results` - Lab/test results (7,958,088 records)
- **Key Fields**: `status`, `effective_datetime`, `issued_datetime`, coded results
- **Note**: Uses VARCHAR(65535) for most fields, indicating complex data storage
- **Data Volume**: Medium-high volume with very large results table requiring performance optimization

#### 2.4 Document References
**Main Table**: `document_references` (4,494,496 records)
- **Primary Key**: `document_reference_id VARCHAR(255) NOT NULL`
- **Foreign Keys**: `patient_id VARCHAR(255) NOT NULL`
- **Related Tables**:
  - `document_reference_authors` - Document creators (2,810,336 records)
  - `document_reference_categories` - Document types (6,339,056 records)
  - `document_reference_content` - File attachments (1,752,579 records)
  - `document_reference_identifiers` - External references (4,494,496 records)
- **Key Fields**: `status`, `type_code/system/display`, `date`, `custodian_id`, context period
- **Data Volume**: High volume entity with substantial related data requiring efficient aggregation

#### 2.5 Medication Requests
**Main Table**: `medication_requests` (9,975,848 records)
- **Primary Key**: `medication_request_id VARCHAR(255) NOT NULL`
- **Foreign Keys**: `patient_id VARCHAR(255) NOT NULL`, `encounter_id VARCHAR(255)`, `medication_id VARCHAR(255)`
- **Related Tables**:
  - `medication_request_categories` - Medication classifications (963,051 records)
  - `medication_request_dosage_instructions` - Dosing information (5,259,138 records)
  - `medication_request_identifiers` - External references (9,975,848 records)
  - `medication_request_notes` - Clinical notes (9,279,557 records)
- **Key Fields**: `status`, `intent`, `reported_boolean`, `authored_on`, medication display info
- **Data Volume**: Highest volume entity after observations, requiring robust performance optimization

#### 2.6 Observations
**Main Table**: `observations` (14,852,844 records)
- **Primary Key**: `observation_id VARCHAR(65535)`
- **Foreign Keys**: `patient_id VARCHAR(65535)`, `encounter_id VARCHAR(65535)`, `specimen_id VARCHAR(65535)`
- **Related Tables**:
  - `observation_categories` - Observation classifications (15,810,798 records)
  - `observation_components` - Multi-component observations (1,673,439 records)
  - `observation_derived_from` - Source observations (0 records - exclude)
  - `observation_interpretations` - Clinical interpretations (4,886,101 records)
  - `observation_members` - Related observations (0 records - exclude)
  - `observation_notes` - Clinical notes (481,504 records)
  - `observation_performers` - Who performed the observation (263,002 records)
  - `observation_reference_ranges` - Normal/abnormal ranges (0 records - exclude)
- **Key Fields**: Extensive value types (string, quantity, codeable concept, datetime, boolean), effective timing, body site, method
- **Note**: Highest volume and most complex entity with 44 fields in main table and active related tables
- **Data Volume**: Massive scale requiring advanced optimization and potentially incremental refresh strategy

#### 2.7 Practitioners
**Main Table**: `practitioners` (7,333 records)
- **Primary Key**: `practitioner_id VARCHAR(255) NOT NULL`
- **Foreign Keys**: None (master data)
- **Related Tables**:
  - `practitioner_names` - Name information (7,332 records)
  - `practitioner_addresses` - Contact addresses (7,333 records)
  - `practitioner_telecoms` - Phone/email contacts (13,838 records)
- **Key Fields**: `active`, `resource_type`, basic FHIR metadata
- **Note**: Minimal main table, most data in related tables
- **Data Volume**: Low volume master data entity suitable for comprehensive materialized view

#### 2.8 Procedures
**Main Table**: `procedures` (610,881 records)
- **Primary Key**: `procedure_id VARCHAR(255) NOT NULL`
- **Foreign Keys**: `patient_id VARCHAR(255) NOT NULL`
- **Related Tables**:
  - `procedure_code_codings` - CPT/SNOMED procedure codes (754,067 records)
  - `procedure_identifiers` - External references (610,881 records)
- **Key Fields**: `status`, `code_text`, `performed_date_time`, basic metadata
- **Data Volume**: Medium volume entity with straightforward related data structure

### 3. Common Relationship Patterns

#### 3.1 Primary Relationships
- **Patient-Centric**: All entities except `practitioners` have `patient_id` foreign key
- **Encounter-Linked**: Most clinical entities optionally reference `encounter_id`
- **Practitioner References**: Various entities reference practitioners through performer/author fields

#### 3.2 Metadata Patterns
All tables include standard FHIR metadata:
- `meta_version_id VARCHAR(50/255/65535)`
- `meta_last_updated TIMESTAMP`
- `created_at TIMESTAMP`
- `updated_at TIMESTAMP`

Additional metadata in some tables:
- `meta_source` - Source system identifier
- `meta_security` - Security labels
- `meta_tag` - Resource tags
- `meta_profile` - Profile compliance

#### 3.3 Code System Patterns
Most entities use consistent coding patterns:
- `{field}_code` - The actual code value
- `{field}_system` - Code system URI (e.g., SNOMED, ICD-10)
- `{field}_display` - Human-readable description
- `{field}_text` - Free text description

### 4. Existing Materialized View Patterns

#### 4.1 Common Structure (from `fact_fhir_patients_view_v1` and `fact_fhir_encounters_view_v1`)
```sql
CREATE MATERIALIZED VIEW fact_fhir_{entity}_view_v1
BACKUP NO
AUTO REFRESH NO
AS SELECT ...
```

#### 4.2 JSON Aggregation Patterns
**Manual JSON Construction**:
```sql
JSON_PARSE(
    '{"field_name":{"subfield":"' || 
    COALESCE(REGEXP_REPLACE(data_field, pattern, replacement), '') || 
    '"}}'
)
```

**Array Construction**:
```sql
JSON_PARSE(
    '[' || LISTAGG(
        '{' ||
        '"field1":"' || COALESCE(REPLACE(field1, '"', '\\"'), '') || '",' ||
        '"field2":"' || COALESCE(REPLACE(field2, '"', '\\"'), '') || '"' ||
        '}',
        ','
    ) || ']'
) AS field_array
```

#### 4.3 String Cleaning Patterns
Extensive use of `REGEXP_REPLACE` for data sanitization:
- Remove quotes: `REGEXP_REPLACE(field, '"', '')`
- Remove backslashes: `REGEXP_REPLACE(field, '\\\\', '')`
- Clean whitespace: `REGEXP_REPLACE(field, '[\\r\\n\\t]', ' ')`
- Remove special chars: `REGEXP_REPLACE(field, '[^a-zA-Z0-9 .-]', '')`

#### 4.4 Name Ranking Logic
Sophisticated ranking for selecting primary names:
```sql
ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY 
    CASE WHEN name_use = 'official' THEN 1 
         WHEN name_use = 'usual' THEN 2 
         ELSE 3 END,
    family_name, given_names) as name_rank
```

### 5. Data Type Considerations

#### 5.1 Field Size Variations
- **Standard Fields**: `VARCHAR(255)` for IDs and standard fields
- **Short Fields**: `VARCHAR(50)` for codes and statuses
- **Large Fields**: `VARCHAR(500)` for titles and descriptions
- **Complex Data**: `VARCHAR(65535)` for extensions, metadata, and complex structures

#### 5.2 Numeric Fields
- **Quantities**: `NUMERIC(10,2)` for measurements and values
- **Integers**: For counts and ordinal values
- **Booleans**: For true/false flags

#### 5.3 Temporal Fields
- **Timestamps**: `TIMESTAMP WITHOUT TIME ZONE` for most datetime fields
- **Dates**: `DATE` for birth dates and simple date fields

### 6. Implementation Recommendations

#### 6.1 Naming Conventions
- View names: `fact_fhir_{entity}_view_v1`
- Follow existing patterns from patients and encounters views
- Use consistent field naming and JSON structure patterns

#### 6.2 JSON Aggregation Strategy
- Use manual JSON construction with `JSON_PARSE()` for complex nested structures
- Implement consistent string cleaning with `REGEXP_REPLACE`
- Use `LISTAGG()` for array construction
- Handle NULL values with `COALESCE()`

#### 6.3 Performance Considerations
- Use `LEFT JOIN` for optional related tables
- Implement `GROUP BY` for all main table fields when aggregating
- Consider `BACKUP NO` for cost optimization
- Use `AUTO REFRESH NO` for all views with scheduled refresh strategy

#### 6.4 Common Patterns to Implement

**Basic Entity Structure**:
```sql
-- Core entity fields
entity.entity_id,
entity.patient_id,
entity.status,
-- ... other main fields

-- Aggregated codes as JSON
JSON_PARSE('[' || LISTAGG(codes_json) || ']') AS codes,

-- Aggregated identifiers as JSON  
JSON_PARSE('[' || LISTAGG(identifiers_json) || ']') AS identifiers
```

**Standard Joins Pattern**:
```sql
FROM public.{main_table} e
    LEFT JOIN public.{related_table1} rt1 ON e.entity_id = rt1.entity_id
    LEFT JOIN public.{related_table2} rt2 ON e.entity_id = rt2.entity_id
WHERE e.entity_id IS NOT NULL
  AND e.status != 'entered-in-error' -- Common FHIR pattern
GROUP BY [all main table fields]
```

### 7. Entity-Specific Implementation Notes

#### 7.1 Critical Priority - High Volume + High Complexity
- **Observations**: 14.8M records with components (1.7M), interpretations (4.9M), categories (15.8M), notes (481K), performers (263K)
- **Conditions**: 3.2M records with codes (5.9M), extensions (8.8M), categories (3.2M), notes (428K)
- **Medication Requests**: 10M records with identifiers (10M), notes (9.3M), dosage instructions (5.3M)

#### 7.2 High Priority - High Volume + Medium Complexity  
- **Document References**: 4.5M records with categories (6.3M), authors (2.8M), identifiers (4.5M), content (1.8M)
- **Diagnostic Reports**: 1.8M records with results (8M), categories (1.8M), performers (1.6M), media (1.4M), based_on (1.2M)

#### 7.3 Medium Priority - Medium Volume
- **Procedures**: 611K records with code codings (754K), identifiers (611K)

#### 7.4 Low Priority - Low Volume
- **Practitioners**: 7,333 records with telecoms (13.8K), addresses (7.3K), names (7.3K)
- **Care Plans**: 171 records with goals (181), categories (171), identifiers (171), care teams (161)

#### 7.5 Implementation Strategy by Data Volume
**Phase 1 (High Volume - Require Performance Optimization):**
- Observations, Medication Requests, Document References, Conditions, Diagnostic Reports

**Phase 2 (Medium Volume - Standard Implementation):**  
- Procedures

**Phase 3 (Low Volume - Comprehensive Views):**
- Practitioners, Care Plans

### 8. Quality and Testing Considerations

#### 8.1 Data Quality Patterns
- Implement NULL handling for all optional fields
- Use string cleaning for user-generated content
- Validate JSON structure with proper escaping

#### 8.2 Testing Strategy
- Verify relationships through sample queries
- Test JSON parsing and query performance
- Validate aggregation accuracy against source tables
- Check for orphaned records and referential integrity

### 9. Production Database Performance Considerations

#### 9.1 High-Volume Entity Optimization Requirements
**Observations (14.8M records):**
- Consider partitioning by patient_id or effective_datetime
- May require incremental refresh strategy instead of full refresh
- Limit components and interpretations aggregation to recent data

**Medication Requests (10M records):**
- High 1:1 ratio with identifiers and notes suggests well-normalized structure
- Consider date-based filtering for materialized view refresh

**Document References (4.5M records) & Diagnostic Reports (1.8M records):**
- Large related tables (6.3M categories, 8M results) require efficient JOIN strategies
- Consider selective aggregation based on recent date ranges

#### 9.2 Related Table Optimization
**Tables with 0 records (exclude from views):**
- `condition_evidence`, `condition_stages`
- `observation_derived_from`, `observation_members`, `observation_reference_ranges`

**High-ratio related tables requiring aggregation limits:**
- `observation_categories` (15.8M) > `observations` (14.8M) - Multiple categories per observation
- `diagnostic_report_results` (8M) > `diagnostic_reports` (1.8M) - Average 4.5 results per report
- `condition_extensions` (8.8M) > `conditions` (3.2M) - Average 2.8 extensions per condition

#### 9.3 Materialized View Refresh Strategy Recommendations
**ALL VIEWS: AUTO REFRESH NO + Scheduled Refresh:**
- All materialized views use consistent scheduled refresh strategy
- External scheduling system (Airflow, cron, Lambda) controls refresh timing
- Ensures consistent refresh patterns across all FHIR entities
- Consider hourly or daily refresh based on business requirements
- Monitor refresh duration and adjust scheduling as needed

**No AUTO REFRESH YES:**
- Eliminated to ensure consistent refresh timing across all views
- Scheduled refresh provides better control and monitoring capabilities

## Conclusion

The production database contains **147M+ total records** across 50 active tables, with significant volume concentration in observations (14.8M) and medication requests (10M). The existing schema follows a consistent, well-normalized FHIR structure with comprehensive metadata tracking. 

Implementation should prioritize high-volume entities with performance optimization strategies, while low-volume entities can use comprehensive materialized views. The existing patterns for JSON aggregation and string cleaning should be consistently applied across all new views, with careful attention to query performance for the largest tables.