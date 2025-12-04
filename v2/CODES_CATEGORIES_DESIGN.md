# Codes and Categories Tables Design Decision

## Current Legacy Pattern

### Codes Tables (All Denormalized)
- `observation_codes` - code_code, code_system, code_display, code_text
- `condition_codes` - code_code, code_system, code_display, code_text  
- `procedure_code_codings` - code_code, code_system, code_display

### Categories Tables (All Denormalized)
- `observation_categories` - category_code, category_system, category_display, category_text
- `condition_categories` - category_code, category_system, category_display, category_text
- `medication_request_categories` - category_code, category_system, category_display, category_text
- `diagnostic_report_categories` - category_code, category_system, category_display
- `care_plan_categories` - category_code, category_system, category_display, category_text
- `document_reference_categories` - category_code, category_system, category_display

## Key Observations

1. **Same Structure**: All codes/categories tables have identical column structures
2. **Code Overlap**: Same codes (LOINC, SNOMED, ICD-10) appear across multiple resources
3. **Category Overlap**: Same categories (e.g., "laboratory", "vital-signs") appear in observations, diagnostic reports, etc.
4. **Storage Duplication**: Same code/category data is duplicated across resource-specific tables

## Design Options

### Option A: Shared Tables (Recommended)
**Single `codes` and `categories` tables for ALL resources**

**Structure:**
- `codes` table - stores ALL codes (observations, conditions, procedures, etc.)
- `categories` table - stores ALL categories (observations, conditions, medications, etc.)
- `{resource}_codes` tables - reference codes via `code_id`
- `{resource}_categories` tables - reference categories via `category_id`

**Pros:**
- ✅ Maximum storage efficiency (codes/categories stored once)
- ✅ Consistent normalization pattern across all resources
- ✅ Easier to query "all codes" or "all categories" across resources
- ✅ Single source of truth for code/category definitions
- ✅ Matches current observation normalization pattern

**Cons:**
- ⚠️ Slightly more complex joins (need to join codes/categories tables)
- ⚠️ All resources must use same code_id generation logic

**Example:**
```sql
-- Shared codes table
codes (code_id, code_code, code_system, code_display, code_text)

-- Resource-specific reference tables
observation_codes (observation_id, code_id, code_rank)
condition_codes (condition_id, code_id, code_rank)
procedure_codes (procedure_id, code_id, code_rank)
```

### Option B: Resource-Specific Normalized Tables
**Separate normalized tables per resource**

**Structure:**
- `observation_codes` table - references `observation_codes_lookup` table
- `condition_codes` table - references `condition_codes_lookup` table
- etc.

**Pros:**
- ✅ Resource isolation (easier to understand per-resource)
- ✅ Can have resource-specific code fields if needed

**Cons:**
- ❌ Code duplication across resources (same LOINC code in multiple lookup tables)
- ❌ More tables to maintain
- ❌ Harder to query codes across resources
- ❌ Less storage efficient

### Option C: Hybrid Approach
**Shared codes, resource-specific categories**

**Rationale:**
- Codes are universal (LOINC, SNOMED, ICD-10) → shared
- Categories might be resource-specific → separate

**Pros:**
- ✅ Codes normalized (most storage savings)
- ✅ Categories can be resource-specific if needed

**Cons:**
- ⚠️ Inconsistent pattern (codes shared, categories separate)
- ⚠️ Categories still duplicated if they overlap

## Recommendation: **Option A - Shared Tables**

### Rationale

1. **Code Universality**: Codes (LOINC, SNOMED, ICD-10, etc.) are universal standards that appear across all resources. A LOINC code like "12345-6" means the same thing whether it's in an observation, condition, or procedure.

2. **Category Overlap**: Many categories overlap:
   - "laboratory" appears in observations and diagnostic reports
   - "vital-signs" appears in observations
   - "problem-list-item" appears in conditions
   - But many are shared across resources

3. **Storage Efficiency**: Based on observation normalization:
   - Codes: 21GB → 100MB (99.5% reduction)
   - Categories: 1.5GB → 1KB (99.99% reduction)
   - Same savings would apply to conditions, procedures, etc.

4. **Consistency**: Matches the pattern we've already established for observations

5. **Query Flexibility**: Can easily query:
   - "All codes used across all resources"
   - "All observations and conditions with LOINC code X"
   - "All resources in category Y"

### Implementation

**Keep current table names:**
- `codes` - shared codes table (already created)
- `categories` - shared categories table (already created)
- `observation_codes` - references codes (already normalized)
- `condition_codes` - will reference codes (future)
- `procedure_codes` - will reference codes (future)
- etc.

**No need to rename** - the `codes` and `categories` tables are already generic enough to serve all resources.

## Migration Path

1. ✅ **Observations** - Already normalized (uses `codes` and `categories` tables)
2. **Conditions** - Migrate `condition_codes` and `condition_categories` to use shared tables
3. **Procedures** - Migrate `procedure_code_codings` to use shared `codes` table
4. **Medications** - Migrate medication categories to use shared `categories` table
5. **Diagnostic Reports** - Migrate categories to use shared `categories` table
6. **Care Plans** - Migrate categories to use shared `categories` table
7. **Document References** - Migrate categories to use shared `categories` table

## Conclusion

**Use the same `codes` and `categories` tables for all resources.** They are already named generically and can serve as the shared normalization layer for all FHIR resources.

