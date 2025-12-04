# Stage and Severity Normalization Recommendation

## Question
Should we normalize `stage` and `severity` fields to use the `codes` table (similar to how we normalized condition codes, categories, and body_sites)?

## Current State

### Severity
- **Location**: Main `conditions` table (denormalized)
- **Fields**: `severity_code`, `severity_system`, `severity_display` (3 VARCHAR columns)
- **Relationship**: 1:1 with condition (each condition has 0 or 1 severity)
- **Cardinality**: Very low (~3-5 unique values: "mild", "moderate", "severe", "moderate-severe", etc.)

### Stage
- **Location**: Separate `condition_stages` table (denormalized)
- **Fields**: 
  - `stage_summary_code/system/display` (3 VARCHAR)
  - `stage_type_code/system/display` (3 VARCHAR)
  - `stage_assessment_code/system/display` (3 VARCHAR)
  - Total: 9 VARCHAR columns per stage
- **Relationship**: 1:many with condition (each condition can have 0 or more stages)
- **Cardinality**: Low-medium (~10-20 unique stage summaries, ~5-10 types, variable assessments)

## Analysis

### Normalization Benefits

**What we normalized and why:**

1. **Codes** (condition_codes, observation_codes):
   - **Storage savings**: ~21GB → ~100MB (95% reduction)
   - **Reason**: 
     - High cardinality (thousands of unique codes)
     - Large display values
     - Many-to-many relationship
     - Shared across observations AND conditions

2. **Categories** (condition_categories, observation_categories):
   - **Storage savings**: ~500MB → ~50MB (90% reduction)
   - **Reason**:
     - Low cardinality but high repetition
     - Many-to-many relationship
     - Shared across observations AND conditions

3. **Body Sites** (condition_body_sites, observation_body_sites):
   - **Storage savings**: ~1.65MB → ~170KB (88% reduction)
   - **Reason**:
     - Very low cardinality (3 unique values in data)
     - Shared across observations AND conditions

### Severity Normalization Analysis

**Current Storage:**
- Per condition: 3 VARCHAR columns (~310 bytes)
- For 1M conditions: ~310MB total

**If Normalized:**
- `severity_codes` lookup table: ~5 rows × ~310 bytes = ~1.5KB
- `condition_severities` junction: condition_id (255) + code_id (8) = ~263 bytes per condition
- For 1M conditions: ~263MB + 1.5KB = ~263MB total
- **Savings**: ~47MB (15% reduction)

**Trade-offs:**
- ❌ **Complexity**: Would need new `condition_severities` junction table (1:1 relationship is awkward)
- ❌ **Query complexity**: Requires JOIN for simple severity lookup
- ❌ **Minimal benefit**: Only ~15% storage reduction
- ❌ **Not shared**: Severity is condition-specific, not used by observations
- ✅ **Consistency**: Would match normalization pattern

**Recommendation: ❌ DON'T NORMALIZE SEVERITY**
- Storage savings too small to justify complexity
- 1:1 relationship doesn't benefit from junction table
- Simpler to keep in main table for query performance

### Stage Normalization Analysis

**Current Storage (per stage):**
- 9 VARCHAR columns (~900 bytes per stage)
- If 100K stages: ~90MB total

**If Normalized:**
- Stage summary codes: ~20 unique × ~310 bytes = ~6KB
- Stage type codes: ~10 unique × ~310 bytes = ~3KB
- Stage assessment codes: Variable (could be references or codes)
- Junction table: condition_id (255) + 3 code_ids (24) + meta_last_updated (8) = ~287 bytes per stage
- For 100K stages: ~28.7MB + 9KB = ~28.7MB total
- **Savings**: ~61MB (68% reduction)

**Trade-offs:**
- ✅ **Good savings**: ~68% storage reduction
- ✅ **Consistency**: Would match normalization pattern
- ❌ **Complexity**: Need 3 joins (summary, type, assessment) or 3 separate junction tables
- ❌ **Query complexity**: More complex queries to get full stage info
- ❌ **Assessment handling**: Assessment can be Reference (not a code), complicating normalization
- ✅ **Shared potential**: Stage codes might be shared if used elsewhere

**Recommendation: ⚠️ MAYBE NORMALIZE STAGE** (with caveats)

**If we normalize stage, we have two approaches:**

#### Approach 1: Normalize all three fields
- Create junction tables: `condition_stage_summaries`, `condition_stage_types`, `condition_stage_assessments`
- Each references `codes` table
- **Issue**: Assessment might be a Reference (Observation/Procedure), not a code

#### Approach 2: Normalize only summary and type
- Keep assessment as-is (can be Reference or CodeableConcept)
- Normalize summary and type to codes table
- **Benefit**: Most storage savings, less complexity

## Final Recommendation

### ✅ **Severity: DON'T NORMALIZE**
**Reasoning:**
1. Minimal storage savings (~15%)
2. 1:1 relationship doesn't benefit from normalization
3. Query simplicity more valuable
4. Not shared with other resources

### ⚠️ **Stage: CONDITIONAL NORMALIZATION**

**Recommended Approach: Hybrid Normalization**
1. **Normalize `stage_summary`** to codes table ✅
   - High repetition, clear benefit
   - Create `condition_stage_summaries` junction table

2. **Normalize `stage_type`** to codes table ✅
   - Lower repetition but consistent pattern
   - Create `condition_stage_types` junction table

3. **Keep `stage_assessment` denormalized** ❌
   - Can be Reference (not always a code)
   - Mixed types complicate normalization
   - Store as-is in `condition_stages` table

**Alternative: Full Normalization (if assessment is always CodeableConcept)**
- If all assessments are codes (not references), normalize all three
- Creates consistency but adds complexity

## Implementation Impact

### If we normalize stage (hybrid approach):

**New Tables:**
```sql
-- Already exists
CREATE TABLE codes (...);

-- New junction tables
CREATE TABLE condition_stage_summaries (
    condition_id VARCHAR(255),
    stage_code_id BIGINT,  -- Reference to codes.code_id
    stage_rank INTEGER
);

CREATE TABLE condition_stage_types (
    condition_id VARCHAR(255),
    type_code_id BIGINT,  -- Reference to codes.code_id
    stage_rank INTEGER
);

-- Modified table
CREATE TABLE condition_stages (
    condition_id VARCHAR(255),
    meta_last_updated TIMESTAMP,
    stage_assessment_code VARCHAR(50),  -- Keep as-is (can be Reference)
    stage_assessment_system VARCHAR(255),
    stage_assessment_display VARCHAR(255),
    -- summary and type moved to junction tables
);
```

**Transformation Changes:**
- Extract stage summaries → codes table → junction table
- Extract stage types → codes table → junction table
- Keep assessment extraction as-is

## Comparison with Observation Pattern

**Observation normalized:**
- ✅ Codes (high cardinality, shared)
- ✅ Categories (low cardinality, shared)
- ✅ Body sites (very low cardinality, shared)

**Observation NOT normalized:**
- Clinical status (stored in main table)
- Verification status (stored in main table)
- Interpretation (normalized to interpretations table, not codes)

**Key Difference:**
- Observation uses separate `interpretations` table (not `codes`)
- Condition severity/stage are similar to interpretation - could have dedicated tables OR use codes

## Decision Matrix

| Field | Cardinality | Repetition | Shared | Current Location | Normalize? | Reason |
|-------|-------------|------------|--------|------------------|------------|--------|
| **Severity** | Very Low (3-5) | High | No | Main table | ❌ No | 1:1, minimal savings |
| **Stage Summary** | Low (10-20) | Medium | Maybe | Child table | ✅ Yes | Good savings, consistent |
| **Stage Type** | Very Low (5-10) | Medium | Maybe | Child table | ✅ Yes | Consistent pattern |
| **Stage Assessment** | Variable | Low | No | Child table | ❌ No | Mixed types (Reference/code) |

## Recommendation Summary

1. **Severity**: ❌ **Keep denormalized** in main `conditions` table
   - Simple, performant, minimal storage impact

2. **Stage Summary & Type**: ✅ **Normalize** to `codes` table
   - Good storage savings
   - Consistent with existing pattern
   - Clear benefit

3. **Stage Assessment**: ❌ **Keep denormalized** 
   - Can be Reference (not always a code)
   - Mixed types complicate normalization
   - Store in `condition_stages` table as-is

This provides the best balance of storage savings, consistency, and simplicity.

