# Body Sites Normalization Analysis

## Current State

### Data Analysis (from Conditions)

**Body Sites Statistics:**
- **Total rows**: 3,471 `condition_body_sites` records
- **Unique body sites**: **Only 3** (SNOMED codes: 419161000, 419465000, 51440002)
- **Average repetitions**: ~1,157 per body site
- **Storage (normalized)**: ~170KB (junction table) + ~1.5KB (body_sites table) = **~171.5KB**
- **Storage (denormalized)**: ~1.65MB
- **Storage savings**: ~1.5MB (88% reduction)

### Current Implementation

**Conditions (Normalized):**
- ✅ Uses `body_sites` lookup table (3 rows)
- ✅ Uses `condition_body_sites` junction table (3,471 rows)
- ✅ Follows same pattern as codes, categories

**Observations (Denormalized):**
- ❌ Body sites stored directly in `observations` table
- ❌ Fields currently set to `None` (not extracted from source)
- ❌ No `observation_body_sites` junction table

---

## Should We Normalize Body Sites?

### Arguments FOR Normalization

1. **Consistency**: Matches pattern used for codes, categories, interpretations
2. **Storage Efficiency**: 88% reduction (though absolute savings are small: ~1.5MB)
3. **Data Integrity**: Single source of truth for body site definitions
4. **Future-Proof**: If body sites grow, normalization will scale better

### Arguments AGAINST Normalization

1. **Very Low Cardinality**: Only 3 unique body sites
2. **Small Storage Savings**: ~1.5MB saved is negligible at scale
3. **Added Complexity**: 
   - Junction table management
   - ID generation and reuse
   - Additional joins for queries
   - More ETL code to maintain
4. **Observations Don't Use It**: Body sites not currently extracted from observations source data
5. **Over-Engineering**: Normalization overhead may exceed benefits for 3 values

---

## Recommendation: **NORMALIZE Observations** (Updated Decision)

### Why Normalize for Consistency?

**For Body Sites Specifically:**

1. **Consistency is Valuable**: Maintaining the same pattern across all code-based fields simplifies maintenance, queries, and understanding
2. **Pattern Alignment**: Matches the pattern used for codes, categories, interpretations
3. **Future-Proof**: If body sites grow, normalization will scale better
4. **Infrastructure Already Exists**: The `body_sites` table and pattern are already established for conditions
5. **Low Overhead**: The normalization overhead is minimal since the infrastructure is already in place

**Trade-offs:**
- ✅ Consistent pattern across all code-based fields
- ✅ Easier to maintain (one pattern to understand)
- ⚠️ Small storage savings (1.5MB) but consistency is more valuable
- ⚠️ Additional joins, but Redshift handles this efficiently

### Implementation Plan

**Option 1: Denormalize Conditions (RECOMMENDED)**

1. **Update `conditions` table DDL:**
   - Add columns: `body_site_code`, `body_site_system`, `body_site_display`, `body_site_text`
   - Remove `condition_body_sites` table
   - Keep `body_sites` table for reference (optional, or remove if not used elsewhere)

2. **Update Conditions ETL:**
   - Modify `transform_condition_body_sites()` to return single row per condition (take first body site, or aggregate)
   - Join body site data directly into main `conditions` DataFrame
   - Remove `condition_body_sites` write logic

3. **Migration:**
   - Migrate existing `condition_body_sites` data to `conditions` table
   - Drop `condition_body_sites` table

**Option 2: Normalize Observations (NOT RECOMMENDED)**

1. Create `observation_body_sites` DDL
2. Extract body sites from FHIR `bodySite` field
3. Add transformation function
4. Remove body site columns from `observations` table
5. **Complexity added for minimal benefit**

---

## Comparison: Normalized vs Denormalized

### Normalized (Current for Conditions)

**Pros:**
- ✅ Consistent with codes/categories pattern
- ✅ 88% storage reduction
- ✅ Single source of truth

**Cons:**
- ❌ Junction table complexity
- ❌ ID generation overhead
- ❌ Additional joins for queries
- ❌ More ETL code to maintain
- ❌ Only 3 unique values (overkill)

### Denormalized (Current for Observations)

**Pros:**
- ✅ Simple queries (no joins)
- ✅ Less ETL complexity
- ✅ Direct access to body site data
- ✅ Appropriate for low cardinality (3 values)

**Cons:**
- ❌ Data duplication (but minimal: only 3 values)
- ❌ Inconsistent with codes/categories pattern
- ❌ Storage overhead (negligible: ~1.5MB)

---

## Decision Matrix

| Factor | Normalize | Denormalize |
|--------|-----------|-------------|
| **Cardinality** | 3 values (too low) | ✅ Appropriate |
| **Storage Savings** | 1.5MB (negligible) | ✅ Negligible loss |
| **Query Complexity** | ❌ Requires joins | ✅ Direct access |
| **ETL Complexity** | ❌ High (junction table) | ✅ Low (direct columns) |
| **Consistency** | ✅ Matches codes pattern | ❌ Different from codes |
| **Maintenance** | ❌ More code | ✅ Less code |
| **Future Scalability** | ✅ Better if grows | ⚠️ May need to normalize later |

---

## Final Recommendation

### **NORMALIZE Observations Body Sites** (Updated)

**Rationale:**
1. **Consistency is valuable** - Maintaining the same pattern across all code-based fields simplifies maintenance
2. **Pattern alignment** - Matches the pattern used for codes, categories, interpretations
3. **Infrastructure exists** - The `body_sites` table and pattern are already established
4. **Low overhead** - Normalization overhead is minimal since infrastructure is in place
5. **Future-proof** - If body sites grow, normalization will scale better

**Implementation:**
- Create `observation_body_sites` junction table (similar to `condition_body_sites`)
- Extract body sites from FHIR `bodySite` field (if present in source data)
- Remove body site columns from main `observations` table
- Use shared `body_sites` table (already exists)

**Comprehensive Strategy:**
- Normalize all code-based fields that can repeat (codes, categories, interpretations, body sites, component codes, range types, etc.)
- Keep denormalized only truly unique data (text, IDs, timestamps, values)
- See `COMPREHENSIVE_NORMALIZATION_STRATEGY.md` for complete strategy

---

## Implementation Steps (If Denormalizing Conditions)

1. **Update DDL:**
   ```sql
   -- Add to conditions table
   ALTER TABLE public.conditions ADD COLUMN body_site_code VARCHAR(50);
   ALTER TABLE public.conditions ADD COLUMN body_site_system VARCHAR(255);
   ALTER TABLE public.conditions ADD COLUMN body_site_display VARCHAR(255);
   ALTER TABLE public.conditions ADD COLUMN body_site_text VARCHAR(500);
   ```

2. **Migrate Data:**
   ```sql
   -- Migrate from condition_body_sites to conditions
   UPDATE public.conditions c
   SET 
       body_site_code = bs.body_site_code,
       body_site_system = bs.body_site_system,
       body_site_display = bs.body_site_display,
       body_site_text = bs.body_site_text
   FROM (
       SELECT DISTINCT ON (condition_id)
           cbs.condition_id,
           bs.body_site_code,
           bs.body_site_system,
           bs.body_site_display,
           bs.body_site_text
       FROM public.condition_body_sites cbs
       JOIN public.body_sites bs ON cbs.body_site_id = bs.body_site_id
       ORDER BY cbs.condition_id, cbs.body_site_rank NULLS LAST
   ) bs
   WHERE c.condition_id = bs.condition_id;
   ```

3. **Update ETL:**
   - Modify `transform_condition_body_sites()` to return single row per condition
   - Join body site data into main `conditions` DataFrame
   - Remove `condition_body_sites` write logic

4. **Cleanup:**
   - Drop `condition_body_sites` table
   - Optionally drop `body_sites` table (if not used elsewhere)

---

**Last Updated**: 2025-12-04  
**Status**: Analysis Complete - Recommendation: Normalize for Consistency

**See Also**: `COMPREHENSIVE_NORMALIZATION_STRATEGY.md` for complete normalization strategy

