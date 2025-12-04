# Missing Items for Normalization Implementation

## Summary

After completing DDL updates and ETL transformations, the following items are still missing:

---

## 1. ❌ **Body Sites Lookup Table Population**

**Issue**: We transform `observation_body_sites` with `body_site_id`, but we never populate the shared `body_sites` lookup table.

**Current State:**
- ✅ `body_sites` table exists (shared with conditions)
- ✅ `observation_body_sites` transformation creates `body_site_id` references
- ❌ **Missing**: Function to extract unique body sites and write to `body_sites` table

**What's Needed:**
1. Extract unique body sites from `observation_body_sites_df` (similar to how we extract unique codes)
2. Create DataFrame with: `body_site_id`, `body_site_code`, `body_site_system`, `body_site_display`, `body_site_text`
3. Write to `body_sites` table using upsert (similar to `write_codes_table_with_upsert`)

**Reference Implementation:**
- See how `unique_codes_df` is extracted and written in `HMUObservation.py` (lines 422-554)
- Use `write_small_lookup_table_with_cache` (like categories/interpretations) or create `write_body_sites_table_with_upsert`

**Location to Add:**
- In `HMUObservation.py` after body sites transformation (around line 446)
- Before writing `observation_body_sites` junction table

---

## 2. ⚠️ **Body Sites Transformation - Extract Unique Body Sites**

**Issue**: We need to extract unique body sites from the transformation to populate the `body_sites` lookup table.

**What's Needed:**
1. Create function `transform_unique_body_sites()` (similar to `transform_unique_codes()`)
2. Extract unique body sites from `observation_body_sites_df`
3. Include: `body_site_id`, `body_site_code`, `body_site_system`, `body_site_display`, `body_site_text`
4. Group by `body_site_id` to get unique body sites

**Implementation Pattern:**
```python
def transform_unique_body_sites(body_sites_df: DataFrame) -> DataFrame:
    """Extract unique body sites for body_sites lookup table"""
    # Join with codes table to get code details, or extract from body_sites_df
    # Group by body_site_id and take first values for code, system, display, text
```

---

## 4. ✅ **Already Complete**

- ✅ DDL files updated
- ✅ ETL transformations updated (main_observation.py, child_tables.py)
- ✅ Body sites transformation created
- ✅ All code_id fields normalized
- ✅ Main ETL script updated

---

## Priority Order

1. **HIGH**: Body sites lookup table population (needed for data integrity)
2. **HIGH**: Extract unique body sites function (needed for #1)

---

## Implementation Checklist

- [ ] Modify `transform_observation_body_sites()` to also return code details (code, system, display, text) for extraction
- [ ] Create `transform_unique_body_sites()` function to extract unique body sites
- [ ] Add body sites extraction in `HMUObservation.py` (after body sites transformation)
- [ ] Write unique body sites to `body_sites` table (use `write_small_lookup_table_with_cache` like categories/interpretations)
- [ ] Test end-to-end: body sites transformation → unique extraction → lookup table write → junction table write

