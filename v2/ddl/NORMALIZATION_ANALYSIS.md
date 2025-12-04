# Normalization Analysis for Condition Child Tables

## Executive Summary

Analysis of condition child tables to determine which should use normalized lookup tables (like `codes` and `categories`).

**Date:** 2025-01-XX  
**Status:** ✅ Analysis Complete

---

## Analysis Results

### ✅ **SHOULD BE NORMALIZED**

#### 1. **condition_codes** → Use `codes` table
- **Status**: ✅ Already updated
- **Reason**: Codes are highly repetitive (ICD-10, SNOMED CT codes used across many conditions)
- **Storage Savings**: ~90%+ reduction
- **Pattern**: Matches observations pattern

#### 2. **condition_categories** → Use `categories` table
- **Status**: ✅ Already updated
- **Reason**: Categories are highly repetitive (problem-list-item, encounter-diagnosis, etc.)
- **Storage Savings**: ~90%+ reduction
- **Pattern**: Matches observations pattern

#### 3. **condition_body_sites** → Should use `body_sites` table
- **Status**: ⚠️ **NEEDS UPDATE**
- **Current Data**:
  - Total rows: 3,471
  - Unique conditions: 1,756
  - **Unique body sites: ONLY 3!**
  - Average repetitions: ~1,157 per body site
- **Body Sites Found**:
  1. `419161000` (SNOMED) - Used 1,628 times across 823 conditions
  2. `419465000` (SNOMED) - Used 1,491 times across 754 conditions
  3. `51440002` (SNOMED) - Used 352 times across 179 conditions
- **Storage Savings**: ~88% reduction (1.65MB → 0.17MB)
- **Recommendation**: Create `body_sites` lookup table (shared with observations if they normalize body sites)

---

### ⚠️ **MAY BENEFIT FROM NORMALIZATION** (Low/No Data Yet)

#### 4. **condition_evidence**
- **Status**: ⚠️ **REVIEW NEEDED**
- **Current Data**: 0 rows (no data yet)
- **Structure**: Has `evidence_code`, `evidence_system`, `evidence_display` (similar to codes)
- **Recommendation**: 
  - If evidence codes repeat across conditions → normalize
  - If evidence codes are unique per condition → keep denormalized
  - **Wait for data** before deciding

#### 5. **condition_stages**
- **Status**: ⚠️ **REVIEW NEEDED**
- **Current Data**: 0 rows (no data yet)
- **Structure**: Has multiple code fields:
  - `stage_summary_code/system/display`
  - `stage_assessment_code/system/display`
  - `stage_type_code/system/display`
- **Recommendation**: 
  - Stages are likely more unique per condition (cancer staging, etc.)
  - **Wait for data** before deciding
  - If any stage code field repeats → consider partial normalization

---

### ❌ **SHOULD NOT BE NORMALIZED**

#### 6. **condition_extensions**
- **Status**: ✅ Keep as-is
- **Reason**: Extensions are highly variable, unique per condition
- **Structure**: Supports nested extensions, various value types
- **Recommendation**: Keep denormalized (extensions are condition-specific)

#### 7. **condition_notes**
- **Status**: ✅ Keep as-is
- **Reason**: Notes are unique text per condition
- **Structure**: Free-form text with author/time
- **Recommendation**: Keep denormalized (notes are condition-specific)

---

## Recommendations

### Immediate Actions

1. ✅ **condition_codes**: Already updated to use `codes` table
2. ✅ **condition_categories**: Already updated to use `categories` table
3. ⚠️ **condition_body_sites**: **UPDATE NEEDED** - Create `body_sites` lookup table

### Future Actions (When Data Available)

4. **condition_evidence**: Monitor data patterns, normalize if codes repeat
5. **condition_stages**: Monitor data patterns, normalize if stage codes repeat

### Keep As-Is

6. **condition_extensions**: Keep denormalized (unique per condition)
7. **condition_notes**: Keep denormalized (unique per condition)

---

## Storage Impact Summary

| Table | Current Approach | Normalized Approach | Savings |
|-------|-----------------|---------------------|---------|
| **condition_codes** | ~5GB+ | ~600MB | **~90%+** |
| **condition_categories** | ~500MB+ | ~50MB | **~90%+** |
| **condition_body_sites** | ~1.65MB | ~0.17MB | **~88%** |
| **condition_evidence** | TBD | TBD | TBD |
| **condition_stages** | TBD | TBD | TBD |

---

## Implementation Plan

### Phase 1: High-Value Normalizations (Immediate)

1. ✅ **condition_codes** → `codes` table (DONE)
2. ✅ **condition_categories** → `categories` table (DONE)
3. ⚠️ **condition_body_sites** → `body_sites` table (TODO)

### Phase 2: Monitor and Evaluate (When Data Available)

4. **condition_evidence** → Evaluate after data collection
5. **condition_stages** → Evaluate after data collection

### Phase 3: Keep Denormalized

6. **condition_extensions** → Keep as-is
7. **condition_notes** → Keep as-is

---

## Notes

- **Shared Lookup Tables**: `codes`, `categories`, and potentially `body_sites` are shared between observations and conditions
- **Hash-Based IDs**: All lookup tables use deterministic hash-based IDs (same code/category/body_site always gets same ID)
- **Storage Efficiency**: Normalization provides 88-90%+ storage reduction for repetitive data
- **Query Performance**: Normalized structure may require joins, but Redshift handles this efficiently with proper DISTKEY/SORTKEY

---

**Document Status**: ✅ Analysis Complete - Ready for Implementation


