# Code Mappings and Normalization Reference

**Last Updated:** 2025-12-04  
**Status:** All implementations complete

This document consolidates all important code mappings, normalization patterns, and implementation details for the HMUObservation ETL.

---

## Overview

The HMUObservation ETL enriches observation codes by mapping text-based codes to standardized LOINC codes. When no LOINC code exists, synthetic codes are created to ensure 100% code coverage.

**Total Mappings:** ~200+ LOINC mappings  
**Total Impact:** ~171,222 observations mapped from synthetic to LOINC codes  
**Synthetic Codes Eliminated:** ~48 unique codes

---

## Code Mappings

### Lab Test Abbreviations (12 mappings)

| Mapping Key | LOINC Code | Display | Observations |
|-------------|------------|---------|--------------|
| `platelet` | `777-3` | Platelets [#/volume] in Blood by Automated count | ~3,071 |
| `hgb` | `718-7` | Hemoglobin [Mass/volume] in Blood | ~2,528 |
| `rdw_cv` | `788-0` | Erythrocyte distribution width [Ratio] by Automated count | ~3,070 |
| `bun` | `3094-0` | Urea nitrogen [Mass/volume] in Serum or Plasma | ~1,900 |
| `co2` | `2028-9` | Carbon dioxide, total [Moles/volume] in Serum or Plasma | ~1,760 |
| `chloride` | `2075-0` | Chloride [Moles/volume] in Serum or Plasma | ~1,763 |
| `alk_phos` | `6768-6` | Alkaline phosphatase [Enzymatic activity/volume] in Serum or Plasma | ~2,066 |
| `e_gfr` / `egfr` | `33914-3` | Glomerular filtration rate (MDRD) | ~1,907 |
| `mean_platelet_vol` | `32623-1` | Platelet [Entitic mean volume] in Blood by Automated count | ~3,053 |
| `a_g_ratio` | `1751-7` | Albumin/Globulin [Mass Ratio] in Serum or Plasma | ~2,063 |
| `total_protein` | `2885-2` | Protein [Mass/volume] in Serum or Plasma | ~2,063 |
| `bun_creat_ratio` | `3097-3` | Urea nitrogen/Creatinine [Mass Ratio] in Serum or Plasma | ~1,788 |

**Total Impact:** ~25,000 observations

---

### CBC Differential Components (11 mappings)

#### Percentage Components:
| Mapping Key | LOINC Code | Display | Observations |
|-------------|------------|---------|--------------|
| `neutrophil_%` | `770-8` | Neutrophils/100 leukocytes in Blood by Automated count | ~1,439 |
| `lymphocyte_%` | `736-9` | Lymphocytes/Leukocytes in Blood by Automated count | ~1,439 |
| `monocyte_%` | `5905-5` | Monocytes/Leukocytes in Blood by Automated count | ~1,439 |
| `eosinophil_%` | `711-2` | Eosinophils [#/volume] in Blood by Automated count | ~1,439 |
| `basophil_%` | `706-2` | Basophils/100 leukocytes in Blood by Automated count | ~1,439 |
| `immature_granulocyte_%` | `38518-7` | Immature granulocytes/Leukocytes in Blood | ~1,433 |

#### Absolute Count Components (#):
| Mapping Key | LOINC Code | Display | Observations |
|-------------|------------|---------|--------------|
| `neutrophil_#` | `770-8` | Neutrophils [#/volume] in Blood by Automated count | ~1,433 |
| `lymphocyte_#` | `731-0` | Lymphocytes [#/volume] in Blood by Automated count | ~1,433 |
| `monocyte_#` | `742-7` | Monocytes [#/volume] in Blood by Automated count | ~1,433 |
| `basophil_#` | `704-7` | Basophils [#/volume] in Blood by Automated count | ~1,433 |
| `immature_granulocyte_#` | `51584-1` | Immature granulocytes [#/volume] in Blood | ~1,434 |

**Total Impact:** ~15,000 observations

---

### Typo Corrections (3 mappings)

| Mapping Key | LOINC Code | Display | Observations | Correction |
|-------------|------------|---------|--------------|------------|
| `appearnace` | `5767-9` | Appearance of Urine | ~2,108 | appearnace → appearance |
| `leudocytes` | `6690-2` | Leukocytes [#/volume] in Blood by Automated count | ~2,182 | leudocytes → leukocytes |
| `nitrate` | `50558-6` | Nitrite [Presence] in Urine by Automated test strip | ~2,642 | nitrate → nitrite (common urinalysis confusion) |

**Total Impact:** ~7,000 observations

---

### Antibiotic Susceptibility (14 mappings)

These represent Minimum Inhibitory Concentration (MIC) susceptibility test results.

| Mapping Key | LOINC Code | Display | Observations | Status |
|-------------|------------|---------|--------------|--------|
| `ampicillin` | `28-1` | Ampicillin [Susceptibility] by Minimum inhibitory concentration (MIC) | ~1,792 | ✅ Verified |
| `levofloxacin` | `20396-8` | levoFLOXacin [Susceptibility] by Minimum inhibitory concentration (MIC) | ~2,237 | ✅ Verified |
| `ciprofloxacin` | `185-9` | Ciprofloxacin [Susceptibility] by Minimum inhibitory concentration (MIC) | ~2,187 | ✅ Verified |
| `trimethoprim_sulfamethoxazole` | `516-5` | Trimethoprim+Sulfamethoxazole [Susceptibility] by Minimum inhibitory concentration (MIC) | ~1,868 | ✅ Verified |
| `cefepime` | `6644-9` | Cefepime [Susceptibility] by Minimum inhibitory concentration (MIC) | ~1,712 | ✅ Verified |
| `amoxicillin_clavulanic_acid` | `20-8` | Amoxicillin+Clavulanate [Susceptibility] by Minimum inhibitory concentration (MIC) | ~1,618 | ✅ Verified |
| `ertapenem` | `35801-0` | Ertapenem [Susceptibility] by Minimum inhibitory concentration (MIC) | ~1,607 | ✅ Verified |
| `nitrofurantoin` | `18863-1` | Nitrofurantoin [Susceptibility] by Minimum inhibitory concentration (MIC) | ~2,133 | ⚠️ Standard pattern |
| `tobramycin` | `18868-0` | Tobramycin [Susceptibility] by Minimum inhibitory concentration (MIC) | ~1,751 | ⚠️ Standard pattern |
| `cefuroxime` | `18867-2` | Cefuroxime [Susceptibility] by Minimum inhibitory concentration (MIC) | ~1,632 | ⚠️ Standard pattern |
| `meropenem` | `18956-7` | Meropenem [Susceptibility] by Minimum inhibitory concentration (MIC) | ~1,629 | ⚠️ Standard pattern |
| `cefpodoxime` | `18866-4` | Cefpodoxime [Susceptibility] by Minimum inhibitory concentration (MIC) | ~1,596 | ⚠️ Standard pattern |
| `ceftriaxone` | `18869-8` | Ceftriaxone [Susceptibility] by Minimum inhibitory concentration (MIC) | ~1,583 | ⚠️ Standard pattern |
| `cefuroxime_axetil` | `18867-2` | Cefuroxime [Susceptibility] by Minimum inhibitory concentration (MIC) | ~1,626 | ⚠️ Standard pattern |

**Note:** `cefuroxime_axetil` uses the same LOINC code as `cefuroxime` (oral prodrug).

**Total Impact:** ~25,000 observations

---

### Generic Terms (3 mappings)

| Mapping Key | LOINC Code | Display | Observations | Category |
|-------------|------------|---------|--------------|----------|
| `marital_status` | `45404-1` | Marital status | ~81,618 | social-history |
| `prostate_biopsy` | `66117-3` | Prostate Pathology biopsy report | ~6,825 | laboratory |
| `surgical_biopsy` | `52121-1` | Biopsy [Interpretation] in Specimen Narrative | ~3,279 | laboratory |

**Total Impact:** ~91,722 observations

---

### Common Urine Test Strip Mappings

| Mapping Key | LOINC Code | Display |
|-------------|------------|---------|
| `leukocytes` / `leukocyte` | `60026-2` | Leukocyte esterase [Presence] in Urine by Automated test strip |
| `glucose` | `53328-1` | Glucose [Mass/volume] in Urine by Automated test strip |
| `protein` | `50561-0` | Protein [Mass/volume] in Urine by Automated test strip |
| `nitrite` / `nitrites` | `50558-6` | Nitrite [Presence] in Urine by Automated test strip |
| `specific gravity` | `53326-5` | Specific gravity of Urine by Automated test strip |
| `color` | `5778-6` | Color of Urine |
| `clarity` | `32167-9` | Clarity of Urine |
| `bilirubin` | `53327-3` | Bilirubin.total [Mass/volume] in Urine by Automated test strip |
| `ketone` | `50557-8` | Ketones [Mass/volume] in Urine by Automated test strip |
| `urobilinogen` | `50563-6` | Urobilinogen [Mass/volume] in Urine by Automated test strip |
| `blood` | `50559-4` | Hemoglobin [Mass/volume] in Urine by Automated test strip |
| `ph` | `50560-2` | pH of Urine by Test strip |

---

### PSA Mappings

**Important:** PSA mappings are punctuation-sensitive. Standard PSA is preferred over ultrasensitive.

| Mapping Key | LOINC Code | Display | Notes |
|-------------|------------|---------|-------|
| `psa` | `2857-1` | Prostate specific Ag [Mass/volume] in Serum or Plasma | Standard PSA |
| `psa, total` | `2857-1` | Prostate specific Ag [Mass/volume] in Serum or Plasma | Standard PSA (with comma) |
| `psa total` | `2857-1` | Prostate specific Ag [Mass/volume] in Serum or Plasma | Standard PSA (no comma) |
| `total psa` | `2857-1` | Prostate specific Ag [Mass/volume] in Serum or Plasma | Word order variation |
| `psa, free` | `10886-0` | Prostate Specific Ag Free [Mass/volume] in Serum or Plasma | Free PSA |
| `free psa` | `10886-0` | Prostate Specific Ag Free [Mass/volume] in Serum or Plasma | Free PSA (word order) |
| `psa free` | `10886-0` | Prostate Specific Ag Free [Mass/volume] in Serum or Plasma | Free PSA (no comma) |

**Note:** Ultrasensitive PSA (`35741-8`) is preserved if already present in HealthLake `code.coding` array.

---

## Normalization Patterns

The ETL applies several normalization patterns before looking up codes in the mapping table:

### 1. Numeric Suffix Stripping

**Pattern:** Remove `_1`, `_2`, `#1`, `#2`, etc. suffixes before lookup

**Examples:**
- `shbg_1` → `shbg` → lookup
- `test_ordered_1` → `test ordered` → lookup
- `result_#1` → `result` → lookup

**Impact:** ~60,000 observations

**Implementation:**
```python
F.regexp_replace(
    F.regexp_replace(..., r'_\d+$', ''),  # Remove _1, _2, etc.
    r'#\d+$', ''  # Remove #1, #2, etc.
)
```

---

### 2. Parentheses Stripping

**Pattern:** Remove parentheses with numbers: `(3)`, `(_3)`, etc.

**Examples:**
- `specimen_source_(3)` → `specimen_source` → lookup
- `component_1_(3)` → `component_1` → lookup (then numeric suffix stripped)
- `stone_weight_(3)` → `stone_weight` → lookup

**Impact:** ~7,500 observations

**Implementation:**
```python
F.regexp_replace(
    F.regexp_replace(..., r'\(_\d+\)', ''),  # Remove (_3), (_1), etc.
    r'\((\d+)\)', ''  # Remove (3), (1), etc.
)
```

---

### 3. Space-Comma Normalization

**Pattern:** Remove space before comma: `"PSA ,total"` → `"PSA,total"`

**Examples:**
- `"PSA ,total"` → `"PSA,total"` → matches `"psa, total"` mapping

**Impact:** Handles edge cases in PSA and other comma-separated terms

**Implementation:**
```python
F.regexp_replace(..., r'\s+,', ',')  # Remove space before comma
```

---

### 4. Plural Variations

**Pattern:** Add plural variations to mapping table

**Examples:**
- `nitrites` → maps to same LOINC code as `nitrite` (`50558-6`)

**Impact:** ~16,000 observations (nitrites)

---

## Synthetic Codes

Synthetic codes are created when:
1. Observation has `code.text` but no `code.coding` in HealthLake
2. Enrichment lookup finds **no LOINC match**
3. ETL creates synthetic code: `code_code = cleaned_text`, `code_system = "http://thirdopinion.io/CodeSystem/observation-text"`

### Appropriate Synthetic Codes

These are intentionally kept as synthetic codes (no LOINC mapping exists):

- **Social History Questions:**
  - `what_is_your_level_of_caffeine_consumption?` (145,612 obs)
  - `are_you_currently_employed?` (129,365 obs)
  - `are_you_able_to_care_for_yourself_independently?` (73,881 obs)
  - `urinary_incontinence_assessment_performed?` (73,304 obs)
  - `how_much_tobacco_do_you_smoke?` (70,036 obs)
  - CDC STEADI fall risk questions

- **Generic Terms:**
  - `education` (50,564 obs) - No standard LOINC code
  - `hiv_risk_factors` (62,038 obs) - Assessment question
  - `abnormal_status` (47,584 obs) - Interpretation qualifier
  - `site`, `source`, `pdf`, `cc` - Generic descriptors

**Total Synthetic Codes:** ~14,527 unique codes  
**Total Observations with Synthetic Codes:** ~1,505,369 observations

---

## Implementation Details

### Files Modified

1. **`v2/HMUObservation/utils/code_enrichment.py`**
   - Contains `LAB_TEST_LOINC_MAPPING` dictionary (~200+ mappings)
   - All mappings use LOINC system: `http://loinc.org`

2. **`v2/HMUObservation/utils/code_enrichment_native.py`**
   - Implements normalization logic (suffix stripping, parentheses, space-comma)
   - Performs broadcast join with mapping table
   - Handles code enrichment for observations

3. **`v2/HMUObservation/transformations/codes_transformation.py`**
   - Adds deduplication: `dropDuplicates(["observation_id", "code_id"])`
   - Prevents duplicate codes in `observation_codes` table

### Code Enrichment Process

1. **Extract code text** from observation `code.text`
2. **Normalize text:**
   - Lowercase and trim
   - Strip numeric suffixes (`_1`, `#1`)
   - Strip parentheses (`(3)`, `(_3)`)
   - Normalize space-comma (`"PSA ,total"` → `"PSA,total"`)
3. **Lookup in mapping table:**
   - Exact match first
   - Cleaned text match if exact fails
4. **Create code:**
   - If LOINC match found → use LOINC code
   - If no match → create synthetic code

### Deduplication

The ETL prevents duplicate codes by:
- Checking if enriched code already exists in HealthLake `code.coding` array
- Using `dropDuplicates(["observation_id", "code_id"])` after explode

---

## Validation Queries

### Verify High-Priority Mappings
```sql
SELECT 
    c.code_code,
    c.code_system,
    COUNT(DISTINCT oc.observation_id) as observation_count
FROM public.observation_codes oc
INNER JOIN public.codes c ON oc.code_id = c.code_id
WHERE c.code_code IN (
    'platelet', 'hgb', 'rdw_cv', 'bun', 'co2', 'chloride', 
    'alk_phos', 'e_gfr', 'egfr', 'mean_platelet_vol',
    'a_g_ratio', 'total_protein', 'bun_creat_ratio',
    'neutrophil_%', 'lymphocyte_%', 'monocyte_%',
    'eosinophil_%', 'basophil_%', 'immature_granulocyte_%',
    'appearnace', 'leudocytes', 'nitrate'
)
GROUP BY c.code_code, c.code_system
ORDER BY observation_count DESC;
```

**Expected:** All should have `code_system = 'http://loinc.org'`

### Verify Antibiotic Susceptibility Mappings
```sql
SELECT 
    c.code_code,
    c.code_system,
    COUNT(DISTINCT oc.observation_id) as observation_count
FROM public.observation_codes oc
INNER JOIN public.codes c ON oc.code_id = c.code_id
WHERE c.code_code IN (
    'ampicillin', 'levofloxacin', 'ciprofloxacin', 'nitrofurantoin',
    'tobramycin', 'trimethoprim_sulfamethoxazole', 'cefepime',
    'cefuroxime', 'meropenem', 'cefuroxime_axetil',
    'amoxicillin_clavulanic_acid', 'ertapenem', 'cefpodoxime', 'ceftriaxone'
)
GROUP BY c.code_code, c.code_system
ORDER BY observation_count DESC;
```

**Expected:** All should have `code_system = 'http://loinc.org'`

### Verify Generic Terms Mappings
```sql
SELECT 
    c.code_code,
    c.code_system,
    COUNT(DISTINCT oc.observation_id) as observation_count
FROM public.observation_codes oc
INNER JOIN public.codes c ON oc.code_id = c.code_id
WHERE c.code_code IN ('marital_status', 'prostate_biopsy', 'surgical_biopsy')
GROUP BY c.code_code, c.code_system
ORDER BY observation_count DESC;
```

**Expected:** All should have `code_system = 'http://loinc.org'`

### Check Synthetic Code Count
```sql
SELECT 
    COUNT(DISTINCT c.code_code) as total_synthetic_codes,
    COUNT(DISTINCT oc.observation_id) as total_observations_with_synthetic
FROM public.observation_codes oc
INNER JOIN public.codes c ON oc.code_id = c.code_id
WHERE c.code_system = 'http://thirdopinion.io/CodeSystem/observation-text';
```

**Expected:** ~14,479 synthetic codes (reduction of ~48 from original ~14,527)

---

## Summary Statistics

| Category | Mappings | Observations Affected | Status |
|----------|----------|----------------------|--------|
| High-Priority (Lab tests, CBC, typos) | 31 | ~54,500 | ✅ Implemented |
| Antibiotic Susceptibility | 14 | ~25,000 | ✅ Implemented |
| Generic Terms | 3 | ~91,722 | ✅ Implemented |
| **TOTAL** | **48** | **~171,222** | **✅ Complete** |

**Estimated Reduction in Synthetic Codes:** ~48 unique codes eliminated

---

## Notes

1. **LOINC vs. SNOMED:** For observation results, LOINC is preferred. SNOMED is better for procedures and concepts, but our observations are results/reports.

2. **PSA Enrichment:** Defaults to standard PSA (`2857-1`). Ultrasensitive PSA (`35741-8`) is preserved if already in HealthLake.

3. **Antibiotic Susceptibility:** All mappings use MIC (Minimum Inhibitory Concentration) method based on HealthLake data patterns.

4. **CBC Components:** The distinction between `#` (absolute count) and `%` (percentage) is preserved with separate mappings.

5. **Deduplication:** Happens after explode in `codes_transformation.py` to prevent duplicate codes per observation.

6. **Normalization Order:** Suffix stripping → Parentheses stripping → Space-comma normalization → Lookup

---

## Related Files

- `v2/HMUObservation/utils/code_enrichment.py` - Main mapping dictionary
- `v2/HMUObservation/utils/code_enrichment_native.py` - Normalization logic
- `v2/HMUObservation/transformations/codes_transformation.py` - Code transformation and deduplication

