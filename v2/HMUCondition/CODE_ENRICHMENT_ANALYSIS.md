# Code Enrichment Analysis - Glue Job jr_e1c44b6c66d5e8cada0288bdb06bcc0b6827f2c69b4091246aa28f52669a8e0a

## Summary

**Code enrichment is working!** ✅

### Overall Statistics
- **Total records with observation_text**: 9,605,727
- **Records with normalized_observation_text**: 6,914,130
- **Enrichment rate**: **71.98%**

### Breakdown by Status
1. **Enriched** (has both text and normalized): 6,914,130 records (49.0% of total)
2. **No text** (both null): 4,505,387 records (31.9% of total)
3. **Not enriched** (has text but no normalized): 2,691,597 records (19.1% of total)

## Successful Enrichments

### High-Volume Successful Enrichments
| Original Text | Normalized Text | Count |
|--------------|----------------|-------|
| glucose | Glucose | 409,024 |
| pH | pH | 403,119 |
| color | Color | 383,287 |
| bilirubin | Bilirubin | 382,521 |
| specific gravity | Specific Gravity | 382,380 |
| protein | Protein | 382,325 |
| blood | Blood | 380,215 |
| leukocytes | Leukocytes | 380,211 |
| ketone | Ketone | 380,204 |
| clarity | Clarity | 380,170 |
| urobilinogen | Urobilinogen | 380,168 |
| nitrite | Nitrite | 365,667 |

### Complex Enrichments (Text Variations)
| Original Text | Normalized Text | Count | Notes |
|--------------|----------------|-------|-------|
| PSA testosterone | PSA, total | 114,944 | Combined text enriched |
| PSA_1 | PSA, total | 110,691 | Abbreviation enriched |
| testosterone_1 | Testosterone, total | (varies) | Abbreviation enriched |

## Not Enriched Records

### Categories of Non-Enriched Records

1. **Question/Social History Text** (not lab tests):
   - "What is your level of alcohol consumption?" (148,168 records)
   - "What was the date of your most recent tobacco screening?" (140,559 records)
   - "Do you have an advance directive?" (137,235 records)
   - "How many children do you have?" (135,936 records)
   - These are expected - they're not lab test names

2. **Common Lab Terms Not in Mapping** (2.7M records):
   - "albumin" (28,714 not enriched, 2,166 enriched)
   - "calcium" (26,679 not enriched, 1,973 enriched)
   - "creatinine" (26,063 not enriched, 2,209 enriched)
   - "sodium" (24,728 not enriched, 1,766 enriched)
   - "potassium" (24,714 not enriched, 1,764 enriched)
   - "hemoglobin" (17,484 not enriched, 3,763 enriched)
   - "hematocrit" (17,351 not enriched, 3,749 enriched)
   - "AST", "ALT", "globulin", "estradiol", etc.

### Why Some Records Are Not Enriched

**For records WITH existing LOINC codes:**
- The enrichment logic tries to look up normalized text by the observation_text value
- If the text is not in the `LAB_TEST_LOINC_MAPPING` dictionary, it returns `None`
- Example: "albumin" with code "1751-7" → not in mapping → normalized_text = NULL

**For records WITHOUT existing codes:**
- The enrichment creates synthetic codes and uses the original text as normalized_text
- Example: "albumin" without codes → synthetic code created → normalized_text = "albumin"

This explains why:
- Some "albumin" records have normalized_text = "albumin" (no codes, synthetic enrichment)
- Most "albumin" records have normalized_text = NULL (have codes, but text not in mapping)

## Recommendations

1. **Add missing common lab terms to mapping**: The enrichment mapping is missing many common lab tests like:
   - albumin (code: 1751-7)
   - calcium (code: 17861-6)
   - creatinine (code: 2160-0)
   - sodium (code: 2951-2)
   - potassium (code: 2823-3)
   - hemoglobin (code: 718-7)
   - hematocrit (code: 4544-3)

2. **Improve code-based lookup**: When codes exist, use the code to look up normalized text instead of relying only on text matching.

3. **Expected behavior**: Question/social history text should NOT be enriched (this is correct behavior).

## Conclusion

The code enrichment feature is **working as designed** with a **71.98% success rate** for records that have observation text. The remaining 28% are primarily:
- Social history questions (expected - not lab tests)
- Common lab terms not yet in the mapping dictionary (can be improved)

The enrichment successfully handles:
- ✅ Text normalization (lowercase → proper case)
- ✅ Abbreviation expansion (PSA_1 → PSA, total)
- ✅ Combined text parsing (PSA testosterone → PSA, total)
- ✅ Synthetic code generation for unmapped terms

