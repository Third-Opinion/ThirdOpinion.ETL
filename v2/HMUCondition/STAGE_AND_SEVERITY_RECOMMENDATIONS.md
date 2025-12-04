# Stage, Severity, and Grade Handling Recommendations

## Summary of Current State

### ✅ Severity - **FULLY IMPLEMENTED**
- **DDL**: Present in `conditions` table (`severity_code`, `severity_system`, `severity_display`)
- **Transformation**: Extracted from `severity.coding` array in `main_condition.py`
- **Status**: ✅ Working correctly

### ⚠️ Stage - **PARTIALLY IMPLEMENTED**
- **DDL**: Complete schema in `condition_stages` table with all fields:
  - `stage_summary_code/system/display`
  - `stage_assessment_code/system/display`  
  - `stage_type_code/system/display`
- **Transformation**: ❌ **BROKEN** - Currently only extracts `assessment.reference`, sets all other fields to `None`
- **Status**: Needs fix

### ❌ Grade - **NOT IN FHIR CONDITION**
- **FHIR Spec**: Grade is NOT a direct field on Condition resource
- **Typical Usage**: Grade may appear in:
  1. `stage.summary` (e.g., "Grade 3" as part of stage summary)
  2. Separate Observation resource (e.g., "Tumor Grade")
- **Recommendation**: Extract from `stage.summary` if present, otherwise not applicable

## HealthLake Data Review

**Sample Conditions Reviewed:**
- No stage data found in sample conditions (mostly SDOH conditions)
- No severity data found in sample conditions
- Conditions checked included cancer conditions (SNOMED: 363346000) but had no stage/severity populated

**Redshift Data Review:**
- `condition_stages` table exists but is empty (0 rows)
- `conditions` table has no severity data currently
- Table structures are correct, just need proper data extraction

## Recommendations

### 1. Fix Stage Transformation ✅ **HIGH PRIORITY**

**Current Issue:** `transform_condition_stages()` in `child_tables.py` only extracts `assessment.reference` and sets everything else to `None`.

**Fix:** Update to extract all three fields (summary, type, assessment) properly:

```python
def transform_condition_stages(df: DataFrame) -> DataFrame:
    """Transform condition stages"""
    logger.info("Transforming condition stages...")
    
    if "stage" not in df.columns:
        logger.warning("stage column not found in data, returning empty DataFrame")
        return df.select(
            F.col("id").alias("condition_id"),
            F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
            F.lit(None).cast("string").alias("stage_summary_code"),
            F.lit(None).cast("string").alias("stage_summary_system"),
            F.lit(None).cast("string").alias("stage_summary_display"),
            F.lit(None).cast("string").alias("stage_assessment_code"),
            F.lit(None).cast("string").alias("stage_assessment_system"),
            F.lit(None).cast("string").alias("stage_assessment_display"),
            F.lit(None).cast("string").alias("stage_type_code"),
            F.lit(None).cast("string").alias("stage_type_system"),
            F.lit(None).cast("string").alias("stage_type_display")
        ).filter(F.lit(False))
    
    # Explode the stage array
    stages_df = df.select(
        F.col("id").alias("condition_id"),
        F.col("meta").getField("lastUpdated").alias("meta_last_updated"),
        F.explode(F.col("stage")).alias("stage_item")
    ).filter(F.col("stage_item").isNotNull())
    
    # Extract stage details - handle both CodeableConcept and Reference types
    stages_final = stages_df.select(
        F.col("condition_id"),
        F.col("meta_last_updated"),
        
        # stage_summary: CodeableConcept (coding array)
        F.when(
            F.col("stage_item.summary").isNotNull() & 
            F.col("stage_item.summary.coding").isNotNull() &
            (F.size(F.col("stage_item.summary.coding")) > 0),
            F.col("stage_item.summary.coding")[0].getField("code")
        ).otherwise(None).alias("stage_summary_code"),
        F.when(
            F.col("stage_item.summary").isNotNull() & 
            F.col("stage_item.summary.coding").isNotNull() &
            (F.size(F.col("stage_item.summary.coding")) > 0),
            F.col("stage_item.summary.coding")[0].getField("system")
        ).otherwise(None).alias("stage_summary_system"),
        F.when(
            F.col("stage_item.summary").isNotNull() & 
            F.col("stage_item.summary.coding").isNotNull() &
            (F.size(F.col("stage_item.summary.coding")) > 0),
            F.col("stage_item.summary.coding")[0].getField("display")
        ).otherwise(None).alias("stage_summary_display"),
        
        # stage_assessment: Can be Reference OR CodeableConcept
        F.when(
            # If it's a Reference (has reference field)
            F.col("stage_item.assessment.reference").isNotNull(),
            F.col("stage_item.assessment.reference")
        ).when(
            # If it's a CodeableConcept (has coding array)
            F.col("stage_item.assessment.coding").isNotNull() &
            (F.size(F.col("stage_item.assessment.coding")) > 0),
            F.col("stage_item.assessment.coding")[0].getField("code")
        ).otherwise(None).alias("stage_assessment_code"),
        F.when(
            F.col("stage_item.assessment.reference").isNull() &
            F.col("stage_item.assessment.coding").isNotNull() &
            (F.size(F.col("stage_item.assessment.coding")) > 0),
            F.col("stage_item.assessment.coding")[0].getField("system")
        ).otherwise(None).alias("stage_assessment_system"),
        F.when(
            F.col("stage_item.assessment.reference").isNull() &
            F.col("stage_item.assessment.coding").isNotNull() &
            (F.size(F.col("stage_item.assessment.coding")) > 0),
            F.col("stage_item.assessment.coding")[0].getField("display")
        ).otherwise(None).alias("stage_assessment_display"),
        
        # stage_type: CodeableConcept (coding array)
        F.when(
            F.col("stage_item.type").isNotNull() & 
            F.col("stage_item.type.coding").isNotNull() &
            (F.size(F.col("stage_item.type.coding")) > 0),
            F.col("stage_item.type.coding")[0].getField("code")
        ).otherwise(None).alias("stage_type_code"),
        F.when(
            F.col("stage_item.type").isNotNull() & 
            F.col("stage_item.type.coding").isNotNull() &
            (F.size(F.col("stage_item.type.coding")) > 0),
            F.col("stage_item.type.coding")[0].getField("system")
        ).otherwise(None).alias("stage_type_system"),
        F.when(
            F.col("stage_item.type").isNotNull() & 
            F.col("stage_item.type.coding").isNotNull() &
            (F.size(F.col("stage_item.type.coding")) > 0),
            F.col("stage_item.type.coding")[0].getField("display")
        ).otherwise(None).alias("stage_type_display")
    ).filter(
        # Keep rows that have at least one field populated
        (F.col("stage_summary_code").isNotNull()) |
        (F.col("stage_assessment_code").isNotNull()) |
        (F.col("stage_type_code").isNotNull())
    )
    
    return stages_final
```

**Key Changes:**
1. ✅ Extract `stage_summary` from `stage_item.summary.coding[0]`
2. ✅ Extract `stage_type` from `stage_item.type.coding[0]`
3. ✅ Handle `stage_assessment` as either Reference OR CodeableConcept
4. ✅ Filter to keep rows with at least one populated field

### 2. Grade Handling ✅ **MEDIUM PRIORITY**

**Recommendation:** Grade is not a separate FHIR field. If grade information exists, it will be in:
- `stage.summary` (e.g., "Grade 3 Breast Cancer, Stage II")
- Separate Observation resource (e.g., "Tumor Grade Observation")

**Approach:**
1. **Extract from stage.summary**: Already handled if we fix stage transformation above
   - Grade text may appear in `stage_summary_display`
   - Example: "Grade 3, Stage II" → `stage_summary_display` = "Grade 3, Stage II"
   
2. **No separate grade table needed**: Grade is descriptive text within stage, not a coded value

3. **Future Enhancement**: If needed, could parse grade from `stage_summary_display`:
   ```sql
   -- Example: Extract grade number from display text
   CASE 
     WHEN stage_summary_display LIKE '%Grade 1%' THEN '1'
     WHEN stage_summary_display LIKE '%Grade 2%' THEN '2'
     WHEN stage_summary_display LIKE '%Grade 3%' THEN '3'
     WHEN stage_summary_display LIKE '%Grade 4%' THEN '4'
   END as grade_number
   ```

### 3. Severity - No Changes Needed ✅

**Current Implementation:** Already correct
- Extracted from `severity.coding` array
- Stored in main `conditions` table
- No normalization needed (severity is condition-specific, not shared like codes)

## Implementation Priority

1. **HIGH**: Fix `transform_condition_stages()` to extract all fields properly
2. **MEDIUM**: Test with actual stage data when available
3. **LOW**: Add grade parsing from `stage_summary_display` if needed

## FHIR Reference

According to FHIR Condition spec:
- **stage.summary**: CodeableConcept - "Simple summary (disease specific)"
- **stage.type**: CodeableConcept - "Kind of staging"  
- **stage.assessment**: Reference(Observation/Procedure) OR CodeableConcept - "Formal record of assessment"
- **severity**: CodeableConcept - "Subjective severity of condition"
- **grade**: NOT a Condition field - may appear in stage.summary or separate Observation

## Testing Recommendations

1. Query HealthLake for Conditions with actual stage data:
   ```python
   # Search for conditions that likely have staging
   search_health_lake(resourceType="Condition", code="http://snomed.info/sct|363346000")
   ```

2. Check if any conditions have `stage` field populated

3. Verify stage structure in Iceberg before/after transformation

4. Test transformation with sample stage data (even if empty initially)

