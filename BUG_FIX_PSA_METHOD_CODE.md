# Bug Fix: PSA Observation Method Code

## Problem

PSA progression observations are incorrectly embedding the observation ID in the `method.coding.code` field.

### Current (Incorrect) Behavior

```json
"method": {
  "coding": [{
    "system": "http://thirdopinion.ai/fhir/CodeSystem/criteria",
    "code": "psa-progression-pcwg3-to.ai-inference-18dab37b-cd2e-4875-b0e6-343f072eba77-v3.0",
    "display": "PSA Progression PCWG3 Criteria v3.0"
  }]
}
```

**Issue**: The observation ID `to.ai-inference-18dab37b-cd2e-4875-b0e6-343f072eba77` is embedded in the code, making each observation have a unique method code.

### Expected (Correct) Behavior

```json
"method": {
  "coding": [{
    "system": "http://thirdopinion.ai/fhir/CodeSystem/criteria",
    "code": "psa-progression-pcwg3",
    "display": "PSA Progression PCWG3 Criteria v3.0"
  }]
}
```

Or with version:

```json
"method": {
  "coding": [{
    "system": "http://thirdopinion.ai/fhir/CodeSystem/criteria",
    "code": "psa-progression-pcwg3-v3.0",
    "display": "PSA Progression PCWG3 Criteria v3.0"
  }]
}
```

## Correct Example (RECIST)

The RadiographicObservation implementation does this correctly:

```json
"method": {
  "coding": [{
    "system": "http://thirdopinion.ai/fhir/CodeSystem/criteria",
    "code": "recist-1.1",
    "display": "RECIST 1.1"
  }]
}
```

## Fix Location

This bug needs to be fixed in the **source application** that generates PSA progression FHIR observations, likely in:
- A C# application/service that creates PSA progression observations
- Look for code that builds the `method` field for PSA observations
- Search for: `psa-progression-pcwg3` or `PSA Progression PCWG3`

## Fix Required

Remove the observation ID from the method code. The code should be:
- **Fixed**: `psa-progression-pcwg3` or `psa-progression-pcwg3-v3.0`
- **Not**: `psa-progression-pcwg3-{observationId}-v3.0`

The observation ID should remain in the `id` field only, not in the `method.coding.code`.

## Impact

This bug causes:
1. **Inability to query by method**: Cannot easily filter/group PSA progression observations by method since each has a unique code
2. **Data inconsistency**: Violates FHIR CodeSystem principles where codes should be reusable across resources
3. **Reporting issues**: SQL views and reports cannot reliably identify PSA progression observations by method code

## ETL/Database Impact

The ETL is already configured to handle the `method` field correctly:

### Database Schema
The `observations` table includes these fields ([ddl/observations.sql](ddl/observations.sql:33-36)):
```sql
method_code VARCHAR(50),
method_system VARCHAR(255),
method_display VARCHAR(255),
method_text VARCHAR(65535),
```

### Views
The fact views are already reading these fields correctly ([views/fact_fhir_observations_vital_signs_view_v1.sql](views/fact_fhir_observations_vital_signs_view_v1.sql:317-320)):
```sql
o.method_system,
o.method_code,
o.method_display,
o.method_text,
```

**No changes needed in ETL** - once the source application is fixed, the corrected data will flow through automatically.

## Expected Data After Fix

### In the observations table:
```
observation_id                                    | method_code
--------------------------------------------------|------------------------
to.ai-inference-18dab37b-cd2e-4875-b0e6-343f...  | psa-progression-pcwg3-v3.0
to.ai-inference-a1b2c3d4-1234-5678-9abc-def0...  | psa-progression-pcwg3-v3.0
to.ai-inference-e5f6g7h8-8765-4321-ijkl-mnop...  | psa-progression-pcwg3-v3.0
```

All observations using the same criteria should have the **same** `method_code`.

## Test Cases

After fix, verify:
1. All PSA progression observations use the same method code (excluding version differences)
2. Observation IDs remain unique in the `id` field
3. Method code does NOT contain the observation ID
4. Queries filtering on method code return all PSA progression observations

### SQL Test Query
```sql
-- Should return all PSA progression observations with the same method_code
SELECT
    observation_id,
    method_code,
    method_display,
    effective_datetime
FROM observations
WHERE method_code LIKE 'psa-progression-pcwg3%'
ORDER BY effective_datetime DESC;

-- Verify uniqueness of method codes (should be 1-2 rows if versioned, 1 row if not)
SELECT
    method_code,
    COUNT(*) as observation_count
FROM observations
WHERE method_code LIKE 'psa-progression-pcwg3%'
GROUP BY method_code;
```
