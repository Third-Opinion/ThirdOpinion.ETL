# Medication Code Systems Guide for Clinical Trials

## Overview

This guide explains the medication code systems available for clinical trials, what level of detail each provides (drug vs ingredient level), and how to query them in your system.

## Current Implementation Status

### ✅ Currently Supported: **RxNorm Only**

Your current implementation (`HMUMedication.py`) enriches medications with **RxNorm codes only**:

- **Lookup Table**: `public.medication_code_lookup`
- **Code System**: `http://www.nlm.nih.gov/research/umls/rxnorm`
- **Enrichment Sources**: 
  - RxNav API (free, 20 req/sec)
  - Amazon Comprehend Medical (paid fallback)
  - Redshift lookup table cache

### ❌ Not Currently Supported:
- SNOMED CT codes
- NDC (National Drug Code)
- UNII (Unique Ingredient Identifier)
- MED-RT (Medication Reference Terminology)
- ATC (Anatomical Therapeutic Chemical)

---

## Code Systems Available for Clinical Trials

### 1. **RxNorm** (Currently Implemented)

**What it provides:**
- **Clinical Drugs**: Specific combinations of ingredients, strengths, and dose forms
  - Example: "Lisinopril 10 MG Oral Tablet" (RxCUI: 314076)
- **Brand Names**: Proprietary names under which drugs are marketed
  - Example: "Prinivil" (RxCUI: 1596450)
- **Ingredients**: Active components of medications
  - Example: "Lisinopril" (RxCUI: 29046)
- **Semantic Types**: 
  - `SCD` (Semantic Clinical Drug) - Ingredients + strength + form
  - `SBD` (Semantic Branded Drug) - Brand name + ingredients + strength + form
  - `SCDC` (Semantic Clinical Drug Component)
  - `IN` (Ingredient)
  - `PIN` (Precise Ingredient)

**Level of Detail:**
- ✅ **Drug Level**: Clinical drugs with strength and form
- ✅ **Ingredient Level**: Individual active ingredients
- ✅ **Brand Level**: Brand name medications

**Best for Clinical Trials:**
- Standardizing medication names across sites
- Mapping to drug classes and ingredients
- Drug-drug interaction checking
- Identifying generic vs brand names

**How to Query RxNorm Data:**

```sql
-- Query medications with RxNorm codes from your table
SELECT 
    m.medication_id,
    m.primary_text AS medication_name,
    m.primary_code AS rxnorm_code,
    m.primary_system AS code_system,
    m.code AS all_codes_json,  -- Contains all code.coding entries
    m.status
FROM public.medications m
WHERE m.primary_system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
    AND m.primary_code IS NOT NULL;

-- Query lookup table to see enriched codes
SELECT 
    normalized_name,
    medication_name,
    rxnorm_code,
    confidence_score,
    enrichment_source,
    updated_at
FROM public.medication_code_lookup
WHERE rxnorm_code IS NOT NULL
ORDER BY updated_at DESC
LIMIT 100;

-- Find medications missing RxNorm codes
SELECT 
    medication_id,
    primary_text AS medication_name,
    primary_code AS existing_code,
    primary_system AS existing_system
FROM public.medications
WHERE primary_code IS NULL 
    OR primary_system != 'http://www.nlm.nih.gov/research/umls/rxnorm'
LIMIT 100;
```

**External API Query (RxNav):**
- **Endpoint**: `https://rxnav.nlm.nih.gov/REST/drugs.json?name={name}`
- **Example**: Get drug info for "Lisinopril 10 MG Oral Tablet"
  ```
  GET https://rxnav.nlm.nih.gov/REST/drugs.json?name=Lisinopril+10+MG+Oral+Tablet
  ```
- **Returns**: RxCUI (RxNorm code), name, semantic type, related concepts

---

### 2. **SNOMED CT** (Not Currently Implemented)

**What it provides:**
- **Pharmaceutical/Biologic Products**: Comprehensive medication codes
- **Active Ingredients**: Individual substances used in medications
- **Dose Forms**: Physical forms (tablet, injection, capsule, etc.)
- **Routes of Administration**: Oral, intravenous, topical, etc.
- **Strength Values**: Dosage amounts with units
- **Product Roles**: Medicinal product, clinical drug, branded product

**Level of Detail:**
- ✅ **Product Level**: Complete pharmaceutical products
- ✅ **Ingredient Level**: Individual active ingredients
- ✅ **Form Level**: Dose forms (tablet, injection, etc.)
- ✅ **Route Level**: Administration routes

**Best for Clinical Trials:**
- Detailed medication representations
- International interoperability (SNOMED is used globally)
- Integration with clinical terminology
- Route and form standardization

**SNOMED CT Structure:**
```
Product (e.g., "Lisinopril-containing product")
  ├── Ingredient (e.g., "Lisinopril")
  ├── Strength (e.g., "10 milligram")
  ├── Dose Form (e.g., "Oral tablet")
  └── Route (e.g., "Oral route")
```

**How to Query SNOMED CT:**
```sql
-- Currently, SNOMED codes would be in the code SUPER column if present
-- Parse JSON to find SNOMED codes:
SELECT 
    m.medication_id,
    m.primary_text,
    JSON_PARSE(m.code) AS codes_json,  -- Parse SUPER column
    -- Extract SNOMED codes from JSON array
    codes_json[0].code AS code_1,
    codes_json[0].system AS system_1,
    codes_json[0].display AS display_1
FROM public.medications m
WHERE m.code IS NOT NULL
    AND JSON_EXTRACT_PATH_TEXT(m.code, '[0].system') = 'http://snomed.info/sct';

-- Or check all codes in the JSON array for SNOMED
SELECT 
    medication_id,
    primary_text,
    code
FROM public.medications
WHERE code::VARCHAR LIKE '%snomed.info/sct%';
```

**External API Query:**
- **SNOMED CT API**: Requires license (not freely available like RxNorm)
- **Snowstorm API** (Community): `https://snowstorm.ihtsdotools.org/snowstorm/snomed-ct`
- **Example**: Search for medications
  ```
  GET https://snowstorm.ihtsdotools.org/snowstorm/snomed-ct/browser/MAIN/concepts?term=lisinopril&activeFilter=true&limit=10
  ```

---

### 3. **NDC (National Drug Code)** (Not Currently Implemented)

**What it provides:**
- **Package-Level Identification**: Unique codes for specific drug packages
- **11-Digit Code Format**: 
  - Labeler (5 digits)
  - Product (4 digits)
  - Package (2 digits)
- **Manufacturer Information**: Who produced the specific package
- **Package Size**: Quantity and packaging details

**Level of Detail:**
- ✅ **Package Level**: Specific packaging and manufacturer
- ⚠️ **Not Product Level**: NDC changes when manufacturer/packaging changes
- ⚠️ **Not Ingredient Level**: Does not directly identify ingredients

**Best for Clinical Trials:**
- Inventory management
- Dispensing records
- FDA regulatory compliance
- Package-level tracking

**NDC Code Format:**
```
Example: 68180-123-45
  ├── 68180: Labeler code (manufacturer)
  ├── 123: Product code (specific drug/strength/form)
  └── 45: Package code (bottle size, count, etc.)
```

**How to Query NDC:**
```sql
-- NDC codes are typically in FHIR identifiers, not code.coding
-- Check medication_identifiers table:
SELECT 
    mi.medication_id,
    m.primary_text AS medication_name,
    mi.identifier_system,
    mi.identifier_value AS ndc_code
FROM public.medication_identifiers mi
JOIN public.medications m ON mi.medication_id = m.medication_id
WHERE mi.identifier_system LIKE '%ndc%'
    OR mi.identifier_system = 'http://hl7.org/fhir/sid/ndc'
ORDER BY m.primary_text;

-- Or check if NDC is in the code JSON:
SELECT 
    medication_id,
    primary_text,
    code
FROM public.medications
WHERE code::VARCHAR LIKE '%ndc%'
    OR code::VARCHAR LIKE '%http://hl7.org/fhir/sid/ndc%';
```

**External API Query:**
- **FDA NDC Directory**: `https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-directory`
- **RxNav API**: Can convert between RxNorm and NDC
  ```
  GET https://rxnav.nlm.nih.gov/REST/ndcstatus.json?ndc=68180-123-45
  ```

---

### 4. **UNII (Unique Ingredient Identifier)** (Not Currently Implemented)

**What it provides:**
- **Unique Substance Identification**: FDA-assigned identifiers for substances
- **Ingredient-Level**: Identifies active ingredients, not products
- **Chemical Substances**: Active pharmaceutical ingredients
- **Biologics**: Biological substances
- **Mixtures**: Complex ingredient combinations

**Level of Detail:**
- ✅ **Ingredient Level**: Individual active ingredients only
- ❌ **Not Product Level**: Does not identify complete medications

**Best for Clinical Trials:**
- Ingredient-level analysis
- Allergen checking
- Drug class grouping
- Regulatory substance tracking

**UNII Format:**
- 10-character alphanumeric code
- Example: `PQ572ZX7ES` (Lisinopril)

**How to Query UNII:**
```sql
-- UNII codes may be in identifiers or code.coding
-- Check medication_identifiers:
SELECT 
    mi.medication_id,
    m.primary_text,
    mi.identifier_system,
    mi.identifier_value AS unii_code
FROM public.medication_identifiers mi
JOIN public.medications m ON mi.medication_id = m.medication_id
WHERE mi.identifier_system LIKE '%unii%'
    OR mi.identifier_system = 'http://fdasis.nlm.nih.gov';

-- Check code JSON for UNII:
SELECT 
    medication_id,
    primary_text,
    code
FROM public.medications
WHERE code::VARCHAR LIKE '%unii%'
    OR code::VARCHAR LIKE '%fdasis%';
```

**External API Query:**
- **FDA Substance Registration System**: `https://www.fda.gov/industry/fda-substance-registration-system-uniique-ingredient-identifier-unii`
- **UNII API** (limited public access)

---

### 5. **MED-RT (Medication Reference Terminology)** (Not Currently Implemented)

**What it provides:**
- **Drug Classes**: Therapeutic and pharmacologic classes
- **Class Hierarchies**: Parent-child relationships between classes
- **Mechanism of Action**: How drugs work
- **Therapeutic Intent**: What conditions drugs treat

**Level of Detail:**
- ✅ **Class Level**: Drug classification hierarchies
- ✅ **Mechanism Level**: How drugs work
- ❌ **Not Product/Ingredient Level**: Classifications, not specific drugs

**Best for Clinical Trials:**
- Drug class grouping
- Mechanism of action analysis
- Therapeutic area identification
- Drug interaction class checking

**MED-RT Structure:**
```
Therapeutic Class
  └── Pharmacologic Class
      └── Mechanism of Action
          └── Clinical Drug
```

**How to Query MED-RT:**
```sql
-- MED-RT would typically be used for classification
-- Not commonly stored directly in medication records
-- Would need separate classification table or external API lookup
```

---

## Recommended Approach for Clinical Trials

### Current State: ✅ RxNorm is a Good Foundation

RxNorm provides excellent coverage for:
1. ✅ Drug-level identification (Clinical Drugs)
2. ✅ Ingredient-level identification (Ingredients)
3. ✅ Brand name standardization
4. ✅ Free API access (RxNav)
5. ✅ Good integration with US healthcare systems

### Recommended Enhancements:

#### Priority 1: **Enhance Code Storage**
Store all code systems from the original FHIR data:

```sql
-- The code SUPER column should contain ALL coding entries
-- Extract all codes, not just primary:
SELECT 
    medication_id,
    primary_text,
    -- Primary code (already extracted)
    primary_code,
    primary_system,
    -- All codes from JSON array
    code AS all_codes_json,
    -- Parse to get SNOMED, NDC, etc.
    JSON_PARSE(code) AS codes_parsed
FROM public.medications
WHERE code IS NOT NULL;
```

#### Priority 2: **Add SNOMED CT Support** (If International/Detailed)
- SNOMED provides more detailed medication representations
- Better for international clinical trials
- Requires license for production use

#### Priority 3: **Extract NDC from Identifiers**
- NDC is often in FHIR identifiers, not code.coding
- Already stored in `medication_identifiers` table
- Useful for package-level tracking

#### Priority 4: **Add UNII Mapping** (For Ingredient Analysis)
- Map RxNorm ingredients to UNII codes
- Useful for ingredient-level analysis
- Can be done via external API or lookup table

---

## Querying Your Current System

### 1. Check Enrichment Status

```sql
-- Summary of enrichment coverage
SELECT 
    COUNT(*) AS total_medications,
    COUNT(CASE WHEN primary_code IS NOT NULL 
          AND primary_system = 'http://www.nlm.nih.gov/research/umls/rxnorm' 
          THEN 1 END) AS has_rxnorm,
    COUNT(CASE WHEN primary_code IS NULL OR 
               primary_system != 'http://www.nlm.nih.gov/research/umls/rxnorm' 
          THEN 1 END) AS missing_rxnorm,
    ROUND(100.0 * COUNT(CASE WHEN primary_code IS NOT NULL 
                            AND primary_system = 'http://www.nlm.nih.gov/research/umls/rxnorm' 
                            THEN 1 END) / COUNT(*), 2) AS enrichment_percentage
FROM public.medications;
```

### 2. Find All Code Systems Present

```sql
-- Extract all code systems from the code JSON column
WITH code_systems AS (
    SELECT DISTINCT
        JSON_EXTRACT_PATH_TEXT(codes.code_entry, 'system') AS code_system
    FROM (
        SELECT 
            medication_id,
            JSON_PARSE(code) AS codes_array
        FROM public.medications
        WHERE code IS NOT NULL
            AND code != 'null'
    ) AS meds,
    LATERAL FLATTEN(INPUT => meds.codes_array) AS codes(code_entry)
)
SELECT 
    code_system,
    COUNT(*) AS medication_count
FROM code_systems
WHERE code_system IS NOT NULL
GROUP BY code_system
ORDER BY medication_count DESC;
```

### 3. Find Medications with Multiple Code Systems

```sql
-- Medications that have multiple coding entries (may include SNOMED, NDC, etc.)
SELECT 
    m.medication_id,
    m.primary_text,
    m.primary_code AS primary_rxnorm_code,
    m.primary_system AS primary_system,
    -- Count codes in JSON array
    JSON_ARRAY_LENGTH(JSON_PARSE(m.code)) AS total_codes,
    m.code AS all_codes_json
FROM public.medications m
WHERE m.code IS NOT NULL
    AND m.code != 'null'
    AND JSON_ARRAY_LENGTH(JSON_PARSE(m.code)) > 1
ORDER BY total_codes DESC
LIMIT 50;
```

### 4. Query Lookup Table

```sql
-- Check enrichment cache
SELECT 
    normalized_name,
    medication_name,
    rxnorm_code,
    rxnorm_system,
    confidence_score,
    enrichment_source,
    created_at,
    updated_at
FROM public.medication_code_lookup
WHERE rxnorm_code IS NOT NULL
ORDER BY updated_at DESC;

-- Statistics on enrichment sources
SELECT 
    enrichment_source,
    COUNT(*) AS count,
    AVG(confidence_score) AS avg_confidence,
    MIN(confidence_score) AS min_confidence,
    MAX(confidence_score) AS max_confidence
FROM public.medication_code_lookup
WHERE enrichment_source IS NOT NULL
GROUP BY enrichment_source;
```

### 5. Find Medications by Ingredient (RxNorm)

```sql
-- This requires RxNorm API to get ingredient relationships
-- For now, search by medication name patterns:
SELECT 
    medication_id,
    primary_text,
    primary_code AS rxnorm_code
FROM public.medications
WHERE primary_system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
    AND (
        primary_text ILIKE '%lisinopril%'
        OR primary_text ILIKE '%metformin%'
        OR primary_text ILIKE '%atorvastatin%'
    )
ORDER BY primary_text;
```

---

## Expanding to Support Additional Code Systems

### Option 1: Store All Codes from FHIR (Recommended)

Modify the transformation to preserve ALL codes from the `code.coding` array, not just the first one:

```python
# In transformations/main_medication.py
# Currently only extracts first code as primary_code/primary_system
# The code SUPER column already has all codes as JSON - use that!

# Query example to get all codes:
# SELECT 
#     medication_id,
#     primary_text,
#     JSON_PARSE(code) AS all_codes  -- All code.coding entries
# FROM medications;
```

### Option 2: Create Multi-Code Lookup Table

```sql
CREATE TABLE IF NOT EXISTS public.medication_code_mappings (
    medication_id VARCHAR(255) NOT NULL,
    code_system VARCHAR(255) NOT NULL,
    code_value VARCHAR(100) NOT NULL,
    code_display VARCHAR(500),
    is_primary BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (medication_id, code_system, code_value)
)
DISTSTYLE KEY
DISTKEY (medication_id)
SORTKEY (medication_id, code_system);
```

### Option 3: Enhance Enrichment to Add Multiple Code Systems

Add enrichment functions for:
- SNOMED CT (requires license or API access)
- UNII mapping (via RxNorm → UNII lookup)
- NDC (already in identifiers, just need to link)

---

## Recommendations for Clinical Trials

### ✅ **Keep RxNorm as Primary Code System**
- Excellent coverage for US clinical trials
- Free API access
- Good for drug-level and ingredient-level identification

### ✅ **Preserve All Original Codes**
- Store all codes from FHIR `code.coding` array
- Don't lose SNOMED/NDC codes that may already be present
- Query the `code` SUPER column for all code systems

### ✅ **Add SNOMED CT if International Trials**
- Better international interoperability
- More detailed medication representations
- Requires license for production use

### ✅ **Extract NDC from Identifiers**
- Already available in `medication_identifiers` table
- Useful for package-level tracking
- No enrichment needed - just query existing data

### ✅ **Consider UNII for Ingredient Analysis**
- Map RxNorm ingredients to UNII
- Useful for allergen checking
- Can be done via lookup table or API

---

## Next Steps

1. **✅ Review Current Enrichment Coverage**
   ```sql
   SELECT COUNT(*) FROM public.medication_code_lookup;
   ```

2. **✅ Check What Code Systems Are Already Present**
   ```sql
   -- Use query #2 above to find all code systems
   ```

3. **⏭️ Consider Expanding Enrichment**
   - Add SNOMED CT support (if needed)
   - Map RxNorm to UNII for ingredients
   - Extract NDC from identifiers table

4. **⏭️ Create Multi-Code Mapping Table** (if needed)
   - Store all code systems in normalized format
   - Enable easier cross-system queries

---

## References

- **RxNorm**: https://www.nlm.nih.gov/research/umls/rxnorm/
- **RxNav API**: https://lhncbc.nlm.nih.gov/RxNav/
- **SNOMED CT**: https://www.snomed.org/
- **NDC Directory**: https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-directory
- **UNII**: https://www.fda.gov/industry/fda-substance-registration-system-uniique-ingredient-identifier-unii
- **MED-RT**: https://www.nlm.nih.gov/research/umls/sourcereleasedocs/current/MEDRT/

