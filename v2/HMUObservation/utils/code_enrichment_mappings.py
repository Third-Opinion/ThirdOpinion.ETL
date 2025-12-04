"""
LOINC code enrichment mappings

Data-driven mapping based on actual Redshift data analysis.
These codes represent the MOST COMMON code for each text in existing data.
normalized_text: Well-formatted canonical name for all observations with this code
"""
from typing import Dict, Any


# Data-driven mapping based on actual Redshift data analysis
# These codes represent the MOST COMMON code for each text in existing data
# normalized_text: Well-formatted canonical name for all observations with this code
LAB_TEST_LOINC_MAPPING: Dict[str, Dict[str, Any]] = {
    # Urine test strip results (most common missing codes)
    # Codes validated against actual data: most common code used when codes exist
    "leukocytes": {
        "code": "60026-2",
        "system": "http://loinc.org",
        "display": "Leukocyte esterase [Presence] in Urine by Automated test strip",
        "normalized_text": "Leukocytes",
        "confidence": "high",
        "validation_date": "2025-01-XX",
        "source": "redshift_data_analysis"
    },
    "leukocyte": {  # Handle singular form
        "code": "60026-2",
        "system": "http://loinc.org",
        "display": "Leukocyte esterase [Presence] in Urine by Automated test strip",
        "normalized_text": "Leukocytes",
        "confidence": "high",
        "validation_date": "2025-01-XX",
        "source": "redshift_data_analysis"
    },
    "glucose": {
        "code": "53328-1",
        "system": "http://loinc.org",
        "display": "Glucose [Mass/volume] in Urine by Automated test strip",
        "normalized_text": "Glucose",
        "confidence": "high",
        "note": "Also used: 2345-7 (serum, 15K), 25428-4 (urine test strip, 1.4K)"
    },
    "protein": {
        "code": "50561-0",
        "system": "http://loinc.org",
        "display": "Protein [Mass/volume] in Urine by Automated test strip",
        "normalized_text": "Protein",
        "confidence": "high",
        "note": "Also used: 20454-5 (urine test strip, 1.8K)"
    },
    "nitrite": {
        "code": "50558-6",
        "system": "http://loinc.org",
        "display": "Nitrite [Presence] in Urine by Automated test strip",
        "normalized_text": "Nitrite",
        "confidence": "high",
        "note": "Also used: 5802-4 (urine test strip, 1.4K)"
    },
    "nitrites": {
        "code": "50558-6",
        "system": "http://loinc.org",
        "display": "Nitrite [Presence] in Urine by Automated test strip",
        "normalized_text": "Nitrite",
        "confidence": "high",
        "note": "Plural variation of nitrite - maps to same LOINC code"
    },
    "specific gravity": {
        "code": "53326-5",
        "system": "http://loinc.org",
        "display": "Specific gravity of Urine by Automated test strip",
        "normalized_text": "Specific Gravity",
        "confidence": "high",
        "note": "Also used: 5811-5 (1.5K), 2965-2 (377)"
    },
    "color": {
        "code": "5778-6",
        "system": "http://loinc.org",
        "display": "Color of Urine",
        "normalized_text": "Color",
        "confidence": "high",
        "note": "Also used: 9796-4 (stone color, 213)"
    },
    "clarity": {
        "code": "32167-9",
        "system": "http://loinc.org",
        "display": "Clarity of Urine",
        "normalized_text": "Clarity",
        "confidence": "high"
    },
    "bilirubin": {
        "code": "53327-3",
        "system": "http://loinc.org",
        "display": "Bilirubin.total [Mass/volume] in Urine by Automated test strip",
        "normalized_text": "Bilirubin",
        "confidence": "high",
        "note": "Also used: 5770-3 (2K), 74434-2 (273)"
    },
    "ketone": {
        "code": "50557-8",
        "system": "http://loinc.org",
        "display": "Ketones [Mass/volume] in Urine by Automated test strip",
        "normalized_text": "Ketone",
        "confidence": "high"
    },
    "urobilinogen": {
        "code": "50563-6",
        "system": "http://loinc.org",
        "display": "Urobilinogen [Mass/volume] in Urine by Automated test strip",
        "normalized_text": "Urobilinogen",
        "confidence": "high"
    },
    "blood": {
        "code": "50559-4",
        "system": "http://loinc.org",
        "display": "Hemoglobin [Mass/volume] in Urine by Automated test strip",
        "normalized_text": "Blood",
        "confidence": "high"
    },
    "ph": {
        "code": "50560-2",  # Updated from 5803-2 based on validation
        "system": "http://loinc.org",
        "display": "pH of Urine by Test strip",
        "normalized_text": "pH",
        "confidence": "high",
        "note": "Validated against actual data"
    },
    # PSA/Testosterone tests
    # NOTE: Word order and punctuation matter! "PSA, total" vs "PSA total" use different codes
    # All variations mapping to code 2857-1 get normalized_text "PSA, total" (most common form)
    # All variations mapping to code 35741-8 get normalized_text "PSA total" (ultrasensitive)
    "psa": {
        "code": "2857-1",
        "system": "http://loinc.org",
        "display": "Prostate specific Ag [Mass/volume] in Serum or Plasma",
        "normalized_text": "PSA, total",
        "confidence": "high",
        "note": "Standard PSA. Decision: Use 2857-1 (standard) not 35741-8 (ultrasensitive)"
    },
    "psa, total": {
        "code": "2857-1",
        "system": "http://loinc.org",
        "display": "Prostate specific Ag [Mass/volume] in Serum or Plasma",
        "normalized_text": "PSA, total",
        "confidence": "high",
        "note": "Standard PSA test. 99.75% code coverage. Most common: 65.72% usage"
    },
    "psa total": {
        "code": "2857-1",
        "system": "http://loinc.org",
        "display": "Prostate specific Ag [Mass/volume] in Serum or Plasma",
        "normalized_text": "PSA, total",
        "confidence": "high",
        "note": "FIXED: Changed to standard PSA (2857-1). Ultrasensitive PSA (35741-8) will be preserved if already in HealthLake code.coding array."
    },
    "total psa": {
        "code": "2857-1",
        "system": "http://loinc.org",
        "display": "Prostate specific Ag [Mass/volume] in Serum or Plasma",
        "normalized_text": "PSA, total",
        "confidence": "high",
        "note": "Word order variation of 'PSA, total'. 0% code coverage - needs mapping (20,560 observations)"
    },
    "psa testosterone": {
        "code": "2857-1",
        "system": "http://loinc.org",
        "display": "Prostate specific Ag [Mass/volume] in Serum or Plasma",
        "normalized_text": "PSA, total",
        "confidence": "high",
        "note": "PSA test, not testosterone. 89,594 observations missing codes. All existing codes use 2857-1"
    },
    "psa_1": {
        "code": "2857-1",
        "system": "http://loinc.org",
        "display": "Prostate specific Ag [Mass/volume] in Serum or Plasma",
        "normalized_text": "PSA, total",
        "confidence": "high",
        "note": "Suffix '_1' suggests first PSA test. 86,123 observations missing codes"
    },
    "psa, free": {
        "code": "10886-0",
        "system": "http://loinc.org",
        "display": "Prostate Specific Ag Free [Mass/volume] in Serum or Plasma",
        "normalized_text": "PSA, free",
        "confidence": "high",
        "note": "Free PSA test. Most common: 54.13% usage"
    },
    "free psa": {
        "code": "10886-0",
        "system": "http://loinc.org",
        "display": "Prostate Specific Ag Free [Mass/volume] in Serum or Plasma",
        "normalized_text": "PSA, free",
        "confidence": "high",
        "note": "Word order variation of 'PSA, free'. 0.02% code coverage - needs mapping (5,405 observations)"
    },
    "psa free": {
        "code": "10886-0",
        "system": "http://loinc.org",
        "display": "Prostate Specific Ag Free [Mass/volume] in Serum or Plasma",
        "normalized_text": "PSA, free",
        "confidence": "high",
        "note": "Punctuation variation of 'PSA, free'. 0% code coverage"
    },
    "psa, % free": {
        "code": "12841-3",
        "system": "http://loinc.org",
        "display": "Prostate Specific Ag Free/Prostate specific Ag.total in Serum or Plasma",
        "normalized_text": "PSA, % free",
        "confidence": "high",
        "note": "PSA free percentage. Most common: 66.09% usage"
    },
    "% free psa": {
        "code": "12841-3",
        "system": "http://loinc.org",
        "display": "Prostate Specific Ag Free/Prostate specific Ag.total in Serum or Plasma",
        "normalized_text": "PSA, % free",
        "confidence": "high",
        "note": "Word order variation of 'PSA, % free'. 92.87% code coverage"
    },
    "free psa %": {
        "code": "12841-3",
        "system": "http://loinc.org",
        "display": "Prostate Specific Ag Free/Prostate specific Ag.total in Serum or Plasma",
        "normalized_text": "PSA, % free",
        "confidence": "high",
        "note": "Word order variation"
    },
    "psa free %": {
        "code": "12841-3",
        "system": "http://loinc.org",
        "display": "Prostate Specific Ag Free/Prostate specific Ag.total in Serum or Plasma",
        "normalized_text": "PSA, % free",
        "confidence": "high",
        "note": "Punctuation variation"
    },
    "psa ratio": {
        "code": "12841-3",
        "system": "http://loinc.org",
        "display": "Prostate Specific Ag Free/Prostate specific Ag.total in Serum or Plasma",
        "normalized_text": "PSA, % free",
        "confidence": "medium",
        "note": "PSA ratio. 5,332 observations missing codes. May need different code for ratio vs percentage"
    },
    "prostate specific ag": {
        "code": "2857-1",
        "system": "http://loinc.org",
        "display": "Prostate specific Ag [Mass/volume] in Serum or Plasma",
        "normalized_text": "PSA, total",
        "confidence": "high",
        "note": "Full name for PSA. Most common: 75.66% usage"
    },
    "prostate specific antigen": {
        "code": "2857-1",
        "system": "http://loinc.org",
        "display": "Prostate specific Ag [Mass/volume] in Serum or Plasma",
        "normalized_text": "PSA, total",
        "confidence": "high",
        "note": "Full name variant. Most common: 71.19% usage"
    },
    "prostate specific ag, serum": {
        "code": "2857-1",
        "system": "http://loinc.org",
        "display": "Prostate specific Ag [Mass/volume] in Serum or Plasma",
        "normalized_text": "PSA, total",
        "confidence": "high"
    },
    "psa, ultrasensitive": {
        "code": "35741-8",
        "system": "http://loinc.org",
        "display": "Prostate specific Ag [Mass/volume] in Serum or Plasma by Detection limit ≤ 0.01 ng/mL",
        "normalized_text": "PSA total",
        "confidence": "high",
        "note": "Ultrasensitive PSA for post-treatment monitoring"
    },
    "psa, icma": {
        "code": "35741-8",
        "system": "http://loinc.org",
        "display": "Prostate specific Ag [Mass/volume] in Serum or Plasma by Detection limit ≤ 0.01 ng/mL",
        "normalized_text": "PSA total",
        "confidence": "high",
        "note": "ICMA method - ultrasensitive"
    },
    "psa, hama": {
        "code": "35741-8",
        "system": "http://loinc.org",
        "display": "Prostate specific Ag [Mass/volume] in Serum or Plasma by Detection limit ≤ 0.01 ng/mL",
        "normalized_text": "PSA total",
        "confidence": "high",
        "note": "HAMA method - ultrasensitive"
    },
    "psa, complexed": {
        "code": "33667-7",
        "system": "http://loinc.org",
        "display": "Prostate specific Ag.protein bound [Mass/volume] in Serum or Plasma",
        "normalized_text": "PSA, complexed",
        "confidence": "high",
        "note": "Complexed (protein-bound) PSA"
    },
    "testosterone_1": {
        "code": "2986-8",
        "system": "http://loinc.org",
        "display": "Testosterone [Mass/volume] in Serum or Plasma",
        "normalized_text": "Testosterone, total",
        "confidence": "high"
    },
    "testosterone": {
        "code": "2986-8",
        "system": "http://loinc.org",
        "display": "Testosterone [Mass/volume] in Serum or Plasma",
        "normalized_text": "Testosterone, total",
        "confidence": "high"
    },
    "testosterone, total": {
        "code": "2986-8",
        "system": "http://loinc.org",
        "display": "Testosterone [Mass/volume] in Serum or Plasma",
        "normalized_text": "Testosterone, total",
        "confidence": "high"
    },
    "testosterone, total, lc/ms/ms": {
        "code": "2986-8",
        "system": "http://loinc.org",
        "display": "Testosterone [Mass/volume] in Serum or Plasma",
        "normalized_text": "Testosterone, total",
        "confidence": "high"
    },
    "testosterone, total, lc/ms": {
        "code": "2986-8",
        "system": "http://loinc.org",
        "display": "Testosterone [Mass/volume] in Serum or Plasma",
        "normalized_text": "Testosterone, total",
        "confidence": "high"
    },
    "testosterone, total, ms": {
        "code": "2986-8",
        "system": "http://loinc.org",
        "display": "Testosterone [Mass/volume] in Serum or Plasma",
        "normalized_text": "Testosterone, total",
        "confidence": "high"
    },
    "testosterone, tot.,s.": {
        "code": "2986-8",
        "system": "http://loinc.org",
        "display": "Testosterone [Mass/volume] in Serum or Plasma",
        "normalized_text": "Testosterone, total",
        "confidence": "high"
    },
    "free testosterone": {
        "code": "2991-8",
        "system": "http://loinc.org",
        "display": "Testosterone Free [Mass/volume] in Serum or Plasma",
        "normalized_text": "Testosterone, free",
        "confidence": "high",
        "note": "Different from 2985-0 - 2991-8 is more common in our data"
    },
    "testosterone, free": {
        "code": "2991-8",
        "system": "http://loinc.org",
        "display": "Testosterone Free [Mass/volume] in Serum or Plasma",
        "normalized_text": "Testosterone, free",
        "confidence": "high"
    },
    "calculated free testosterone_1": {
        "code": "2991-8",
        "system": "http://loinc.org",
        "display": "Testosterone Free [Mass/volume] in Serum or Plasma",
        "normalized_text": "Testosterone, free",
        "confidence": "medium",
        "note": "May need separate code for calculated vs measured"
    },
    "calculated_free_testosterone_1": {
        "code": "2991-8",
        "system": "http://loinc.org",
        "display": "Testosterone Free [Mass/volume] in Serum or Plasma",
        "normalized_text": "Testosterone, free",
        "confidence": "high",
        "note": "Calculated free testosterone with numeric suffix. Numeric suffix normalization should handle this, but adding explicit mapping as backup."
    },
    "calculated_free_testosterone": {
        "code": "2991-8",
        "system": "http://loinc.org",
        "display": "Testosterone Free [Mass/volume] in Serum or Plasma",
        "normalized_text": "Testosterone, free",
        "confidence": "high",
        "note": "Calculated free testosterone (after suffix normalization)"
    },
    "testosterone,bioavailable": {
        "code": "2990-0",
        "system": "http://loinc.org",
        "display": "Testosterone.free+weakly bound [Mass/volume] in Serum or Plasma",
        "normalized_text": "Testosterone, bioavailable",
        "confidence": "high"
    },
    "free testosterone %": {
        "code": "15432-8",
        "system": "http://loinc.org",
        "display": "TESTOSTERONE.FREE/TESTOSTERONE.TOTAL",
        "normalized_text": "Free Testosterone %",
        "confidence": "high",
        "note": "Free/Total testosterone ratio (percentage). Code exists in database."
    },
    "free testosterone%": {
        "code": "15432-8",
        "system": "http://loinc.org",
        "display": "TESTOSTERONE.FREE/TESTOSTERONE.TOTAL",
        "normalized_text": "Free Testosterone %",
        "confidence": "high",
        "note": "Punctuation variation (no space before %)"
    },
    # SHBG (Sex Hormone Binding Globulin)
    "shbg": {
        "code": "2942-1",
        "system": "http://loinc.org",
        "display": "Sex hormone binding globulin [Mass/volume] in Serum or Plasma",
        "normalized_text": "SHBG",
        "confidence": "high",
        "note": "Code exists in database. Suffix normalization will handle shbg_1"
    },
    "sex hormone binding globulin": {
        "code": "2942-1",
        "system": "http://loinc.org",
        "display": "Sex hormone binding globulin [Mass/volume] in Serum or Plasma",
        "normalized_text": "SHBG",
        "confidence": "high"
    },
    # FISH Analysis (Fluorescence In Situ Hybridization)
    "fish analysis": {
        "code": "56030-0",
        "system": "http://loinc.org",
        "display": "Karyotype [Identifier] in Urine by FISH Narrative",
        "normalized_text": "FISH Analysis",
        "confidence": "medium",
        "note": "FISH is a method - code may vary by specific test type. Code exists in database. Review actual test types for refinement."
    },
    "fish": {
        "code": "56030-0",
        "system": "http://loinc.org",
        "display": "Karyotype [Identifier] in Urine by FISH Narrative",
        "normalized_text": "FISH Analysis",
        "confidence": "medium",
        "note": "Generic FISH - may need refinement based on actual test type"
    },
    # Other common tests
    "urine culture": {
        "code": "630-4",
        "system": "http://loinc.org",
        "display": "Bacteria identified in Urine by Culture",
        "normalized_text": "Urine Culture",
        "confidence": "medium"
    },
    # Common lab tests - Basic Metabolic Panel and Comprehensive Metabolic Panel
    "albumin": {
        "code": "1751-7",
        "system": "http://loinc.org",
        "display": "Albumin [Mass/volume] in Serum or Plasma",
        "normalized_text": "Albumin",
        "confidence": "high",
        "note": "Most common LOINC code for albumin in serum/plasma"
    },
    "calcium": {
        "code": "17861-6",
        "system": "http://loinc.org",
        "display": "Calcium [Mass/volume] in Serum or Plasma",
        "normalized_text": "Calcium",
        "confidence": "high",
        "note": "Most common LOINC code for calcium in serum/plasma"
    },
    "creatinine": {
        "code": "2160-0",
        "system": "http://loinc.org",
        "display": "Creatinine [Mass/volume] in Serum or Plasma",
        "normalized_text": "Creatinine",
        "confidence": "high",
        "note": "Most common LOINC code for creatinine in serum/plasma"
    },
    "sodium": {
        "code": "2951-2",
        "system": "http://loinc.org",
        "display": "Sodium [Moles/volume] in Serum or Plasma",
        "normalized_text": "Sodium",
        "confidence": "high",
        "note": "Most common LOINC code for sodium in serum/plasma"
    },
    "potassium": {
        "code": "2823-3",
        "system": "http://loinc.org",
        "display": "Potassium [Moles/volume] in Serum or Plasma",
        "normalized_text": "Potassium",
        "confidence": "high",
        "note": "Most common LOINC code for potassium in serum/plasma"
    },
    "hemoglobin": {
        "code": "718-7",
        "system": "http://loinc.org",
        "display": "Hemoglobin [Mass/volume] in Blood",
        "normalized_text": "Hemoglobin",
        "confidence": "high",
        "note": "Most common LOINC code for hemoglobin"
    },
    "hematocrit": {
        "code": "4544-3",
        "system": "http://loinc.org",
        "display": "Hematocrit [Volume Fraction] of Blood by Automated count",
        "normalized_text": "Hematocrit",
        "confidence": "high",
        "note": "Most common LOINC code for hematocrit"
    },
    "ast": {
        "code": "1920-8",
        "system": "http://loinc.org",
        "display": "Aspartate aminotransferase [Enzymatic activity/volume] in Serum or Plasma",
        "normalized_text": "AST",
        "confidence": "high",
        "note": "AST (Aspartate aminotransferase) - also known as SGOT"
    },
    "alt": {
        "code": "1742-6",
        "system": "http://loinc.org",
        "display": "Alanine aminotransferase [Enzymatic activity/volume] in Serum or Plasma",
        "normalized_text": "ALT",
        "confidence": "high",
        "note": "ALT (Alanine aminotransferase) - also known as SGPT"
    },
    "globulin": {
        "code": "2339-0",
        "system": "http://loinc.org",
        "display": "Globulin [Mass/volume] in Serum or Plasma",
        "normalized_text": "Globulin",
        "confidence": "high",
        "note": "Most common LOINC code for globulin"
    },
    "estradiol": {
        "code": "2013-7",
        "system": "http://loinc.org",
        "display": "Estradiol [Mass/volume] in Serum or Plasma",
        "normalized_text": "Estradiol",
        "confidence": "high",
        "note": "Most common LOINC code for estradiol"
    },
    "bilirubin, total": {
        "code": "1975-2",
        "system": "http://loinc.org",
        "display": "Bilirubin.total [Mass/volume] in Serum or Plasma",
        "normalized_text": "Bilirubin, total",
        "confidence": "high",
        "note": "Total bilirubin in serum/plasma"
    },
    "bilirubin total": {
        "code": "1975-2",
        "system": "http://loinc.org",
        "display": "Bilirubin.total [Mass/volume] in Serum or Plasma",
        "normalized_text": "Bilirubin, total",
        "confidence": "high"
    },
    "total bilirubin": {
        "code": "1975-2",
        "system": "http://loinc.org",
        "display": "Bilirubin.total [Mass/volume] in Serum or Plasma",
        "normalized_text": "Bilirubin, total",
        "confidence": "high"
    },
    # Additional common lab abbreviations
    "rbc": {
        "code": "789-8",
        "system": "http://loinc.org",
        "display": "Erythrocytes [#/volume] in Blood by Automated count",
        "normalized_text": "RBC",
        "confidence": "high",
        "note": "Red blood cell count"
    },
    "wbc": {
        "code": "6690-2",
        "system": "http://loinc.org",
        "display": "Leukocytes [#/volume] in Blood by Automated count",
        "normalized_text": "WBC",
        "confidence": "high",
        "note": "White blood cell count"
    },
    "platelet count": {
        "code": "777-3",
        "system": "http://loinc.org",
        "display": "Platelets [#/volume] in Blood by Automated count",
        "normalized_text": "Platelet Count",
        "confidence": "high"
    },
    "rdw": {
        "code": "788-0",
        "system": "http://loinc.org",
        "display": "Erythrocyte distribution width [Ratio] by Automated count",
        "normalized_text": "RDW",
        "confidence": "high",
        "note": "Red cell distribution width"
    },
    "hct": {
        "code": "4544-3",
        "system": "http://loinc.org",
        "display": "Hematocrit [Volume Fraction] of Blood by Automated count",
        "normalized_text": "Hematocrit",
        "confidence": "high",
        "note": "HCT abbreviation for hematocrit"
    },
    "mcv": {
        "code": "787-2",
        "system": "http://loinc.org",
        "display": "Erythrocyte mean corpuscular volume [Entitic volume] by Automated count",
        "normalized_text": "MCV",
        "confidence": "high",
        "note": "Mean corpuscular volume"
    },
    "mch": {
        "code": "785-6",
        "system": "http://loinc.org",
        "display": "Erythrocyte mean corpuscular hemoglobin [Entitic mass] by Automated count",
        "normalized_text": "MCH",
        "confidence": "high",
        "note": "Mean corpuscular hemoglobin"
    },
    "mchc": {
        "code": "786-4",
        "system": "http://loinc.org",
        "display": "Erythrocyte mean corpuscular hemoglobin concentration [Mass/volume] by Automated count",
        "normalized_text": "MCHC",
        "confidence": "high",
        "note": "Mean corpuscular hemoglobin concentration"
    },
    "basos": {
        "code": "770-8",
        "system": "http://loinc.org",
        "display": "Basophils [#/volume] in Blood by Automated count",
        "normalized_text": "Basophils",
        "confidence": "high",
        "note": "Basophil count"
    },
    "e-gfr, african american": {
        "code": "48642-3",
        "system": "http://loinc.org",
        "display": "Glomerular filtration rate/1.73 sq M.predicted [Volume Rate/Area] in Serum, Plasma or Blood by Creatinine-based formula (MDRD) adjusted for African American",
        "normalized_text": "eGFR, African American",
        "confidence": "high",
        "note": "eGFR adjusted for African American"
    },
    "ldl/hdl ratio": {
        "code": "2085-9",
        "system": "http://loinc.org",
        "display": "Cholesterol in LDL [Mass/volume] in Serum or Plasma",
        "normalized_text": "LDL/HDL Ratio",
        "confidence": "medium",
        "note": "LDL to HDL ratio - may need separate calculation"
    },
    "non-hdl cholesterol": {
        "code": "2089-1",
        "system": "http://loinc.org",
        "display": "Cholesterol non-HDL [Mass/volume] in Serum or Plasma",
        "normalized_text": "Non-HDL Cholesterol",
        "confidence": "high"
    },
    "vldl, calculated": {
        "code": "2088-3",
        "system": "http://loinc.org",
        "display": "Cholesterol in VLDL [Mass/volume] in Serum or Plasma",
        "normalized_text": "VLDL, calculated",
        "confidence": "high",
        "note": "VLDL cholesterol calculated"
    },
    # Common lab test abbreviations (high priority - from synthetic codes analysis)
    "platelet": {
        "code": "777-3",
        "system": "http://loinc.org",
        "display": "Platelets [#/volume] in Blood by Automated count",
        "normalized_text": "Platelet",
        "confidence": "high",
        "note": "Abbreviation for platelet count"
    },
    "hgb": {
        "code": "718-7",
        "system": "http://loinc.org",
        "display": "Hemoglobin [Mass/volume] in Blood",
        "normalized_text": "Hemoglobin",
        "confidence": "high",
        "note": "Abbreviation for hemoglobin (HGB)"
    },
    "rdw_cv": {
        "code": "788-0",
        "system": "http://loinc.org",
        "display": "Erythrocyte distribution width [Ratio] by Automated count",
        "normalized_text": "RDW",
        "confidence": "high",
        "note": "RDW-CV (coefficient of variation) - maps to standard RDW code"
    },
    "bun": {
        "code": "3094-0",
        "system": "http://loinc.org",
        "display": "Urea nitrogen [Mass/volume] in Serum or Plasma",
        "normalized_text": "Urea Nitrogen",
        "confidence": "high",
        "note": "BUN (Blood Urea Nitrogen) abbreviation"
    },
    "co2": {
        "code": "2028-9",
        "system": "http://loinc.org",
        "display": "Carbon dioxide, total [Moles/volume] in Serum or Plasma",
        "normalized_text": "Carbon Dioxide",
        "confidence": "high",
        "note": "CO2 (carbon dioxide) abbreviation"
    },
    "chloride": {
        "code": "2075-0",
        "system": "http://loinc.org",
        "display": "Chloride [Moles/volume] in Serum or Plasma",
        "normalized_text": "Chloride",
        "confidence": "high",
        "note": "Chloride in serum/plasma"
    },
    "alk_phos": {
        "code": "6768-6",
        "system": "http://loinc.org",
        "display": "Alkaline phosphatase [Enzymatic activity/volume] in Serum or Plasma",
        "normalized_text": "Alkaline Phosphatase",
        "confidence": "high",
        "note": "Alkaline phosphatase abbreviation"
    },
    "e_gfr": {
        "code": "33914-3",
        "system": "http://loinc.org",
        "display": "Glomerular filtration rate [Volume Rate/Area] in Serum or Plasma by Creatinine-based formula (MDRD)/1.73 sq M",
        "normalized_text": "eGFR",
        "confidence": "high",
        "note": "eGFR (estimated Glomerular Filtration Rate) - using MDRD formula as default"
    },
    "egfr": {
        "code": "33914-3",
        "system": "http://loinc.org",
        "display": "Glomerular filtration rate [Volume Rate/Area] in Serum or Plasma by Creatinine-based formula (MDRD)/1.73 sq M",
        "normalized_text": "eGFR",
        "confidence": "high",
        "note": "eGFR without underscore - alternative spelling"
    },
    "mean_platelet_vol": {
        "code": "32623-1",
        "system": "http://loinc.org",
        "display": "Platelet [Entitic mean volume] in Blood by Automated count",
        "normalized_text": "Mean Platelet Volume",
        "confidence": "high",
        "note": "MPV (Mean Platelet Volume)"
    },
    "a_g_ratio": {
        "code": "1759-0",
        "system": "http://loinc.org",
        "display": "Albumin/Globulin [Mass Ratio] in Serum or Plasma",
        "normalized_text": "Albumin/Globulin Ratio",
        "confidence": "high",
        "note": "A/G ratio (Albumin to Globulin ratio). FIXED: Changed from 1751-7 (Albumin) to 1759-0 (Albumin/Globulin Ratio)"
    },
    "total_protein": {
        "code": "2885-2",
        "system": "http://loinc.org",
        "display": "Protein [Mass/volume] in Serum or Plasma",
        "normalized_text": "Total Protein",
        "confidence": "high",
        "note": "Total protein in serum/plasma"
    },
    "bun_creat_ratio": {
        "code": "3097-3",
        "system": "http://loinc.org",
        "display": "Urea nitrogen/Creatinine [Mass Ratio] in Serum or Plasma",
        "normalized_text": "BUN/Creatinine Ratio",
        "confidence": "high",
        "note": "BUN to Creatinine ratio"
    },
    # CBC differential components (high priority - from synthetic codes analysis)
    "neutrophil_%": {
        "code": "770-8",
        "system": "http://loinc.org",
        "display": "Neutrophils/100 leukocytes in Blood by Automated count",
        "normalized_text": "Neutrophil %",
        "confidence": "high",
        "note": "Neutrophil percentage in CBC differential"
    },
    "lymphocyte_%": {
        "code": "736-9",
        "system": "http://loinc.org",
        "display": "Lymphocytes/Leukocytes in Blood by Automated count",
        "normalized_text": "Lymphocyte %",
        "confidence": "high",
        "note": "Lymphocyte percentage in CBC differential"
    },
    "monocyte_%": {
        "code": "5905-5",
        "system": "http://loinc.org",
        "display": "Monocytes/Leukocytes in Blood by Automated count",
        "normalized_text": "Monocyte %",
        "confidence": "high",
        "note": "Monocyte percentage in CBC differential"
    },
    "eosinophil_%": {
        "code": "711-2",
        "system": "http://loinc.org",
        "display": "Eosinophils [#/volume] in Blood by Automated count",
        "normalized_text": "Eosinophil %",
        "confidence": "high",
        "note": "Eosinophil percentage in CBC differential"
    },
    "basophil_%": {
        "code": "706-2",
        "system": "http://loinc.org",
        "display": "Basophils/100 leukocytes in Blood by Automated count",
        "normalized_text": "Basophil %",
        "confidence": "high",
        "note": "Basophil percentage in CBC differential"
    },
    "immature_granulocyte_%": {
        "code": "38518-7",
        "system": "http://loinc.org",
        "display": "Immature granulocytes/Leukocytes in Blood",
        "normalized_text": "Immature Granulocyte %",
        "confidence": "high",
        "note": "Immature granulocyte percentage in CBC differential"
    },
    "neutrophil_#": {
        "code": "770-8",
        "system": "http://loinc.org",
        "display": "Neutrophils [#/volume] in Blood by Automated count",
        "normalized_text": "Neutrophil #",
        "confidence": "high",
        "note": "Neutrophil absolute count in CBC differential"
    },
    "lymphocyte_#": {
        "code": "731-0",
        "system": "http://loinc.org",
        "display": "Lymphocytes [#/volume] in Blood by Automated count",
        "normalized_text": "Lymphocyte #",
        "confidence": "high",
        "note": "Lymphocyte absolute count in CBC differential"
    },
    "monocyte_#": {
        "code": "742-7",
        "system": "http://loinc.org",
        "display": "Monocytes [#/volume] in Blood by Automated count",
        "normalized_text": "Monocyte #",
        "confidence": "high",
        "note": "Monocyte absolute count in CBC differential"
    },
    "basophil_#": {
        "code": "704-7",
        "system": "http://loinc.org",
        "display": "Basophils [#/volume] in Blood by Automated count",
        "normalized_text": "Basophil #",
        "confidence": "high",
        "note": "Basophil absolute count in CBC differential"
    },
    "immature_granulocyte_#": {
        "code": "51584-1",
        "system": "http://loinc.org",
        "display": "Immature granulocytes [#/volume] in Blood",
        "normalized_text": "Immature Granulocyte #",
        "confidence": "high",
        "note": "Immature granulocyte absolute count in CBC differential"
    },
    # Typo corrections (high priority - from synthetic codes analysis)
    "appearnace": {
        "code": "5767-9",
        "system": "http://loinc.org",
        "display": "Appearance of Urine",
        "normalized_text": "Appearance",
        "confidence": "high",
        "note": "Typo correction: appearnace -> appearance (urine appearance)"
    },
    "leudocytes": {
        "code": "6690-2",
        "system": "http://loinc.org",
        "display": "Leukocytes [#/volume] in Blood by Automated count",
        "normalized_text": "Leukocytes",
        "confidence": "high",
        "note": "Typo correction: leudocytes -> leukocytes"
    },
    "nitrate": {
        "code": "50558-6",
        "system": "http://loinc.org",
        "display": "Nitrite [Presence] in Urine by Automated test strip",
        "normalized_text": "Nitrite",
        "confidence": "high",
        "note": "Common confusion: nitrate vs nitrite in urinalysis - maps to nitrite LOINC code"
    },
    # Antibiotic Susceptibility Tests (Phase 2 - from HealthLake data analysis)
    # These are confirmed to be antibiotic susceptibility tests based on HealthLake structure:
    # - valueString contains "S, <=X" (Susceptible), "R, >=X" (Resistant), "I, X" (Intermediate)
    # - interpretation code "N" (Normal/Susceptible)
    # - All are laboratory category observations
    "ampicillin": {
        "code": "28-1",
        "system": "http://loinc.org",
        "display": "Ampicillin [Susceptibility] by Minimum inhibitory concentration (MIC)",
        "normalized_text": "Ampicillin [Susceptibility]",
        "confidence": "high",
        "note": "Antibiotic susceptibility test by MIC. Code verified in database."
    },
    "levofloxacin": {
        "code": "20396-8",
        "system": "http://loinc.org",
        "display": "levoFLOXacin [Susceptibility] by Minimum inhibitory concentration (MIC)",
        "normalized_text": "Levofloxacin [Susceptibility]",
        "confidence": "high",
        "note": "Antibiotic susceptibility test by MIC. Code verified in database."
    },
    "ciprofloxacin": {
        "code": "185-9",
        "system": "http://loinc.org",
        "display": "Ciprofloxacin [Susceptibility] by Minimum inhibitory concentration (MIC)",
        "normalized_text": "Ciprofloxacin [Susceptibility]",
        "confidence": "high",
        "note": "Antibiotic susceptibility test by MIC. Code verified in database."
    },
    "nitrofurantoin": {
        "code": "18863-1",
        "system": "http://loinc.org",
        "display": "Nitrofurantoin [Susceptibility] by Minimum inhibitory concentration (MIC)",
        "normalized_text": "Nitrofurantoin [Susceptibility]",
        "confidence": "high",
        "note": "Antibiotic susceptibility test by MIC. Standard LOINC code for nitrofurantoin susceptibility."
    },
    "tobramycin": {
        "code": "18868-0",
        "system": "http://loinc.org",
        "display": "Tobramycin [Susceptibility] by Minimum inhibitory concentration (MIC)",
        "normalized_text": "Tobramycin [Susceptibility]",
        "confidence": "high",
        "note": "Antibiotic susceptibility test by MIC. Standard LOINC code for tobramycin susceptibility."
    },
    "trimethoprim_sulfamethoxazole": {
        "code": "516-5",
        "system": "http://loinc.org",
        "display": "Trimethoprim+Sulfamethoxazole [Susceptibility] by Minimum inhibitory concentration (MIC)",
        "normalized_text": "Trimethoprim/Sulfamethoxazole [Susceptibility]",
        "confidence": "high",
        "note": "Antibiotic susceptibility test by MIC. Code verified in database. Also known as TMP/SMX or Bactrim."
    },
    "cefepime": {
        "code": "6644-9",
        "system": "http://loinc.org",
        "display": "Cefepime [Susceptibility] by Minimum inhibitory concentration (MIC)",
        "normalized_text": "Cefepime [Susceptibility]",
        "confidence": "high",
        "note": "Antibiotic susceptibility test by MIC. Code verified in database."
    },
    "cefuroxime": {
        "code": "18867-2",
        "system": "http://loinc.org",
        "display": "Cefuroxime [Susceptibility] by Minimum inhibitory concentration (MIC)",
        "normalized_text": "Cefuroxime [Susceptibility]",
        "confidence": "high",
        "note": "Antibiotic susceptibility test by MIC. Standard LOINC code for cefuroxime susceptibility."
    },
    "meropenem": {
        "code": "18956-7",
        "system": "http://loinc.org",
        "display": "Meropenem [Susceptibility] by Minimum inhibitory concentration (MIC)",
        "normalized_text": "Meropenem [Susceptibility]",
        "confidence": "high",
        "note": "Antibiotic susceptibility test by MIC. Standard LOINC code for meropenem susceptibility."
    },
    "cefuroxime_axetil": {
        "code": "18867-2",
        "system": "http://loinc.org",
        "display": "Cefuroxime [Susceptibility] by Minimum inhibitory concentration (MIC)",
        "normalized_text": "Cefuroxime [Susceptibility]",
        "confidence": "high",
        "note": "Cefuroxime axetil is the oral prodrug of cefuroxime. Maps to same LOINC code as cefuroxime."
    },
    "amoxicillin_clavulanic_acid": {
        "code": "20-8",
        "system": "http://loinc.org",
        "display": "Amoxicillin+Clavulanate [Susceptibility] by Minimum inhibitory concentration (MIC)",
        "normalized_text": "Amoxicillin/Clavulanic Acid [Susceptibility]",
        "confidence": "high",
        "note": "Antibiotic susceptibility test by MIC. Code verified in database. Also known as Augmentin."
    },
    "ertapenem": {
        "code": "35801-0",
        "system": "http://loinc.org",
        "display": "Ertapenem [Susceptibility] by Minimum inhibitory concentration (MIC)",
        "normalized_text": "Ertapenem [Susceptibility]",
        "confidence": "high",
        "note": "Antibiotic susceptibility test by MIC. Code verified in database."
    },
    "cefpodoxime": {
        "code": "18866-4",
        "system": "http://loinc.org",
        "display": "Cefpodoxime [Susceptibility] by Minimum inhibitory concentration (MIC)",
        "normalized_text": "Cefpodoxime [Susceptibility]",
        "confidence": "high",
        "note": "Antibiotic susceptibility test by MIC. Standard LOINC code for cefpodoxime susceptibility."
    },
    "ceftriaxone": {
        "code": "18869-8",
        "system": "http://loinc.org",
        "display": "Ceftriaxone [Susceptibility] by Minimum inhibitory concentration (MIC)",
        "normalized_text": "Ceftriaxone [Susceptibility]",
        "confidence": "high",
        "note": "Antibiotic susceptibility test by MIC. Standard LOINC code for ceftriaxone susceptibility."
    },
    # Generic Terms (Phase 2.2 - from HealthLake data analysis)
    # Social History and Demographics
    "marital_status": {
        "code": "45404-1",
        "system": "http://loinc.org",
        "display": "Marital status",
        "normalized_text": "Marital Status",
        "confidence": "high",
        "note": "Social determinant of health. Code verified in database. Standard LOINC code for marital status."
    },
    # Pathology/Procedure Reports
    "prostate_biopsy": {
        "code": "66117-3",
        "system": "http://loinc.org",
        "display": "Prostate Pathology biopsy report",
        "normalized_text": "Prostate Biopsy",
        "confidence": "high",
        "note": "Prostate pathology biopsy report. Code verified in database. Laboratory category observation."
    },
    "surgical_biopsy": {
        "code": "52121-1",
        "system": "http://loinc.org",
        "display": "Biopsy [Interpretation] in Specimen Narrative",
        "normalized_text": "Surgical Biopsy",
        "confidence": "high",
        "note": "Generic biopsy interpretation code. Code verified in database. Laboratory category observation."
    }
    # Note: The following terms are appropriately kept as synthetic codes:
    # - education: No standard LOINC code found. Typically in demographics/social history narrative.
    # - hiv_risk_factors: Assessment/screening question, not standard observation.
    # - abnormal_status: Interpretation qualifier, not a test itself.
    # - appearance: Already mapped to 5767-9 (urine) or 33511-7 (specimen).
    # - fish_analysis: Already mapped to 56030-0.
}

