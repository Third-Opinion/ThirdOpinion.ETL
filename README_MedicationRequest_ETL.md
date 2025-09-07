# MedicationRequest ETL Job

## Overview
This ETL job processes FHIR MedicationRequest resources from AWS HealthLake and loads them into Redshift tables. It follows the same pattern as the existing `condition_etl.py` but is specifically designed for MedicationRequest data.

## Key Features

### Data Extraction
- **Patient ID**: Extracted from `subject.reference` using regex pattern `Patient/(.+)`
- **Medication ID**: Extracted from `medicationReference.reference` using regex pattern `Medication/(.+)`
- **Medication Display**: Direct extraction from `medicationReference.display`
- **Extensions**: Ignored as requested

### Redshift Tables Created

#### 1. Main Table: `medication_requests`
```sql
CREATE TABLE public.medication_requests (
    medication_request_id VARCHAR(255) PRIMARY KEY,
    patient_id VARCHAR(255) NOT NULL,
    medication_id VARCHAR(255),
    medication_display VARCHAR(500),
    status VARCHAR(50),
    intent VARCHAR(50),
    reported_boolean BOOLEAN,
    authored_on TIMESTAMP,
    meta_version_id VARCHAR(50),
    meta_last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DISTKEY (patient_id) SORTKEY (patient_id, authored_on);
```

#### 2. Supporting Tables

**`medication_request_identifiers`**
- Stores identifier system/value pairs from the identifier array
- Columns: medication_request_id, identifier_system, identifier_value

**`medication_request_notes`**
- Stores notes and annotations from the note array
- Columns: medication_request_id, note_text, note_author_reference, note_time

**`medication_request_dosage_instructions`**
- Stores dosage instructions with timing, route, and dose information
- Columns: medication_request_id, dosage_text, dosage_timing_frequency, dosage_timing_period, dosage_timing_period_unit, dosage_route_code, dosage_route_system, dosage_route_display, dosage_dose_value, dosage_dose_unit, dosage_dose_system, dosage_dose_code, dosage_as_needed_boolean

## Configuration

### AWS Glue Settings
- **Database**: `hmu-healthlake-database`
- **Table**: `medicationrequest` (lowercase for Glue catalog)
- **Redshift Connection**: `Redshift connection`
- **S3 Temp Directory**: `s3://aws-glue-assets-442042533707-us-east-2/temporary/`

### Data Processing
- **Source**: AWS HealthLake via Glue Data Catalog
- **Target**: Redshift (4 tables)
- **Processing**: 7-step ETL process (Read → Transform → Convert → Resolve → Validate → Write)

## Key Transformations

### Main Data Transformation
```python
def transform_main_medication_request_data(df):
    select_columns = [
        F.col("id").alias("medication_request_id"),
        F.when(F.col("subject").isNotNull(), 
               F.regexp_extract(F.col("subject").getField("reference"), r"Patient/(.+)", 1)
              ).otherwise(None).alias("patient_id"),
        F.when(F.col("medicationReference").isNotNull(),
               F.regexp_extract(F.col("medicationReference").getField("reference"), r"Medication/(.+)", 1)
              ).otherwise(None).alias("medication_id"),
        F.col("medicationReference").getField("display").alias("medication_display"),
        F.col("status").alias("status"),
        F.col("intent").alias("intent"),
        F.col("reportedBoolean").alias("reported_boolean"),
        F.to_timestamp(F.col("authoredOn"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("authored_on"),
        F.col("meta").getField("versionId").alias("meta_version_id"),
        F.to_timestamp(F.col("meta").getField("lastUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("meta_last_updated"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    ]
```

### Multi-valued Data Handling
- **Identifiers**: Exploded from identifier array
- **Notes**: Exploded from note array with author and timestamp
- **Dosage Instructions**: Complex nested structure flattened with timing, route, and dose details

## Data Quality Features

### Validation
- NULL value analysis in key fields
- Critical field validation (medication_request_id, patient_id)
- Data type casting and schema validation
- Redshift compatibility checks

### Error Handling
- Graceful handling of missing columns
- Safe field extraction with fallbacks
- Comprehensive logging and error reporting
- Data quality metrics and statistics

## Usage

### Running the Job
```bash
# Deploy to AWS Glue and run with:
# Job name: medication-request-etl
# Python version: 3
# Glue version: 5.0 (Spark 3.5)
```

### Expected Output
- **Main Table**: Core medication request data with patient and medication references
- **Supporting Tables**: Normalized multi-valued data for identifiers, notes, and dosage instructions
- **Data Expansion**: Typically 1.5-3x expansion ratio depending on data complexity

## Differences from Condition ETL

1. **Simpler Structure**: Fewer complex nested fields than Condition resources
2. **No Extensions**: Extensions are ignored as requested
3. **Different Multi-valued Fields**: identifiers, notes, dosage instructions vs categories, codes, evidence
4. **Medication Reference**: Both ID and display extracted from medicationReference
5. **Dosage Instructions**: More complex nested structure with timing, route, dose information

## Monitoring and Logging

The job provides comprehensive logging including:
- Processing statistics and timing
- Data quality metrics
- Sample data previews
- Error handling and validation results
- Final ETL statistics and expansion ratios

## Performance Considerations

- **Distribution Key**: patient_id for optimal query performance
- **Sort Key**: patient_id, authored_on for time-based queries
- **Data Types**: Optimized for Redshift with proper casting
- **Batch Processing**: Handles large datasets efficiently with Spark
