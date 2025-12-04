# Medication Enrichment Architecture

## Overview

The medication enrichment system is organized into separate modules with clear responsibilities:

```
utils/
├── enrichment.py              # Core enrichment functions (API calls, rate limiters)
├── api_batch_enrichment.py    # Batch processing with rate limiting
└── lookup_table.py            # Lookup table management

transformations/
└── enrich_medication.py       # DataFrame-level transformation (orchestration)
```

## Module Responsibilities

### 1. `utils/enrichment.py` - Core Enrichment Functions
**Purpose**: Low-level API integration and rate limiting

**Key Components**:
- `RxNavRateLimiter`: Thread-safe rate limiter (20 req/sec)
- `normalize_medication_name()`: Normalize medication names for matching
- `lookup_rxnorm_code()`: RxNav API integration
- `enrich_with_comprehend_medical()`: AWS Comprehend Medical integration
- `enrich_medication_code()`: Main enrichment function with fallback chain

**Rate Limiting**:
- Global rate limiter instance
- Automatic rate limit enforcement
- Thread-safe for concurrent requests

### 2. `utils/api_batch_enrichment.py` - Batch Processing
**Purpose**: Batch API enrichment with error handling and progress tracking

**Key Components**:
- `enrich_medications_batch()`: Process multiple medications with rate limiting
- `prepare_medications_for_batch_enrichment()`: Prepare Spark Rows for batch processing

**Features**:
- Batch size limits (configurable, default 1000)
- Progress logging (every 50 successful, every 100 failures)
- Automatic deduplication
- Error handling per medication (continues on failure)
- Cache management

### 3. `transformations/enrich_medication.py` - DataFrame Transformation
**Purpose**: Orchestrate enrichment at the DataFrame level

**Key Components**:
- `enrich_medications_with_codes()`: Main transformation function
- `write_enrichment_results_to_lookup_table()`: Write results to Redshift
- `update_dataframe_with_enrichment()`: Update DataFrame with enriched codes

**Flow**:
1. Read lookup cache from Redshift
2. Enrich from cache (broadcast join)
3. Identify medications needing API enrichment
4. Call batch enrichment API
5. Write results to lookup table
6. Update DataFrame

### 4. `utils/lookup_table.py` - Lookup Table Management
**Purpose**: Manage the Redshift lookup table

**Key Components**:
- `ensure_lookup_table_exists()`: Create table if it doesn't exist

## Data Flow

```
1. Transform Medication DataFrame
   ↓
2. Check Lookup Cache (Redshift)
   ↓
3. Enrich from Cache (Fast, no API)
   ↓
4. Identify Missing Medications
   ↓
5. Batch API Enrichment
   ├─> Rate Limiter (20 req/sec)
   ├─> RxNav API
   └─> Comprehend Medical (fallback)
   ↓
6. Write to Lookup Table (Redshift)
   ↓
7. Update DataFrame
   ↓
8. Return Enriched DataFrame
```

## Rate Limiting Strategy

### RxNav API
- **Limit**: 20 requests per second
- **Implementation**: `RxNavRateLimiter` class
- **Location**: `utils/enrichment.py`
- **Mechanism**: Thread-safe deque with time window tracking

### Batch Processing
- **Batch Size**: Configurable (default 1000 per run)
- **Progress Logging**: Every 50 successful enrichments
- **Error Handling**: Continue on individual failures
- **Location**: `utils/api_batch_enrichment.py`

## Enrichment Modes

1. **"hybrid"** (default):
   - Check lookup cache
   - Try RxNav API
   - Fallback to Comprehend Medical

2. **"rxnav_only"**:
   - Check lookup cache
   - Try RxNav API only

3. **"comprehend_only"**:
   - Check lookup cache
   - Use Comprehend Medical only

## Caching Strategy

### Lookup Table (Redshift)
- **Purpose**: Persistent cache of enrichment results
- **Table**: `public.medication_code_lookup`
- **Key**: `normalized_name`
- **Benefits**: 
  - Fast lookups (no API calls)
  - Shared across ETL runs
  - Reduces API costs

### In-Memory Cache
- **Purpose**: Temporary cache during single run
- **Location**: Dictionary in memory
- **Lifetime**: Single ETL job execution
- **Benefits**: 
  - Avoids duplicate API calls in same run
  - Faster than Redshift lookups

## Error Handling

### Individual Medication Failures
- Log warning and continue
- Don't block other enrichments
- Track success/failure counts

### API Failures
- Rate limit exceeded: Automatic wait and retry
- HTTP errors: Log and skip
- Timeouts: Log and skip

### Database Failures
- Lookup table read: Continue without cache
- Lookup table write: Log warning, continue
- DataFrame updates: Log warning, continue

## Configuration

### Environment Variables
- `ENABLE_ENRICHMENT`: Enable/disable enrichment (default: `True`)
- `ENRICHMENT_MODE`: "hybrid", "rxnav_only", "comprehend_only" (default: `"hybrid"`)
- `USE_LOOKUP_TABLE`: Use lookup table cache (default: `True`)

### Code Configuration
- `max_batch_size`: Maximum medications per batch (default: 1000)
- Rate limit: 20 req/sec for RxNav (hardcoded in rate limiter)

## Performance Considerations

### First Run
- Slower: API calls for all medications
- Rate limited: 20 req/sec max
- Typical: ~50 medications/second

### Subsequent Runs
- Much faster: Most results cached
- API calls: Only for new medications
- Typical: <1 second for cache lookup

### Optimization Tips
1. Run initial enrichment separately to populate cache
2. Use larger batch sizes for offline processing
3. Monitor lookup table growth
4. Consider periodic cache cleanup

## Testing

### Unit Tests
- Rate limiter behavior
- Normalization functions
- API response parsing

### Integration Tests
- End-to-end enrichment flow
- Lookup table read/write
- DataFrame updates

### Load Tests
- Rate limit enforcement
- Batch processing performance
- Memory usage with large batches



