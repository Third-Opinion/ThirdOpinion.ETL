# Performance Optimization Guide

## Issue Analysis

Based on cluster failure analysis:
- **Root Cause**: Executors running out of memory during large shuffle operations (17.7GB average)
- **Window Operations**: Memory pressure from window functions in deduplication
- **Worker Utilization**: Only 10% - suggesting resource constraints
- **Executor Death**: Containers exceeding memory limits during shuffle operations

## Code-Level Optimizations Implemented

### 1. Reduced Shuffle Partitions
- Changed from 400 to 100 partitions in `SparkConfig`
- Reduces shuffle data volume and executor memory pressure

### 2. Partition Optimization Before Writes
- Coalescing/repartitioning to 50 partitions before Redshift writes
- Reduces shuffle overhead during write operations

### 3. Optimized Deduplication Window Operations
- Repartitioning to 75 partitions before window functions
- Reduces shuffle data volume during window operations

### 4. Removed Count Operations
- Removed all `.count()` calls that trigger expensive shuffles
- Counts were only used for logging and caused cluster instability

## Infrastructure Recommendations (AWS Glue Configuration)

These require changes to the Glue job configuration, not code:

### 1. Upgrade Worker Type
**Current**: G.2X (8GB memory, 4 vCPU)
**Recommended**: G.4X (16GB memory, 16 vCPU)
- **Benefit**: Doubles executor memory, significantly reducing out-of-memory errors
- **How**: Update Glue job → Job details → Worker type → G.4X

### 2. Increase Number of Workers
**Recommended**: Increase maximum workers to handle larger datasets
- **Current**: Dynamic allocation up to 30 executors
- **Consider**: Increase if worker utilization remains low

### 3. Enable Cloud Shuffle Storage
**Configuration to add**:
```
--write-shuffle-files-to-s3 true
--conf spark.shuffle.storage.path=s3://your-shuffle-bucket/shuffle/
```
- **Benefit**: Offloads shuffle data to S3, reducing executor memory pressure
- **How**: Add to Glue job → Job parameters → Additional Python modules

### 4. Enable Auto-Scaling
- **Benefit**: Dynamically adjusts resources based on workload
- **How**: Configure in Glue job settings

## Configuration Parameters

Add these to your Glue job configuration:

### Spark Configuration
```json
{
  "spark.sql.shuffle.partitions": "100",
  "spark.sql.adaptive.enabled": "true",
  "spark.sql.adaptive.coalescePartitions.enabled": "true",
  "spark.sql.adaptive.skewJoin.enabled": "true",
  "spark.shuffle.service.enabled": "true",
  "spark.dynamicAllocation.enabled": "true"
}
```

### For Cloud Shuffle Storage
```json
{
  "--write-shuffle-files-to-s3": "true",
  "--conf": "spark.shuffle.storage.path=s3://aws-glue-assets-442042533707-us-east-2/temporary/shuffle/"
}
```

## Monitoring Recommendations

1. **Watch executor memory metrics** - Should stay below 80% utilization
2. **Monitor shuffle data volume** - Current 17.7GB is high, aim for <10GB
3. **Check worker utilization** - Currently 10%, should increase with more workers/memory
4. **Track executor deaths** - Should decrease with optimizations

## Expected Improvements

After implementing code + infrastructure changes:
- ✅ Reduced shuffle data volume (from 17.7GB)
- ✅ Lower executor memory pressure
- ✅ Fewer executor deaths
- ✅ Better worker utilization
- ✅ Faster job completion



