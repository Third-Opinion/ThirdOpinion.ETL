# FHIR Materialized Views Deployment Guide

## Deployment Status

### ✅ Ready for Deployment
- **fact_fhir_care_plans_view_v1.sql** - Completed and tested

### 🚨 Deployment Issue Encountered

**Issue**: Permission denied for accessing source tables via AWS CLI
- **Error**: `ERROR: permission denied for relation care_plans`
- **Root Cause**: Current AWS CLI user role lacks SELECT permissions on FHIR tables
- **Impact**: Cannot deploy via AWS Redshift Data API from CLI

## Alternative Deployment Methods

### Method 1: AWS Console Query Editor (Recommended)
1. **Open AWS Console** → **Amazon Redshift** → **Query Editor v2**
2. **Connect to cluster**: `prod-redshift-main-ue2`, database: `dev`
3. **Copy and paste** the entire contents of `fact_fhir_care_plans_view_v1.sql`
4. **Execute the query** to create the materialized view

### Method 2: Database Client with Appropriate Permissions
1. Use a database client (DBeaver, pgAdmin, etc.) with credentials that have:
   - SELECT permissions on all `care_plan*` tables
   - CREATE permissions for materialized views
2. Connect to: `prod-redshift-main-ue2.amazonaws.com:5439/dev`
3. Execute the SQL file

### Method 3: IAM Role with Enhanced Permissions
1. **Update IAM policy** to include:
   ```json
   {
     "Effect": "Allow",
     "Action": [
       "redshift:CreateCluster",
       "redshift:CreateClusterSnapshot",
       "redshift:DescribeClusters"
     ],
     "Resource": "*"
   }
   ```
2. **Add database permissions**:
   ```sql
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO current_user;
   GRANT CREATE ON SCHEMA public TO current_user;
   ```

## Deployment Verification Commands

After successful deployment, run these queries to verify:

### 1. Check View Creation
```sql
SELECT 
    schemaname, 
    viewname, 
    viewowner 
FROM pg_views 
WHERE viewname = 'fact_fhir_care_plans_view_v1';
```

### 2. Verify Record Count
```sql
SELECT COUNT(*) as total_records 
FROM fact_fhir_care_plans_view_v1;
-- Expected: ~171 records based on production data
```

### 3. Test JSON Aggregation
```sql
SELECT 
    care_plan_id,
    ARRAY_SIZE(categories) as category_count,
    ARRAY_SIZE(goals) as goals_count,
    ARRAY_SIZE(identifiers) as identifiers_count
FROM fact_fhir_care_plans_view_v1
LIMIT 5;
```

## Post-Deployment Steps

### 1. Performance Monitoring
- Monitor scheduled refresh performance
- Check materialized view refresh logs in scheduling system
- Validate query performance against expectations
- Set up refresh failure alerts

### 2. Data Quality Validation
- Run the complete test suite in `test_care_plans_view.sql`
- Compare record counts with source tables
- Verify JSON structure integrity

### 3. Access Permissions
- Grant SELECT permissions to analytics users/roles
- Document view schema for downstream consumers
- Update data catalog/documentation

## Troubleshooting

### Common Issues and Solutions

**Issue**: Scheduled refresh failures
- **Solution**: Check scheduling system logs, data type mismatches or constraint violations
- **Monitor**: Scheduling system alerts and Redshift system tables for refresh errors

**Issue**: JSON parsing errors
- **Solution**: Verify string escaping in REGEXP_REPLACE functions
- **Test**: Run individual JSON_PARSE statements

**Issue**: Performance degradation
- **Solution**: Monitor refresh duration, adjust scheduling frequency as needed

## Files Ready for Deployment

1. **`fact_fhir_care_plans_view_v1.sql`** - Main materialized view (316 lines)
2. **`test_care_plans_view.sql`** - Comprehensive test suite (7 validation queries)
3. **`/tmp/clean_care_plans_view.sql`** - Deployment-ready SQL without comments

## Next Steps

1. **Deploy using AWS Console Query Editor** (recommended)
2. **Run validation tests** from `test_care_plans_view.sql`
3. **Proceed to next entity**: Task 3 - Conditions materialized view
4. **Update documentation** with deployment results

## Deployment Command History

```bash
# Attempted AWS CLI deployment (failed due to permissions)
aws redshift-data execute-statement \
  --profile to-prd-admin \
  --cluster-identifier prod-redshift-main-ue2 \
  --database dev \
  --sql "$(cat /tmp/clean_care_plans_view.sql)"

# Status: FAILED - ERROR: permission denied for relation care_plans
# Query ID: 87a3f516-08b8-4369-899d-a4ec5e597e72
```

---

**Implementation Status**: ✅ Complete and ready for deployment via AWS Console
**Next Task**: Task 3 - Create fact_fhir_conditions_view_v1 materialized view