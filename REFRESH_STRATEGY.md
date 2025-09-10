# FHIR Materialized Views Refresh Strategy

## Overview
All FHIR materialized views use **AUTO REFRESH NO** with external scheduled refresh for consistent timing and better control.

## Refresh Strategy by Entity

### Phase 1: High-Volume Entities (Require Careful Scheduling) 
| View | Records | Refresh Frequency | Strategy | Status |
|------|---------|------------------|----------|---------|
| **fact_fhir_observations_view_v1** | 14.8M | Every 4-6 hours | Full refresh | ✅ Production Ready |
| **fact_fhir_medication_requests_view_v1** | 10M | Every 2-4 hours | Full refresh | ✅ Production Ready |
| **fact_fhir_document_references_view_v1** | 4.5M | Every 2-4 hours | Full refresh | ✅ Production Ready |
| **fact_fhir_conditions_view_v1** | 3.2M | Every 2-4 hours | Full refresh | ✅ Production Ready |
| **fact_fhir_diagnostic_reports_view_v1** | 1.8M | Every 1-2 hours | Full refresh | ✅ Production Ready |

### Phase 2: Medium-Volume Entities
| View | Records | Refresh Frequency | Strategy | Status |
|------|---------|------------------|----------|---------|
| **fact_fhir_encounters_view_v2** | 1.1M | Every 1-2 hours | Full refresh | ✅ Production Ready |
| **fact_fhir_procedures_view_v1** | 611K | Hourly | Full refresh | ✅ Production Ready |

### Phase 3: Low-Volume Entities (High-Frequency Refresh)
| View | Records | Refresh Frequency | Strategy | Status |
|------|---------|------------------|----------|---------|
| **fact_fhir_patients_view_v2** | 350K | Hourly | Full refresh | ✅ Production Ready |
| **fact_fhir_practitioners_view_v1** | 7,333 | Every 15-30 minutes | Full refresh | ✅ Production Ready |

### Additional Views (All Production Ready)
| View | Records | Refresh Frequency | Strategy | Status |
|------|---------|------------------|----------|---------|
| **fact_fhir_patients_view_v1** | 350K | Hourly | Full refresh | ✅ Production Ready |
| **fact_fhir_encounters_view_v1** | 1.1M | Every 1-2 hours | Full refresh | ✅ Production Ready |

## Implementation Options

### Option 1: AWS Lambda + CloudWatch Events (Recommended)
**Pros:**
- Serverless, cost-effective
- Easy to monitor and manage
- Built-in retry mechanisms
- CloudWatch logging

**Implementation:**
```python
import boto3
import json

def lambda_handler(event, context):
    redshift_data = boto3.client('redshift-data')
    
    views_to_refresh = [
        'fact_fhir_care_plans_view_v1',
        'fact_fhir_practitioners_view_v1',
        'fact_fhir_procedures_view_v1',
        'fact_fhir_conditions_view_v1',
        # ... add other views
    ]
    
    for view in views_to_refresh:
        try:
            response = redshift_data.execute_statement(
                ClusterIdentifier='prod-redshift-main-ue2',
                Database='dev',
                Sql=f'REFRESH MATERIALIZED VIEW {view};'
            )
            print(f'Refreshing {view}: {response["Id"]}')
        except Exception as e:
            print(f'Error refreshing {view}: {str(e)}')
    
    return {'statusCode': 200, 'body': json.dumps('Refresh initiated')}
```

### Option 2: AWS Batch + ECS (For Complex Scheduling)
**Pros:**
- Better for complex dependencies
- More control over resource allocation
- Can handle long-running refreshes

### Option 3: Apache Airflow (For Complex Workflows)
**Pros:**
- Advanced dependency management
- Rich monitoring and alerting
- Complex scheduling patterns

**DAG Example:**
```python
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'fhir_materialized_views_refresh',
    default_args=default_args,
    description='Refresh FHIR materialized views',
    schedule_interval='@hourly',
    catchup=False
)

# Low-volume views (fast refresh)
refresh_care_plans = RedshiftSQLOperator(
    task_id='refresh_care_plans',
    sql='REFRESH MATERIALIZED VIEW fact_fhir_care_plans_view_v1;',
    redshift_conn_id='redshift_default',
    dag=dag
)

refresh_practitioners = RedshiftSQLOperator(
    task_id='refresh_practitioners',
    sql='REFRESH MATERIALIZED VIEW fact_fhir_practitioners_view_v1;',
    redshift_conn_id='redshift_default',
    dag=dag
)

# Set dependencies
refresh_care_plans >> refresh_practitioners
```

## Monitoring and Alerting

### Key Metrics to Monitor
1. **Refresh Duration**: Track execution time trends
2. **Success Rate**: Monitor refresh failures
3. **Data Freshness**: Alert if refresh hasn't run recently
4. **Resource Usage**: Monitor cluster resource consumption

### CloudWatch Alarms
```bash
# Create alarm for refresh failures
aws cloudwatch put-metric-alarm \
  --alarm-name "FHIR-Views-Refresh-Failures" \
  --alarm-description "Alert when materialized view refresh fails" \
  --metric-name "Errors" \
  --namespace "AWS/Lambda" \
  --statistic "Sum" \
  --period 300 \
  --threshold 1 \
  --comparison-operator "GreaterThanOrEqualToThreshold" \
  --evaluation-periods 1
```

### Recommended Dashboard Widgets
- Refresh success/failure rates by view
- Average refresh duration by view
- Data freshness indicators
- Redshift cluster utilization during refresh

## Refresh Commands

### Automated Refresh Scripts (Recommended)
```bash
# Use the automated refresh script
./refresh_all_fhir_views.sh
```

### Manual Refresh Commands
```sql
-- Refresh all views in optimal order (low to high volume)
REFRESH MATERIALIZED VIEW fact_fhir_practitioners_view_v1;
REFRESH MATERIALIZED VIEW fact_fhir_patients_view_v1;
REFRESH MATERIALIZED VIEW fact_fhir_patients_view_v2;
REFRESH MATERIALIZED VIEW fact_fhir_procedures_view_v1;
REFRESH MATERIALIZED VIEW fact_fhir_encounters_view_v1;
REFRESH MATERIALIZED VIEW fact_fhir_encounters_view_v2;
REFRESH MATERIALIZED VIEW fact_fhir_diagnostic_reports_view_v1;
REFRESH MATERIALIZED VIEW fact_fhir_conditions_view_v1;
REFRESH MATERIALIZED VIEW fact_fhir_document_references_view_v1;
REFRESH MATERIALIZED VIEW fact_fhir_medication_requests_view_v1;
REFRESH MATERIALIZED VIEW fact_fhir_observations_view_v1;
```

### Check Refresh Status
```sql
-- Check last refresh times
SELECT 
    schemaname,
    matviewname,
    hasindexes,
    ispopulated,
    definition
FROM pg_matviews 
WHERE matviewname LIKE 'fact_fhir%'
ORDER BY matviewname;

-- Check refresh activity
SELECT 
    query,
    starttime,
    endtime,
    status
FROM stl_query 
WHERE querytxt LIKE '%REFRESH MATERIALIZED VIEW fact_fhir%'
ORDER BY starttime DESC
LIMIT 20;
```

## Performance Optimization

### Refresh Order Strategy
1. **Start with low-volume views** (care plans, practitioners)
2. **Medium-volume views next** (procedures)
3. **High-volume views during off-peak hours** (observations, medication requests)

### Resource Management
- Monitor cluster CPU/memory during refresh
- Consider scaling cluster for large view refreshes
- Schedule heavy refreshes during low-usage periods

## Error Handling

### Common Refresh Errors
1. **Lock conflicts**: Retry with exponential backoff
2. **Memory issues**: Split large refreshes or scale cluster
3. **Data quality issues**: Validate source data before refresh

### Recovery Procedures
```sql
-- If refresh fails, check for blocking queries
SELECT 
    query,
    pid,
    starttime,
    substring(query,1,100) as query_text
FROM stv_recents 
WHERE query LIKE '%fact_fhir%'
AND status = 'Running';

-- Kill blocking queries if necessary (use with caution)
CANCEL pid;
```

## Current Implementation Status

### ✅ Phase 1: Complete - All Views Production Ready
- All 11 materialized views created and tested
- Redshift compatibility issues resolved (LISTAGG/COUNT mixing, REGEXP patterns, boolean casting)
- Enhanced deployment scripts with record count validation
- Comprehensive error handling for "already exists" scenarios

### 🔄 Phase 2: Ready for Implementation - Automated Refresh
**Infrastructure Setup (Ready to Deploy):**
- Set up Lambda function for automated refresh
- Configure CloudWatch Events for scheduling by data volume
- Implement view-specific refresh frequencies
- Add retry logic and failure notifications

**Recommended Implementation Order:**
1. Deploy Lambda function with the refresh logic from the provided examples
2. Configure different CloudWatch Events for each refresh tier
3. Set up CloudWatch monitoring and alerting
4. Test refresh schedules and adjust frequencies

### 📊 Phase 3: Monitoring and Optimization (Ready to Implement)
**Infrastructure Available:**
- Performance monitoring dashboards
- Data freshness alerting
- Resource utilization tracking during refresh
- Query performance optimization

---

**Status**: ✅ **All views complete and ready for production deployment**

**Deployment Command**: `./create_all_fhir_views.sh`

**Next Steps**: 
1. Deploy all materialized views to production
2. Implement automated refresh scheduling (Lambda + CloudWatch)
3. Set up monitoring and alerting infrastructure
4. Create QuickSight dashboards using the views