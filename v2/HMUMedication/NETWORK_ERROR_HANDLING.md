# Network Error Handling for Medication Enrichment

## Error: Network is Unreachable

When you see this error:
```
HTTPSConnectionPool(host='rxnav.nlm.nih.gov', port=443): Max retries exceeded
Network is unreachable
```

This means your AWS Glue job **cannot access the internet** to call the RxNav API.

## What Happens Now

✅ **The job will continue successfully** - it won't fail
- Medications without codes are still processed
- Lookup table cache is still used (if available)
- Only API enrichment is skipped

## Solutions

### Option 1: Use Comprehend Medical (Recommended)
**Best option** - AWS service, no internet required!

Set environment variable:
```
MEDICATION_ENRICHMENT_MODE=comprehend_only
```

Benefits:
- ✅ Works in VPC without internet access
- ✅ Better accuracy for medical terms
- ✅ AWS-native service
- ✅ No network configuration needed

Requirements:
- IAM permission: `comprehendmedical:DetectEntitiesV2`
- AWS service (works within VPC)

### Option 2: Configure VPC Networking
If you want to use RxNav API:

1. **Add NAT Gateway to your VPC**
   - Create NAT Gateway in public subnet
   - Route private subnet traffic through NAT Gateway

2. **Configure Glue Job VPC Connection**
   - Add VPC connection to Glue job
   - Use private subnets with NAT Gateway routes
   - Security Group must allow outbound HTTPS (port 443)

3. **Update Glue Job Configuration**
   ```json
   "Connections": {
     "Connections": ["Redshift connection", "your-vpc-connection"]
   }
   ```

### Option 3: Disable API Enrichment
If network access is not possible:

Set environment variable:
```
ENABLE_MEDICATION_ENRICHMENT=false
```

Or only use lookup table:
- Lookup table will still work (reads from Redshift)
- No API calls will be made

## Current Behavior

The enrichment system **gracefully handles network errors**:
- ✅ Job continues without failing
- ✅ Logs warning once (not for every medication)
- ✅ Uses lookup table cache if available
- ✅ Medications processed normally

## Error Messages

You'll see:
```
⚠️  Network unreachable - RxNav API enrichment disabled.
   Glue job requires VPC with NAT Gateway for external API access.
   Consider using Comprehend Medical (AWS service) or disable API enrichment.
```

This is **expected** - the job continues successfully!

## Recommendation

**Use Comprehend Medical** - it's the easiest solution:
- No network configuration
- Better for medical terminology  
- AWS service (works anywhere)

Just set: `MEDICATION_ENRICHMENT_MODE=comprehend_only`



