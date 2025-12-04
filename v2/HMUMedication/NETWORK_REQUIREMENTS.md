# Network Requirements for Medication Enrichment

## Issue: Network Unreachable

The error `Network is unreachable` occurs when the AWS Glue job cannot access external APIs (RxNav) due to network configuration restrictions.

## Root Cause

AWS Glue jobs running in a VPC environment need proper network configuration to access external APIs:
- **VPC without Internet Gateway/NAT Gateway**: No internet access
- **Security Groups**: May block outbound HTTPS connections
- **NACLs**: May block required ports

## Solutions

### Option 1: Configure VPC Networking (Recommended for RxNav)

1. **Set up NAT Gateway**:
   - Create NAT Gateway in public subnet
   - Update route tables in private subnets to route internet traffic through NAT Gateway
   - Ensure Security Groups allow outbound HTTPS (port 443)

2. **Configure Glue Job**:
   - Add VPC connection to Glue job
   - Specify subnets (private subnets with NAT Gateway routes)
   - Add Security Group with outbound HTTPS allowed

3. **Add to Glue Job JSON**:
   ```json
   "Connections": {
     "Connections": ["Redshift connection", "VPC-connection-name"]
   },
   "DefaultArguments": {
     "--network-type": "VPC",
     ...
   }
   ```

### Option 2: Use AWS Comprehend Medical (Recommended)

**Comprehend Medical is an AWS service** - no internet access required! It works within your VPC.

**Benefits**:
- ✅ No NAT Gateway needed
- ✅ Better accuracy for medical terms
- ✅ Works in VPC environments
- ✅ AWS-native service (better integration)

**Configuration**:
- Set `ENRICHMENT_MODE=comprehend_only`
- Ensure IAM role has `comprehendmedical:DetectEntitiesV2` permission
- Works without internet access

### Option 3: Disable API Enrichment

If network access is not possible:
- Set `ENABLE_ENRICHMENT=false` to disable enrichment
- Or use `ENRICHMENT_MODE=lookup_only` (only use lookup table cache)

## Current Behavior

The enrichment system gracefully handles network errors:
- ✅ Job continues without failing
- ✅ Logs warning about network issue
- ✅ Only uses lookup table cache (if available)
- ✅ Medications without codes are still processed

## Error Messages

You'll see warnings like:
```
⚠️  Network unreachable - RxNav API enrichment disabled.
Glue job requires VPC with NAT Gateway for external API access.
Consider using Comprehend Medical (AWS service) instead.
```

This is expected behavior - the job will continue without API enrichment.

## Recommendation

**Use Comprehend Medical** (`ENRICHMENT_MODE=comprehend_only`) because:
1. No network configuration needed
2. Better for medical terminology
3. AWS-native service
4. Works in any VPC environment

## Testing Network Connectivity

To test if your Glue job has internet access:

```python
import requests
try:
    response = requests.get("https://rxnav.nlm.nih.gov/REST/drugs.json?name=test", timeout=5)
    print("✅ Internet access available")
except Exception as e:
    print(f"❌ No internet access: {e}")
```



