# HTTPS Connectivity Issue Diagnosis

## Problem Summary

**Symptom**: DNS resolution works ✅, but HTTPS connections fail ❌

```
✅ DNS resolution successful for rxnav.nlm.nih.gov
❌ HTTPS connectivity test failed: Network is unreachable [Errno 101]
```

## Root Cause Analysis

### What's Working:
1. ✅ **DNS Resolution**: Can resolve `rxnav.nlm.nih.gov` to IP address
2. ✅ **Security Group**: Allows all outbound traffic (`0.0.0.0/0`)
3. ✅ **Route Table**: Has route to Internet Gateway (`0.0.0.0/0` → `igw-06f2e6b59a8e12566`)
4. ✅ **Subnet**: Public subnet (`MapPublicIpOnLaunch: true`)
5. ✅ **Connections**: Both "Redshift connection" and "Network connection" are configured

### What's Failing:
- ❌ **HTTPS Connection**: Cannot establish TCP connection to port 443
- ❌ **Error**: `[Errno 101] Network is unreachable`

## Possible Causes

### 1. **Subnet Route Table Association Issue** ⚠️ **MOST LIKELY**

The subnet might not be explicitly associated with the route table that has the Internet Gateway route.

**Check:**
```bash
aws ec2 describe-route-tables \
  --filters "Name=association.subnet-id,Values=subnet-04bd8136894aef63b" \
  --query 'RouteTables[0].Routes[?GatewayId!=`local`]' \
  --profile to-prod-admin --region us-east-2
```

**If returns null/empty**: The subnet is using the main route table, which should work, but there might be an issue.

### 2. **Glue Multiple Connections Issue**

When Glue uses multiple connections, it might not properly route traffic through the Network connection's VPC settings.

**Solution**: Ensure the Network connection is properly prioritized or use a single connection with both database and internet access.

### 3. **Network ACL Blocking**

Network ACLs might be blocking outbound HTTPS traffic.

**Check:**
```bash
aws ec2 describe-network-acls \
  --filters "Name=association.subnet-id,Values=subnet-04bd8136894aef63b" \
  --query 'NetworkAcls[0].Entries[?Egress==`true` && PortRange.FromPort==`443`]' \
  --profile to-prod-admin --region us-east-2
```

### 4. **Glue Job Not Using Network Connection**

The job might be configured with the Network connection, but Glue might not be using it for outbound internet traffic.

## Solutions

### Solution 1: Verify Subnet Route Table Association (Recommended)

**Explicitly associate the subnet with the route table that has the IGW route:**

```bash
# Get the main route table ID
MAIN_RT=$(aws ec2 describe-route-tables \
  --filters "Name=vpc-id,Values=vpc-0c92c92a7da0153ad" "Name=association.main,Values=true" \
  --query 'RouteTables[0].RouteTableId' \
  --output text \
  --profile to-prod-admin --region us-east-2)

# Associate subnet with main route table (if not already)
aws ec2 associate-route-table \
  --subnet-id subnet-04bd8136894aef63b \
  --route-table-id $MAIN_RT \
  --profile to-prod-admin --region us-east-2
```

### Solution 2: Create Single Unified Connection

Instead of using two separate connections, create a single connection that provides both database and internet access:

1. **Update "Redshift connection"** to use the same subnet/security group
2. **Remove "Network connection"** from job
3. **Use only "Redshift connection"** (which already has internet access)

### Solution 3: Verify Glue Job Connection Priority

Ensure the Network connection is being used. Check in AWS Glue console:
1. Go to HMUMedication_v2 job
2. Check "Connections" section
3. Verify both connections are listed
4. Try reordering or using only Network connection for testing

### Solution 4: Add Explicit Security Group Rule

Even though the security group allows all traffic, add an explicit HTTPS rule:

```bash
aws ec2 authorize-security-group-egress \
  --group-id sg-0453e0a7b2deb55b8 \
  --ip-permissions '[{"IpProtocol": "tcp", "FromPort": 443, "ToPort": 443, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}]' \
  --profile to-prod-admin --region us-east-2
```

### Solution 5: Test with Single Connection

Temporarily test with only the Network connection to isolate the issue:

1. Update job to use only "Network connection"
2. Run job and check if HTTPS works
3. If it works, the issue is with multiple connections
4. If it doesn't, the issue is with the Network connection itself

## Diagnostic Commands

### Check Route Table Association:
```bash
aws ec2 describe-route-tables \
  --filters "Name=association.subnet-id,Values=subnet-04bd8136894aef63b" \
  --query 'RouteTables[*].{RouteTableId:RouteTableId,Routes:Routes[?GatewayId!=`local`]}' \
  --profile to-prod-admin --region us-east-2
```

### Check Network ACLs:
```bash
aws ec2 describe-network-acls \
  --filters "Name=association.subnet-id,Values=subnet-04bd8136894aef63b" \
  --query 'NetworkAcls[0].Entries[?Egress==`true`]' \
  --profile to-prod-admin --region us-east-2
```

### Check Security Group:
```bash
aws ec2 describe-security-groups \
  --group-ids sg-0453e0a7b2deb55b8 \
  --query 'SecurityGroups[0].IpPermissionsEgress' \
  --profile to-prod-admin --region us-east-2
```

### Test Connectivity from Glue:
The job already has connectivity tests built-in. Check CloudWatch logs for detailed diagnostics.

## Expected Behavior After Fix

After applying the fix, you should see:
```
✅ DNS resolution successful for rxnav.nlm.nih.gov -> <IP>
✅ TCP connection to port 443 successful
✅ HTTPS connectivity test passed
```

## Next Steps

1. **Run diagnostic commands** above to identify the specific issue
2. **Apply Solution 1** (route table association) - most likely fix
3. **If still failing**, try Solution 2 (single unified connection)
4. **Monitor CloudWatch logs** for connectivity test results
5. **Verify** HTTPS connections succeed in next job run



