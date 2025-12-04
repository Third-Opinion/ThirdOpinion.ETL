# VPC Internet Access Summary

## All VPCs Have Internet Gateways ✅

All four VPCs visible in the Glue console have Internet Gateways attached:

| VPC ID | Name | Internet Gateway | Status |
|--------|------|----------------|--------|
| `vpc-0e212dc3b1a167e81` | prd-app-vpc-vpc | `igw-0b1a6955e7c9a63a7` | ✅ Available |
| `vpc-0c92c92a7da0153ad` | (Default VPC) | `igw-06f2e6b59a8e12566` | ✅ Available |
| `vpc-0cba796ba95c0fb07` | prod-tm-vpc-ue2-main | `igw-09a8ef3d001bd9ef7` | ✅ Available |
| `vpc-068da3b68e0679c43` | prod-tfv2-vpc-ue2-main | `igw-0856645dbe8d25326` | ✅ Available |

## Public Subnets Available

### ✅ vpc-0c92c92a7da0153ad (Default VPC - Currently Used)
- **Subnet**: `subnet-04bd8136894aef63b` (us-east-2a) - **Currently used by Redshift connection**
- **Subnet**: `subnet-0f2e16ecec363bc7c` (us-east-2b)
- **Subnet**: `subnet-0a592331f1cc56a34` (us-east-2c)
- **Status**: ✅ Has Internet Gateway route configured

### ✅ vpc-0cba796ba95c0fb07 (prod-tm-vpc-ue2-main)
- **Subnet**: `subnet-0c7920431e946fcc1` (us-east-2a)
- **Subnet**: `subnet-0946fb4666e55afde` (us-east-2b)
- **Subnet**: `subnet-0fcd08c8ecfe80e15` (us-east-2c)

### ✅ vpc-068da3b68e0679c43 (prod-tfv2-vpc-ue2-main)
- **Subnet**: `subnet-0fdd08600e3b5e256` (us-east-2a)
- **Subnet**: `subnet-08b52b8d6041632dc` (us-east-2b)
- **Subnet**: `subnet-06011de2301f43930` (us-east-2c)

### ⚠️ vpc-0e212dc3b1a167e81 (prd-app-vpc-vpc)
- **No public subnets found** (may only have private subnets with NAT Gateway)

## Recommendation

### ✅ **Use: vpc-0c92c92a7da0153ad (Default VPC)**

**Why:**
1. ✅ Already configured in "Redshift connection"
2. ✅ Has Internet Gateway attached and routes configured
3. ✅ Has public subnets available
4. ✅ Security group allows all outbound traffic
5. ✅ Currently being used by the job

**Current Configuration:**
- **Connection**: "Redshift connection"
- **Subnet**: `subnet-04bd8136894aef63b` (public subnet)
- **Security Group**: `sg-0453e0a7b2deb55b8` (allows all outbound)
- **VPC**: `vpc-0c92c92a7da0153ad` (default VPC)

## What to Set in Glue Console

When configuring the connection in AWS Glue console:

1. **VPC**: Select `vpc-0c92c92a7da0153ad` (Default VPC)
2. **Subnet**: Select `subnet-04bd8136894aef63b` (or any public subnet in this VPC)
3. **Security Group**: Select `sg-0453e0a7b2deb55b8` (or ensure it allows HTTPS outbound)

**Note**: The "Redshift connection" already uses this VPC and subnet, so the job should already have internet access. If you're still getting "Network is unreachable" errors, verify:

1. The route table for the subnet has a route to `0.0.0.0/0` via the Internet Gateway
2. The security group allows outbound HTTPS (port 443)
3. The Glue job is actually using the connection's VPC settings

## Alternative VPCs (If Needed)

If you need to use a different VPC for any reason:

### Option 1: vpc-0cba796ba95c0fb07 (prod-tm-vpc-ue2-main)
- Has public subnets
- Has Internet Gateway
- May need to create/update Glue connection

### Option 2: vpc-068da3b68e0679c43 (prod-tfv2-vpc-ue2-main)
- Has public subnets
- Has Internet Gateway
- May need to create/update Glue connection

## Verification Steps

To verify internet access is working:

```bash
# Check route table for subnet
aws ec2 describe-route-tables \
  --filters "Name=association.subnet-id,Values=subnet-04bd8136894aef63b" \
  --query 'RouteTables[0].Routes[?GatewayId!=`local`]' \
  --profile to-prod-admin --region us-east-2

# Should show route: 0.0.0.0/0 → igw-06f2e6b59a8e12566
```



