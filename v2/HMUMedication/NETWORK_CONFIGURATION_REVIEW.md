# HMUMedication Glue Job - Network Configuration Review

## Current Status

### ✅ What's Already Configured:

1. **Glue Connection**: The job uses "Redshift connection" which has VPC settings:
   - **Subnet**: `subnet-04bd8136894aef63b` (public subnet in default VPC)
   - **Security Group**: `sg-0453e0a7b2deb55b8` (default security group)
   - **VPC**: `vpc-0c92c92a7da0153ad` (default VPC)
   - **Availability Zone**: `us-east-2a`

2. **Security Group Egress Rules**: ✅ **ALLOWED**
   - Protocol: All traffic (`-1`)
   - Destination: `0.0.0.0/0` (all IPs)
   - **This allows HTTPS (port 443) outbound to the internet**

3. **Subnet Configuration**: ✅ **PUBLIC SUBNET**
   - `MapPublicIpOnLaunch: true` - This is a public subnet
   - CIDR: `172.31.0.0/20`

### ❌ What Needs Verification/Configuration:

1. **Route Table**: Need to verify the subnet has a route to an Internet Gateway
   - Public subnets require a route to `0.0.0.0/0` via an Internet Gateway (igw-*)
   - If missing, Glue workers won't have internet access

2. **Glue Job VPC Configuration**: The job should inherit VPC settings from the connection, but we should verify:
   - The job is using the connection's VPC settings
   - The connection's subnet has internet access

## Required Configuration for Internet Access

### Option 1: Verify Route Table (Recommended First Step)

Check if the subnet's route table has an Internet Gateway route:

```bash
aws ec2 describe-route-tables \
  --filters "Name=association.subnet-id,Values=subnet-04bd8136894aef63b" \
  --query 'RouteTables[0].Routes[?GatewayId!=`local`]' \
  --profile to-prod-admin --region us-east-2
```

**Expected Result**: Should see a route with:
- `DestinationCidrBlock: "0.0.0.0/0"`
- `GatewayId: "igw-xxxxxxxxx"` (Internet Gateway)

### Option 2: Create/Update Glue Connection with Internet Access

If the current subnet doesn't have internet access, you may need to:

1. **Find a subnet with internet access** (public subnet with IGW route)
2. **Create a new Glue connection** for internet access, OR
3. **Update the existing connection** to use a subnet with internet access

### Option 3: Use VPC Endpoints (Alternative - No Internet Required)

Instead of internet access, you could use AWS PrivateLink/VPC Endpoints for specific services, but this is more complex and RxNav API is external.

## Recommended Actions

### Immediate Actions:

1. **Verify Route Table**:
   ```bash
   # Check if subnet has Internet Gateway route
   aws ec2 describe-route-tables \
     --filters "Name=association.subnet-id,Values=subnet-04bd8136894aef63b" \
     --query 'RouteTables[0].Routes[?contains(GatewayId, `igw`)]' \
     --profile to-prod-admin --region us-east-2
   ```

2. **If Route Table is Missing IGW Route**:
   - Find the default VPC's Internet Gateway
   - Add route `0.0.0.0/0` → Internet Gateway to the route table
   - OR use a different subnet that already has internet access

3. **Verify Glue Job Uses Connection**:
   - The job configuration already includes `"Connections": ["Redshift connection"]`
   - This should automatically use the connection's VPC settings
   - Verify in AWS Glue console that the job shows VPC configuration

### Alternative: Create Dedicated Internet Connection

If you need to keep Redshift connection separate from internet access:

1. **Create a new Glue connection** specifically for internet access:
   - Name: `Internet Access Connection`
   - Type: `NETWORK`
   - Subnet: Public subnet with Internet Gateway route
   - Security Group: Same or different (must allow HTTPS outbound)

2. **Add both connections to the job**:
   ```json
   "Connections": {
     "Connections": [
       "Redshift connection",
       "Internet Access Connection"
     ]
   }
   ```

## Current Configuration Summary

| Component | Status | Details |
|-----------|--------|---------|
| **Glue Connection** | ✅ Configured | "Redshift connection" with VPC settings |
| **Subnet** | ✅ Public | `subnet-04bd8136894aef63b` (MapPublicIpOnLaunch: true) |
| **Security Group** | ✅ Allows Outbound | `sg-0453e0a7b2deb55b8` (all traffic to 0.0.0.0/0) |
| **Route Table** | ⚠️ **NEEDS VERIFICATION** | Must have route to Internet Gateway |
| **Job Configuration** | ✅ Uses Connection | Job includes connection in Connections array |

## Testing Network Connectivity

After configuration, test by running the job and checking logs for:
- ✅ Success: API calls to `rxnav.nlm.nih.gov` succeed
- ❌ Failure: "Network is unreachable" errors continue

## Next Steps

1. **Run route table verification command** (see above)
2. **If IGW route exists**: The configuration should work - verify job execution
3. **If IGW route missing**: Add route or use different subnet
4. **Update job configuration** if needed (add internet connection)
5. **Redeploy and test**



