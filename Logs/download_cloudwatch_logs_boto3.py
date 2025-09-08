#!/usr/bin/env python3
"""
CloudWatch Logs Downloader using boto3

This script downloads logs from AWS CloudWatch using the boto3 SDK.
It uses the hardcoded log group ARN and accepts the log stream name as a parameter.

Usage:
    python download_cloudwatch_logs_boto3.py <log_stream_name> [output_file]

Example:
    python download_cloudwatch_logs_boto3.py jr_84004174dfa87cc635d131bca6f88dc3f7cee062a6b208f29d72c1c1f86d2f37
"""

import boto3
import sys
import json
import os
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError

# Hardcoded log group ARN
LOG_GROUP_ARN = "arn:aws:logs:us-east-2:442042533707:log-group:/aws-glue/jobs/output:*"
LOG_GROUP_NAME = "/aws-glue/jobs/output"
PROFILE = "to-prod-admin"
REGION = "us-east-2"

def get_cloudwatch_logs_client():
    """Create and return a CloudWatch Logs client."""
    try:
        session = boto3.Session(profile_name=PROFILE, region_name=REGION)
        client = session.client('logs')
        return client
    except NoCredentialsError:
        print(f"Error: AWS credentials not found for profile '{PROFILE}'")
        print("Make sure the profile is configured in ~/.aws/credentials and ~/.aws/config")
        return None
    except Exception as e:
        print(f"Error creating AWS client: {e}")
        return None

def check_log_stream_exists(client, log_stream_name):
    """Check if the log stream exists."""
    try:
        response = client.describe_log_streams(
            logGroupName=LOG_GROUP_NAME,
            logStreamNamePrefix=log_stream_name
        )
        
        log_streams = response.get('logStreams', [])
        for stream in log_streams:
            if stream['logStreamName'] == log_stream_name:
                return True, stream
        
        return False, None
        
    except ClientError as e:
        print(f"Error checking log stream: {e}")
        return False, None

def download_logs(log_stream_name, output_file=None):
    """Download logs from CloudWatch using boto3."""
    
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"cloudwatch_logs_{log_stream_name[:20]}_{timestamp}.txt"
    
    print(f"Downloading logs from log stream: {log_stream_name}")
    print(f"Log group: {LOG_GROUP_NAME}")
    print(f"Profile: {PROFILE}")
    print(f"Region: {REGION}")
    print(f"Output file: {output_file}")
    print("-" * 50)
    
    # Create CloudWatch Logs client
    client = get_cloudwatch_logs_client()
    if not client:
        return False
    
    # Check if log stream exists
    print("Checking if log stream exists...")
    exists, stream_info = check_log_stream_exists(client, log_stream_name)
    
    if not exists:
        print(f"Log stream '{log_stream_name}' not found in log group '{LOG_GROUP_NAME}'")
        return False
    
    print("Log stream found!")
    if stream_info:
        print(f"Stream creation time: {datetime.fromtimestamp(stream_info['creationTime']/1000)}")
        print(f"Last event time: {datetime.fromtimestamp(stream_info['lastEventTime']/1000) if 'lastEventTime' in stream_info else 'N/A'}")
    
    # Download the logs
    print("Downloading logs...")
    try:
        all_events = []
        next_token = None
        
        while True:
            kwargs = {
                'logGroupName': LOG_GROUP_NAME,
                'logStreamName': log_stream_name,
                'startFromHead': True
            }
            
            if next_token:
                kwargs['nextToken'] = next_token
            
            response = client.get_log_events(**kwargs)
            
            events = response.get('events', [])
            all_events.extend(events)
            
            print(f"Downloaded {len(events)} events (total: {len(all_events)})")
            
            # Check if there are more events
            if 'nextForwardToken' in response and response['nextForwardToken'] != next_token:
                next_token = response['nextForwardToken']
            else:
                break
        
        if not all_events:
            print("No log events found in the stream.")
            return True
        
        # Format and save logs
        print("Formatting and saving logs...")
        with open(output_file, 'w', encoding='utf-8') as f:
            for event in all_events:
                timestamp = datetime.fromtimestamp(event['timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                message = event['message']
                f.write(f"{timestamp}\t{message}\n")
        
        print(f"Logs successfully saved to: {output_file}")
        print(f"Downloaded {len(all_events)} log events")
        
        # Show first few lines as preview
        print("\nPreview of downloaded logs:")
        print("-" * 30)
        with open(output_file, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= 5:  # Show first 5 lines
                    break
                print(line.rstrip())
        
        if len(all_events) > 5:
            print("...")
            print(f"(showing first 5 of {len(all_events)} lines)")
        
        return True
        
    except ClientError as e:
        print(f"Error downloading logs: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python download_cloudwatch_logs_boto3.py <log_stream_name> [output_file]")
        print("\nExample:")
        print("python download_cloudwatch_logs_boto3.py jr_84004174dfa87cc635d131bca6f88dc3f7cee062a6b208f29d72c1c1f86d2f37")
        sys.exit(1)
    
    log_stream_name = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    success = download_logs(log_stream_name, output_file)
    
    if success:
        print("\n✅ Log download completed successfully!")
    else:
        print("\n❌ Log download failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
