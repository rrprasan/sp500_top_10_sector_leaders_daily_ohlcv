#!/usr/bin/env python3
"""
Test AWS S3 access with the configured credentials.
"""

import sys
import os

def test_s3_access():
    """Test S3 access with current credentials."""
    print("=" * 60)
    print("AWS S3 ACCESS TEST")
    print("=" * 60)
    
    # Check if credentials file exists
    credentials_path = "/Users/prajagopal/.aws/credentials"
    config_path = "/Users/prajagopal/.aws/config"
    
    print("1. Checking AWS credential files...")
    if os.path.exists(credentials_path):
        print(f"   ✅ Found credentials file: {credentials_path}")
    else:
        print(f"   ❌ Missing credentials file: {credentials_path}")
        return False
    
    if os.path.exists(config_path):
        print(f"   ✅ Found config file: {config_path}")
    else:
        print(f"   ⚠️  Config file not found: {config_path} (optional)")
    
    # Try to import boto3
    print("\n2. Testing boto3 import...")
    try:
        import boto3
        from botocore.exceptions import ClientError, NoCredentialsError
        print("   ✅ boto3 imported successfully")
    except ImportError as e:
        print(f"   ❌ Failed to import boto3: {e}")
        print("   Try: pip install boto3")
        return False
    
    # Test S3 client creation
    print("\n3. Creating S3 client...")
    try:
        s3_client = boto3.client('s3')
        print("   ✅ S3 client created successfully")
    except Exception as e:
        print(f"   ❌ Failed to create S3 client: {e}")
        return False
    
    # Test credentials by listing buckets
    print("\n4. Testing credentials (listing buckets)...")
    try:
        response = s3_client.list_buckets()
        buckets = response.get('Buckets', [])
        print(f"   ✅ Credentials valid - found {len(buckets)} buckets")
        
        # Show available buckets
        if buckets:
            print("   Available buckets:")
            for bucket in buckets[:10]:  # Show first 10
                print(f"      - {bucket['Name']}")
            if len(buckets) > 10:
                print(f"      ... and {len(buckets) - 10} more")
        
    except NoCredentialsError:
        print("   ❌ No AWS credentials found")
        return False
    except ClientError as e:
        print(f"   ❌ AWS error: {e}")
        return False
    except Exception as e:
        print(f"   ❌ Unexpected error: {e}")
        return False
    
    # Test access to our specific bucket
    print("\n5. Testing access to target bucket...")
    bucket_name = 'sp500-top-10-sector-leaders-ohlcv-s3bkt'
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"   ✅ Target bucket '{bucket_name}' is accessible")
        
        # Test listing objects (if any)
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=5)
            object_count = response.get('KeyCount', 0)
            print(f"   ✅ Bucket contains {object_count} objects (showing first 5)")
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    print(f"      - {obj['Key']} ({obj['Size']} bytes)")
        except Exception as e:
            print(f"   ⚠️  Could not list objects: {e}")
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"   ❌ Bucket '{bucket_name}' not found")
            print("   You may need to create this bucket first")
        elif error_code == '403':
            print(f"   ❌ Access denied to bucket '{bucket_name}'")
            print("   Check your AWS permissions")
        else:
            print(f"   ❌ Error accessing bucket: {e}")
        return False
    
    # Test upload capability with a small test file
    print("\n6. Testing upload capability...")
    try:
        test_content = "Test file for S3 upload capability"
        test_key = "test/pipeline_test.txt"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=test_content.encode('utf-8')
        )
        print(f"   ✅ Successfully uploaded test file: {test_key}")
        
        # Clean up test file
        s3_client.delete_object(Bucket=bucket_name, Key=test_key)
        print(f"   ✅ Test file cleaned up")
        
    except Exception as e:
        print(f"   ❌ Upload test failed: {e}")
        return False
    
    print(f"\n{'='*60}")
    print("🎉 S3 ACCESS TEST SUCCESSFUL!")
    print(f"{'='*60}")
    print("✅ AWS credentials are properly configured")
    print("✅ S3 client working correctly")
    print(f"✅ Target bucket '{bucket_name}' is accessible")
    print("✅ Upload/delete permissions confirmed")
    print("\n🚀 Ready for full pipeline execution!")
    
    return True

def main():
    """Main test function."""
    success = test_s3_access()
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())

