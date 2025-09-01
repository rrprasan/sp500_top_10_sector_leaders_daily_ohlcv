#!/usr/bin/env python3
"""
Test script to validate the setup and configuration for the equity data pipeline.

This script performs basic validation checks without running the full pipeline:
1. Checks if all required dependencies are installed
2. Validates configuration file
3. Tests Snowflake connection
4. Tests AWS S3 access
5. Tests Polygon.io API connectivity

Run this script before executing the main pipeline to ensure everything is configured correctly.
"""

import json
import sys
import os
from typing import Dict, Any

def test_imports():
    """Test if all required packages can be imported."""
    print("Testing package imports...")
    
    required_packages = [
        ('snowflake.connector', 'snowflake-connector-python'),
        ('requests', 'requests'),
        ('pandas', 'pandas'),
        ('pyarrow', 'pyarrow'),
        ('boto3', 'boto3')
    ]
    
    missing_packages = []
    
    for package, pip_name in required_packages:
        try:
            __import__(package)
            print(f"  ✓ {package}")
        except ImportError:
            print(f"  ✗ {package} (install with: pip install {pip_name})")
            missing_packages.append(pip_name)
    
    if missing_packages:
        print(f"\nMissing packages: {', '.join(missing_packages)}")
        print("Install with: pip install " + " ".join(missing_packages))
        return False
    
    print("All required packages are available.\n")
    return True

def test_config():
    """Test if environment variable is set."""
    print("Testing environment variables...")
    
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        print("  ✗ POLYGON_API_KEY environment variable not set")
        print("  Please set it with: export POLYGON_API_KEY=your_api_key_here")
        return False
    
    if api_key == 'your_api_key_here' or api_key == 'your_polygon_io_api_key_here':
        print("  ✗ Please set a valid Polygon.io API key in POLYGON_API_KEY environment variable")
        return False
    
    print("  ✓ POLYGON_API_KEY environment variable is set")
    return True

def test_snowflake_connection():
    """Test Snowflake connection."""
    print("Testing Snowflake connection...")
    
    try:
        import snowflake.connector
        
        # Try to connect using the DEMO_PRAJAGOPAL connection
        conn = snowflake.connector.connect(
            connection_name='DEMO_PRAJAGOPAL'
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"  ✓ Connected to Snowflake (version: {version})")
        return True
        
    except Exception as e:
        print(f"  ✗ Snowflake connection failed: {e}")
        print("  Check your ~/.snowflake/connections.toml configuration")
        return False

def test_s3_access():
    """Test AWS S3 access."""
    print("Testing AWS S3 access...")
    
    try:
        import boto3
        from botocore.exceptions import ClientError, NoCredentialsError
    except ImportError as e:
        print(f"  ✗ Failed to import boto3: {e}")
        return False
    
    try:
        s3_client = boto3.client('s3')
        bucket_name = 'sp500-top-10-sector-leaders-ohlcv-s3bkt'
        
        # Test bucket access
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"  ✓ S3 bucket '{bucket_name}' is accessible")
        return True
        
    except NoCredentialsError:
        print("  ✗ AWS credentials not found")
        print("  Configure with: aws configure")
        return False
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"  ✗ S3 bucket '{bucket_name}' not found")
        elif error_code == '403':
            print(f"  ✗ Access denied to S3 bucket '{bucket_name}'")
        else:
            print(f"  ✗ S3 error: {e}")
        return False
    except Exception as e:
        print(f"  ✗ S3 test failed: {e}")
        return False

def test_polygon_api():
    """Test Polygon.io API connectivity."""
    print("Testing Polygon.io API...")
    
    try:
        import requests
        
        api_key = os.getenv('POLYGON_API_KEY')
        if not api_key:
            print("  ✗ POLYGON_API_KEY environment variable not set")
            return False
        
        # Test API with a simple request
        url = "https://api.polygon.io/v3/reference/tickers"
        params = {
            'apikey': api_key,
            'limit': 1
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        if data.get('status') == 'OK':
            print("  ✓ Polygon.io API is accessible")
            return True
        else:
            print(f"  ✗ Polygon.io API error: {data.get('error', 'Unknown error')}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"  ✗ Polygon.io API request failed: {e}")
        return False
    except Exception as e:
        print(f"  ✗ Polygon.io API test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("=" * 50)
    print("EQUITY DATA PIPELINE - SETUP VALIDATION")
    print("=" * 50)
    
    tests = [
        ("Package Imports", test_imports),
        ("Environment Variables", test_config),
        ("Snowflake Connection", test_snowflake_connection),
        ("AWS S3 Access", test_s3_access),
        ("Polygon.io API", test_polygon_api)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"  ✗ Test failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("TEST SUMMARY")
    print("=" * 50)
    
    passed = 0
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nPassed: {passed}/{len(results)} tests")
    
    if passed == len(results):
        print("\n✓ All tests passed! The pipeline is ready to run.")
        print("Execute: python equity_data_pipeline.py")
        return 0
    else:
        print(f"\n✗ {len(results) - passed} test(s) failed. Please fix the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
