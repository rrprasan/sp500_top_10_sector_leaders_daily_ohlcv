#!/usr/bin/env python3
"""
Simple test script to validate basic functionality.
"""

import json
import sys
import os

def test_config():
    """Test environment variable configuration."""
    print("Testing environment variables...")
    try:
        api_key = os.getenv('POLYGON_API_KEY')
        
        if api_key:
            print("  ✓ POLYGON_API_KEY environment variable is set")
            return True
        else:
            print("  ✗ POLYGON_API_KEY environment variable not set")
            print("  Please set it with: export POLYGON_API_KEY=your_api_key_here")
            return False
    except Exception as e:
        print(f"  ✗ Environment variable error: {e}")
        return False

def test_polygon_api():
    """Test Polygon.io API."""
    print("Testing Polygon.io API...")
    try:
        import requests
        
        api_key = os.getenv('POLYGON_API_KEY')
        if not api_key:
            print("  ✗ POLYGON_API_KEY environment variable not set")
            return False
        
        # Test with a simple ticker
        url = "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2024-01-01/2024-01-05"
        params = {
            'apikey': api_key,
            'adjusted': 'true'
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        if data.get('status') == 'OK' and data.get('results'):
            print(f"  ✓ API working - got {len(data['results'])} records for AAPL")
            return True
        else:
            print(f"  ✗ API error: {data}")
            return False
            
    except Exception as e:
        print(f"  ✗ API test failed: {e}")
        return False

def main():
    print("=" * 40)
    print("SIMPLE PIPELINE TEST")
    print("=" * 40)
    
    tests = [
        ("Environment Variables", test_config),
        ("Polygon.io API", test_polygon_api)
    ]
    
    passed = 0
    for name, test_func in tests:
        print(f"\n{name}:")
        if test_func():
            passed += 1
    
    print(f"\nPassed: {passed}/{len(tests)} tests")
    
    if passed == len(tests):
        print("\n✓ Basic tests passed! You can try running the pipeline.")
        return 0
    else:
        print("\n✗ Some tests failed.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
