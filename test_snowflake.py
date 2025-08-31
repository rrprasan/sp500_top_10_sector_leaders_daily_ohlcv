#!/usr/bin/env python3
"""
Focused test for Snowflake connectivity and ticker retrieval.
"""

import snowflake.connector
import sys

def test_snowflake_connection():
    """Test Snowflake connection and ticker retrieval."""
    print("=" * 60)
    print("SNOWFLAKE CONNECTION AND TICKER RETRIEVAL TEST")
    print("=" * 60)
    
    try:
        # Connect to Snowflake
        print("1. Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            connection_name='DEMO_PRAJAGOPAL'
        )
        print("   ✓ Connection successful")
        
        # Test basic query
        print("\n2. Testing basic query...")
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_DATABASE()")
        result = cursor.fetchone()
        print(f"   ✓ Snowflake version: {result[0]}")
        print(f"   ✓ Current user: {result[1]}")
        print(f"   ✓ Current database: {result[2]}")
        
        # Test ticker table access
        print("\n3. Testing ticker table access...")
        query = "SELECT TICKER FROM DEMODB.EQUITY_RESEARCH.SP_SECTOR_COMPANIES LIMIT 10"
        print(f"   Query: {query}")
        
        cursor.execute(query)
        tickers = cursor.fetchall()
        
        print(f"   ✓ Successfully retrieved {len(tickers)} sample tickers:")
        for i, ticker in enumerate(tickers, 1):
            print(f"      {i}. {ticker[0]}")
        
        # Get total count
        print("\n4. Getting total ticker count...")
        cursor.execute("SELECT COUNT(*) FROM DEMODB.EQUITY_RESEARCH.SP_SECTOR_COMPANIES")
        total_count = cursor.fetchone()[0]
        print(f"   ✓ Total tickers available: {total_count}")
        
        cursor.close()
        conn.close()
        
        print(f"\n{'='*60}")
        print("✅ SNOWFLAKE TEST SUCCESSFUL")
        print(f"{'='*60}")
        print(f"✓ Connection established with PAT authentication")
        print(f"✓ Database access confirmed")
        print(f"✓ Ticker table accessible")
        print(f"✓ {total_count} tickers available for processing")
        
        return True
        
    except Exception as e:
        print(f"\n❌ SNOWFLAKE TEST FAILED")
        print(f"Error: {e}")
        return False

def main():
    """Main test function."""
    success = test_snowflake_connection()
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())

