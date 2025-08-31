#!/usr/bin/env python3
"""
Complete test for Snowflake integration with ticker retrieval.
"""

import snowflake.connector
import sys

def test_complete_snowflake_integration():
    """Test complete Snowflake integration including ticker retrieval."""
    print("=" * 70)
    print("COMPLETE SNOWFLAKE INTEGRATION TEST")
    print("=" * 70)
    
    try:
        # Connect to Snowflake
        print("1. Establishing Snowflake connection...")
        conn = snowflake.connector.connect(
            connection_name='DEMO_PRAJAGOPAL'
        )
        cursor = conn.cursor()
        print("   ✅ Connection successful with PAT authentication")
        
        # Get connection details
        print("\n2. Verifying connection details...")
        cursor.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        result = cursor.fetchone()
        print(f"   ✅ Snowflake version: {result[0]}")
        print(f"   ✅ Current user: {result[1]}")
        print(f"   ✅ Current database: {result[2]}")
        print(f"   ✅ Current schema: {result[3]}")
        
        # Test the exact query from the pipeline
        print("\n3. Testing ticker retrieval query...")
        query = "SELECT TICKER_SYMBOL FROM DEMODB.EQUITY_RESEARCH.SP_SECTOR_COMPANIES"
        print(f"   Query: {query}")
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Extract ticker symbols (same logic as pipeline)
        tickers = [row[0] for row in results]
        
        print(f"   ✅ Successfully retrieved {len(tickers)} tickers")
        
        # Show sample tickers
        print(f"\n4. Sample tickers (first 10):")
        for i, ticker in enumerate(tickers[:10], 1):
            print(f"      {i:2d}. {ticker}")
        
        # Show ticker distribution by sector
        print(f"\n5. Analyzing ticker distribution by sector...")
        cursor.execute("""
            SELECT SP_SECTOR, COUNT(*) as ticker_count 
            FROM DEMODB.EQUITY_RESEARCH.SP_SECTOR_COMPANIES 
            GROUP BY SP_SECTOR 
            ORDER BY ticker_count DESC
        """)
        sectors = cursor.fetchall()
        
        print("   Sector distribution:")
        for sector, count in sectors:
            print(f"      {sector}: {count} tickers")
        
        # Test a few specific tickers that should exist
        print(f"\n6. Verifying presence of expected major tickers...")
        expected_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        found_tickers = []
        missing_tickers = []
        
        for expected in expected_tickers:
            if expected in tickers:
                found_tickers.append(expected)
                print(f"      ✅ {expected} - Found")
            else:
                missing_tickers.append(expected)
                print(f"      ❌ {expected} - Missing")
        
        # Summary
        print(f"\n{'='*70}")
        print("📊 INTEGRATION TEST SUMMARY")
        print(f"{'='*70}")
        print(f"✅ Connection: Successful (PAT authentication)")
        print(f"✅ Database access: DEMODB.EQUITY_RESEARCH.SP_SECTOR_COMPANIES")
        print(f"✅ Total tickers available: {len(tickers)}")
        print(f"✅ Sectors covered: {len(sectors)}")
        print(f"✅ Expected tickers found: {len(found_tickers)}/{len(expected_tickers)}")
        
        if missing_tickers:
            print(f"⚠️  Missing expected tickers: {', '.join(missing_tickers)}")
        
        cursor.close()
        conn.close()
        
        print(f"\n🎉 SNOWFLAKE INTEGRATION FULLY OPERATIONAL!")
        print(f"The pipeline can now retrieve {len(tickers)} tickers for processing.")
        
        return True, tickers
        
    except Exception as e:
        print(f"\n❌ SNOWFLAKE INTEGRATION TEST FAILED")
        print(f"Error: {e}")
        return False, []

def main():
    """Main test function."""
    success, tickers = test_complete_snowflake_integration()
    
    if success:
        print(f"\n🚀 READY FOR PIPELINE EXECUTION")
        print(f"The main pipeline can now process all {len(tickers)} tickers from Snowflake!")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())

