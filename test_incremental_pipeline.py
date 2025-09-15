#!/usr/bin/env python3
"""
Test script for the Incremental OHLCV Pipeline
Tests the functionality of querying max dates and determining incremental data needs.
"""

import logging
import sys
from datetime import datetime, timedelta

# Import the incremental pipeline
from incremental_ohlcv_pipeline import IncrementalOHLCVPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_incremental_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def test_snowflake_connection():
    """Test Snowflake connection and basic queries."""
    logger.info("=" * 60)
    logger.info("TESTING SNOWFLAKE CONNECTION")
    logger.info("=" * 60)
    
    try:
        pipeline = IncrementalOHLCVPipeline()
        
        # Test connection
        conn = pipeline._initialize_snowflake_connection()
        logger.info("✓ Snowflake connection successful")
        
        # Test getting tickers
        tickers = pipeline.get_all_tickers_from_snowflake()
        logger.info(f"✓ Retrieved {len(tickers)} tickers from source table")
        logger.info(f"Sample tickers: {tickers[:5]}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"✗ Snowflake connection test failed: {e}")
        return False


def test_max_date_queries():
    """Test querying max dates for specific tickers."""
    logger.info("=" * 60)
    logger.info("TESTING MAX DATE QUERIES")
    logger.info("=" * 60)
    
    try:
        pipeline = IncrementalOHLCVPipeline()
        
        # Test with a few sample tickers
        test_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        
        for ticker in test_tickers:
            max_date = pipeline.get_max_date_for_ticker(ticker)
            if max_date:
                logger.info(f"✓ {ticker}: Last data date = {max_date}")
            else:
                logger.info(f"ℹ {ticker}: No existing data found")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Max date query test failed: {e}")
        return False


def test_date_range_calculation():
    """Test the calculation of date ranges for incremental loading."""
    logger.info("=" * 60)
    logger.info("TESTING DATE RANGE CALCULATION")
    logger.info("=" * 60)
    
    try:
        pipeline = IncrementalOHLCVPipeline()
        
        # Get ticker date ranges
        ticker_ranges = pipeline.get_ticker_date_ranges()
        
        logger.info(f"Found {len(ticker_ranges)} tickers needing updates:")
        
        # Show first 10 tickers that need updates
        count = 0
        for ticker, (start_date, end_date) in ticker_ranges.items():
            if count < 10:
                logger.info(f"  {ticker}: {start_date} to {end_date}")
                count += 1
            else:
                break
        
        if len(ticker_ranges) > 10:
            logger.info(f"  ... and {len(ticker_ranges) - 10} more tickers")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Date range calculation test failed: {e}")
        return False


def test_s3_connection():
    """Test S3 connection."""
    logger.info("=" * 60)
    logger.info("TESTING S3 CONNECTION")
    logger.info("=" * 60)
    
    try:
        pipeline = IncrementalOHLCVPipeline()
        
        # Test S3 connection
        s3_client = pipeline._initialize_s3_client()
        logger.info("✓ S3 connection successful")
        
        # List some files in the bucket
        response = s3_client.list_objects_v2(
            Bucket=pipeline.S3_BUCKET,
            MaxKeys=10
        )
        
        if 'Contents' in response:
            logger.info(f"✓ Found {len(response['Contents'])} files in S3 bucket")
            logger.info("Sample files:")
            for obj in response['Contents'][:5]:
                logger.info(f"  - {obj['Key']} ({obj['Size']} bytes)")
        else:
            logger.info("ℹ S3 bucket is empty")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ S3 connection test failed: {e}")
        return False


def test_polygon_api():
    """Test Polygon.io API connection."""
    logger.info("=" * 60)
    logger.info("TESTING POLYGON.IO API CONNECTION")
    logger.info("=" * 60)
    
    try:
        pipeline = IncrementalOHLCVPipeline()
        
        # Test with a small date range for AAPL
        test_start = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        test_end = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        logger.info(f"Testing API call for AAPL from {test_start} to {test_end}")
        
        data = pipeline._get_polygon_data('AAPL', test_start, test_end)
        
        if data and data.get('results'):
            logger.info(f"✓ Polygon.io API working - retrieved {len(data['results'])} records")
        else:
            logger.warning("⚠ Polygon.io API returned no data (might be weekend/holiday)")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Polygon.io API test failed: {e}")
        return False


def main():
    """Run all tests."""
    logger.info("🚀 STARTING INCREMENTAL PIPELINE TESTS")
    logger.info("=" * 80)
    
    tests = [
        ("Snowflake Connection", test_snowflake_connection),
        ("Max Date Queries", test_max_date_queries),
        ("Date Range Calculation", test_date_range_calculation),
        ("S3 Connection", test_s3_connection),
        ("Polygon.io API", test_polygon_api)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n🧪 Running: {test_name}")
        try:
            results[test_name] = test_func()
        except Exception as e:
            logger.error(f"Test {test_name} crashed: {e}")
            results[test_name] = False
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("TEST SUMMARY")
    logger.info("=" * 80)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{status} - {test_name}")
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! The incremental pipeline is ready to use.")
        logger.info("\nTo run the incremental pipeline:")
        logger.info("python incremental_ohlcv_pipeline.py")
    else:
        logger.error("❌ Some tests failed. Please check the configuration and try again.")
        sys.exit(1)


if __name__ == "__main__":
    main()
