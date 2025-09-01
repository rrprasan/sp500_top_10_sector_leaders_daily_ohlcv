#!/usr/bin/env python3
"""
Test version of the equity data pipeline that works without Snowflake and S3.
This version uses a hardcoded list of tickers and saves files locally for testing.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import os
import sys

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class TestEquityDataPipeline:
    """
    Test version of the equity data pipeline for validation.
    """
    
    def __init__(self):
        """Initialize the test pipeline."""
        self.polygon_api_key = self._get_polygon_api_key()
        
        # Test constants - shorter date range and fewer tickers
        self.RATE_LIMIT_DELAY = 12.5  # seconds between API calls
        self.START_DATE = '2024-01-01'
        self.END_DATE = '2024-01-31'  # Just January for testing
        self.TEST_TICKERS = ['AAPL', 'MSFT', 'GOOGL']  # Small set for testing
        
        # Create output directory
        self.output_dir = 'test_output'
        os.makedirs(self.output_dir, exist_ok=True)
        
        logger.info("Test Equity Data Pipeline initialized")
    
    def _get_polygon_api_key(self) -> str:
        """Get Polygon.io API key from environment variable."""
        api_key = os.getenv('POLYGON_API_KEY')
        if not api_key:
            logger.error("POLYGON_API_KEY environment variable not set")
            logger.error("Please set it with: export POLYGON_API_KEY=your_api_key_here")
            raise ValueError("POLYGON_API_KEY environment variable is required")
        logger.info("Polygon.io API key loaded from environment variable")
        return api_key
    
    def _get_polygon_data(self, ticker: str, start_date: str, end_date: str) -> Optional[Dict[str, Any]]:
        """Fetch OHLCV data from Polygon.io API."""
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
        params = {
            'apikey': self.polygon_api_key,
            'adjusted': 'true',
            'sort': 'asc'
        }
        
        try:
            logger.info(f"Fetching data for {ticker} from {start_date} to {end_date}")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') == 'OK' and data.get('results'):
                logger.info(f"Successfully retrieved {len(data['results'])} records for {ticker}")
                return data
            else:
                logger.warning(f"No data available for {ticker}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to fetch data for {ticker}: {e}")
            return None
    
    def _process_monthly_data(self, ticker: str, data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Process API data and organize it by month."""
        if not data.get('results'):
            return {}
        
        # Convert API results to DataFrame
        records = []
        for result in data['results']:
            date = datetime.fromtimestamp(result['t'] / 1000)
            
            record = {
                'TICKER': ticker,
                'OHLC_DATE': date.strftime('%Y-%m-%d'),
                'OPEN_PRICE': result['o'],
                'HIGH_PRICE': result['h'],
                'LOW_PRICE': result['l'],
                'CLOSE_PRICE': result['c'],
                'TRADING_VOLUME': result['v'],
                'OHLC_TIMESTAMP': result['t']  # Keep milliseconds for TIMESTAMP_MILLIS
            }
            records.append(record)
        
        df = pd.DataFrame(records)
        df['OHLC_DATE'] = pd.to_datetime(df['OHLC_DATE'])
        # Convert OHLC_TIMESTAMP to datetime for TIMESTAMP_MILLIS format
        df['OHLC_TIMESTAMP'] = pd.to_datetime(df['OHLC_TIMESTAMP'], unit='ms')
        
        # Group by year-month
        monthly_data = {}
        for (year, month), group in df.groupby([df['OHLC_DATE'].dt.year, df['OHLC_DATE'].dt.month]):
            month_key = f"{year:04d}-{month:02d}"
            monthly_data[month_key] = group.copy()
            logger.info(f"Processed {len(group)} records for {ticker} in {month_key}")
        
        return monthly_data
    
    def _save_to_parquet_local(self, ticker: str, month: str, df: pd.DataFrame) -> bool:
        """Save DataFrame to Parquet format locally."""
        try:
            # Create ticker directory
            ticker_dir = os.path.join(self.output_dir, ticker)
            os.makedirs(ticker_dir, exist_ok=True)
            
            # Create filename with ticker prefix
            month_formatted = month.replace('-', '_')  # Convert 2024-01 to 2024_01
            filename = f"{ticker}_{month_formatted}.parquet"
            filepath = os.path.join(ticker_dir, filename)
            
            # Create schema with TIMESTAMP_MILLIS for OHLC_TIMESTAMP
            schema = pa.schema([
                pa.field('TICKER', pa.string()),
                pa.field('OHLC_DATE', pa.timestamp('us')),
                pa.field('OPEN_PRICE', pa.float64()),
                pa.field('HIGH_PRICE', pa.float64()),
                pa.field('LOW_PRICE', pa.float64()),
                pa.field('CLOSE_PRICE', pa.float64()),
                pa.field('TRADING_VOLUME', pa.float64()),
                pa.field('OHLC_TIMESTAMP', pa.timestamp('us'))  # TIMESTAMP_MICROS for Snowflake TIMESTAMP_NTZ
            ])
            
            # Convert to Parquet format with explicit schema
            table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            pq.write_table(table, filepath, compression='snappy')
            
            logger.info(f"Saved {filepath} ({len(df)} records)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save data for {ticker} {month}: {e}")
            return False
    
    def process_ticker(self, ticker: str) -> bool:
        """Process a single ticker."""
        logger.info(f"Processing ticker: {ticker}")
        success_count = 0
        total_months = 0
        
        try:
            # For testing, we'll just fetch the entire date range at once
            data = self._get_polygon_data(ticker, self.START_DATE, self.END_DATE)
            
            if data:
                # Process and organize data by month
                monthly_data = self._process_monthly_data(ticker, data)
                
                # Save each month's data locally
                for month, df in monthly_data.items():
                    total_months += 1
                    if self._save_to_parquet_local(ticker, month, df):
                        success_count += 1
                    else:
                        logger.error(f"Failed to save {ticker} for month {month}")
            else:
                logger.warning(f"No data retrieved for {ticker}")
            
            logger.info(f"Completed {ticker}: {success_count}/{total_months} months successful")
            return success_count == total_months and total_months > 0
            
        except Exception as e:
            logger.error(f"Error processing ticker {ticker}: {e}")
            return False
    
    def run(self) -> None:
        """Run the test pipeline."""
        logger.info("Starting Test Equity Data Pipeline")
        logger.info(f"Date range: {self.START_DATE} to {self.END_DATE}")
        logger.info(f"Test tickers: {self.TEST_TICKERS}")
        logger.info(f"Output directory: {self.output_dir}")
        
        successful_tickers = 0
        failed_tickers = []
        
        for i, ticker in enumerate(self.TEST_TICKERS, 1):
            logger.info(f"Processing ticker {i}/{len(self.TEST_TICKERS)}: {ticker}")
            
            try:
                if self.process_ticker(ticker):
                    successful_tickers += 1
                    logger.info(f"✓ Successfully processed {ticker}")
                else:
                    failed_tickers.append(ticker)
                    logger.error(f"✗ Failed to process {ticker}")
                
                # Rate limiting between tickers
                if i < len(self.TEST_TICKERS):
                    logger.info(f"Rate limiting: waiting {self.RATE_LIMIT_DELAY} seconds...")
                    time.sleep(self.RATE_LIMIT_DELAY)
                    
            except Exception as e:
                failed_tickers.append(ticker)
                logger.error(f"Exception processing {ticker}: {e}")
        
        # Summary
        logger.info("=" * 50)
        logger.info("TEST PIPELINE SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total tickers: {len(self.TEST_TICKERS)}")
        logger.info(f"Successful: {successful_tickers}")
        logger.info(f"Failed: {len(failed_tickers)}")
        
        if failed_tickers:
            logger.warning(f"Failed tickers: {', '.join(failed_tickers)}")
        
        # Show output structure
        if successful_tickers > 0:
            logger.info("\nOutput structure:")
            for root, dirs, files in os.walk(self.output_dir):
                level = root.replace(self.output_dir, '').count(os.sep)
                indent = ' ' * 2 * level
                logger.info(f"{indent}{os.path.basename(root)}/")
                subindent = ' ' * 2 * (level + 1)
                for file in files:
                    logger.info(f"{subindent}{file}")
        
        logger.info("Test pipeline completed")


def main():
    """Main entry point."""
    try:
        if not os.path.exists('config.json'):
            logger.error("config.json not found")
            sys.exit(1)
        
        pipeline = TestEquityDataPipeline()
        pipeline.run()
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
