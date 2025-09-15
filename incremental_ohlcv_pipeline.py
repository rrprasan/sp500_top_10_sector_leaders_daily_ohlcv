#!/usr/bin/env python3
"""
Incremental OHLCV Pipeline for SP500 Sector Leaders
Downloads OHLCV data only for dates after the last stored date for each ticker in Snowflake.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import os
import sys

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import snowflake.connector
from botocore.exceptions import ClientError, NoCredentialsError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('incremental_ohlcv_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class IncrementalOHLCVPipeline:
    """
    Incremental pipeline for OHLCV data download for SP500 sector leaders.
    Only downloads data after the last stored date for each ticker.
    """
    
    def __init__(self):
        """Initialize the incremental pipeline."""
        self.polygon_api_key = self._get_polygon_api_key()
        self.s3_client = None
        self.snowflake_conn = None
        
        # Pipeline constants
        self.S3_BUCKET = 'sp500-top-10-sector-leaders-ohlcv-s3bkt'
        self.RATE_LIMIT_DELAY = 12.5  # seconds between API calls (5 calls per minute)
        
        # AWS credentials path
        self.aws_credentials_dir = '/Users/prajagopal/.aws'
        
        # Snowflake connection details
        self.SNOWFLAKE_CONNECTION = 'DEMO_PRAJAGOPAL'
        self.SNOWFLAKE_CONNECTIONS_FILE = '/Users/prajagopal/.snowflake/connections.toml'
        self.SNOWFLAKE_TABLE = 'sp500_top10_sector_ohlcv_itbl'
        self.TICKER_SOURCE_TABLE = 'DEMODB.EQUITY_RESEARCH.SP_SECTOR_COMPANIES'
        
        # Calculate end date (latest available trading date - typically yesterday or Friday)
        today = datetime.now()
        # If today is weekend, use Friday as end date
        if today.weekday() == 5:  # Saturday
            self.END_DATE = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        elif today.weekday() == 6:  # Sunday
            self.END_DATE = (today - timedelta(days=2)).strftime('%Y-%m-%d')
        else:
            # Use yesterday for weekdays (markets close after trading day)
            self.END_DATE = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        
        logger.info("Incremental OHLCV Pipeline initialized")
        logger.info(f"End date: {self.END_DATE}")
    
    def _get_polygon_api_key(self) -> str:
        """Get Polygon.io API key from environment variable."""
        api_key = os.getenv('POLYGON_API_KEY')
        if not api_key:
            logger.error("POLYGON_API_KEY environment variable not set")
            logger.error("Please set it with: export POLYGON_API_KEY=your_api_key_here")
            raise ValueError("POLYGON_API_KEY environment variable is required")
        logger.info("Polygon.io API key loaded from environment variable")
        return api_key
    
    def _initialize_snowflake_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Initialize Snowflake connection using connections.toml file."""
        try:
            conn = snowflake.connector.connect(
                connection_name=self.SNOWFLAKE_CONNECTION
            )
            logger.info(f"Snowflake connection initialized successfully using {self.SNOWFLAKE_CONNECTION}")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            logger.error(f"Ensure {self.SNOWFLAKE_CONNECTIONS_FILE} exists with {self.SNOWFLAKE_CONNECTION} connection")
            raise
    
    def _initialize_s3_client(self) -> boto3.client:
        """Initialize AWS S3 client with specified credentials."""
        try:
            # Set AWS credentials directory
            os.environ['AWS_SHARED_CREDENTIALS_FILE'] = os.path.join(self.aws_credentials_dir, 'credentials')
            
            s3_client = boto3.client('s3')
            # Test the connection
            s3_client.head_bucket(Bucket=self.S3_BUCKET)
            logger.info(f"S3 client initialized successfully for bucket: {self.S3_BUCKET}")
            return s3_client
        except NoCredentialsError:
            logger.error(f"AWS credentials not found in {self.aws_credentials_dir}")
            raise
        except ClientError as e:
            logger.error(f"Failed to access S3 bucket {self.S3_BUCKET}: {e}")
            raise
    
    def get_all_tickers_from_snowflake(self) -> List[str]:
        """Retrieve all ticker symbols from the Snowflake source table."""
        try:
            if not self.snowflake_conn:
                self.snowflake_conn = self._initialize_snowflake_connection()
            
            cursor = self.snowflake_conn.cursor()
            
            # Query to get all ticker symbols
            query = f"SELECT TICKER_SYMBOL FROM {self.TICKER_SOURCE_TABLE} ORDER BY TICKER_SYMBOL"
            cursor.execute(query)
            
            tickers = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            logger.info(f"Retrieved {len(tickers)} ticker symbols from Snowflake")
            logger.info(f"Sample tickers: {tickers[:10]}")
            
            return tickers
            
        except Exception as e:
            logger.error(f"Failed to retrieve tickers from Snowflake: {e}")
            raise
    
    def get_max_date_for_ticker(self, ticker: str) -> Optional[str]:
        """Get the maximum date for which OHLCV data exists for a specific ticker."""
        try:
            if not self.snowflake_conn:
                self.snowflake_conn = self._initialize_snowflake_connection()
            
            cursor = self.snowflake_conn.cursor()
            
            # Query to get max date for the ticker
            query = f"SELECT MAX(OHLC_DATE) FROM {self.SNOWFLAKE_TABLE} WHERE TICKER = %s"
            cursor.execute(query, (ticker,))
            
            result = cursor.fetchone()
            cursor.close()
            
            if result and result[0]:
                max_date = result[0]
                if isinstance(max_date, datetime):
                    max_date_str = max_date.strftime('%Y-%m-%d')
                else:
                    max_date_str = str(max_date)
                logger.info(f"Max date for {ticker}: {max_date_str}")
                return max_date_str
            else:
                logger.info(f"No existing data found for {ticker}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to get max date for {ticker}: {e}")
            return None
    
    def get_ticker_date_ranges(self) -> Dict[str, Tuple[str, str]]:
        """Get start and end dates for each ticker based on existing data."""
        try:
            tickers = self.get_all_tickers_from_snowflake()
            ticker_ranges = {}
            
            for ticker in tickers:
                max_date = self.get_max_date_for_ticker(ticker)
                
                if max_date:
                    # Start from the day after the max date
                    max_datetime = datetime.strptime(max_date, '%Y-%m-%d')
                    start_date = (max_datetime + timedelta(days=1)).strftime('%Y-%m-%d')
                    
                    # Check if start_date is after end_date (no new data needed)
                    if start_date > self.END_DATE:
                        logger.info(f"No new data needed for {ticker} (last date: {max_date})")
                        continue
                    
                    ticker_ranges[ticker] = (start_date, self.END_DATE)
                    logger.info(f"Will download {ticker} from {start_date} to {self.END_DATE}")
                else:
                    # No existing data, download from 2 years ago
                    start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
                    ticker_ranges[ticker] = (start_date, self.END_DATE)
                    logger.info(f"No existing data for {ticker}, will download from {start_date} to {self.END_DATE}")
            
            logger.info(f"Found {len(ticker_ranges)} tickers that need data updates")
            return ticker_ranges
            
        except Exception as e:
            logger.error(f"Failed to get ticker date ranges: {e}")
            raise
    
    def _get_polygon_data(self, ticker: str, start_date: str, end_date: str) -> Optional[Dict[str, Any]]:
        """Fetch OHLCV data from Polygon.io API."""
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
        params = {
            'apikey': self.polygon_api_key,
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000  # Maximum limit to get all data
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
                logger.warning(f"No data available for {ticker} in the specified date range")
                return None
                
        except Exception as e:
            logger.error(f"Failed to fetch data for {ticker}: {e}")
            return None
    
    def _process_data_by_month(self, ticker: str, data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
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
                'OHLC_TIMESTAMP': result['t']  # Keep milliseconds for conversion
            }
            records.append(record)
        
        df = pd.DataFrame(records)
        df['OHLC_DATE'] = pd.to_datetime(df['OHLC_DATE'])
        # Convert OHLC_TIMESTAMP to datetime for TIMESTAMP_MICROS format
        df['OHLC_TIMESTAMP'] = pd.to_datetime(df['OHLC_TIMESTAMP'], unit='ms')
        
        # Group by year-month
        monthly_data = {}
        for (year, month), group in df.groupby([df['OHLC_DATE'].dt.year, df['OHLC_DATE'].dt.month]):
            month_key = f"{year:04d}-{month:02d}"
            monthly_data[month_key] = group.copy()
            logger.info(f"Processed {len(group)} records for {ticker} in {month_key}")
        
        return monthly_data
    
    def _save_and_upload_to_s3(self, ticker: str, month: str, df: pd.DataFrame) -> bool:
        """Save DataFrame to Parquet and upload directly to S3 bucket root."""
        try:
            # Create filename for S3 root (no subdirectories)
            month_formatted = month.replace('-', '_')  # Convert 2024-01 to 2024_01
            s3_filename = f"{ticker}_{month_formatted}.parquet"
            local_filename = s3_filename
            
            # Create schema with TIMESTAMP_MICROS for Snowflake compatibility
            schema = pa.schema([
                pa.field('TICKER', pa.string()),
                pa.field('OHLC_DATE', pa.timestamp('us')),
                pa.field('OPEN_PRICE', pa.float64()),
                pa.field('HIGH_PRICE', pa.float64()),
                pa.field('LOW_PRICE', pa.float64()),
                pa.field('CLOSE_PRICE', pa.float64()),
                pa.field('TRADING_VOLUME', pa.float64()),
                pa.field('OHLC_TIMESTAMP', pa.timestamp('us'))  # Microsecond precision
            ])
            
            # Convert to Parquet format
            table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            pq.write_table(table, local_filename, compression='snappy')
            
            logger.info(f"Created Parquet file: {local_filename}")
            
            # Upload directly to S3 bucket root
            if not self.s3_client:
                self.s3_client = self._initialize_s3_client()
            
            self.s3_client.upload_file(
                local_filename,
                self.S3_BUCKET,
                s3_filename  # No subdirectory - directly in bucket root
            )
            
            logger.info(f"Successfully uploaded {s3_filename} to S3 bucket root")
            
            # Clean up local file
            os.remove(local_filename)
            logger.info(f"Cleaned up local file: {local_filename}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to save and upload data for {ticker} {month}: {e}")
            # Clean up local file if it exists
            if os.path.exists(local_filename):
                try:
                    os.remove(local_filename)
                except:
                    pass
            return False
    
    def process_ticker(self, ticker: str, start_date: str, end_date: str) -> bool:
        """Process a single ticker for the specified date range."""
        logger.info(f"Starting processing for ticker: {ticker} ({start_date} to {end_date})")
        success_count = 0
        total_files = 0
        
        try:
            # Fetch data from Polygon.io for the specified date range
            data = self._get_polygon_data(ticker, start_date, end_date)
            
            if data:
                # Process and organize data by month
                monthly_data = self._process_data_by_month(ticker, data)
                
                # Save each month's data to S3
                for month, df in monthly_data.items():
                    total_files += 1
                    if self._save_and_upload_to_s3(ticker, month, df):
                        success_count += 1
                    else:
                        logger.error(f"Failed to process {ticker} for month {month}")
            else:
                logger.warning(f"No data retrieved for {ticker}")
                return False
            
            logger.info(f"Completed processing {ticker}: {success_count}/{total_files} files successful")
            return success_count == total_files and total_files > 0
            
        except Exception as e:
            logger.error(f"Error processing ticker {ticker}: {e}")
            return False
    
    def run(self) -> None:
        """Run the incremental pipeline."""
        logger.info("=" * 80)
        logger.info("INCREMENTAL OHLCV PIPELINE FOR SP500 SECTOR LEADERS")
        logger.info("=" * 80)
        logger.info(f"End date: {self.END_DATE}")
        logger.info(f"S3 Bucket: {self.S3_BUCKET}")
        logger.info(f"Snowflake Table: {self.SNOWFLAKE_TABLE}")
        logger.info(f"Snowflake Connection: {self.SNOWFLAKE_CONNECTION}")
        
        try:
            # Step 1: Get ticker date ranges based on existing data
            logger.info("Step 1: Analyzing existing data and determining date ranges...")
            ticker_ranges = self.get_ticker_date_ranges()
            
            if not ticker_ranges:
                logger.info("No tickers need data updates. All data is current!")
                return
            
            logger.info(f"Found {len(ticker_ranges)} tickers that need data updates")
            
            # Step 2: Initialize S3 client
            logger.info("Step 2: Initializing S3 client...")
            self.s3_client = self._initialize_s3_client()
            
            # Step 3: Process each ticker
            logger.info("Step 3: Processing tickers with incremental data...")
            successful_tickers = 0
            failed_tickers = []
            skipped_tickers = 0
            
            for i, (ticker, (start_date, end_date)) in enumerate(ticker_ranges.items(), 1):
                logger.info(f"Processing ticker {i}/{len(ticker_ranges)}: {ticker}")
                
                try:
                    # Rate limiting
                    if i > 1:  # Don't wait before the first ticker
                        logger.info(f"Rate limiting: waiting {self.RATE_LIMIT_DELAY} seconds...")
                        time.sleep(self.RATE_LIMIT_DELAY)
                    
                    if self.process_ticker(ticker, start_date, end_date):
                        successful_tickers += 1
                        logger.info(f"✓ Successfully processed {ticker}")
                    else:
                        failed_tickers.append(ticker)
                        logger.error(f"✗ Failed to process {ticker}")
                        
                except Exception as e:
                    failed_tickers.append(ticker)
                    logger.error(f"Exception processing {ticker}: {e}")
            
            # Step 4: Summary
            logger.info("=" * 80)
            logger.info("INCREMENTAL PIPELINE SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Total tickers analyzed: {len(self.get_all_tickers_from_snowflake())}")
            logger.info(f"Tickers needing updates: {len(ticker_ranges)}")
            logger.info(f"Successfully processed: {successful_tickers}")
            logger.info(f"Failed: {len(failed_tickers)}")
            
            if len(ticker_ranges) > 0:
                logger.info(f"Success rate: {(successful_tickers/len(ticker_ranges)*100):.1f}%")
            
            if failed_tickers:
                logger.warning(f"Failed tickers ({len(failed_tickers)}): {', '.join(failed_tickers)}")
            
            if successful_tickers == len(ticker_ranges):
                logger.info("🎉 All required tickers processed successfully!")
            elif successful_tickers > 0:
                logger.info(f"✅ Partial success: {successful_tickers} out of {len(ticker_ranges)} tickers processed")
            else:
                logger.error("❌ No tickers were processed successfully")
            
            logger.info("Incremental pipeline completed")
            logger.info("Ready for Snowflake COPY command execution to load new data.")
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
        finally:
            # Clean up connections
            if self.snowflake_conn:
                self.snowflake_conn.close()
                logger.info("Snowflake connection closed")


def main():
    """Main entry point."""
    try:
        pipeline = IncrementalOHLCVPipeline()
        pipeline.run()
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
