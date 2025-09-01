#!/usr/bin/env python3
"""
FIXED Production pipeline for downloading OHLCV data and uploading to S3.
Downloads Jan-Dec 2024 data for AAPL, MSFT, GOOGL and uploads to S3 bucket root.
FIXED: Properly handles daily data without cross-month contamination.
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
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fixed_production_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class FixedProductionOHLCVPipeline:
    """
    FIXED Production pipeline for OHLCV data download and S3 upload.
    """
    
    def __init__(self):
        """Initialize the production pipeline."""
        self.polygon_api_key = self._get_polygon_api_key()
        self.s3_client = None
        
        # Production constants
        self.S3_BUCKET = 'sp500-top-10-sector-leaders-ohlcv-s3bkt'
        self.RATE_LIMIT_DELAY = 12.5  # seconds between API calls
        self.START_DATE = '2024-01-01'
        self.END_DATE = '2024-12-31'
        self.TICKERS = ['AAPL', 'MSFT', 'GOOGL']  # Specified tickers
        
        # AWS credentials path
        self.aws_credentials_dir = '/Users/prajagopal/.aws'
        
        logger.info("FIXED Production OHLCV Pipeline initialized")
    
    def _get_polygon_api_key(self) -> str:
        """Get Polygon.io API key from environment variable."""
        api_key = os.getenv('POLYGON_API_KEY')
        if not api_key:
            logger.error("POLYGON_API_KEY environment variable not set")
            logger.error("Please set it with: export POLYGON_API_KEY=your_api_key_here")
            raise ValueError("POLYGON_API_KEY environment variable is required")
        logger.info("Polygon.io API key loaded from environment variable")
        return api_key
    
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
                logger.warning(f"No data available for {ticker} in the specified date range")
                return None
                
        except Exception as e:
            logger.error(f"Failed to fetch data for {ticker}: {e}")
            return None
    
    def _process_month_data(self, ticker: str, data: Dict[str, Any], expected_month: str) -> Optional[pd.DataFrame]:
        """
        FIXED: Process API data for a specific month only.
        This prevents cross-month contamination.
        """
        if not data.get('results'):
            return None
        
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
        
        # FIXED: Filter to only the expected month to prevent contamination
        expected_year, expected_month_num = expected_month.split('-')
        expected_year = int(expected_year)
        expected_month_num = int(expected_month_num)
        
        # Filter to only records from the expected month
        month_filter = (df['OHLC_DATE'].dt.year == expected_year) & (df['OHLC_DATE'].dt.month == expected_month_num)
        filtered_df = df[month_filter].copy()
        
        logger.info(f"Processed {len(filtered_df)} records for {ticker} in {expected_month} (filtered from {len(df)} total)")
        
        return filtered_df if len(filtered_df) > 0 else None
    
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
            
            logger.info(f"Created Parquet file: {local_filename} with {len(df)} records")
            
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
    
    def _generate_monthly_ranges(self) -> List[tuple]:
        """Generate monthly date ranges for the full year."""
        date_ranges = []
        current_date = datetime.strptime(self.START_DATE, '%Y-%m-%d')
        end_date = datetime.strptime(self.END_DATE, '%Y-%m-%d')
        
        while current_date <= end_date:
            # Calculate the last day of the current month
            if current_date.month == 12:
                next_month = current_date.replace(year=current_date.year + 1, month=1, day=1)
            else:
                next_month = current_date.replace(month=current_date.month + 1, day=1)
            
            month_end = next_month - timedelta(days=1)
            
            # Don't go beyond the specified end date
            if month_end > end_date:
                month_end = end_date
            
            month_key = f"{current_date.year:04d}-{current_date.month:02d}"
            date_ranges.append((
                month_key,
                current_date.strftime('%Y-%m-%d'),
                month_end.strftime('%Y-%m-%d')
            ))
            
            current_date = next_month
            
            # Break if we've reached the end date
            if current_date > end_date:
                break
        
        logger.info(f"Generated {len(date_ranges)} monthly date ranges")
        return date_ranges
    
    def process_ticker(self, ticker: str) -> bool:
        """FIXED: Process a single ticker for the full year."""
        logger.info(f"Starting processing for ticker: {ticker}")
        success_count = 0
        total_months = 0
        
        try:
            monthly_ranges = self._generate_monthly_ranges()
            
            for month_key, start_date, end_date in monthly_ranges:
                total_months += 1
                
                # Rate limiting
                logger.info(f"Rate limiting: waiting {self.RATE_LIMIT_DELAY} seconds...")
                time.sleep(self.RATE_LIMIT_DELAY)
                
                # Fetch data from Polygon.io for this month
                data = self._get_polygon_data(ticker, start_date, end_date)
                
                if data:
                    # FIXED: Process data for this specific month only
                    month_df = self._process_month_data(ticker, data, month_key)
                    
                    if month_df is not None and len(month_df) > 0:
                        # Save this month's data to S3
                        if self._save_and_upload_to_s3(ticker, month_key, month_df):
                            success_count += 1
                        else:
                            logger.error(f"Failed to upload {ticker} for month {month_key}")
                    else:
                        logger.warning(f"No data for {ticker} in month {month_key}")
                else:
                    logger.warning(f"No data retrieved for {ticker} from {start_date} to {end_date}")
            
            logger.info(f"Completed processing {ticker}: {success_count}/{total_months} months successful")
            return success_count == total_months and total_months > 0
            
        except Exception as e:
            logger.error(f"Error processing ticker {ticker}: {e}")
            return False
    
    def run(self) -> None:
        """Run the FIXED production pipeline."""
        logger.info("Starting FIXED Production OHLCV Pipeline")
        logger.info(f"Date range: {self.START_DATE} to {self.END_DATE}")
        logger.info(f"Tickers: {self.TICKERS}")
        logger.info(f"S3 Bucket: {self.S3_BUCKET}")
        logger.info(f"AWS Credentials: {self.aws_credentials_dir}")
        
        try:
            # Initialize S3 client
            logger.info("Initializing S3 client...")
            self.s3_client = self._initialize_s3_client()
            
            # Process each ticker
            successful_tickers = 0
            failed_tickers = []
            
            for i, ticker in enumerate(self.TICKERS, 1):
                logger.info(f"Processing ticker {i}/{len(self.TICKERS)}: {ticker}")
                
                try:
                    if self.process_ticker(ticker):
                        successful_tickers += 1
                        logger.info(f"✓ Successfully processed {ticker}")
                    else:
                        failed_tickers.append(ticker)
                        logger.error(f"✗ Failed to process {ticker}")
                        
                except Exception as e:
                    failed_tickers.append(ticker)
                    logger.error(f"Exception processing {ticker}: {e}")
            
            # Summary
            logger.info("=" * 60)
            logger.info("FIXED PRODUCTION PIPELINE SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Total tickers processed: {len(self.TICKERS)}")
            logger.info(f"Successful: {successful_tickers}")
            logger.info(f"Failed: {len(failed_tickers)}")
            
            if failed_tickers:
                logger.warning(f"Failed tickers: {', '.join(failed_tickers)}")
            
            if successful_tickers == len(self.TICKERS):
                logger.info("🎉 All tickers processed successfully with FIXED pipeline!")
                logger.info("Ready for Snowflake COPY command execution.")
            
            logger.info("FIXED production pipeline completed")
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise


def main():
    """Main entry point."""
    try:
        if not os.path.exists('config.json'):
            logger.error("config.json not found")
            sys.exit(1)
        
        pipeline = FixedProductionOHLCVPipeline()
        pipeline.run()
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
