#!/usr/bin/env python3
"""
Vectorized Scanner Test Pipeline
Downloads OHLCV data from Aug 1, 2023 to Aug 29, 2025 for all 110 tickers
and stores them as flat Parquet files in S3 for Snowflake VECTORIZED_SCANNER testing.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import os
import sys

import snowflake.connector
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
        logging.FileHandler('vectorized_scanner_test_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class VectorizedScannerTestPipeline:
    """
    Pipeline for downloading OHLCV data for Snowflake VECTORIZED_SCANNER testing.
    Downloads data for all 110 tickers from Aug 1, 2023 to Aug 29, 2025.
    Stores files in flat structure (no directories) in S3 bucket.
    """
    
    def __init__(self):
        """Initialize the pipeline."""
        self.polygon_api_key = self._get_polygon_api_key()
        self.s3_client = None
        self.snowflake_conn = None
        
        # Pipeline constants for vectorized scanner testing
        self.S3_BUCKET = 'sp500-top-10-sector-leaders-ohlcv-s3bkt'
        self.RATE_LIMIT_DELAY = 12.5  # seconds between API calls
        self.START_DATE = '2023-08-01'  # Aug 1, 2023
        self.END_DATE = '2025-08-29'    # Aug 29, 2025
        
        # AWS credentials path
        self.aws_credentials_dir = '/Users/prajagopal/.aws'
        
        logger.info("Vectorized Scanner Test Pipeline initialized")
        logger.info(f"Date range: {self.START_DATE} to {self.END_DATE}")
        logger.info(f"Target S3 bucket: {self.S3_BUCKET}")
    
    def _get_polygon_api_key(self) -> str:
        """Get Polygon.io API key from environment variable."""
        api_key = os.getenv('POLYGON_API_KEY')
        if not api_key:
            logger.error("POLYGON_API_KEY environment variable not set")
            logger.error("Please set it with: export POLYGON_API_KEY=your_api_key_here")
            raise ValueError("POLYGON_API_KEY environment variable is required")
        logger.info("Polygon.io API key loaded from environment variable")
        return api_key
    
    def _connect_to_snowflake(self) -> snowflake.connector.SnowflakeConnection:
        """Establish connection to Snowflake using the DEMO_PRAJAGOPAL connection."""
        try:
            conn = snowflake.connector.connect(
                connection_name='DEMO_PRAJAGOPAL'
            )
            logger.info("Successfully connected to Snowflake")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def _get_all_tickers_from_snowflake(self) -> List[str]:
        """Retrieve all 110 ticker symbols from Snowflake database."""
        try:
            self.snowflake_conn = self._connect_to_snowflake()
            cursor = self.snowflake_conn.cursor()
            
            query = "SELECT TICKER_SYMBOL FROM DEMODB.EQUITY_RESEARCH.SP_SECTOR_COMPANIES ORDER BY TICKER_SYMBOL"
            logger.info(f"Executing query: {query}")
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Extract ticker symbols from results
            tickers = [row[0] for row in results]
            logger.info(f"Retrieved {len(tickers)} tickers from Snowflake")
            
            cursor.close()
            return tickers
            
        except Exception as e:
            logger.error(f"Failed to retrieve tickers from Snowflake: {e}")
            raise
        finally:
            if self.snowflake_conn:
                self.snowflake_conn.close()
                logger.info("Snowflake connection closed")
    
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
    
    def _process_data_to_dataframe(self, ticker: str, data: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Process API data and convert to DataFrame."""
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
        
        logger.info(f"Processed {len(df)} records for {ticker}")
        return df
    
    def _save_and_upload_to_s3_flat(self, ticker: str, df: pd.DataFrame) -> bool:
        """Save DataFrame to Parquet and upload directly to S3 bucket root (flat structure)."""
        try:
            # Create filename for S3 root (no subdirectories) - single file per ticker
            s3_filename = f"{ticker}_2023_2025.parquet"
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
            
            logger.info(f"Created Parquet file: {local_filename} ({len(df)} records)")
            
            # Upload directly to S3 bucket root (flat structure)
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
            logger.error(f"Failed to save and upload data for {ticker}: {e}")
            # Clean up local file if it exists
            if 'local_filename' in locals() and os.path.exists(local_filename):
                try:
                    os.remove(local_filename)
                except:
                    pass
            return False
    
    def _generate_date_ranges(self) -> List[tuple]:
        """Generate date ranges for API calls (monthly to respect rate limits)."""
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
            
            date_ranges.append((
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
        """Process a single ticker for the entire date range."""
        logger.info(f"Starting processing for ticker: {ticker}")
        
        try:
            date_ranges = self._generate_date_ranges()
            all_dataframes = []
            
            for start_date, end_date in date_ranges:
                # Rate limiting
                logger.info(f"Rate limiting: waiting {self.RATE_LIMIT_DELAY} seconds...")
                time.sleep(self.RATE_LIMIT_DELAY)
                
                # Fetch data from Polygon.io
                data = self._get_polygon_data(ticker, start_date, end_date)
                
                if data:
                    # Process data to DataFrame
                    df = self._process_data_to_dataframe(ticker, data)
                    if df is not None and not df.empty:
                        all_dataframes.append(df)
                else:
                    logger.warning(f"No data retrieved for {ticker} from {start_date} to {end_date}")
            
            if all_dataframes:
                # Combine all monthly data into single DataFrame
                combined_df = pd.concat(all_dataframes, ignore_index=True)
                combined_df = combined_df.sort_values('OHLC_DATE').reset_index(drop=True)
                
                logger.info(f"Combined {len(combined_df)} total records for {ticker}")
                
                # Save and upload single file for entire date range
                success = self._save_and_upload_to_s3_flat(ticker, combined_df)
                
                if success:
                    logger.info(f"✓ Successfully processed {ticker} ({len(combined_df)} records)")
                    return True
                else:
                    logger.error(f"✗ Failed to upload data for {ticker}")
                    return False
            else:
                logger.warning(f"No data available for {ticker} in the entire date range")
                return False
            
        except Exception as e:
            logger.error(f"Error processing ticker {ticker}: {e}")
            return False
    
    def run(self) -> None:
        """Run the vectorized scanner test pipeline."""
        logger.info("=" * 80)
        logger.info("VECTORIZED SCANNER TEST PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Date range: {self.START_DATE} to {self.END_DATE}")
        logger.info(f"S3 Bucket: {self.S3_BUCKET}")
        logger.info(f"File structure: Flat (no directories)")
        logger.info(f"AWS Credentials: {self.aws_credentials_dir}")
        
        try:
            # Step 1: Get all tickers from Snowflake
            logger.info("Step 1: Retrieving all tickers from Snowflake...")
            tickers = self._get_all_tickers_from_snowflake()
            
            if not tickers:
                logger.error("No tickers retrieved from Snowflake. Exiting.")
                return
            
            logger.info(f"Retrieved {len(tickers)} tickers for processing")
            
            # Step 2: Initialize S3 client
            logger.info("Step 2: Initializing S3 client...")
            self.s3_client = self._initialize_s3_client()
            
            # Step 3: Process each ticker
            logger.info(f"Step 3: Processing {len(tickers)} tickers...")
            successful_tickers = 0
            failed_tickers = []
            
            # Calculate estimated time
            total_months = len(self._generate_date_ranges())
            total_api_calls = len(tickers) * total_months
            estimated_hours = (total_api_calls * self.RATE_LIMIT_DELAY) / 3600
            logger.info(f"Estimated processing time: ~{estimated_hours:.1f} hours ({total_api_calls} API calls)")
            
            start_time = datetime.now()
            
            for i, ticker in enumerate(tickers, 1):
                logger.info(f"Processing ticker {i}/{len(tickers)}: {ticker}")
                
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
                
                # Progress update every 10 tickers
                if i % 10 == 0:
                    elapsed = datetime.now() - start_time
                    remaining = len(tickers) - i
                    avg_time_per_ticker = elapsed.total_seconds() / i
                    estimated_remaining = timedelta(seconds=avg_time_per_ticker * remaining)
                    
                    logger.info(f"Progress: {i}/{len(tickers)} ({i/len(tickers)*100:.1f}%)")
                    logger.info(f"Elapsed: {elapsed}, Estimated remaining: {estimated_remaining}")
            
            # Summary
            total_time = datetime.now() - start_time
            logger.info("=" * 80)
            logger.info("VECTORIZED SCANNER TEST PIPELINE SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Total execution time: {total_time}")
            logger.info(f"Total tickers processed: {len(tickers)}")
            logger.info(f"Successful: {successful_tickers}")
            logger.info(f"Failed: {len(failed_tickers)}")
            
            if failed_tickers:
                logger.warning(f"Failed tickers: {', '.join(failed_tickers)}")
            
            if successful_tickers == len(tickers):
                logger.info("🎉 All tickers processed successfully!")
                logger.info("S3 bucket now contains flat Parquet files ready for Snowflake COPY command.")
                logger.info("")
                logger.info("Next steps:")
                logger.info("1. Test COPY INTO with USE_VECTORIZED_SCANNER = FALSE")
                logger.info("2. Test COPY INTO with USE_VECTORIZED_SCANNER = TRUE")
                logger.info("3. Compare performance metrics")
            else:
                logger.warning(f"Pipeline completed with {len(failed_tickers)} failures")
            
            logger.info("Vectorized Scanner Test Pipeline completed")
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise


def main():
    """Main entry point."""
    try:
        if not os.path.exists('config.json'):
            logger.error("config.json not found")
            sys.exit(1)
        
        pipeline = VectorizedScannerTestPipeline()
        pipeline.run()
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
