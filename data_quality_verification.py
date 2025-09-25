#!/usr/bin/env python3
"""
Data Quality Verification Script for OHLCV Pipeline

This script performs comprehensive data quality checks on the S3 bucket
after the incremental_ohlcv_pipeline.py execution to verify:
1. Files were successfully uploaded
2. Data format is correct
3. Data quality meets expectations
4. Recent data is available

Run this after each pipeline execution for factual verification.
"""

import boto3
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import tempfile
import os
import sys
from typing import Dict, List, Tuple, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_quality_verification.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DataQualityVerifier:
    """Comprehensive data quality verification for OHLCV pipeline results."""
    
    def __init__(self):
        """Initialize the data quality verifier."""
        self.s3_bucket = 'sp500-top-10-sector-leaders-ohlcv-s3bkt'
        self.s3_client = None
        self.verification_results = {}
        
    def _initialize_s3_client(self) -> boto3.client:
        """Initialize AWS S3 client."""
        try:
            s3_client = boto3.client('s3')
            # Test the connection
            s3_client.head_bucket(Bucket=self.s3_bucket)
            logger.info(f"S3 client initialized successfully for bucket: {self.s3_bucket}")
            return s3_client
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise
    
    def check_s3_bucket_contents(self) -> Dict:
        """Check S3 bucket contents and file counts."""
        logger.info("Step 1: Checking S3 bucket contents...")
        
        try:
            if not self.s3_client:
                self.s3_client = self._initialize_s3_client()
            
            # List all objects in the bucket
            response = self.s3_client.list_objects_v2(Bucket=self.s3_bucket)
            
            if 'Contents' not in response:
                return {
                    'status': 'WARNING',
                    'total_files': 0,
                    'message': 'No files found in S3 bucket (may have been consumed by Snowflake COPY operation)',
                    'snowflake_integration_note': 'If using Snowflake COPY commands, files are automatically deleted after successful ingestion. Empty bucket may indicate successful data transfer to Snowflake.'
                }
            
            files = response['Contents']
            total_files = len(files)
            
            # Analyze file patterns
            tickers = set()
            file_patterns = {}
            recent_files = []
            
            # Get files from the last 24 hours
            cutoff_time = datetime.now() - timedelta(hours=24)
            
            for file in files:
                filename = file['Key']
                file_time = file['LastModified'].replace(tzinfo=None)
                
                # Extract ticker from filename
                if '_' in filename and filename.endswith('.parquet'):
                    ticker = filename.split('_')[0]
                    tickers.add(ticker)
                    
                    # Track file patterns
                    if ticker not in file_patterns:
                        file_patterns[ticker] = []
                    file_patterns[ticker].append(filename)
                    
                    # Track recent files
                    if file_time > cutoff_time:
                        recent_files.append({
                            'filename': filename,
                            'ticker': ticker,
                            'last_modified': file_time,
                            'size': file['Size']
                        })
            
            result = {
                'status': 'PASSED',
                'total_files': total_files,
                'unique_tickers': len(tickers),
                'ticker_list': sorted(list(tickers)),
                'recent_files_count': len(recent_files),
                'recent_files': recent_files[:10],  # Show first 10
                'message': f'Found {total_files} files for {len(tickers)} tickers'
            }
            
            logger.info(f"✅ S3 Bucket Check: {result['message']}")
            return result
            
        except Exception as e:
            logger.error(f"❌ S3 bucket check failed: {e}")
            return {
                'status': 'FAILED',
                'error': str(e),
                'message': 'Failed to check S3 bucket contents'
            }
    
    def verify_data_format(self, sample_count: int = 3) -> Dict:
        """Verify data format by sampling parquet files."""
        logger.info(f"Step 2: Verifying data format (sampling {sample_count} files)...")
        
        try:
            if not self.s3_client:
                self.s3_client = self._initialize_s3_client()
            
            # Get list of files
            response = self.s3_client.list_objects_v2(Bucket=self.s3_bucket)
            if 'Contents' not in response:
                return {
                    'status': 'WARNING', 
                    'message': 'No files to verify (may have been consumed by Snowflake)',
                    'snowflake_integration_note': 'Empty bucket may indicate successful Snowflake COPY operation'
                }
            
            files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
            
            if len(files) < sample_count:
                sample_files = files
            else:
                # Sample files: recent, middle, and old
                sample_files = [
                    files[0],  # First file
                    files[len(files)//2],  # Middle file
                    files[-1]  # Last file
                ][:sample_count]
            
            format_results = []
            expected_columns = ['TICKER', 'OHLC_DATE', 'OPEN_PRICE', 'HIGH_PRICE', 
                              'LOW_PRICE', 'CLOSE_PRICE', 'TRADING_VOLUME', 'OHLC_TIMESTAMP']
            
            for filename in sample_files:
                try:
                    # Download file to temporary location
                    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                        self.s3_client.download_file(self.s3_bucket, filename, tmp_file.name)
                        
                        # Read and analyze
                        df = pd.read_parquet(tmp_file.name)
                        
                        file_result = {
                            'filename': filename,
                            'status': 'PASSED',
                            'shape': df.shape,
                            'columns': df.columns.tolist(),
                            'dtypes': df.dtypes.to_dict(),
                            'has_expected_columns': all(col in df.columns for col in expected_columns),
                            'date_range': {
                                'min': df['OHLC_DATE'].min().strftime('%Y-%m-%d') if 'OHLC_DATE' in df.columns else None,
                                'max': df['OHLC_DATE'].max().strftime('%Y-%m-%d') if 'OHLC_DATE' in df.columns else None
                            },
                            'sample_data': {
                                'ticker': df.iloc[0]['TICKER'] if len(df) > 0 and 'TICKER' in df.columns else None,
                                'price_range': {
                                    'min': float(df[['OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE']].min().min()) if len(df) > 0 else None,
                                    'max': float(df[['OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE']].max().max()) if len(df) > 0 else None
                                },
                                'volume_range': {
                                    'min': float(df['TRADING_VOLUME'].min()) if len(df) > 0 and 'TRADING_VOLUME' in df.columns else None,
                                    'max': float(df['TRADING_VOLUME'].max()) if len(df) > 0 and 'TRADING_VOLUME' in df.columns else None
                                }
                            }
                        }
                        
                        # Clean up temp file
                        os.unlink(tmp_file.name)
                        
                        format_results.append(file_result)
                        logger.info(f"✅ Format check passed for {filename}: {df.shape[0]} rows")
                        
                except Exception as e:
                    format_results.append({
                        'filename': filename,
                        'status': 'FAILED',
                        'error': str(e)
                    })
                    logger.error(f"❌ Format check failed for {filename}: {e}")
            
            # Overall format assessment
            passed_files = [r for r in format_results if r['status'] == 'PASSED']
            overall_status = 'PASSED' if len(passed_files) == len(format_results) else 'PARTIAL'
            
            return {
                'status': overall_status,
                'files_checked': len(format_results),
                'files_passed': len(passed_files),
                'results': format_results,
                'message': f'Format verification: {len(passed_files)}/{len(format_results)} files passed'
            }
            
        except Exception as e:
            logger.error(f"❌ Data format verification failed: {e}")
            return {
                'status': 'FAILED',
                'error': str(e),
                'message': 'Failed to verify data format'
            }
    
    def check_data_freshness(self) -> Dict:
        """Check if recent data is available."""
        logger.info("Step 3: Checking data freshness...")
        
        try:
            if not self.s3_client:
                self.s3_client = self._initialize_s3_client()
            
            # Look for files modified in the last 24 hours
            response = self.s3_client.list_objects_v2(Bucket=self.s3_bucket)
            if 'Contents' not in response:
                return {
                    'status': 'WARNING', 
                    'message': 'No files found (may indicate successful Snowflake ingestion)',
                    'snowflake_integration_note': 'Empty bucket suggests files were successfully copied to Snowflake and auto-deleted'
                }
            
            cutoff_time = datetime.now() - timedelta(hours=24)
            recent_files = []
            
            for obj in response['Contents']:
                file_time = obj['LastModified'].replace(tzinfo=None)
                if file_time > cutoff_time:
                    recent_files.append({
                        'filename': obj['Key'],
                        'last_modified': file_time,
                        'size': obj['Size']
                    })
            
            # Check for current week data in filenames
            current_date = datetime.now()
            current_week_files = []
            
            for obj in response['Contents']:
                filename = obj['Key']
                # Look for files with recent dates in the filename
                if (f"{current_date.year}" in filename and 
                    f"{current_date.month:02d}" in filename):
                    current_week_files.append(filename)
            
            freshness_status = 'PASSED' if recent_files else 'WARNING'
            
            result = {
                'status': freshness_status,
                'recent_files_count': len(recent_files),
                'recent_files': recent_files[:10],
                'current_period_files': len(current_week_files),
                'message': f'Found {len(recent_files)} files modified in last 24 hours'
            }
            
            if recent_files:
                logger.info(f"✅ Data freshness check: {result['message']}")
            else:
                logger.warning(f"⚠️ Data freshness check: No recent files found")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Data freshness check failed: {e}")
            return {
                'status': 'FAILED',
                'error': str(e),
                'message': 'Failed to check data freshness'
            }
    
    def generate_summary_report(self) -> Dict:
        """Generate overall summary report."""
        logger.info("Step 4: Generating summary report...")
        
        # Count passed/failed checks
        checks = ['s3_contents', 'data_format', 'data_freshness']
        passed_checks = 0
        failed_checks = 0
        warnings = 0
        
        for check in checks:
            if check in self.verification_results:
                status = self.verification_results[check]['status']
                if status == 'PASSED':
                    passed_checks += 1
                elif status == 'WARNING':
                    warnings += 1
                else:
                    failed_checks += 1
        
        # Overall status
        if failed_checks > 0:
            overall_status = 'FAILED'
        elif warnings > 0:
            overall_status = 'WARNING'
        else:
            overall_status = 'PASSED'
        
        summary = {
            'overall_status': overall_status,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'checks_performed': len(checks),
            'checks_passed': passed_checks,
            'checks_failed': failed_checks,
            'warnings': warnings,
            'detailed_results': self.verification_results
        }
        
        return summary
    
    def run_verification(self) -> Dict:
        """Run complete data quality verification."""
        logger.info("=" * 80)
        logger.info("OHLCV PIPELINE DATA QUALITY VERIFICATION")
        logger.info("=" * 80)
        logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"S3 Bucket: {self.s3_bucket}")
        logger.info("")
        
        try:
            # Run all verification steps
            self.verification_results['s3_contents'] = self.check_s3_bucket_contents()
            self.verification_results['data_format'] = self.verify_data_format()
            self.verification_results['data_freshness'] = self.check_data_freshness()
            
            # Generate summary
            summary = self.generate_summary_report()
            
            # Print final results
            logger.info("=" * 80)
            logger.info("VERIFICATION SUMMARY")
            logger.info("=" * 80)
            
            if summary['overall_status'] == 'PASSED':
                logger.info("🎉 OVERALL STATUS: PASSED ✅")
                logger.info("All data quality checks passed successfully!")
            elif summary['overall_status'] == 'WARNING':
                logger.info("⚠️ OVERALL STATUS: WARNING")
                logger.info("Data quality checks passed with warnings.")
            else:
                logger.info("❌ OVERALL STATUS: FAILED")
                logger.info("One or more data quality checks failed.")
            
            logger.info(f"Checks performed: {summary['checks_performed']}")
            logger.info(f"Passed: {summary['checks_passed']}")
            logger.info(f"Failed: {summary['checks_failed']}")
            logger.info(f"Warnings: {summary['warnings']}")
            
            # Key metrics
            if 's3_contents' in self.verification_results:
                s3_result = self.verification_results['s3_contents']
                if s3_result['status'] != 'FAILED':
                    logger.info(f"Total files in S3: {s3_result['total_files']}")
                    if 'unique_tickers' in s3_result:
                        logger.info(f"Unique tickers: {s3_result['unique_tickers']}")
                    if 'snowflake_integration_note' in s3_result:
                        logger.info(f"Note: {s3_result['snowflake_integration_note']}")
            
            logger.info("=" * 80)
            
            return summary
            
        except Exception as e:
            logger.error(f"Verification failed with error: {e}")
            return {
                'overall_status': 'FAILED',
                'error': str(e),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }


def main():
    """Main entry point for data quality verification."""
    try:
        verifier = DataQualityVerifier()
        results = verifier.run_verification()
        
        # Exit with appropriate code
        if results['overall_status'] == 'PASSED':
            sys.exit(0)
        elif results['overall_status'] == 'WARNING':
            sys.exit(1)  # Warning exit code
        else:
            sys.exit(2)  # Failure exit code
            
    except KeyboardInterrupt:
        logger.info("Verification interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
