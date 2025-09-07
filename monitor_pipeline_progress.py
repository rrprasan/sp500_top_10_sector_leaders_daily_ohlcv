#!/usr/bin/env python3
"""
Monitor the progress of the comprehensive OHLCV pipeline.
"""

import boto3
import time
import os
from datetime import datetime

def monitor_progress():
    """Monitor the pipeline progress by checking S3 uploads and log file."""
    
    # S3 setup
    bucket = 'sp500-top-10-sector-leaders-ohlcv-s3bkt'
    s3 = boto3.client('s3')
    
    # Expected totals
    total_tickers = 111
    files_per_ticker = 3  # 2023, 2024, 2025
    expected_total_files = total_tickers * files_per_ticker
    
    print("=" * 80)
    print("COMPREHENSIVE OHLCV PIPELINE PROGRESS MONITOR")
    print("=" * 80)
    print(f"Expected total files: {expected_total_files} ({total_tickers} tickers × {files_per_ticker} years)")
    print()
    
    try:
        # Check S3 files
        response = s3.list_objects_v2(Bucket=bucket)
        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents']]
            files.sort()
            
            # Count files by ticker
            tickers_processed = set()
            for file in files:
                ticker = file.split('_')[0]
                tickers_processed.add(ticker)
            
            print(f"📊 CURRENT STATUS:")
            print(f"   Files uploaded: {len(files)} / {expected_total_files}")
            print(f"   Tickers processed: {len(tickers_processed)} / {total_tickers}")
            print(f"   Progress: {(len(files)/expected_total_files)*100:.1f}%")
            print()
            
            # Show latest processed tickers
            print(f"🎯 LATEST PROCESSED TICKERS:")
            latest_tickers = sorted(list(tickers_processed))[-10:]
            for ticker in latest_tickers:
                ticker_files = [f for f in files if f.startswith(ticker + '_')]
                print(f"   {ticker}: {len(ticker_files)}/3 files")
            print()
            
        else:
            print("❌ No files found in S3 bucket")
            return
        
        # Check log file for current status
        log_file = 'comprehensive_ohlcv_pipeline.log'
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                lines = f.readlines()
            
            # Find the last "Processing ticker" line
            current_ticker = None
            for line in reversed(lines):
                if "Processing ticker" in line and "/" in line:
                    parts = line.split("Processing ticker ")[1].split(":")
                    current_ticker = parts[0].strip()
                    ticker_name = parts[1].strip()
                    break
            
            if current_ticker:
                print(f"🔄 CURRENTLY PROCESSING:")
                print(f"   {current_ticker}: {ticker_name}")
            
            # Estimate completion time
            if len(tickers_processed) > 0:
                # Each ticker takes about 12.5 seconds (rate limit) + processing time (~2-3 seconds)
                avg_time_per_ticker = 15  # seconds
                remaining_tickers = total_tickers - len(tickers_processed)
                estimated_remaining_time = remaining_tickers * avg_time_per_ticker
                
                hours = estimated_remaining_time // 3600
                minutes = (estimated_remaining_time % 3600) // 60
                
                print()
                print(f"⏱️  ESTIMATED COMPLETION:")
                if hours > 0:
                    print(f"   Remaining time: ~{hours}h {minutes}m")
                else:
                    print(f"   Remaining time: ~{minutes}m")
                
                completion_time = datetime.now().timestamp() + estimated_remaining_time
                completion_datetime = datetime.fromtimestamp(completion_time)
                print(f"   Expected completion: {completion_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        
        print()
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ Error monitoring progress: {e}")

if __name__ == "__main__":
    monitor_progress()
