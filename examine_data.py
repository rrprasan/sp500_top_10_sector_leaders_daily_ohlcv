#!/usr/bin/env python3
"""
Script to examine the generated parquet files.
"""

import pandas as pd
import pyarrow.parquet as pq
import os

def examine_parquet_file(filepath):
    """Examine a parquet file and display its contents."""
    print(f"\n{'='*60}")
    print(f"Examining: {filepath}")
    print(f"{'='*60}")
    
    try:
        # Read the parquet file
        df = pd.read_parquet(filepath)
        
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"\nData types:")
        print(df.dtypes)
        
        print(f"\nFirst 5 rows:")
        print(df.head())
        
        print(f"\nSummary statistics:")
        print(df.describe())
        
        print(f"\nDate range:")
        print(f"From: {df['ohlc_date'].min()}")
        print(f"To: {df['ohlc_date'].max()}")
        
        return True
        
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return False

def main():
    """Examine all generated parquet files."""
    output_dir = 'test_output'
    
    if not os.path.exists(output_dir):
        print(f"Output directory {output_dir} not found. Run test_pipeline.py first.")
        return
    
    print("EXAMINING GENERATED PARQUET FILES")
    print("="*60)
    
    # Find all parquet files
    parquet_files = []
    for root, dirs, files in os.walk(output_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_files.append(os.path.join(root, file))
    
    print(f"Found {len(parquet_files)} parquet files")
    
    # Examine each file
    for filepath in sorted(parquet_files):
        examine_parquet_file(filepath)
    
    print(f"\n{'='*60}")
    print("EXAMINATION COMPLETE")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
