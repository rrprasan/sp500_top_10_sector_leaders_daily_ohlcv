# Data Quality Verification Tools

## Overview

These tools provide automated data quality verification for the OHLCV pipeline, ensuring factual confirmation that data processing completed successfully.

## Files

### `data_quality_verification.py`
Comprehensive standalone data quality verification script.

**What it checks:**
- ✅ S3 bucket contents (file counts, unique tickers)
- ✅ Data format validation (schema, data types)
- ✅ Data freshness (recent uploads)
- ✅ Sample data quality (price ranges, volumes)

**Usage:**
```bash
python data_quality_verification.py
```

**Exit codes:**
- `0`: All checks passed
- `1`: Passed with warnings
- `2`: Failed

### `run_pipeline_with_verification.py`
Complete workflow that runs the pipeline followed by verification.

**What it does:**
1. Runs `incremental_ohlcv_pipeline.py`
2. Runs `data_quality_verification.py`
3. Provides comprehensive success/failure reporting
4. Handles misleading pipeline error messages

**Usage:**
```bash
python run_pipeline_with_verification.py
```

## Sample Output

### Successful Verification
```
================================================================================
VERIFICATION SUMMARY
================================================================================
🎉 OVERALL STATUS: PASSED ✅
All data quality checks passed successfully!
Checks performed: 3
Passed: 3
Failed: 0
Warnings: 0
Total files in S3: 189
Unique tickers: 69
================================================================================
```

### Detailed Checks Performed

1. **S3 Bucket Contents**
   - Counts total files uploaded
   - Identifies unique tickers processed
   - Lists recent files (last 24 hours)
   - Verifies file naming patterns

2. **Data Format Validation**
   - Samples 3 files (recent, middle, old)
   - Verifies expected columns: `TICKER`, `OHLC_DATE`, `OPEN_PRICE`, `HIGH_PRICE`, `LOW_PRICE`, `CLOSE_PRICE`, `TRADING_VOLUME`, `OHLC_TIMESTAMP`
   - Checks data types and formats
   - Validates price and volume ranges

3. **Data Freshness**
   - Identifies files modified in last 24 hours
   - Checks for current period data
   - Warns if no recent data found

## Log Files

The verification tools create detailed log files:
- `data_quality_verification.log` - Verification results
- `pipeline_with_verification.log` - Combined pipeline and verification logs
- `incremental_ohlcv_pipeline.log` - Pipeline execution logs

## Why Use These Tools?

1. **Factual Verification**: Get concrete proof that data was uploaded successfully
2. **Bypass Misleading Errors**: The main pipeline may report "No tickers processed" while actually succeeding
3. **Quality Assurance**: Automated checks ensure data meets expected standards
4. **Confidence**: Know for certain your pipeline execution worked
5. **Automated Reporting**: No manual S3 bucket checking required

## Recommended Workflow

**Always use the combined script:**
```bash
python run_pipeline_with_verification.py
```

This ensures you get factual verification regardless of misleading error messages from the main pipeline.

## Troubleshooting

### Common Issues

1. **AWS Credentials**: Ensure AWS credentials are properly configured
2. **S3 Bucket Access**: Verify access to `sp500-top-10-sector-leaders-ohlcv-s3bkt`
3. **Dependencies**: Ensure all required Python packages are installed

### Exit Codes

- **0**: Success - All checks passed
- **1**: Warning - Some checks passed with warnings
- **2**: Failure - One or more checks failed
- **130**: Interrupted by user (Ctrl+C)

## Integration

These tools are designed to be run after each pipeline execution to provide immediate feedback on data quality and upload success.
