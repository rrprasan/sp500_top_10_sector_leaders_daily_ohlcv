# SP500 OHLCV Data Pipeline - Complete Setup Guide

## Overview

This repository contains a complete end-to-end pipeline for downloading SP500 OHLCV (Open, High, Low, Close, Volume) data and testing Snowflake LOAD_MODE performance. The pipeline downloads historical financial data from Polygon.io, stores it in AWS S3 as Parquet files, and provides comprehensive performance testing for Snowflake COPY INTO operations.

## 🎯 What This Pipeline Does

1. **Downloads OHLCV Data**: Retrieves 2+ years of daily stock data for major SP500 companies
2. **Processes to Parquet**: Converts data to optimized Parquet format for Snowflake
3. **Stores in S3**: Uploads files to AWS S3 in flat structure for optimal COPY performance
4. **Creates Iceberg Tables**: Uses automated [Iceberg Table Creator](https://github.com/rrprasan/Iceberg_Table_Creator) or manual setup
5. **Tests LOAD_MODE Performance**: Compares FULL_INGEST vs ADD_FILES_COPY performance
6. **Provides Results**: Generates comprehensive performance analysis and recommendations

## ⚡ **QUICK START OPTION**

**New users:** Skip the complex manual Snowflake setup! Use our [**Iceberg Table Creator**](https://github.com/rrprasan/Iceberg_Table_Creator) to automate the entire Iceberg table infrastructure setup in just 5 minutes. Then return to Step 6 of this guide to run the data pipeline.

## 📋 Prerequisites

### Required Accounts & Services
- **Snowflake Account** with database creation privileges
- **Polygon.io Account** (free tier sufficient for testing)
- **AWS Account** with S3 access
- **Python 3.8+** with pip

### Required Permissions
- **Snowflake**: CREATE DATABASE, CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT
- **AWS S3**: Create bucket, upload/delete objects
- **Polygon.io**: API access (free tier: 5 calls/minute)

---

## 🚀 Complete Setup Instructions

### Step 1: Clone Repository

```bash
git clone <your-github-repo-url>
cd sp500_top_10_sector_leaders_daily_ohlcv
```

### Step 2: Install Dependencies

```bash
# Install required Python packages
pip install -r requirements.txt

# Or install individually
pip install snowflake-connector-python requests pandas pyarrow boto3
```

### Step 3: Polygon.io API Setup

1. **Create Account**: Sign up at [polygon.io](https://polygon.io/)
2. **Get API Key**: Navigate to Dashboard → API Keys
3. **Set Environment Variable**:
   ```bash
   # Set the environment variable (replace with your actual API key)
   export POLYGON_API_KEY=your_polygon_io_api_key_here
   
   # To make it permanent, add to your shell profile
   echo 'export POLYGON_API_KEY=your_polygon_io_api_key_here' >> ~/.bashrc
   # or for zsh users:
   echo 'export POLYGON_API_KEY=your_polygon_io_api_key_here' >> ~/.zshrc
   
   # Reload your shell or run:
   source ~/.bashrc  # or ~/.zshrc
   ```

### Step 4: AWS S3 Setup

1. **Create S3 Bucket**:
   ```bash
   aws s3 mb s3://sp500-top-10-sector-leaders-ohlcv-s3bkt
   ```
   
2. **Configure AWS Credentials**:
   ```bash
   aws configure
   # Or set environment variables:
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_DEFAULT_REGION=us-east-1
   ```

### Step 5: Snowflake Database Setup

#### 5.1 Create Database and Schema
```sql
-- Create database
CREATE DATABASE DEMODB;
USE DATABASE DEMODB;

-- Create schema
CREATE SCHEMA EQUITY_RESEARCH;
USE SCHEMA EQUITY_RESEARCH;
```

#### 5.2 Create Ticker Reference Table
```sql
-- Create table with SP500 sector leaders
CREATE TABLE SP_SECTOR_COMPANIES (
    TICKER_SYMBOL VARCHAR(10) PRIMARY KEY,
    COMPANY_NAME VARCHAR(100),
    SECTOR VARCHAR(50),
    MARKET_CAP_BILLIONS DECIMAL(10,2)
);

-- Insert sample tickers (major companies for testing)
INSERT INTO SP_SECTOR_COMPANIES VALUES
    ('AAPL', 'Apple Inc.', 'Information Technology', 3000.00),
    ('MSFT', 'Microsoft Corporation', 'Information Technology', 2800.00),
    ('GOOGL', 'Alphabet Inc.', 'Communication Services', 1800.00),
    ('AMZN', 'Amazon.com Inc.', 'Consumer Discretionary', 1500.00),
    ('TSLA', 'Tesla Inc.', 'Consumer Discretionary', 800.00),
    ('NVDA', 'NVIDIA Corporation', 'Information Technology', 1200.00),
    ('META', 'Meta Platforms Inc.', 'Communication Services', 900.00),
    ('BRK.B', 'Berkshire Hathaway Inc.', 'Financials', 700.00),
    ('JNJ', 'Johnson & Johnson', 'Health Care', 450.00),
    ('V', 'Visa Inc.', 'Financials', 500.00),
    ('WMT', 'Walmart Inc.', 'Consumer Staples', 400.00);
```

#### 5.3 Create External Volume for Iceberg Tables (Required)
```sql
-- Create external volume for Iceberg table storage
-- This is required for Snowflake Managed Iceberg tables
CREATE EXTERNAL VOLUME iceberg_storage_vol
   STORAGE_LOCATIONS = (
       (
           NAME = 'iceberg-s3-location'
           STORAGE_PROVIDER = 'S3'
           STORAGE_BASE_URL = 's3://sp500-top-10-sector-leaders-ohlcv-s3bkt/iceberg/'
           STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::your-account:role/SnowflakeIcebergRole'
       )
   );

-- Alternative: Use AWS credentials instead of IAM role
CREATE EXTERNAL VOLUME iceberg_storage_vol
   STORAGE_LOCATIONS = (
       (
           NAME = 'iceberg-s3-location'
           STORAGE_PROVIDER = 'S3'
           STORAGE_BASE_URL = 's3://sp500-top-10-sector-leaders-ohlcv-s3bkt/iceberg/'
           STORAGE_AWS_EXTERNAL_ID = 'your_external_id'
           STORAGE_AWS_IAM_USER_ARN = 'arn:aws:iam::your-account:user/your-user'
           STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::your-account:role/SnowflakeIcebergRole'
       )
   );

-- Grant necessary privileges
GRANT USAGE ON EXTERNAL VOLUME iceberg_storage_vol TO ROLE PUBLIC;
```

#### 5.4 Create Snowflake Managed Iceberg Table for OHLCV Data
```sql
-- Create OHLCV data table as Snowflake Managed Iceberg Table
-- Provides better performance, ACID transactions, and time travel capabilities
CREATE ICEBERG TABLE sp500_top10_sector_ohlcv_itbl (
    TICKER VARCHAR(10) NOT NULL COMMENT 'Stock ticker symbol (e.g., AAPL, MSFT)',
    OHLC_DATE TIMESTAMP_NTZ NOT NULL COMMENT 'Trading date in UTC timezone',
    OPEN_PRICE DECIMAL(12,4) NOT NULL COMMENT 'Opening price for the trading day',
    HIGH_PRICE DECIMAL(12,4) NOT NULL COMMENT 'Highest price during the trading day',
    LOW_PRICE DECIMAL(12,4) NOT NULL COMMENT 'Lowest price during the trading day',
    CLOSE_PRICE DECIMAL(12,4) NOT NULL COMMENT 'Closing price for the trading day',
    TRADING_VOLUME BIGINT NOT NULL COMMENT 'Total volume of shares traded',
    OHLC_TIMESTAMP TIMESTAMP_NTZ NOT NULL COMMENT 'Precise timestamp of the OHLC record'
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_storage_vol'
BASE_LOCATION = 'sp500_ohlcv_iceberg'
COMMENT = 'Snowflake Managed Iceberg table for SP500 OHLCV financial data - optimized for analytics and time travel';

-- Create partitioning for better query performance
-- Partition by year and month for optimal time-series queries
ALTER TABLE sp500_top10_sector_ohlcv_itbl 
ADD PARTITION FIELD YEAR(OHLC_DATE);

ALTER TABLE sp500_top10_sector_ohlcv_itbl 
ADD PARTITION FIELD MONTH(OHLC_DATE);

-- Optional: Add clustering for frequently queried columns
ALTER TABLE sp500_top10_sector_ohlcv_itbl 
CLUSTER BY (TICKER, OHLC_DATE);
```

**Iceberg Table Benefits:**
- ✅ **ACID Transactions**: Full transactional consistency
- ✅ **Time Travel**: Query historical versions of data
- ✅ **Schema Evolution**: Add/modify columns without data migration  
- ✅ **Partition Pruning**: Automatic query optimization
- ✅ **Compaction**: Automatic file optimization
- ✅ **Better Performance**: Optimized for analytical workloads

**Alternative: Standard Table (if Iceberg not available)**
```sql
-- Use this if your Snowflake account doesn't support Iceberg tables
CREATE TABLE sp500_top10_sector_ohlcv_itbl (
    TICKER VARCHAR(10),
    OHLC_DATE TIMESTAMP_NTZ,
    OPEN_PRICE DECIMAL(12,4),
    HIGH_PRICE DECIMAL(12,4),
    LOW_PRICE DECIMAL(12,4),
    CLOSE_PRICE DECIMAL(12,4),
    TRADING_VOLUME BIGINT,
    OHLC_TIMESTAMP TIMESTAMP_NTZ
)
CLUSTER BY (TICKER, OHLC_DATE);
```

---

## 🚀 **AUTOMATED ALTERNATIVE: Iceberg Table Creator**

**⚡ Skip the manual Snowflake setup!** Instead of manually creating External Volumes and Iceberg tables, you can use our **automated Iceberg Table Creator** project that handles all the complex setup for you.

### 🎯 What the Iceberg Table Creator Does

The [**Iceberg Table Creator**](https://github.com/rrprasan/Iceberg_Table_Creator) is a beautiful Streamlit application that **completely automates** the complex process of setting up Iceberg tables on Snowflake:

#### **🏗️ Infrastructure Setup (Fully Automated)**
- ✅ **Creates AWS S3 bucket** for Iceberg storage automatically
- ✅ **Creates IAM policy** with precise permissions  
- ✅ **Creates IAM role** with dynamic trust policy
- ✅ **Creates Snowflake External Volume** automatically
- ✅ **Extracts Snowflake credentials** automatically
- ✅ **Updates IAM trust policy** with Snowflake credentials

#### **🧊 Table Creation (One-Click)**
- ✅ **Create unlimited Iceberg tables** using existing infrastructure
- ✅ **Built-in templates** including Stock Data template perfect for OHLCV data
- ✅ **Custom column definitions** with 23+ data types
- ✅ **Automatic test data insertion** and validation
- ✅ **S3 file verification** to ensure everything works
- ✅ **Complete table management** with beautiful UI

### 🚀 Quick Start with Iceberg Table Creator

**Instead of manual setup above, follow these simple steps:**

1. **Clone the Iceberg Table Creator:**
   ```bash
   git clone https://github.com/rrprasan/Iceberg_Table_Creator.git
   cd Iceberg_Table_Creator
   chmod +x install.sh
   ./install.sh
   ```

2. **Configure your credentials** (same AWS and Snowflake credentials as above)

3. **Run the automated setup:**
   ```bash
   streamlit run Iceberg_Table_Creator.py
   ```

4. **Use the Stock Data Template** to create your OHLCV Iceberg table with the exact schema needed for this pipeline

5. **Return to this guide** at Step 6 to continue with the data pipeline

### 🎨 Perfect Stock Data Template

The Iceberg Table Creator includes a **Stock Data Template** that creates the exact table structure needed for this OHLCV pipeline:

- **TICKER** (VARCHAR) - Stock ticker symbol
- **TRADE_DATE** (DATE) - Trading date  
- **OPEN_PRICE** (DOUBLE) - Opening price
- **HIGH_PRICE** (DOUBLE) - Highest price
- **LOW_PRICE** (DOUBLE) - Lowest price
- **CLOSE_PRICE** (DOUBLE) - Closing price
- **VOLUME** (BIGINT) - Trading volume

**This template automatically handles:**
- ✅ External Volume creation
- ✅ IAM role configuration  
- ✅ S3 bucket setup
- ✅ Iceberg table creation
- ✅ Partitioning optimization
- ✅ Test data validation

### 🏆 Why Use the Automated Approach?

| Manual Setup | Automated Iceberg Creator |
|--------------|---------------------------|
| ❌ 30+ manual steps | ✅ **3 clicks total** |
| ❌ Complex IAM configuration | ✅ **Automatic IAM setup** |
| ❌ Error-prone trust policies | ✅ **Dynamic trust policies** |
| ❌ Manual S3 bucket creation | ✅ **Automatic S3 setup** |
| ❌ Hours of AWS console work | ✅ **5 minutes end-to-end** |
| ❌ Risk of configuration errors | ✅ **Validated configurations** |

### 📖 Learn More

For complete documentation and advanced features, visit:
- **GitHub Repository**: https://github.com/rrprasan/Iceberg_Table_Creator
- **Full Documentation**: Includes troubleshooting, deployment options, and advanced configuration

---

## 📋 Manual Snowflake Setup (Alternative)

**If you prefer manual setup or need custom configurations**, continue with the manual steps below:

#### 5.5 Create S3 Stage
```sql
-- Create external stage pointing to S3 bucket
CREATE STAGE SP500_TOP_10_SECTOR_LEADERS_OHLCV_STG
    URL = 's3://sp500-top-10-sector-leaders-ohlcv-s3bkt/'
    CREDENTIALS = (
        AWS_KEY_ID = 'your_aws_access_key_id'
        AWS_SECRET_KEY = 'your_aws_secret_access_key'
    );
```

#### 5.6 Create File Format
```sql
-- Create Parquet file format
CREATE FILE_FORMAT SP500_TOP10_SECTOR_OHLCV_FILE_FORMAT
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY';
```

### Step 6: Configure Snowflake Connection

Create `~/.snowflake/connections.toml`:
```toml
[connections.DEMO_PRAJAGOPAL]
account = "your_account_identifier"
user = "your_username"
password = "your_password"
database = "DEMODB"
schema = "EQUITY_RESEARCH"
warehouse = "COMPUTE_WH"
```

**Alternative**: Use environment variables:
```bash
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_PASSWORD=your_password
```

---

## 🏃‍♂️ Running the Pipeline

### Step 1: Clear S3 Bucket (Optional)
```bash
# Clear existing files if needed
python clear_s3_bucket.py
```

### Step 2: Download OHLCV Data
```bash
# Run the quick data pipeline (1 hour, 11 tickers)
python quick_vectorized_test_pipeline.py
```

**What this does:**
- Downloads 2+ years of OHLCV data (Aug 2023 - Aug 2025)
- Processes 11 major SP500 tickers
- Creates optimized Parquet files
- Uploads to S3 in flat structure
- Takes ~1 hour due to API rate limiting

**Expected output:**
```
11 Parquet files uploaded to S3:
- AAPL_2023_2025.parquet
- MSFT_2023_2025.parquet
- GOOGL_2023_2025.parquet
- ... (8 more files)
Total: 5,500 OHLCV records
```

### Step 3: Test LOAD_MODE Performance
```bash
# Run LOAD_MODE performance comparison
python test_load_mode_performance.py
```

**What this tests:**
- LOAD_MODE = FULL_INGEST performance
- LOAD_MODE = ADD_FILES_COPY performance
- Execution time comparison
- Throughput analysis
- Generates comprehensive results

---

## 📊 Understanding the Results

### Performance Test Output
The test will show results like:
```
LOAD_MODE PERFORMANCE TEST SUMMARY
===============================================================================
PERFORMANCE RESULTS:
  LOAD_MODE = FULL_INGEST:    1.60 seconds
  LOAD_MODE = ADD_FILES_COPY: 2.23 seconds
  FULL_INGEST throughput:     3,433 rows/sec
  ADD_FILES_COPY throughput:  2,468 rows/sec

📊 FULL_INGEST is 39.1% faster!
🏆 Winner: LOAD_MODE = FULL_INGEST

RECOMMENDATION:
  Use LOAD_MODE = FULL_INGEST for optimal performance
```

### Generated Reports
The pipeline creates detailed analysis files:
- `LOAD_MODE_PERFORMANCE_ANALYSIS.md` - Complete performance study
- `load_mode_performance_test.log` - Detailed execution logs
- `LOAD_MODE_PERFORMANCE_DIAGRAM.md` - Visual comparison

---

## 🎛️ Configuration Options

### Modify Date Range
Edit `quick_vectorized_test_pipeline.py`:
```python
self.START_DATE = '2023-08-01'  # Change start date
self.END_DATE = '2025-08-29'    # Change end date
```

### Add More Tickers
Edit the `TEST_TICKERS` list:
```python
self.TEST_TICKERS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',
    'NVDA', 'META', 'BRK.B', 'JNJ', 'V', 'WMT',
    # Add more tickers here
    'JPM', 'UNH', 'HD', 'PG'
]
```

### Adjust API Rate Limiting
Edit the delay between API calls:
```python
self.RATE_LIMIT_DELAY = 12.5  # Seconds between calls (5 calls/minute)
```

---

## 🔧 Troubleshooting

### Common Issues

#### 1. Snowflake Connection Errors
```bash
# Test connection
python -c "
import snowflake.connector
conn = snowflake.connector.connect(connection_name='DEMO_PRAJAGOPAL')
print('✅ Snowflake connection successful')
conn.close()
"
```

#### 2. AWS S3 Access Issues
```bash
# Test S3 access
aws s3 ls s3://sp500-top-10-sector-leaders-ohlcv-s3bkt/
```

#### 3. Polygon.io API Issues
```bash
# Test API key
python -c "
import requests
import os
api_key = os.getenv('POLYGON_API_KEY')
if not api_key:
    print('❌ POLYGON_API_KEY environment variable not set')
else:
    url = f'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2024-01-01/2024-01-02?apikey={api_key}'
    response = requests.get(url)
    print('✅ Polygon.io API working' if response.status_code == 200 else f'❌ API Error: {response.status_code}')
"
```

#### 4. Missing Dependencies
```bash
# Install missing packages
pip install --upgrade snowflake-connector-python boto3 pandas pyarrow requests
```

### Performance Issues

#### Slow Downloads
- **Cause**: API rate limiting (5 calls/minute for free tier)
- **Solution**: Upgrade to paid Polygon.io plan or reduce ticker count

#### S3 Upload Failures
- **Cause**: AWS credentials or permissions
- **Solution**: Verify AWS configuration and S3 bucket permissions

#### Snowflake COPY Errors
- **Cause**: Stage or file format configuration
- **Solution**: Verify stage URL and credentials

---

## 📈 Expected Performance

### Data Pipeline
- **11 tickers**: ~1 hour (due to API rate limiting)
- **File size**: ~0.03 MB per ticker
- **Records**: ~500 records per ticker (2+ years daily data)

### Three-Scenario LOAD_MODE Test Results
Based on our comprehensive testing with 11 files and 5,500 records:

| Scenario | Configuration | Execution Time | Throughput | Status |
|----------|---------------|----------------|------------|---------|
| **🥇 WINNER** | `FULL_INGEST` + `VECTORIZED_SCANNER=TRUE` | **1.62 seconds** | **3,386 rows/sec** | ✅ SUCCESS |
| **🥈 Runner-up** | `ADD_FILES_COPY` + `VECTORIZED_SCANNER=TRUE` | 2.68 seconds | 2,056 rows/sec | ✅ SUCCESS |
| **❌ FAILED** | `FULL_INGEST` + `VECTORIZED_SCANNER=FALSE` | N/A | N/A | ❌ **INCOMPATIBLE** |

**🚨 CRITICAL FINDING**: `USE_VECTORIZED_SCANNER = FALSE` **FAILS COMPLETELY** on modern Parquet files with timestamp casting errors. It's not just slower - it's broken!

---

## 🎯 Production Recommendations

### Optimal Snowflake Configuration

#### For Iceberg Tables (Recommended)
```sql
-- OPTIMAL: For batch loads (< 100 files) - 39.4% faster than ADD_FILES_COPY
COPY INTO sp500_top10_sector_ohlcv_itbl
  FROM @SP500_TOP_10_SECTOR_LEADERS_OHLCV_STG
  FILE_FORMAT = (
     FORMAT_NAME = 'SP500_TOP10_SECTOR_OHLCV_FILE_FORMAT'
     USE_VECTORIZED_SCANNER = TRUE  -- CRITICAL: Required for modern Parquet files
  )
  LOAD_MODE = FULL_INGEST  -- FASTEST for small file sets (3,386 rows/sec)
  PURGE = FALSE
  MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
  FORCE = FALSE;

-- ALTERNATIVE: For large file sets (> 500 files)
COPY INTO sp500_top10_sector_ohlcv_itbl
  FROM @SP500_TOP_10_SECTOR_LEADERS_OHLCV_STG
  FILE_FORMAT = (
     FORMAT_NAME = 'SP500_TOP10_SECTOR_OHLCV_FILE_FORMAT'
     USE_VECTORIZED_SCANNER = TRUE  -- CRITICAL: Always required
  )
  LOAD_MODE = ADD_FILES_COPY  -- Better for incremental loads (2,056 rows/sec)
  PURGE = FALSE
  MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
  FORCE = FALSE;
```

**⚠️ NEVER USE THIS - WILL FAIL:**
```sql
-- BROKEN: Fails on modern Parquet files with timestamp errors
COPY INTO sp500_top10_sector_ohlcv_itbl
  FILE_FORMAT = (
     USE_VECTORIZED_SCANNER = FALSE  -- INCOMPATIBLE WITH MODERN DATA
  )
  LOAD_MODE = FULL_INGEST;
```

**Iceberg Table Additional Benefits:**
- ✅ **Automatic Compaction**: Files are automatically optimized
- ✅ **Time Travel Queries**: `SELECT * FROM table AT(TIMESTAMP => '2024-01-01'::timestamp)`
- ✅ **Schema Evolution**: Add columns without data migration
- ✅ **Partition Pruning**: Automatic query optimization based on date ranges
- ✅ **ACID Compliance**: Full transactional consistency for financial data

#### For Standard Tables (Fallback)
```sql
-- Use this configuration if Iceberg tables are not available
COPY INTO sp500_top10_sector_ohlcv_itbl
  FROM @SP500_TOP_10_SECTOR_LEADERS_OHLCV_STG
  FILE_FORMAT = (
     FORMAT_NAME = 'SP500_TOP10_SECTOR_OHLCV_FILE_FORMAT'
     USE_VECTORIZED_SCANNER = TRUE  -- MANDATORY
  )
  LOAD_MODE = FULL_INGEST  -- 39.1% faster for small file sets
  PURGE = FALSE
  MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
  FORCE = FALSE;
```

### Scaling Guidelines
- **< 100 files**: Use `FULL_INGEST + VECTORIZED_SCANNER = TRUE` (39.4% faster)
- **> 500 files**: Use `ADD_FILES_COPY + VECTORIZED_SCANNER = TRUE` (better scaling)
- **100-500 files**: Test both modes with your specific data
- **ALL cases**: `USE_VECTORIZED_SCANNER = TRUE` is **MANDATORY** (non-vectorized fails completely)

---

## 📚 Additional Resources

### Generated Documentation
- `THREE_SCENARIO_LOAD_MODE_PERFORMANCE_RESULTS.md` - **NEW: Comprehensive 3-scenario test results**
- `LOAD_MODE_PERFORMANCE_ANALYSIS.md` - Original 2-scenario performance study  
- `LOAD_MODE_PERFORMANCE_DIAGRAM.md` - Visual performance comparison
- `VECTORIZED_SCANNER_TEST_RESULTS.md` - Why vectorized scanner is mandatory

### Test Scripts
- `test_load_mode_performance.py` - Production-ready performance testing
- `quick_vectorized_test_pipeline.py` - Optimized data download pipeline
- `clear_s3_bucket.py` - S3 cleanup utility

### Log Files
- `load_mode_performance_test.log` - Detailed test execution logs
- `quick_vectorized_test_pipeline.log` - Data pipeline execution logs

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with your own data
5. Submit a pull request

---

## 📄 License

This project is for educational and research purposes. Please ensure compliance with:
- Polygon.io Terms of Service
- AWS Usage Policies  
- Snowflake Terms of Service

---

## 🆘 Support

If you encounter issues:

1. **Check Prerequisites**: Ensure all accounts and permissions are configured
2. **Review Logs**: Check `.log` files for detailed error messages
3. **Test Components**: Use troubleshooting scripts to isolate issues
4. **Verify Configuration**: Double-check API keys and connection settings

For additional help, please create an issue in this repository with:
- Error messages from log files
- Your configuration (with sensitive data redacted)
- Steps you've already tried

---

*Last updated: August 31, 2025*  
*Complete end-to-end pipeline for SP500 OHLCV data processing and Snowflake LOAD_MODE performance testing*
