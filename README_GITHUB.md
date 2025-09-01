# SP500 OHLCV Data Pipeline & Snowflake LOAD_MODE Performance Testing

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Snowflake](https://img.shields.io/badge/snowflake-compatible-blue.svg)](https://www.snowflake.com/)
[![AWS S3](https://img.shields.io/badge/aws-s3-orange.svg)](https://aws.amazon.com/s3/)

## 🎯 Overview

This repository provides a complete end-to-end pipeline for downloading SP500 OHLCV (Open, High, Low, Close, Volume) financial data and conducting comprehensive Snowflake COPY INTO performance testing. 

**Key Result**: Our testing shows that `LOAD_MODE = FULL_INGEST` is **39.1% faster** than `ADD_FILES_COPY` for small to medium file sets.

## 🚀 Quick Start

```bash
# 1. Clone repository
git clone <your-github-repo-url>
cd sp500_top_10_sector_leaders_daily_ohlcv

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure API key
cp config.json.template config.json
# Edit with your Polygon.io API key

# 4. Run complete pipeline
python quick_vectorized_test_pipeline.py    # Download data (~1 hour)
python test_load_mode_performance.py        # Test performance (~30 seconds)
```

## 📊 What You Get

### Performance Results
- **FULL_INGEST**: 1.60 seconds (3,433 rows/sec) ⚡
- **ADD_FILES_COPY**: 2.23 seconds (2,468 rows/sec) 📊
- **Performance Gain**: 39.1% faster with FULL_INGEST 🏆

### Data Pipeline
- **11 major SP500 tickers** (AAPL, MSFT, GOOGL, AMZN, TSLA, etc.)
- **2+ years of daily OHLCV data** (Aug 2023 - Aug 2025)
- **5,500 records** in optimized Parquet format
- **S3 storage** with flat structure for optimal Snowflake performance

### Comprehensive Analysis
- 📋 **15-page performance study** (`LOAD_MODE_PERFORMANCE_ANALYSIS.md`)
- 📊 **Visual comparisons** with Mermaid diagrams
- 🔬 **Production-ready test scripts**
- 📈 **Scaling recommendations** for different file volumes

## 🛠️ Prerequisites

- **Snowflake Account** (database creation privileges)
- **Polygon.io API Key** (free tier works)
- **AWS Account** (S3 access)
- **Python 3.8+**

## 📋 Complete Setup Instructions

👉 **[See END_TO_END_SETUP_GUIDE.md](./END_TO_END_SETUP_GUIDE.md)** for detailed setup instructions including:

- Snowflake database and table creation
- AWS S3 bucket configuration  
- Polygon.io API setup
- Connection configuration
- Troubleshooting guide

## 🎯 Key Findings

### 1. VECTORIZED_SCANNER = TRUE is Mandatory
- `USE_VECTORIZED_SCANNER = FALSE` **fails** with timestamp data
- `USE_VECTORIZED_SCANNER = TRUE` **required** for `LOAD_MODE = ADD_FILES_COPY`
- No performance comparison needed - TRUE is the only viable option

### 2. LOAD_MODE Performance Comparison
| Metric | FULL_INGEST | ADD_FILES_COPY | Winner |
|--------|-------------|----------------|---------|
| **Execution Time** | **1.60s** | 2.23s | 🥇 FULL_INGEST |
| **Throughput** | **3,433 rows/sec** | 2,468 rows/sec | 🥇 FULL_INGEST |
| **Best For** | **< 100 files** | > 500 files | Depends on scale |

### 3. Production Recommendations
```sql
-- RECOMMENDED: For batch processing < 100 files
COPY INTO sp500_top10_sector_ohlcv_itbl
  FROM @SP500_TOP_10_SECTOR_LEADERS_OHLCV_STG
  FILE_FORMAT = (
     FORMAT_NAME = 'SP500_TOP10_SECTOR_OHLCV_FILE_FORMAT'
     USE_VECTORIZED_SCANNER = TRUE  -- MANDATORY
  )
  LOAD_MODE = FULL_INGEST  -- 39.1% faster
  PURGE = FALSE
  MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
  FORCE = FALSE;
```

## 📁 Repository Structure

```
├── END_TO_END_SETUP_GUIDE.md              # Complete setup instructions
├── quick_vectorized_test_pipeline.py      # Data download pipeline
├── test_load_mode_performance.py          # LOAD_MODE performance test
├── clear_s3_bucket.py                     # S3 cleanup utility
├── config.json.template                   # API key configuration template
├── requirements.txt                       # Python dependencies
│
├── LOAD_MODE_PERFORMANCE_ANALYSIS.md      # 15-page performance study
├── LOAD_MODE_PERFORMANCE_DIAGRAM.md       # Visual performance comparison
├── VECTORIZED_SCANNER_TEST_RESULTS.md     # Vectorized scanner analysis
│
└── *.log                                  # Detailed execution logs
```

## 🎨 Visual Results

The pipeline generates visual performance comparisons showing:
- Execution time differences
- Throughput comparisons  
- Scaling projections
- Decision matrices for different use cases

## 🔧 Customization

### Add More Tickers
Edit `TEST_TICKERS` in `quick_vectorized_test_pipeline.py`:
```python
self.TEST_TICKERS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',
    # Add your tickers here
    'JPM', 'UNH', 'HD', 'PG'
]
```

### Modify Date Range
```python
self.START_DATE = '2023-08-01'  # Change start date
self.END_DATE = '2025-08-29'    # Change end date
```

## 📈 Performance Scaling

Based on our analysis:
- **< 100 files**: FULL_INGEST recommended (39.1% faster)
- **100-500 files**: FULL_INGEST likely faster
- **> 500 files**: ADD_FILES_COPY may be better
- **Break-even**: ~400-500 files

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch
3. Test with your data
4. Submit a pull request

## 📄 License

Educational and research use. Ensure compliance with:
- Polygon.io Terms of Service
- AWS Usage Policies
- Snowflake Terms of Service

## 🆘 Support

For issues or questions:
1. Check the [setup guide](./END_TO_END_SETUP_GUIDE.md)
2. Review log files for error details
3. Create an issue with configuration details

---

**Ready to optimize your Snowflake COPY INTO performance?** 🚀

[Get Started with Setup Guide →](./END_TO_END_SETUP_GUIDE.md)
