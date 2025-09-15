# SP500 Top 10 Sector Leaders Daily OHLCV Data Pipeline

This application automates the extraction of historical equity data for a list of tickers from Snowflake, processes it into columnar format, and stores it in AWS S3 for use in a Snowflake data pipeline.

## Features

- **Ticker Retrieval**: Connects to Snowflake to retrieve ticker symbols from `DEMODB.EQUITY_RESEARCH.SP_SECTOR_COMPANIES`
- **Data Extraction**: Fetches daily OHLCV data from Polygon.io API (January 2024 - July 2025)
- **Rate Limiting**: Implements 5 API calls per minute limit for Polygon.io free tier
- **Data Processing**: Organizes data by month and converts to efficient Parquet format
- **Cloud Storage**: Uploads processed data to AWS S3 with hierarchical structure
- **Error Handling**: Comprehensive error handling and logging throughout the pipeline

## Prerequisites

1. **Snowflake Connection**: Pre-configured `DEMO_PRAJAGOPAL` connection in `~/.snowflake/connections.toml`
2. **Polygon.io API Key**: Free or paid API key from Polygon.io (set as `POLYGON_API_KEY` environment variable)
3. **AWS Credentials**: Configured AWS credentials with S3 access
4. **Python 3.8+**: Required for all dependencies

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure API Key

Set the Polygon.io API key as an environment variable:

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

**Security Note**: Using environment variables is more secure than storing API keys in files, as it prevents accidental commits of sensitive data to version control.

**Verify Setup**:
```bash
# Test that your API key is properly set
echo $POLYGON_API_KEY

# Quick API test (optional)
python -c "
import os
import requests
api_key = os.getenv('POLYGON_API_KEY')
if not api_key:
    print('❌ POLYGON_API_KEY not set')
else:
    url = f'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2024-01-01/2024-01-02?apikey={api_key}'
    response = requests.get(url)
    print('✅ API key working' if response.status_code == 200 else f'❌ API Error: {response.status_code}')
"
```

### 3. AWS Configuration

Ensure your AWS credentials are configured. You can use:
- AWS CLI: `aws configure`
- Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- IAM roles (if running on EC2)

Required S3 permissions:
- `s3:PutObject` on bucket `sp500-top-10-sector-leaders-ohlcv-s3bkt`
- `s3:HeadBucket` on the same bucket

### 4. Snowflake Configuration

Ensure your Snowflake connection is configured in `~/.snowflake/connections.toml`:

```toml
[connections.DEMO_PRAJAGOPAL]
account = "your_account"
user = "your_username"
# ... other connection parameters
```

## Usage

Run the pipeline:

```bash
python equity_data_pipeline.py
```

The application will:
1. Connect to Snowflake and retrieve ticker symbols
2. For each ticker, fetch daily OHLCV data from Polygon.io
3. Process data monthly and convert to Parquet format
4. Upload files to S3 with structure: `ticker/TICKER_YYYY_MM.parquet`

## Output Structure

Data is stored in S3 with the following structure:
```
sp500-top-10-sector-leaders-ohlcv-s3bkt/
├── AAPL/
│   ├── AAPL_2024_01.parquet
│   ├── AAPL_2024_02.parquet
│   └── ...
├── MSFT/
│   ├── MSFT_2024_01.parquet
│   └── ...
└── ...
```

Each Parquet file contains columns:
- `ticker`: Stock symbol
- `date`: Trading date
- `open`: Opening price
- `high`: Highest price
- `low`: Lowest price
- `close`: Closing price
- `volume`: Trading volume
- `timestamp`: Unix timestamp

## Logging

The application creates detailed logs in:
- Console output (INFO level and above)
- `equity_data_pipeline.log` file (all levels)

## Rate Limiting

The application implements a 12.5-second delay between API calls to respect Polygon.io's free tier limit of 5 calls per minute.

## Error Handling

The pipeline includes comprehensive error handling for:
- Snowflake connection failures
- API rate limiting and network errors
- S3 upload failures
- Data processing errors

Failed tickers are logged and reported in the final summary.

## Troubleshooting

### Common Issues

1. **Snowflake Connection Error**
   - Verify `connections.toml` configuration
   - Check network connectivity
   - Ensure credentials are valid

2. **Polygon.io API Errors**
   - Verify API key is set: `echo $POLYGON_API_KEY`
   - Check API quota limits
   - Ensure ticker symbols are valid

3. **S3 Upload Errors**
   - Verify AWS credentials
   - Check S3 bucket permissions
   - Ensure bucket exists and is accessible

4. **Memory Issues**
   - The application processes data monthly to minimize memory usage
   - For very large datasets, consider further chunking

### Performance Optimization

- The pipeline processes one ticker at a time to respect API limits
- Data is processed monthly to balance memory usage and efficiency
- Parquet format with Snappy compression provides optimal storage

## License

This project is for internal use in the SEC equity research pipeline.
