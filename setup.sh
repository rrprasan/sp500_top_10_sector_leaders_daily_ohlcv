#!/bin/bash

# Setup script for SP500 Equity Data Pipeline
# This script helps set up the environment and dependencies

echo "=================================================="
echo "SP500 Equity Data Pipeline - Setup Script"
echo "=================================================="

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed."
    echo "Please install Python 3.8 or higher."
    exit 1
fi

echo "✅ Python 3 found: $(python3 --version)"

# Check if pip is available
if ! command -v pip3 &> /dev/null && ! command -v pip &> /dev/null; then
    echo "❌ pip is required but not installed."
    echo "Please install pip."
    exit 1
fi

# Use pip3 if available, otherwise pip
PIP_CMD="pip3"
if ! command -v pip3 &> /dev/null; then
    PIP_CMD="pip"
fi

echo "✅ pip found: $PIP_CMD"

# Install dependencies
echo ""
echo "Installing Python dependencies..."
echo "=================================================="

if $PIP_CMD install -r requirements.txt; then
    echo "✅ Dependencies installed successfully"
else
    echo "❌ Failed to install dependencies"
    echo "You may need to run: $PIP_CMD install --user -r requirements.txt"
    exit 1
fi

# Create config file if it doesn't exist
echo ""
echo "Setting up configuration..."
echo "=================================================="

if [ ! -f "config.json" ]; then
    echo "Creating config.json from template..."
    cp config.json.template config.json
    echo "✅ config.json created"
    echo "⚠️  Please edit config.json and add your Polygon.io API key"
else
    echo "✅ config.json already exists"
fi

# Check AWS CLI (optional)
echo ""
echo "Checking AWS configuration..."
echo "=================================================="

if command -v aws &> /dev/null; then
    echo "✅ AWS CLI found: $(aws --version)"
    
    if aws sts get-caller-identity &> /dev/null; then
        echo "✅ AWS credentials are configured"
    else
        echo "⚠️  AWS credentials not configured. Run 'aws configure' to set them up."
    fi
else
    echo "⚠️  AWS CLI not found. You can install it or configure credentials via environment variables."
fi

# Final instructions
echo ""
echo "=================================================="
echo "Setup Complete!"
echo "=================================================="
echo ""
echo "Next steps:"
echo "1. Edit config.json and add your Polygon.io API key"
echo "2. Ensure your Snowflake connection is configured in ~/.snowflake/connections.toml"
echo "3. Configure AWS credentials if not already done"
echo "4. Run the test script: python3 test_setup.py"
echo "5. If tests pass, run the pipeline: python3 equity_data_pipeline.py"
echo ""
echo "For detailed instructions, see README.md"
