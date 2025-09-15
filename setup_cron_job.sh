#!/bin/bash

# Setup Script for Incremental OHLCV Pipeline Cron Job
# This script sets up a weekly cron job to run every Saturday at 7 AM PT

echo "🔧 Setting up weekly cron job for Incremental OHLCV Pipeline"
echo "============================================================"

# Get the current script directory (absolute path)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEDULER_SCRIPT="$SCRIPT_DIR/schedule_incremental_pipeline.sh"

# Check if the scheduler script exists
if [ ! -f "$SCHEDULER_SCRIPT" ]; then
    echo "❌ ERROR: Scheduler script not found at $SCHEDULER_SCRIPT"
    exit 1
fi

# Make sure the scheduler script is executable
chmod +x "$SCHEDULER_SCRIPT"

# Create the cron job entry
# Saturday at 7:00 AM PT (0 7 * * 6)
CRON_ENTRY="0 7 * * 6 $SCHEDULER_SCRIPT"

echo "📅 Cron job details:"
echo "   Schedule: Every Saturday at 7:00 AM PT"
echo "   Command: $SCHEDULER_SCRIPT"
echo "   Cron expression: 0 7 * * 6"
echo ""

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "$SCHEDULER_SCRIPT"; then
    echo "⚠️  Cron job for this script already exists!"
    echo "   Current crontab entries for this script:"
    crontab -l 2>/dev/null | grep "$SCHEDULER_SCRIPT"
    echo ""
    read -p "Do you want to replace the existing cron job? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Aborted. No changes made to crontab."
        exit 0
    fi
    
    # Remove existing entries for this script
    crontab -l 2>/dev/null | grep -v "$SCHEDULER_SCRIPT" | crontab -
    echo "🗑️  Removed existing cron job entries for this script"
fi

# Add the new cron job
(crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -

# Verify the cron job was added
if crontab -l 2>/dev/null | grep -q "$SCHEDULER_SCRIPT"; then
    echo "✅ Cron job successfully added!"
    echo ""
    echo "📋 Current crontab entries:"
    echo "----------------------------"
    crontab -l 2>/dev/null
    echo ""
    echo "🔍 Your new cron job:"
    crontab -l 2>/dev/null | grep "$SCHEDULER_SCRIPT"
    echo ""
    echo "📝 Log file location: $SCRIPT_DIR/cron_incremental_pipeline.log"
    echo ""
    echo "🎯 Next scheduled run:"
    # Calculate next Saturday 7 AM
    if command -v python3 &> /dev/null; then
        python3 -c "
import datetime
now = datetime.datetime.now()
# Find next Saturday
days_until_saturday = (5 - now.weekday()) % 7
if days_until_saturday == 0 and now.hour >= 7:
    days_until_saturday = 7
next_saturday = now + datetime.timedelta(days=days_until_saturday)
next_run = next_saturday.replace(hour=7, minute=0, second=0, microsecond=0)
print(f'   {next_run.strftime(\"%A, %B %d, %Y at %I:%M %p PT\")}')
"
    else
        echo "   Next Saturday at 7:00 AM PT"
    fi
    echo ""
    echo "✨ Setup complete! The incremental pipeline will now run automatically."
else
    echo "❌ ERROR: Failed to add cron job. Please check your crontab manually."
    exit 1
fi

echo ""
echo "📚 Additional Information:"
echo "-------------------------"
echo "• To view current cron jobs: crontab -l"
echo "• To edit cron jobs manually: crontab -e"
echo "• To remove this cron job: run this script again and choose to replace"
echo "• To view logs: tail -f $SCRIPT_DIR/cron_incremental_pipeline.log"
echo "• To test the scheduler manually: $SCHEDULER_SCRIPT"
echo ""
echo "⚠️  Important Notes:"
echo "• Ensure POLYGON_API_KEY is set in your shell profile (~/.bashrc or ~/.zshrc)"
echo "• Ensure AWS credentials are configured in ~/.aws/credentials"
echo "• Ensure Snowflake connection is configured in ~/.snowflake/connections.toml"
echo "• The script runs in Pacific Time (PT) - adjust if you're in a different timezone"
