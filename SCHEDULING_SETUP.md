# Incremental Pipeline Scheduling Setup

## Overview

This guide helps you set up automated weekly execution of the Incremental OHLCV Pipeline every **Saturday at 7:00 AM PT** using cron jobs.

## Quick Setup

### 1. **Automated Setup (Recommended)**
```bash
# Run the setup script to automatically configure the cron job
./setup_cron_job.sh
```

This script will:
- ✅ Create a cron job for every Saturday at 7 AM PT
- ✅ Set up proper logging
- ✅ Verify all required files exist
- ✅ Show you the next scheduled run time

### 2. **Manual Setup (Alternative)**
If you prefer to set up the cron job manually:

```bash
# Open your crontab for editing
crontab -e

# Add this line (replace /path/to with your actual path):
0 7 * * 6 /Users/prajagopal/Documents/SEC/code/sp500_top_10_sector_leaders_daily_ohlcv/schedule_incremental_pipeline.sh

# Save and exit
```

## Files Created

### 1. `schedule_incremental_pipeline.sh`
- **Purpose**: Main scheduler script that runs the incremental pipeline
- **Features**:
  - Environment setup and validation
  - Comprehensive logging
  - Error handling and exit codes
  - Optional email notifications (commented out)

### 2. `setup_cron_job.sh`
- **Purpose**: Interactive setup script for the cron job
- **Features**:
  - Automatic cron job creation
  - Duplicate detection and replacement
  - Next run time calculation
  - Comprehensive setup verification

### 3. `cron_incremental_pipeline.log`
- **Purpose**: Log file for all scheduled pipeline runs
- **Location**: Same directory as the scripts
- **Content**: Timestamped logs of each execution

## Cron Job Details

| Setting | Value | Description |
|---------|--------|-------------|
| **Schedule** | `0 7 * * 6` | Every Saturday at 7:00 AM |
| **Timezone** | Pacific Time (PT) | Adjust if needed |
| **Frequency** | Weekly | Once per week |
| **Log File** | `cron_incremental_pipeline.log` | All output captured |

### Cron Expression Breakdown
```
0 7 * * 6
│ │ │ │ │
│ │ │ │ └─── Day of week (6 = Saturday)
│ │ │ └───── Month (1-12, * = any)
│ │ └─────── Day of month (1-31, * = any)
│ └───────── Hour (0-23, 7 = 7 AM)
└─────────── Minute (0-59, 0 = :00)
```

## Prerequisites Verification

Before setting up the cron job, ensure these are configured:

### ✅ **Environment Variables**
```bash
# Check if POLYGON_API_KEY is set
echo $POLYGON_API_KEY

# If not set, add to your shell profile:
echo 'export POLYGON_API_KEY=your_api_key_here' >> ~/.zshrc
# or for bash users:
echo 'export POLYGON_API_KEY=your_api_key_here' >> ~/.bashrc

# Reload your shell
source ~/.zshrc  # or ~/.bashrc
```

### ✅ **AWS Credentials**
```bash
# Verify AWS credentials exist
ls -la ~/.aws/credentials

# Test AWS access
aws s3 ls s3://sp500-top-10-sector-leaders-ohlcv-s3bkt --region us-east-1
```

### ✅ **Snowflake Connection**
```bash
# Verify Snowflake connection file exists
ls -la ~/.snowflake/connections.toml

# Test connection (optional)
python3 test_incremental_pipeline.py
```

## Management Commands

### View Current Cron Jobs
```bash
crontab -l
```

### View Recent Logs
```bash
# View last 50 lines of the log file
tail -n 50 cron_incremental_pipeline.log

# Follow logs in real-time
tail -f cron_incremental_pipeline.log
```

### Test the Scheduler Manually
```bash
# Run the scheduler script manually to test
./schedule_incremental_pipeline.sh
```

### Remove the Cron Job
```bash
# Edit crontab and remove the line
crontab -e

# Or remove all cron jobs (careful!)
crontab -r
```

## Monitoring and Notifications

### Log File Monitoring
The scheduler creates detailed logs in `cron_incremental_pipeline.log`:

```
========================================
[2025-09-14 07:00:01] Starting scheduled incremental pipeline
========================================
[2025-09-14 07:00:01] Starting incremental OHLCV pipeline...
[2025-09-14 07:00:02] Incremental OHLCV Pipeline initialized
[2025-09-14 07:00:02] End date: 2025-09-12
...
[2025-09-14 07:23:45] ✅ Incremental pipeline completed successfully
[2025-09-14 07:23:45] Scheduled run completed
```

### Email Notifications (Optional)
To enable email notifications, uncomment and configure the mail commands in `schedule_incremental_pipeline.sh`:

```bash
# For success notifications
echo "Pipeline completed successfully at $DATE_TIME" | mail -s "Pipeline Success" your-email@example.com

# For failure notifications  
echo "Pipeline failed at $DATE_TIME with exit code $EXIT_CODE" | mail -s "Pipeline Failure" your-email@example.com
```

**Note**: Requires mail system setup on your macOS (like `postfix` or external SMTP).

## Troubleshooting

### Common Issues

1. **Cron Job Not Running**
   ```bash
   # Check if cron service is running
   sudo launchctl list | grep cron
   
   # View system logs for cron
   log show --predicate 'process == "cron"' --last 1h
   ```

2. **Environment Variables Not Available**
   - Cron runs with minimal environment
   - Ensure variables are set in shell profile
   - The scheduler script sources `.bashrc` and `.zshrc`

3. **Permission Issues**
   ```bash
   # Ensure scripts are executable
   chmod +x schedule_incremental_pipeline.sh
   chmod +x setup_cron_job.sh
   ```

4. **Path Issues**
   ```bash
   # Use absolute paths in cron jobs
   # The setup script automatically uses absolute paths
   ```

### Debugging Steps

1. **Test Manual Execution**
   ```bash
   ./schedule_incremental_pipeline.sh
   ```

2. **Check Cron Job Syntax**
   ```bash
   crontab -l | grep schedule_incremental_pipeline.sh
   ```

3. **Monitor Logs**
   ```bash
   tail -f cron_incremental_pipeline.log
   ```

4. **Verify Next Run Time**
   ```bash
   # The setup script shows next run time
   ./setup_cron_job.sh
   ```

## Customization

### Change Schedule
Edit the cron expression in `setup_cron_job.sh`:

```bash
# Current: Every Saturday at 7 AM PT
CRON_ENTRY="0 7 * * 6 $SCHEDULER_SCRIPT"

# Examples:
# Daily at 7 AM: "0 7 * * *"
# Weekdays at 7 AM: "0 7 * * 1-5"  
# Every 6 hours: "0 */6 * * *"
```

### Change Timezone
The cron job runs in system timezone. To change:

1. **System-wide**: Change macOS timezone in System Preferences
2. **Script-level**: Modify the scheduler script to set `TZ` environment variable

### Add More Logging
Modify `schedule_incremental_pipeline.sh` to add:
- Performance metrics
- Data volume statistics  
- Success/failure notifications
- Integration with monitoring systems

## Next Steps

After setting up the cron job:

1. **✅ Verify Setup**: Run `./setup_cron_job.sh` to confirm everything is configured
2. **📊 Monitor First Run**: Check logs after the first scheduled execution
3. **🔔 Set Up Alerts**: Configure email notifications if desired
4. **📈 Track Performance**: Monitor log files for execution times and success rates
5. **🔄 Regular Maintenance**: Periodically review logs and update API keys as needed

---

## 🎯 **Ready for Automation**

Your incremental pipeline is now scheduled to run every Saturday at 7 AM PT, ensuring your OHLCV data stays current with minimal manual intervention!

**Next Saturday's Run**: The setup script will show you exactly when the next execution is scheduled.
