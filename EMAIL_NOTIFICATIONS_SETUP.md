# Email Notifications Setup Guide

## Overview

The Incremental OHLCV Pipeline now includes comprehensive email notifications that will send you detailed reports when the scheduled pipeline runs complete. You'll receive different emails for success and failure scenarios.

## 🔐 Secure Configuration

**Important**: Email credentials are stored as **environment variables** and will **NOT** be included in any code files that could be committed to GitHub.

## Quick Setup

### 1. **Run the Secure Email Setup Script**
```bash
./setup_secure_email.sh
```

This interactive script will:
- ✅ Guide you through Gmail app password setup
- ✅ Securely store credentials as environment variables
- ✅ Test the email configuration
- ✅ Ensure no credentials are stored in code files

### 2. **Provide Your Gmail App Password**
When prompted, you'll need:
- **Your Gmail address**: `your.email@gmail.com`
- **Gmail app password**: 16-character password from Google
- **Recipient email**: Where to send notifications (can be same as sender)

## Gmail App Password Setup

### Step-by-Step Instructions:

1. **Enable 2-Factor Authentication** (if not already enabled):
   - Go to [Google Account Security](https://myaccount.google.com/security)
   - Turn on 2-Step Verification

2. **Generate App Password**:
   - In the same Security page, click **"App passwords"**
   - Select **"Mail"** as the app
   - Click **"Generate"**
   - Copy the 16-character password (e.g., `abcd efgh ijkl mnop`)

3. **Use in Setup Script**:
   - Run `./setup_secure_email.sh`
   - Enter your Gmail address
   - Paste the 16-character app password (not your regular Gmail password)

## Email Notification Types

### 📊 **Success Notifications**
Sent when the pipeline completes successfully:

```
Subject: ✅ OHLCV Pipeline Success - 2025-09-20

Content includes:
• Execution statistics (tickers processed, success rate)
• Performance metrics (execution time, files uploaded)
• Date range of data downloaded
• Next steps for loading data into Snowflake
• Pipeline configuration details
```

### 🚨 **Failure Notifications**
Sent when the pipeline encounters errors:

```
Subject: ❌ OHLCV Pipeline Failure - 2025-09-20

Content includes:
• Error details and exit codes
• Troubleshooting steps
• Log file locations
• Support information for debugging
• Next scheduled run information
```

## Security Features

### ✅ **What's Secure:**
- Credentials stored as environment variables only
- No passwords in code files
- Safe to commit all code to GitHub
- Credentials automatically loaded in new terminal sessions

### ❌ **What's Protected:**
- Gmail app passwords
- Email addresses 
- SMTP configuration details
- Any temporary configuration files

## File Structure

```
📁 Email Notification Files:
├── email_notifier.py              # Email notification system
├── setup_secure_email.sh          # Interactive secure setup
├── setup_email_notifications.py   # Alternative setup method
├── .gitignore                      # Protects sensitive files
└── EMAIL_NOTIFICATIONS_SETUP.md   # This documentation

🔐 Environment Variables (in ~/.zshrc or ~/.bashrc):
├── SENDER_EMAIL                    # Your Gmail address
├── EMAIL_PASSWORD                  # Gmail app password  
├── RECIPIENT_EMAIL                 # Where to send notifications
├── SMTP_SERVER                     # smtp.gmail.com
└── SMTP_PORT                       # 587
```

## Testing

### Test Email Configuration
```bash
# Test the email system manually
python3 email_notifier.py
```

### Test Full Pipeline with Notifications
```bash
# Run the scheduler manually to test everything
./schedule_incremental_pipeline.sh
```

### Test Individual Components
```bash
# Test just the incremental pipeline
python3 incremental_ohlcv_pipeline.py

# Test all pipeline components
python3 test_incremental_pipeline.py
```

## Automated Schedule

Your cron job is configured to run **every Saturday at 7:00 AM PT** and will:

1. **Execute Pipeline**: Download incremental OHLCV data
2. **Parse Results**: Extract statistics and performance metrics
3. **Send Email**: Automatically notify you of success/failure
4. **Log Everything**: Detailed logs in `cron_incremental_pipeline.log`

## Email Examples

### Success Email Preview
```html
✅ Incremental OHLCV Pipeline - Success Report

🎉 Pipeline Completed Successfully!
• Execution Time: 00:08:34
• Date Range: from 2025-09-13 to 2025-09-19
• Success Rate: 100.0%

📊 Execution Statistics
• Total Tickers Processed: 111
• Successfully Processed: 111  
• Failed: 0
• Parquet Files Uploaded: 111

🔄 Next Steps
• Snowflake Ready: New OHLCV data files are uploaded to S3
• Load Data: Run COPY INTO command in Snowflake
• Next Run: Scheduled for next Saturday at 7:00 AM PT
```

### Failure Email Preview
```html
❌ Incremental OHLCV Pipeline - Failure Report

🚨 Pipeline Execution Failed
• Failure Time: 2025-09-20 07:15:32 PT
• Execution Duration: 00:02:15
• Exit Code: 1

🔍 Error Details
API rate limit exceeded. Please check your Polygon.io quota.

🛠️ Troubleshooting Steps
1. Check Logs: Review detailed logs in cron_incremental_pipeline.log
2. Verify Credentials: Ensure API keys are valid
3. Test Manually: Run ./schedule_incremental_pipeline.sh
```

## Troubleshooting

### Common Issues

1. **"Email test failed" Error**
   ```bash
   # Check if environment variables are loaded
   echo $SENDER_EMAIL
   echo $RECIPIENT_EMAIL
   
   # If empty, reload your shell profile
   source ~/.zshrc  # or ~/.bashrc
   ```

2. **"App Password Not Accepted" Error**
   - Verify 2-Factor Authentication is enabled
   - Generate a new app password
   - Use the app password, not your regular Gmail password

3. **Environment Variables Not Persistent**
   ```bash
   # Check which shell you're using
   echo $SHELL
   
   # Ensure variables are in the correct profile file
   grep -A 10 "OHLCV Pipeline Email" ~/.zshrc
   # or
   grep -A 10 "OHLCV Pipeline Email" ~/.bashrc
   ```

4. **Notifications Not Received**
   - Check spam/junk folder
   - Verify recipient email address
   - Test manually with `python3 email_notifier.py`

### Debug Commands

```bash
# Check environment variables
env | grep EMAIL
env | grep SENDER
env | grep RECIPIENT

# Test email connection
python3 -c "
from email_notifier import PipelineEmailNotifier
notifier = PipelineEmailNotifier()
print('✅ Success' if notifier.test_email_connection() else '❌ Failed')
"

# View recent cron job logs
tail -n 50 cron_incremental_pipeline.log

# Check cron job status
crontab -l
```

## Customization

### Modify Email Templates
Edit `email_notifier.py` to customize:
- Email subjects
- HTML formatting
- Statistics included
- Notification triggers

### Change Email Provider
To use a different email provider, update environment variables:
```bash
export SMTP_SERVER="smtp.outlook.com"  # For Outlook
export SMTP_PORT="587"
# Then run setup_secure_email.sh again
```

### Add More Recipients
To send to multiple recipients, modify the `recipient_email` in the notification functions.

## Security Best Practices

### ✅ **Do:**
- Use Gmail app passwords (not regular passwords)
- Store credentials as environment variables
- Keep credentials out of code files
- Regularly rotate app passwords

### ❌ **Don't:**
- Store passwords in configuration files
- Commit credentials to version control
- Share app passwords in plain text
- Use regular Gmail passwords for SMTP

---

## 🎉 **You're All Set!**

Your incremental OHLCV pipeline now includes professional email notifications that will keep you informed of every scheduled run. The system is secure, automated, and ready for production use.

**Next Saturday at 7:00 AM PT**, you'll receive your first automated pipeline notification! 📧
