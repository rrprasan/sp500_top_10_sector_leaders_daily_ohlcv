#!/usr/bin/env python3
"""
Email Notifier for Incremental OHLCV Pipeline
Sends success/failure notifications with detailed pipeline summaries.
Based on the email system from stock-monitor project.
"""

import json
import smtplib
import os
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Optional
import pytz

class PipelineEmailNotifier:
    """Email notifier for OHLCV pipeline results."""
    
    def __init__(self, config_file: str = 'email_config.json'):
        """Initialize with email configuration."""
        self.config = self.load_email_config(config_file)
        
    def load_email_config(self, config_file: str) -> Dict:
        """Load email configuration from JSON file or environment variables."""
        
        # Try to load from stock-monitor config first
        stock_monitor_config = '/Users/prajagopal/Documents/SEC/code/stock-monitor/config.json'
        if os.path.exists(stock_monitor_config):
            try:
                with open(stock_monitor_config, 'r') as f:
                    stock_config = json.load(f)
                    if 'email' in stock_config:
                        print(f"📧 Using email configuration from stock-monitor project")
                        return stock_config['email']
            except Exception as e:
                print(f"⚠️  Could not load stock-monitor config: {e}")
        
        # Try to load from local config file
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                    print(f"📧 Using email configuration from {config_file}")
                    return config
            except Exception as e:
                print(f"⚠️  Could not load {config_file}: {e}")
        
        # Fallback to environment variables or default
        config = {
            "smtp_server": os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
            "smtp_port": int(os.getenv('SMTP_PORT', '587')),
            "sender_email": os.getenv('SENDER_EMAIL', 'your_email@gmail.com'),
            "sender_password": os.getenv('EMAIL_PASSWORD', 'your_app_password'),
            "recipient_email": os.getenv('RECIPIENT_EMAIL', 'recipient@gmail.com')
        }
        
        # Check if configuration is properly set
        if config['sender_email'] == 'your_email@gmail.com':
            print("⚠️  Email not configured. Using default values.")
            print("   To configure email, either:")
            print("   1. Copy config from stock-monitor project, or")
            print("   2. Set environment variables: SENDER_EMAIL, EMAIL_PASSWORD, RECIPIENT_EMAIL")
        
        return config
    
    def send_success_notification(self, execution_stats: Dict) -> bool:
        """Send success notification with pipeline statistics."""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.config['sender_email']
            msg['To'] = self.config['recipient_email']
            msg['Subject'] = f"✅ OHLCV Pipeline Success - {datetime.now().strftime('%Y-%m-%d')}"
            
            # Extract statistics
            total_tickers = execution_stats.get('total_tickers', 0)
            successful_tickers = execution_stats.get('successful_tickers', 0)
            failed_tickers = execution_stats.get('failed_tickers', 0)
            execution_time = execution_stats.get('execution_time', 'Unknown')
            date_range = execution_stats.get('date_range', 'Unknown')
            files_uploaded = execution_stats.get('files_uploaded', 0)
            
            # Calculate success rate
            success_rate = (successful_tickers / total_tickers * 100) if total_tickers > 0 else 0
            
            # Build HTML email body
            body = f"""
            <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #2e7d32; border-bottom: 2px solid #4caf50; padding-bottom: 10px;">
                ✅ Incremental OHLCV Pipeline - Success Report
            </h2>
            
            <div style="background-color: #e8f5e8; padding: 20px; border-radius: 5px; margin: 20px 0; border-left: 4px solid #4caf50;">
                <h3 style="margin: 0 0 15px 0; color: #2e7d32;">🎉 Pipeline Completed Successfully!</h3>
                <p style="margin: 5px 0; font-size: 16px;"><strong>Execution Time:</strong> {execution_time}</p>
                <p style="margin: 5px 0; font-size: 16px;"><strong>Date Range:</strong> {date_range}</p>
                <p style="margin: 5px 0; font-size: 16px;"><strong>Success Rate:</strong> {success_rate:.1f}%</p>
            </div>
            
            <div style="background-color: #f5f5f5; padding: 20px; border-radius: 5px; margin: 20px 0;">
                <h3 style="margin: 0 0 15px 0; color: #1976d2;">📊 Execution Statistics</h3>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd; font-weight: bold;">Total Tickers Processed:</td>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd; color: #1976d2; font-weight: bold;">{total_tickers}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd; font-weight: bold;">Successfully Processed:</td>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd; color: #2e7d32; font-weight: bold;">{successful_tickers}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd; font-weight: bold;">Failed:</td>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd; color: #d32f2f; font-weight: bold;">{failed_tickers}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd; font-weight: bold;">Parquet Files Uploaded:</td>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd; color: #1976d2; font-weight: bold;">{files_uploaded}</td>
                    </tr>
                </table>
            </div>
            
            <div style="background-color: #fff3e0; padding: 20px; border-radius: 5px; margin: 20px 0; border-left: 4px solid #ff9800;">
                <h3 style="margin: 0 0 15px 0; color: #e65100;">🔄 Next Steps</h3>
                <ul style="margin: 0; padding-left: 20px;">
                    <li><strong>Snowflake Ready:</strong> New OHLCV data files are uploaded to S3</li>
                    <li><strong>Load Data:</strong> Run COPY INTO command in Snowflake to load new data</li>
                    <li><strong>Next Run:</strong> Scheduled for next Saturday at 7:00 AM PT</li>
                    <li><strong>Monitoring:</strong> Check pipeline logs for detailed execution info</li>
                </ul>
            </div>
            
            <div style="background-color: #e3f2fd; padding: 15px; border-radius: 5px; margin: 20px 0;">
                <h4 style="margin: 0 0 10px 0; color: #1976d2;">📝 Pipeline Details</h4>
                <ul style="margin: 0; padding-left: 20px; font-size: 14px;">
                    <li><strong>Pipeline Type:</strong> Incremental (downloads only new data)</li>
                    <li><strong>Data Source:</strong> Polygon.io API</li>
                    <li><strong>Storage:</strong> AWS S3 → sp500-top-10-sector-leaders-ohlcv-s3bkt</li>
                    <li><strong>Format:</strong> Parquet files with Snowflake-compatible schema</li>
                    <li><strong>Rate Limiting:</strong> 5 calls/minute (12.5s delays)</li>
                </ul>
            </div>
            
            <p style="font-size: 12px; color: #888; margin-top: 30px; text-align: center;">
                <em>This is an automated report from your Incremental OHLCV Pipeline.<br>
                Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S PT')}</em>
            </p>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(body, 'html', 'utf-8'))
            
            with smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port']) as server:
                server.starttls()
                server.login(self.config['sender_email'], self.config['sender_password'])
                server.send_message(msg)
            
            print(f"✅ Success notification sent to {self.config['recipient_email']}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to send success notification: {str(e)}")
            return False
    
    def send_failure_notification(self, error_details: Dict) -> bool:
        """Send failure notification with error details."""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.config['sender_email']
            msg['To'] = self.config['recipient_email']
            msg['Subject'] = f"❌ OHLCV Pipeline Failure - {datetime.now().strftime('%Y-%m-%d')}"
            
            # Extract error information
            exit_code = error_details.get('exit_code', 'Unknown')
            error_message = error_details.get('error_message', 'No details available')
            execution_time = error_details.get('execution_time', 'Unknown')
            failed_step = error_details.get('failed_step', 'Unknown')
            
            body = f"""
            <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #d32f2f; border-bottom: 2px solid #f44336; padding-bottom: 10px;">
                ❌ Incremental OHLCV Pipeline - Failure Report
            </h2>
            
            <div style="background-color: #ffebee; padding: 20px; border-radius: 5px; margin: 20px 0; border-left: 4px solid #f44336;">
                <h3 style="margin: 0 0 15px 0; color: #d32f2f;">🚨 Pipeline Execution Failed</h3>
                <p style="margin: 5px 0; font-size: 16px;"><strong>Failure Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S PT')}</p>
                <p style="margin: 5px 0; font-size: 16px;"><strong>Execution Duration:</strong> {execution_time}</p>
                <p style="margin: 5px 0; font-size: 16px;"><strong>Exit Code:</strong> {exit_code}</p>
            </div>
            
            <div style="background-color: #fff3e0; padding: 20px; border-radius: 5px; margin: 20px 0;">
                <h3 style="margin: 0 0 15px 0; color: #e65100;">🔍 Error Details</h3>
                <p style="margin: 0; padding: 15px; background-color: #f5f5f5; border-radius: 3px; font-family: monospace; font-size: 14px; white-space: pre-wrap;">{error_message}</p>
                <p style="margin: 15px 0 0 0; font-size: 14px;"><strong>Failed Step:</strong> {failed_step}</p>
            </div>
            
            <div style="background-color: #e8f5e8; padding: 20px; border-radius: 5px; margin: 20px 0; border-left: 4px solid #4caf50;">
                <h3 style="margin: 0 0 15px 0; color: #2e7d32;">🛠️ Troubleshooting Steps</h3>
                <ol style="margin: 0; padding-left: 20px;">
                    <li><strong>Check Logs:</strong> Review detailed logs in <code>cron_incremental_pipeline.log</code></li>
                    <li><strong>Verify Credentials:</strong> Ensure POLYGON_API_KEY, AWS, and Snowflake credentials are valid</li>
                    <li><strong>Test Manually:</strong> Run <code>./schedule_incremental_pipeline.sh</code> to debug</li>
                    <li><strong>Check Connectivity:</strong> Verify internet connection and API access</li>
                    <li><strong>Review Quotas:</strong> Check if API rate limits or quotas were exceeded</li>
                </ol>
            </div>
            
            <div style="background-color: #e3f2fd; padding: 15px; border-radius: 5px; margin: 20px 0;">
                <h4 style="margin: 0 0 10px 0; color: #1976d2;">📞 Support Information</h4>
                <ul style="margin: 0; padding-left: 20px; font-size: 14px;">
                    <li><strong>Log File:</strong> <code>/Users/prajagopal/Documents/SEC/code/sp500_top_10_sector_leaders_daily_ohlcv/cron_incremental_pipeline.log</code></li>
                    <li><strong>Test Script:</strong> <code>python test_incremental_pipeline.py</code></li>
                    <li><strong>Manual Run:</strong> <code>python incremental_ohlcv_pipeline.py</code></li>
                    <li><strong>Next Scheduled Run:</strong> Next Saturday at 7:00 AM PT</li>
                </ul>
            </div>
            
            <p style="font-size: 12px; color: #888; margin-top: 30px; text-align: center;">
                <em>This is an automated error report from your Incremental OHLCV Pipeline.<br>
                Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S PT')}</em>
            </p>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(body, 'html', 'utf-8'))
            
            with smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port']) as server:
                server.starttls()
                server.login(self.config['sender_email'], self.config['sender_password'])
                server.send_message(msg)
            
            print(f"📧 Failure notification sent to {self.config['recipient_email']}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to send failure notification: {str(e)}")
            return False
    
    def test_email_connection(self) -> bool:
        """Test email connection and send a test message."""
        try:
            # Test connection
            with smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port']) as server:
                server.starttls()
                server.login(self.config['sender_email'], self.config['sender_password'])
            
            # Send test email
            msg = MIMEMultipart('alternative')
            msg['From'] = self.config['sender_email']
            msg['To'] = self.config['recipient_email']
            msg['Subject'] = "OHLCV Pipeline Email Test"
            
            # Simple text body without special characters
            body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
            <h2 style="color: #2e7d32;">Email Configuration Test Successful!</h2>
            <p>This is a test email from your Incremental OHLCV Pipeline.</p>
            <p><strong>Configuration Status:</strong> Successfully configured</p>
            <p><strong>Recipient:</strong> {self.config['recipient_email']}</p>
            <p><strong>Pipeline:</strong> SP500 Top 10 Sector Leaders OHLCV Data</p>
            <p><strong>Schedule:</strong> Every Saturday at 7:00 AM PT</p>
            
            <p style="color: green; font-weight: bold;">
            Your email notifications are ready!
            </p>
            
            <p style="font-size: 12px; color: #888;">
            <em>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em>
            </p>
            </body>
            </html>
            """.encode('utf-8').decode('utf-8')
            
            msg.attach(MIMEText(body, 'html', 'utf-8'))
            
            with smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port']) as server:
                server.starttls()
                server.login(self.config['sender_email'], self.config['sender_password'])
                server.send_message(msg)
            
            print(f"Test email sent successfully to {self.config['recipient_email']}")
            return True
            
        except Exception as e:
            print(f"Email test failed: {str(e)}")
            return False

def main():
    """Test the email notifier."""
    print("📧 Testing OHLCV Pipeline Email Notifier")
    print("=" * 50)
    
    notifier = PipelineEmailNotifier()
    
    if notifier.test_email_connection():
        print("✅ Email system is working correctly!")
    else:
        print("❌ Email system needs configuration.")

if __name__ == "__main__":
    main()
