#!/usr/bin/env python3
"""
Email Notification Setup for Incremental OHLCV Pipeline
Interactive script to configure email credentials for pipeline notifications.
"""

import json
import getpass
import os
from email_notifier import PipelineEmailNotifier

def main():
    """Interactive email setup for pipeline notifications."""
    print("📧 OHLCV Pipeline Email Notification Setup")
    print("=" * 50)
    print()
    print("This script will configure email notifications for your incremental pipeline.")
    print("You'll receive emails when the pipeline:")
    print("  • ✅ Completes successfully (with statistics)")
    print("  • ❌ Fails (with error details)")
    print()
    
    # Check if we can use stock-monitor config
    stock_monitor_config = '/Users/prajagopal/Documents/SEC/code/stock-monitor/config.json'
    use_existing = False
    
    if os.path.exists(stock_monitor_config):
        try:
            with open(stock_monitor_config, 'r') as f:
                stock_config = json.load(f)
                if 'email' in stock_config and stock_config['email']['sender_email'] != 'your_email@gmail.com':
                    print(f"🔍 Found existing email configuration in stock-monitor project:")
                    print(f"   📧 Sender: {stock_config['email']['sender_email']}")
                    print(f"   📬 Recipient: {stock_config['email']['recipient_email']}")
                    print(f"   🌐 SMTP: {stock_config['email']['smtp_server']}:{stock_config['email']['smtp_port']}")
                    print()
                    
                    use_existing_input = input("Use existing email configuration? (Y/n): ").strip().lower()
                    if use_existing_input in ['', 'y', 'yes']:
                        use_existing = True
                        email_config = stock_config['email']
                        print("✅ Using existing email configuration from stock-monitor")
        except Exception as e:
            print(f"⚠️  Could not read stock-monitor config: {e}")
    
    if not use_existing:
        print("🔧 Setting up new email configuration...")
        print()
        
        # Get email provider choice
        print("📮 Email Provider:")
        print("1. Gmail (smtp.gmail.com) - Recommended")
        print("2. Outlook/Hotmail (smtp-mail.outlook.com)")
        print("3. Yahoo (smtp.mail.yahoo.com)")
        print("4. Custom SMTP server")
        
        choice = input("\nSelect your email provider (1-4): ").strip()
        
        if choice == "1":
            smtp_server = "smtp.gmail.com"
            smtp_port = 587
            print("✅ Gmail selected")
            print()
            print("📝 For Gmail, you need to:")
            print("   1. Enable 2-Factor Authentication")
            print("   2. Generate an App Password:")
            print("      • Go to: https://myaccount.google.com/security")
            print("      • Select 'App passwords'")
            print("      • Choose 'Mail' and generate password")
            print("   3. Use the app password below (not your regular password)")
            print()
        elif choice == "2":
            smtp_server = "smtp-mail.outlook.com"
            smtp_port = 587
            print("✅ Outlook selected")
        elif choice == "3":
            smtp_server = "smtp.mail.yahoo.com"
            smtp_port = 587
            print("✅ Yahoo selected")
        elif choice == "4":
            smtp_server = input("Enter SMTP server: ").strip()
            smtp_port = int(input("Enter SMTP port (usually 587): ").strip())
            print(f"✅ Custom SMTP: {smtp_server}:{smtp_port}")
        else:
            print("❌ Invalid choice. Exiting.")
            return
        
        # Get email credentials
        print("\n🔐 Email Credentials:")
        sender_email = input("Enter your email address: ").strip()
        
        if choice == "1":  # Gmail
            print("Enter your Gmail app password (not your regular password):")
        
        password = getpass.getpass("Enter password: ")
        
        # Get recipient email
        print("\n📬 Notification Recipient:")
        recipient_email = input("Enter recipient email address: ").strip()
        
        # Create email config
        email_config = {
            "smtp_server": smtp_server,
            "smtp_port": smtp_port,
            "sender_email": sender_email,
            "sender_password": password,
            "recipient_email": recipient_email
        }
    
    # Test email configuration
    print("\n🔍 Testing email configuration...")
    
    # Create temporary config file for testing
    temp_config_file = 'temp_email_config.json'
    with open(temp_config_file, 'w') as f:
        json.dump(email_config, f, indent=4)
    
    try:
        # Test the email configuration
        notifier = PipelineEmailNotifier(temp_config_file)
        
        if notifier.test_email_connection():
            print("✅ Email configuration successful!")
            
            # Save permanent configuration
            config_file = 'email_config.json'
            with open(config_file, 'w') as f:
                json.dump(email_config, f, indent=4)
            
            print(f"💾 Email configuration saved to {config_file}")
            print()
            print("🎉 Email notifications are now configured!")
            print("=" * 50)
            print("Your pipeline will now send emails when:")
            print(f"  📊 Success: Detailed statistics and execution summary")
            print(f"  🚨 Failure: Error details and troubleshooting steps")
            print(f"  📧 Recipient: {email_config['recipient_email']}")
            print()
            print("📅 Next Steps:")
            print("  • The cron job will automatically send notifications")
            print("  • Test manually: ./schedule_incremental_pipeline.sh")
            print("  • View logs: tail -f cron_incremental_pipeline.log")
            
        else:
            print("❌ Email configuration failed. Please check your credentials and try again.")
    
    except Exception as e:
        print(f"❌ Error testing email configuration: {str(e)}")
    
    finally:
        # Clean up temporary config file
        if os.path.exists(temp_config_file):
            os.remove(temp_config_file)

if __name__ == "__main__":
    main()
