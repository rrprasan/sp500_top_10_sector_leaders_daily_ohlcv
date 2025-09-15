#!/bin/bash

# Secure Email Setup for OHLCV Pipeline
# This script helps you set up email notifications using environment variables
# Your credentials will NOT be stored in code files

echo "🔐 Secure Email Setup for OHLCV Pipeline"
echo "========================================"
echo ""
echo "This script will help you configure email notifications securely."
echo "Your email credentials will be stored as environment variables,"
echo "NOT in code files that could be committed to GitHub."
echo ""

# Determine which shell profile to use
if [ "$SHELL" = "/bin/zsh" ] || [ "$SHELL" = "/usr/bin/zsh" ]; then
    PROFILE_FILE="$HOME/.zshrc"
    SHELL_NAME="zsh"
elif [ "$SHELL" = "/bin/bash" ] || [ "$SHELL" = "/usr/bin/bash" ]; then
    PROFILE_FILE="$HOME/.bashrc"
    SHELL_NAME="bash"
else
    echo "⚠️  Unsupported shell: $SHELL"
    echo "Please manually add the environment variables to your shell profile."
    exit 1
fi

echo "🐚 Detected shell: $SHELL_NAME"
echo "📁 Profile file: $PROFILE_FILE"
echo ""

# Get email configuration
echo "📧 Email Configuration:"
echo "----------------------"

read -p "Enter your Gmail address: " SENDER_EMAIL
echo ""

echo "📝 For Gmail App Password:"
echo "1. Go to: https://myaccount.google.com/security"
echo "2. Enable 2-Factor Authentication (if not already enabled)"
echo "3. Click 'App passwords'"
echo "4. Select 'Mail' and generate a 16-character app password"
echo "5. Use that app password below (not your regular Gmail password)"
echo ""

read -s -p "Enter your Gmail app password: " EMAIL_PASSWORD
echo ""
echo ""

read -p "Enter recipient email address (can be same as sender): " RECIPIENT_EMAIL
echo ""

# Confirm settings
echo "🔍 Confirming Settings:"
echo "----------------------"
echo "Sender Email: $SENDER_EMAIL"
echo "Recipient Email: $RECIPIENT_EMAIL"
echo "App Password: [HIDDEN - $(echo "$EMAIL_PASSWORD" | wc -c | tr -d ' ') characters]"
echo ""

read -p "Are these settings correct? (y/N): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Setup cancelled."
    exit 0
fi

# Check if variables already exist in profile
echo ""
echo "💾 Adding environment variables to $PROFILE_FILE..."

# Remove any existing OHLCV pipeline email variables
if grep -q "# OHLCV Pipeline Email Configuration" "$PROFILE_FILE"; then
    echo "🔄 Removing existing OHLCV email configuration..."
    # Create a temporary file without the OHLCV email section
    awk '
    /# OHLCV Pipeline Email Configuration/ {skip=1}
    /# End OHLCV Pipeline Email Configuration/ {skip=0; next}
    !skip {print}
    ' "$PROFILE_FILE" > "${PROFILE_FILE}.tmp"
    mv "${PROFILE_FILE}.tmp" "$PROFILE_FILE"
fi

# Add new configuration
cat >> "$PROFILE_FILE" << EOF

# OHLCV Pipeline Email Configuration
# Added by setup_secure_email.sh on $(date)
export SENDER_EMAIL="$SENDER_EMAIL"
export EMAIL_PASSWORD="$EMAIL_PASSWORD"
export RECIPIENT_EMAIL="$RECIPIENT_EMAIL"
export SMTP_SERVER="smtp.gmail.com"
export SMTP_PORT="587"
# End OHLCV Pipeline Email Configuration
EOF

echo "✅ Environment variables added to $PROFILE_FILE"
echo ""

# Source the profile to make variables available immediately
echo "🔄 Reloading shell profile..."
source "$PROFILE_FILE"

echo "✅ Environment variables loaded!"
echo ""

# Test the configuration
echo "🧪 Testing email configuration..."
echo ""

# Create a simple test script
cat > test_email_config.py << 'EOF'
#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, '.')

try:
    from email_notifier import PipelineEmailNotifier
    
    # Check if environment variables are set
    required_vars = ['SENDER_EMAIL', 'EMAIL_PASSWORD', 'RECIPIENT_EMAIL', 'SMTP_SERVER', 'SMTP_PORT']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("Please restart your terminal or run: source ~/.zshrc (or ~/.bashrc)")
        sys.exit(1)
    
    print(f"📧 Sender: {os.getenv('SENDER_EMAIL')}")
    print(f"📬 Recipient: {os.getenv('RECIPIENT_EMAIL')}")
    print(f"🌐 SMTP: {os.getenv('SMTP_SERVER')}:{os.getenv('SMTP_PORT')}")
    print("")
    
    # Test email connection
    notifier = PipelineEmailNotifier()
    
    if notifier.test_email_connection():
        print("🎉 Email configuration successful!")
        print("")
        print("✅ Your OHLCV pipeline is now configured to send:")
        print("   📊 Success notifications with detailed statistics")
        print("   🚨 Failure notifications with error details")
        print("")
        print("🔐 Security Notes:")
        print("   • Your credentials are stored as environment variables")
        print("   • They will NOT be included in any code files")
        print("   • Safe to commit code to GitHub")
    else:
        print("❌ Email test failed. Please check your credentials.")
        print("")
        print("🔍 Troubleshooting:")
        print("   • Verify your Gmail app password is correct")
        print("   • Ensure 2-Factor Authentication is enabled")
        print("   • Check that the app password has 'Mail' permissions")

except Exception as e:
    print(f"❌ Error: {str(e)}")
    print("")
    print("💡 Make sure you've run: python3 -m pip install -r requirements.txt")

EOF

# Run the test
python3 test_email_config.py

# Clean up test file
rm -f test_email_config.py

echo ""
echo "📋 Setup Complete!"
echo "=================="
echo ""
echo "🔐 Your email credentials are now securely stored as environment variables."
echo "🚀 The cron job will automatically use these for notifications."
echo ""
echo "📝 Important Notes:"
echo "   • Your credentials are in: $PROFILE_FILE"
echo "   • They will be loaded automatically in new terminal sessions"
echo "   • Code files remain safe to commit to GitHub"
echo ""
echo "🧪 To test manually:"
echo "   ./schedule_incremental_pipeline.sh"
echo ""
echo "📅 Your weekly cron job will now send email notifications every Saturday!"
