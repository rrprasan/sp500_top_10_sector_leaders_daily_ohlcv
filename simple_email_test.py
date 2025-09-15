#!/usr/bin/env python3
"""
Simple Email Test - Minimal test to identify and fix encoding issues
"""

import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

def simple_email_test():
    """Simple email test with minimal content."""
    
    # Get email configuration from environment
    sender_email = os.getenv('SENDER_EMAIL')
    email_password = os.getenv('EMAIL_PASSWORD') 
    recipient_email = os.getenv('RECIPIENT_EMAIL')
    smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
    smtp_port = int(os.getenv('SMTP_PORT', '587'))
    
    print("Simple Email Test")
    print("=" * 30)
    print(f"Sender: {sender_email}")
    print(f"Recipient: {recipient_email}")
    print(f"SMTP: {smtp_server}:{smtp_port}")
    print()
    
    if not all([sender_email, email_password, recipient_email]):
        print("❌ Missing email configuration. Please set environment variables:")
        print("   SENDER_EMAIL, EMAIL_PASSWORD, RECIPIENT_EMAIL")
        return False
    
    try:
        # Create message with minimal content
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = "OHLCV Pipeline Test Email"
        
        # Simple HTML body without special characters
        body = """
        <html>
        <body>
        <h2>Email Test Successful</h2>
        <p>This is a test email from your OHLCV Pipeline.</p>
        <p>Configuration: Working</p>
        <p>Recipient: """ + recipient_email + """</p>
        <p>Time: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</p>
        <p>Your email notifications are ready!</p>
        </body>
        </html>
        """
        
        # Attach the body
        msg.attach(MIMEText(body, 'html', 'utf-8'))
        
        # Send email
        print("Connecting to SMTP server...")
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            print("Starting TLS...")
            server.starttls()
            
            print("Logging in...")
            server.login(sender_email, email_password)
            
            print("Sending email...")
            server.send_message(msg)
        
        print(f"✅ Email sent successfully to {recipient_email}")
        print("📧 Check your inbox (and spam folder)")
        return True
        
    except Exception as e:
        print(f"❌ Email failed: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        
        # Debug information
        if "ascii" in str(e).lower():
            print("\n🔍 Encoding issue detected:")
            print("   • This appears to be a character encoding problem")
            print("   • Checking for non-ASCII characters in configuration...")
            
            # Check for non-ASCII characters
            for var_name, var_value in [
                ('SENDER_EMAIL', sender_email),
                ('RECIPIENT_EMAIL', recipient_email),
                ('SMTP_SERVER', smtp_server)
            ]:
                if var_value:
                    try:
                        var_value.encode('ascii')
                        print(f"   • {var_name}: OK (ASCII)")
                    except UnicodeEncodeError as ue:
                        print(f"   • {var_name}: ❌ Contains non-ASCII character at position {ue.start}")
                        print(f"     Value: {repr(var_value)}")
        
        return False

if __name__ == "__main__":
    simple_email_test()
