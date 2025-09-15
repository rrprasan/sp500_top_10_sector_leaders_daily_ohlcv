#!/usr/bin/env python3
"""
Fix Email Password - Clean up non-ASCII characters from the email password
"""

import os
import re

def clean_password():
    """Clean the email password of non-ASCII characters."""
    
    password = os.getenv('EMAIL_PASSWORD', '')
    
    print("Email Password Cleanup")
    print("=" * 30)
    print(f"Original length: {len(password)}")
    print(f"Original (repr): {repr(password)}")
    
    # Remove non-ASCII characters and control sequences
    # Gmail app passwords should only contain letters and numbers
    clean_password = re.sub(r'[^a-zA-Z0-9]', '', password)
    
    print(f"Cleaned length: {len(clean_password)}")
    print(f"Cleaned password: {clean_password}")
    
    # Gmail app passwords are exactly 16 characters
    if len(clean_password) == 16:
        print("✅ Password length is correct (16 characters)")
        
        # Test if it's all ASCII
        try:
            clean_password.encode('ascii')
            print("✅ Password is ASCII-only")
            
            print(f"\n📝 Your cleaned Gmail app password is: {clean_password}")
            print("\n🔧 To fix this permanently, update your ~/.zshrc:")
            print(f'export EMAIL_PASSWORD="{clean_password}"')
            
            return clean_password
            
        except UnicodeEncodeError:
            print("❌ Still contains non-ASCII characters")
            return None
            
    else:
        print(f"⚠️ Password length is {len(clean_password)}, but Gmail app passwords should be 16 characters")
        print("📝 Please double-check your Gmail app password")
        return None

def test_cleaned_password(clean_password):
    """Test email with the cleaned password."""
    if not clean_password:
        return False
        
    print(f"\n🧪 Testing email with cleaned password...")
    
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    sender_email = os.getenv('SENDER_EMAIL')
    recipient_email = os.getenv('RECIPIENT_EMAIL')
    smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
    smtp_port = int(os.getenv('SMTP_PORT', '587'))
    
    try:
        # Create simple test message
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = "OHLCV Pipeline - Password Fixed Test"
        
        body = """
        <html>
        <body>
        <h2>Email Password Fixed!</h2>
        <p>This test email confirms your Gmail app password is now working correctly.</p>
        <p>Your OHLCV Pipeline email notifications are ready!</p>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(body, 'html', 'utf-8'))
        
        # Test with cleaned password
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, clean_password)
            server.send_message(msg)
        
        print(f"✅ Test email sent successfully to {recipient_email}")
        print("📧 Check your inbox - the password fix worked!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        return False

if __name__ == "__main__":
    cleaned = clean_password()
    if cleaned:
        test_cleaned_password(cleaned)
