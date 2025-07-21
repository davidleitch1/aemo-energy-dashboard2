#!/usr/bin/env python3
"""
Test script to verify email functionality for gen_dash.py
"""

import os
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_file = Path('.env')
if env_file.exists():
    load_dotenv(env_file)
    print(f"✅ Loaded configuration from {env_file}")
else:
    print("⚠️  No .env file found")
    print("Please create a .env file with your email configuration")
    sys.exit(1)

def test_email_connection():
    """Test the email configuration"""
    print("\n🔧 Testing Email Configuration...")
    
    # Get configuration
    sender_email = os.getenv('ALERT_EMAIL')
    sender_password = os.getenv('ALERT_PASSWORD')
    recipient_email = os.getenv('RECIPIENT_EMAIL', sender_email)
    smtp_server = os.getenv('SMTP_SERVER', 'smtp.mail.me.com')
    smtp_port = int(os.getenv('SMTP_PORT', '587'))
    
    print(f"Email: {sender_email}")
    print(f"Recipient: {recipient_email}")
    print(f"SMTP Server: {smtp_server}")
    print(f"SMTP Port: {smtp_port}")
    
    if not all([sender_email, sender_password]):
        print("❌ Email credentials not configured!")
        print("Please set ALERT_EMAIL and ALERT_PASSWORD in your .env file")
        return False
    
    try:
        print(f"\n📧 Attempting to connect to {smtp_server}:{smtp_port}...")
        
        # Create test message
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = f"🧪 Test Email from Energy Dashboard - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        
        body = f"""
        <html>
        <body>
            <h2>✅ Email Configuration Test Successful!</h2>
            <p>This is a test email from your Energy Dashboard to verify email alerts are working correctly.</p>
            
            <h3>Configuration Details:</h3>
            <ul>
                <li>SMTP Server: {smtp_server}</li>
                <li>SMTP Port: {smtp_port}</li>
                <li>From: {sender_email}</li>
                <li>To: {recipient_email}</li>
                <li>Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</li>
            </ul>
            
            <p>If you're seeing this email, your configuration is working correctly! 🎉</p>
            
            <h3>Environment Variables Detected:</h3>
            <ul>
                <li>ENABLE_EMAIL_ALERTS: {os.getenv('ENABLE_EMAIL_ALERTS', 'true')}</li>
                <li>ALERT_COOLDOWN_HOURS: {os.getenv('ALERT_COOLDOWN_HOURS', '24')}</li>
                <li>AUTO_ADD_TO_EXCEPTIONS: {os.getenv('AUTO_ADD_TO_EXCEPTIONS', 'true')}</li>
            </ul>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(body, 'html'))
        
        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            print("✅ Connected to SMTP server")
            
            print("🔐 Starting TLS...")
            server.starttls()
            print("✅ TLS established")
            
            print("🔑 Logging in...")
            server.login(sender_email, sender_password)
            print("✅ Login successful")
            
            print("📤 Sending test email...")
            server.send_message(msg)
            print("✅ Email sent successfully!")
        
        print(f"\n🎉 Success! Check your inbox at {recipient_email}")
        return True
        
    except smtplib.SMTPAuthenticationError as e:
        print(f"\n❌ Authentication failed: {e}")
        print("\nPossible solutions:")
        print("1. Check your email and password in .env file")
        print("2. For iCloud: Use an app-specific password from appleid.apple.com")
        print("3. For Gmail: Enable 2FA and use an app password")
        return False
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print(f"Error type: {type(e).__name__}")
        return False

def main():
    """Main test function"""
    print("=" * 60)
    print("Energy Dashboard Email Test")
    print("=" * 60)
    
    if test_email_connection():
        print("\n✅ Email system is working correctly!")
        print("Your gen_dash.py should now be able to send DUID alerts.")
    else:
        print("\n❌ Email system test failed!")
        print("Please check your configuration and try again.")

if __name__ == "__main__":
    main()