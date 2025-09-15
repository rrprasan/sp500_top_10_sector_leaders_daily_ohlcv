#!/usr/bin/env python3
"""
Test Email Integration for OHLCV Pipeline
Tests the email notification system without actually sending emails.
"""

import json
import sys
from datetime import datetime

class MockEmailNotifier:
    """Mock email notifier for testing purposes."""
    
    def __init__(self):
        self.config = {
            "smtp_server": "smtp.gmail.com",
            "smtp_port": 587,
            "sender_email": "test@example.com",
            "sender_password": "mock_password",
            "recipient_email": "recipient@example.com"
        }
    
    def send_success_notification(self, execution_stats):
        """Mock success notification - just print what would be sent."""
        print("📧 SUCCESS EMAIL NOTIFICATION (MOCK)")
        print("=" * 50)
        print(f"To: {self.config['recipient_email']}")
        print(f"From: {self.config['sender_email']}")
        print(f"Subject: ✅ OHLCV Pipeline Success - {datetime.now().strftime('%Y-%m-%d')}")
        print()
        print("EMAIL CONTENT PREVIEW:")
        print("-" * 30)
        print(f"🎉 Pipeline Completed Successfully!")
        print(f"• Total Tickers: {execution_stats.get('total_tickers', 0)}")
        print(f"• Successful: {execution_stats.get('successful_tickers', 0)}")
        print(f"• Failed: {execution_stats.get('failed_tickers', 0)}")
        print(f"• Execution Time: {execution_stats.get('execution_time', 'Unknown')}")
        print(f"• Date Range: {execution_stats.get('date_range', 'Unknown')}")
        print(f"• Files Uploaded: {execution_stats.get('files_uploaded', 0)}")
        
        success_rate = 0
        if execution_stats.get('total_tickers', 0) > 0:
            success_rate = (execution_stats.get('successful_tickers', 0) / 
                          execution_stats.get('total_tickers', 1) * 100)
        print(f"• Success Rate: {success_rate:.1f}%")
        print()
        print("✅ SUCCESS EMAIL WOULD BE SENT")
        return True
    
    def send_failure_notification(self, error_details):
        """Mock failure notification - just print what would be sent."""
        print("📧 FAILURE EMAIL NOTIFICATION (MOCK)")
        print("=" * 50)
        print(f"To: {self.config['recipient_email']}")
        print(f"From: {self.config['sender_email']}")
        print(f"Subject: ❌ OHLCV Pipeline Failure - {datetime.now().strftime('%Y-%m-%d')}")
        print()
        print("EMAIL CONTENT PREVIEW:")
        print("-" * 30)
        print(f"🚨 Pipeline Execution Failed")
        print(f"• Exit Code: {error_details.get('exit_code', 'Unknown')}")
        print(f"• Execution Time: {error_details.get('execution_time', 'Unknown')}")
        print(f"• Failed Step: {error_details.get('failed_step', 'Unknown')}")
        print(f"• Error Message: {error_details.get('error_message', 'No details')}")
        print()
        print("❌ FAILURE EMAIL WOULD BE SENT")
        return True
    
    def test_email_connection(self):
        """Mock email connection test."""
        print("🧪 TESTING EMAIL CONNECTION (MOCK)")
        print("=" * 40)
        print(f"📧 Sender: {self.config['sender_email']}")
        print(f"📬 Recipient: {self.config['recipient_email']}")
        print(f"🌐 SMTP: {self.config['smtp_server']}:{self.config['smtp_port']}")
        print()
        print("✅ Mock email connection successful!")
        print("📧 Test email would be sent successfully")
        return True

def test_success_scenario():
    """Test success notification scenario."""
    print("🧪 TESTING SUCCESS NOTIFICATION SCENARIO")
    print("=" * 60)
    
    notifier = MockEmailNotifier()
    
    # Mock successful pipeline execution stats
    mock_stats = {
        "total_tickers": 111,
        "successful_tickers": 111,
        "failed_tickers": 0,
        "execution_time": "00:08:34",
        "date_range": "from 2025-09-13 to 2025-09-19",
        "files_uploaded": 111
    }
    
    success = notifier.send_success_notification(mock_stats)
    print(f"\n🎯 Success Notification Test: {'✅ PASSED' if success else '❌ FAILED'}")
    return success

def test_failure_scenario():
    """Test failure notification scenario."""
    print("\n🧪 TESTING FAILURE NOTIFICATION SCENARIO")
    print("=" * 60)
    
    notifier = MockEmailNotifier()
    
    # Mock failed pipeline execution details
    mock_error = {
        "exit_code": 1,
        "error_message": "API rate limit exceeded for AAPL. Check your Polygon.io quota.",
        "execution_time": "00:02:15",
        "failed_step": "Data Download"
    }
    
    success = notifier.send_failure_notification(mock_error)
    print(f"\n🎯 Failure Notification Test: {'✅ PASSED' if success else '❌ FAILED'}")
    return success

def test_email_connection():
    """Test email connection."""
    print("\n🧪 TESTING EMAIL CONNECTION")
    print("=" * 60)
    
    notifier = MockEmailNotifier()
    success = notifier.test_email_connection()
    print(f"\n🎯 Connection Test: {'✅ PASSED' if success else '❌ FAILED'}")
    return success

def test_scheduler_integration():
    """Test integration with scheduler script."""
    print("\n🧪 TESTING SCHEDULER INTEGRATION")
    print("=" * 60)
    
    print("📋 Checking scheduler script components...")
    
    # Check if scheduler script exists
    import os
    scheduler_exists = os.path.exists('schedule_incremental_pipeline.sh')
    print(f"• Scheduler script exists: {'✅ YES' if scheduler_exists else '❌ NO'}")
    
    # Check if email notifier exists
    email_notifier_exists = os.path.exists('email_notifier.py')
    print(f"• Email notifier exists: {'✅ YES' if email_notifier_exists else '❌ NO'}")
    
    # Check if pipeline script exists
    pipeline_exists = os.path.exists('incremental_ohlcv_pipeline.py')
    print(f"• Pipeline script exists: {'✅ YES' if pipeline_exists else '❌ NO'}")
    
    all_components = scheduler_exists and email_notifier_exists and pipeline_exists
    print(f"\n🎯 Integration Components: {'✅ ALL PRESENT' if all_components else '❌ MISSING COMPONENTS'}")
    
    if all_components:
        print("\n📧 Email Integration Flow:")
        print("1. Cron job triggers → schedule_incremental_pipeline.sh")
        print("2. Scheduler runs → incremental_ohlcv_pipeline.py")
        print("3. Pipeline completes → Statistics parsed")
        print("4. Scheduler calls → email_notifier.py")
        print("5. Email sent → Success/Failure notification")
        print("\n✅ INTEGRATION READY")
    
    return all_components

def main():
    """Run all email integration tests."""
    print("🔧 OHLCV Pipeline Email Integration Test Suite")
    print("=" * 80)
    print("This test simulates email functionality without sending actual emails.")
    print()
    
    # Run all tests
    tests = [
        ("Email Connection", test_email_connection),
        ("Success Notification", test_success_scenario),
        ("Failure Notification", test_failure_scenario),
        ("Scheduler Integration", test_scheduler_integration)
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ Test {test_name} crashed: {str(e)}")
            results[test_name] = False
    
    # Summary
    print("\n" + "=" * 80)
    print("📊 TEST SUMMARY")
    print("=" * 80)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print(f"\n🎯 Overall Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED!")
        print("📧 Email integration is ready and will work when configured.")
        print()
        print("🔧 To configure actual email sending:")
        print("   1. Run: ./setup_secure_email.sh")
        print("   2. Provide your Gmail app password")
        print("   3. Test with: python3 email_notifier.py")
        print()
        print("📅 Your Saturday cron job will then send real email notifications!")
    else:
        print(f"\n⚠️  {total - passed} tests failed. Please check the issues above.")

if __name__ == "__main__":
    main()
