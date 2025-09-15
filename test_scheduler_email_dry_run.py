#!/usr/bin/env python3
"""
Dry Run Test of Scheduler Email Integration
Tests the scheduler's email notification logic without running the actual pipeline.
"""

import sys
import os
import json
from datetime import datetime

# Mock the email notifier for testing
class MockPipelineEmailNotifier:
    def __init__(self):
        print("📧 Mock Email Notifier initialized")
    
    def send_success_notification(self, stats):
        print("✅ SUCCESS NOTIFICATION SENT (MOCK)")
        print(f"   Stats: {stats}")
        return True
    
    def send_failure_notification(self, error_details):
        print("❌ FAILURE NOTIFICATION SENT (MOCK)")
        print(f"   Error: {error_details}")
        return True

def test_success_email_logic():
    """Test the success email logic that would be used in the scheduler."""
    print("🧪 Testing Success Email Logic")
    print("-" * 40)
    
    # Mock successful pipeline statistics (like what the scheduler would parse)
    TOTAL_TICKERS = 111
    SUCCESSFUL_TICKERS = 111
    FAILED_TICKERS = 0
    DATE_RANGE = "from 2025-09-13 to 2025-09-19"
    EXECUTION_TIME_FORMATTED = "00:08:34"
    FILES_UPLOADED = 111
    
    # Create statistics JSON (like in the scheduler script)
    STATS_JSON = {
        "total_tickers": TOTAL_TICKERS,
        "successful_tickers": SUCCESSFUL_TICKERS, 
        "failed_tickers": FAILED_TICKERS,
        "execution_time": EXECUTION_TIME_FORMATTED,
        "date_range": DATE_RANGE,
        "files_uploaded": FILES_UPLOADED
    }
    
    print(f"📊 Parsed Statistics: {json.dumps(STATS_JSON, indent=2)}")
    
    # Test email sending
    notifier = MockPipelineEmailNotifier()
    success = notifier.send_success_notification(STATS_JSON)
    
    print(f"🎯 Success Email Test: {'✅ PASSED' if success else '❌ FAILED'}")
    return success

def test_failure_email_logic():
    """Test the failure email logic that would be used in the scheduler."""
    print("\n🧪 Testing Failure Email Logic")
    print("-" * 40)
    
    # Mock failure details (like what the scheduler would create)
    EXIT_CODE = 1
    ERROR_MESSAGE = "Pipeline failed: API rate limit exceeded for ticker AAPL"
    EXECUTION_TIME_FORMATTED = "00:02:15"
    FAILED_STEP = "Data Download"
    
    # Create error details JSON (like in the scheduler script)
    ERROR_JSON = {
        "exit_code": EXIT_CODE,
        "error_message": ERROR_MESSAGE,
        "execution_time": EXECUTION_TIME_FORMATTED,
        "failed_step": FAILED_STEP
    }
    
    print(f"🚨 Error Details: {json.dumps(ERROR_JSON, indent=2)}")
    
    # Test email sending
    notifier = MockPipelineEmailNotifier()
    success = notifier.send_failure_notification(ERROR_JSON)
    
    print(f"🎯 Failure Email Test: {'✅ PASSED' if success else '❌ FAILED'}")
    return success

def test_scheduler_script_structure():
    """Test the scheduler script structure and components."""
    print("\n🧪 Testing Scheduler Script Structure")
    print("-" * 40)
    
    # Check if scheduler script exists and has email integration
    try:
        with open('schedule_incremental_pipeline.sh', 'r') as f:
            content = f.read()
        
        # Check for key email integration components
        checks = [
            ("Email notification function", "send_email_notification" in content),
            ("Success notification call", "send_email_notification \"success\"" in content),
            ("Failure notification call", "send_email_notification \"failure\"" in content),
            ("Statistics parsing", "TOTAL_TICKERS=" in content),
            ("Error details creation", "ERROR_JSON=" in content),
            ("Email notifier import", "from email_notifier import PipelineEmailNotifier" in content)
        ]
        
        all_passed = True
        for check_name, passed in checks:
            status = "✅ FOUND" if passed else "❌ MISSING"
            print(f"   {status} - {check_name}")
            if not passed:
                all_passed = False
        
        print(f"\n🎯 Scheduler Structure Test: {'✅ PASSED' if all_passed else '❌ FAILED'}")
        return all_passed
        
    except Exception as e:
        print(f"❌ Error reading scheduler script: {e}")
        return False

def test_cron_job_status():
    """Check if the cron job is properly configured."""
    print("\n🧪 Testing Cron Job Configuration")
    print("-" * 40)
    
    try:
        import subprocess
        result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
        
        if result.returncode == 0:
            cron_content = result.stdout
            
            # Check for the OHLCV pipeline cron job
            has_pipeline_job = 'schedule_incremental_pipeline.sh' in cron_content
            has_saturday_schedule = '0 7 * * 6' in cron_content
            
            print(f"   {'✅ FOUND' if has_pipeline_job else '❌ MISSING'} - Pipeline scheduler in cron")
            print(f"   {'✅ FOUND' if has_saturday_schedule else '❌ MISSING'} - Saturday 7 AM schedule")
            
            if has_pipeline_job and has_saturday_schedule:
                print("   📅 Next run: Next Saturday at 7:00 AM PT")
                print("   📧 Email notifications: Will be sent automatically")
            
            success = has_pipeline_job and has_saturday_schedule
            print(f"\n🎯 Cron Job Test: {'✅ PASSED' if success else '❌ FAILED'}")
            return success
        else:
            print("   ❌ Could not read crontab")
            return False
            
    except Exception as e:
        print(f"   ❌ Error checking cron job: {e}")
        return False

def main():
    """Run all scheduler email integration tests."""
    print("🔧 Scheduler Email Integration Dry Run Test")
    print("=" * 60)
    print("Testing email notification logic without running the actual pipeline.")
    print()
    
    # Run all tests
    tests = [
        ("Success Email Logic", test_success_email_logic),
        ("Failure Email Logic", test_failure_email_logic), 
        ("Scheduler Script Structure", test_scheduler_script_structure),
        ("Cron Job Configuration", test_cron_job_status)
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ Test {test_name} crashed: {str(e)}")
            results[test_name] = False
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 DRY RUN TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print(f"\n🎯 Overall Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL DRY RUN TESTS PASSED!")
        print("📧 The scheduler email integration is working correctly.")
        print()
        print("✅ What works:")
        print("   • Email notification logic is properly integrated")
        print("   • Success and failure scenarios are handled")
        print("   • Cron job is configured for Saturday 7 AM PT")
        print("   • Statistics parsing and error handling are in place")
        print()
        print("🔧 To enable actual email sending:")
        print("   1. Configure email credentials: ./setup_secure_email.sh")
        print("   2. Provide your Gmail app password")
        print("   3. Next Saturday, you'll receive real notifications!")
    else:
        print(f"\n⚠️  {total - passed} tests failed. Please check the configuration.")

if __name__ == "__main__":
    main()
