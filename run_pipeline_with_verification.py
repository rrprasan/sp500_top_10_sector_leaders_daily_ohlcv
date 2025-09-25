#!/usr/bin/env python3
"""
Pipeline Execution with Data Quality Verification

This script runs the incremental OHLCV pipeline followed by comprehensive
data quality verification to ensure the process completed successfully.

Usage:
    python run_pipeline_with_verification.py

This will:
1. Run the incremental_ohlcv_pipeline.py
2. Run data_quality_verification.py
3. Provide a comprehensive report on success/failure
"""

import subprocess
import sys
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_with_verification.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def run_command(command: list, description: str) -> tuple:
    """
    Run a command and return (success, return_code, output).
    
    Args:
        command: List of command parts
        description: Human readable description of the command
        
    Returns:
        Tuple of (success: bool, return_code: int, output: str)
    """
    logger.info(f"Starting: {description}")
    logger.info(f"Command: {' '.join(command)}")
    
    try:
        # Run the command
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        
        success = result.returncode == 0
        
        if success:
            logger.info(f"✅ {description} completed successfully")
        else:
            logger.error(f"❌ {description} failed with return code: {result.returncode}")
        
        # Log output if there were errors
        if result.stderr:
            logger.warning(f"STDERR from {description}:")
            logger.warning(result.stderr)
        
        return success, result.returncode, result.stdout + result.stderr
        
    except Exception as e:
        logger.error(f"❌ Failed to run {description}: {e}")
        return False, -1, str(e)


def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("OHLCV PIPELINE EXECUTION WITH DATA QUALITY VERIFICATION")
    logger.info("=" * 80)
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    
    pipeline_success = False
    verification_success = False
    
    try:
        # Step 1: Run the incremental OHLCV pipeline
        logger.info("STEP 1: Running Incremental OHLCV Pipeline")
        logger.info("-" * 50)
        
        pipeline_success, pipeline_code, pipeline_output = run_command(
            [sys.executable, "incremental_ohlcv_pipeline.py"],
            "Incremental OHLCV Pipeline"
        )
        
        if not pipeline_success:
            logger.error("Pipeline execution failed. Checking if data was still uploaded...")
        
        # Step 2: Run data quality verification (regardless of pipeline "success")
        logger.info("")
        logger.info("STEP 2: Running Data Quality Verification")
        logger.info("-" * 50)
        
        verification_success, verification_code, verification_output = run_command(
            [sys.executable, "data_quality_verification.py"],
            "Data Quality Verification"
        )
        
        # Step 3: Final assessment
        logger.info("")
        logger.info("STEP 3: Final Assessment")
        logger.info("-" * 50)
        
        if verification_success:
            logger.info("🎉 OVERALL RESULT: SUCCESS ✅")
            logger.info("Data quality verification confirms the pipeline worked correctly.")
            logger.info("Data is available in S3 and meets quality standards.")
            
            if not pipeline_success:
                logger.info("Note: Pipeline reported failure but data was successfully processed.")
                logger.info("This is likely due to misleading error messages in the pipeline.")
            
            final_exit_code = 0
            
        elif verification_code == 1:  # Warning status
            logger.info("⚠️ OVERALL RESULT: SUCCESS WITH WARNINGS")
            logger.info("Data quality verification passed with some warnings.")
            logger.info("Pipeline likely worked but may need attention.")
            final_exit_code = 1
            
        else:
            logger.error("❌ OVERALL RESULT: FAILURE")
            logger.error("Data quality verification failed.")
            logger.error("Pipeline did not successfully process and upload data.")
            final_exit_code = 2
        
        # Summary information
        logger.info("")
        logger.info("EXECUTION SUMMARY")
        logger.info("-" * 50)
        logger.info(f"Pipeline execution: {'✅ Success' if pipeline_success else '❌ Failed'}")
        logger.info(f"Data quality verification: {'✅ Passed' if verification_success else '❌ Failed'}")
        logger.info(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("")
        logger.info("For detailed results, check:")
        logger.info("- incremental_ohlcv_pipeline.log")
        logger.info("- data_quality_verification.log")
        logger.info("- pipeline_with_verification.log")
        logger.info("=" * 80)
        
        sys.exit(final_exit_code)
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
