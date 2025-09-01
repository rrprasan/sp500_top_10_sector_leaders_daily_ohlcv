#!/usr/bin/env python3
"""
Modified Snowflake Vectorized Scanner Performance Test
Tests COPY INTO command performance with different configurations based on Snowflake constraints.
"""

import snowflake.connector
import logging
import time
import sys
from datetime import datetime
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('vectorized_scanner_modified_test.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ModifiedVectorizedScannerTest:
    """
    Modified class to test Snowflake COPY INTO performance with vectorized scanner settings.
    Adapts to Snowflake's constraint that LOAD_MODE = ADD_FILES_COPY requires USE_VECTORIZED_SCANNER = TRUE.
    """
    
    def __init__(self):
        """Initialize the test."""
        self.conn = None
        self.cursor = None
        
        # COPY INTO command for USE_VECTORIZED_SCANNER = FALSE (without ADD_FILES_COPY)
        self.copy_command_false = """
        COPY INTO sp500_top10_sector_ohlcv_itbl
          FROM @SP500_TOP_10_SECTOR_LEADERS_OHLCV_STG
          FILE_FORMAT = (
             FORMAT_NAME = 'SP500_TOP10_SECTOR_OHLCV_FILE_FORMAT'
             USE_VECTORIZED_SCANNER = FALSE
          )
          PURGE = FALSE
          MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
          FORCE = FALSE;
        """
        
        # COPY INTO command for USE_VECTORIZED_SCANNER = TRUE (with ADD_FILES_COPY)
        self.copy_command_true = """
        COPY INTO sp500_top10_sector_ohlcv_itbl
          FROM @SP500_TOP_10_SECTOR_LEADERS_OHLCV_STG
          FILE_FORMAT = (
             FORMAT_NAME = 'SP500_TOP10_SECTOR_OHLCV_FILE_FORMAT'
             USE_VECTORIZED_SCANNER = TRUE
          )
          LOAD_MODE = ADD_FILES_COPY
          PURGE = FALSE
          MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
          FORCE = FALSE;
        """
        
        logger.info("Modified Vectorized Scanner Test initialized")
    
    def _connect_to_snowflake(self) -> bool:
        """Establish connection to Snowflake."""
        try:
            self.conn = snowflake.connector.connect(
                connection_name='DEMO_PRAJAGOPAL'
            )
            self.cursor = self.conn.cursor()
            logger.info("Successfully connected to Snowflake")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            return False
    
    def _disconnect_from_snowflake(self):
        """Close Snowflake connection."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logger.info("Snowflake connection closed")
        except Exception as e:
            logger.error(f"Error closing Snowflake connection: {e}")
    
    def _get_table_row_count(self) -> int:
        """Get current row count in the target table."""
        try:
            self.cursor.execute("SELECT COUNT(*) FROM sp500_top10_sector_ohlcv_itbl")
            result = self.cursor.fetchone()
            count = result[0] if result else 0
            logger.info(f"Current table row count: {count:,}")
            return count
        except Exception as e:
            logger.error(f"Failed to get table row count: {e}")
            return -1
    
    def _get_stage_file_count(self) -> int:
        """Get count of files in the stage."""
        try:
            self.cursor.execute("LIST @SP500_TOP_10_SECTOR_LEADERS_OHLCV_STG")
            results = self.cursor.fetchall()
            file_count = len(results)
            logger.info(f"Files in stage: {file_count}")
            
            # Show sample files
            if results:
                logger.info("Sample files in stage:")
                for i, row in enumerate(results[:5]):
                    logger.info(f"  {i+1}. {row[0]}")
                if len(results) > 5:
                    logger.info(f"  ... and {len(results) - 5} more files")
            
            return file_count
        except Exception as e:
            logger.error(f"Failed to list stage files: {e}")
            return -1
    
    def _truncate_table(self) -> bool:
        """Truncate the target table to prepare for fresh load."""
        try:
            logger.info("Truncating target table...")
            self.cursor.execute("TRUNCATE TABLE sp500_top10_sector_ohlcv_itbl")
            logger.info("Table truncated successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to truncate table: {e}")
            return False
    
    def _execute_copy_command(self, vectorized_setting: str, copy_command: str) -> Dict[str, Any]:
        """Execute COPY INTO command and measure performance."""
        try:
            logger.info(f"Executing COPY INTO with USE_VECTORIZED_SCANNER = {vectorized_setting}")
            logger.info("COPY command:")
            for line in copy_command.strip().split('\n'):
                logger.info(f"  {line.strip()}")
            
            # Record start time
            start_time = time.time()
            start_datetime = datetime.now()
            
            # Execute the COPY command
            self.cursor.execute(copy_command)
            
            # Record end time
            end_time = time.time()
            end_datetime = datetime.now()
            execution_time = end_time - start_time
            
            # Get results
            results = self.cursor.fetchall()
            
            # Parse results (COPY INTO returns status information)
            result_info = {
                'vectorized_setting': vectorized_setting,
                'start_time': start_datetime,
                'end_time': end_datetime,
                'execution_time_seconds': execution_time,
                'execution_time_formatted': f"{execution_time:.2f} seconds",
                'copy_results': results,
                'success': True
            }
            
            # Log results
            logger.info(f"COPY command completed in {execution_time:.2f} seconds")
            if results:
                total_rows = sum(row[2] for row in results if len(row) > 2)  # Sum rows_loaded
                logger.info(f"Total rows loaded: {total_rows:,}")
                for result in results:
                    logger.info(f"COPY result: {result}")
            
            return result_info
            
        except Exception as e:
            logger.error(f"Failed to execute COPY command with {vectorized_setting}: {e}")
            return {
                'vectorized_setting': vectorized_setting,
                'execution_time_seconds': -1,
                'error': str(e),
                'success': False
            }
    
    def run_performance_test(self) -> Dict[str, Any]:
        """Run the complete performance test comparing vectorized scanner settings."""
        logger.info("=" * 80)
        logger.info("MODIFIED SNOWFLAKE VECTORIZED SCANNER PERFORMANCE TEST")
        logger.info("=" * 80)
        logger.info("Note: Testing different COPY configurations due to Snowflake constraints:")
        logger.info("  FALSE: Standard COPY without ADD_FILES_COPY")
        logger.info("  TRUE:  Optimized COPY with ADD_FILES_COPY")
        
        test_results = {
            'test_start_time': datetime.now(),
            'false_result': None,
            'true_result': None,
            'comparison': None
        }
        
        try:
            # Step 1: Connect to Snowflake
            logger.info("Step 1: Connecting to Snowflake...")
            if not self._connect_to_snowflake():
                logger.error("Failed to connect to Snowflake")
                return test_results
            
            # Step 2: Check stage files
            logger.info("Step 2: Checking stage files...")
            file_count = self._get_stage_file_count()
            if file_count <= 0:
                logger.error("No files found in stage. Please run the data pipeline first.")
                return test_results
            
            # Step 3: Test with USE_VECTORIZED_SCANNER = FALSE (standard COPY)
            logger.info("Step 3: Testing with USE_VECTORIZED_SCANNER = FALSE (standard COPY)...")
            
            # Truncate table for clean test
            if not self._truncate_table():
                logger.error("Failed to truncate table")
                return test_results
            
            # Get initial row count
            initial_count = self._get_table_row_count()
            
            # Execute COPY with vectorized scanner FALSE
            false_result = self._execute_copy_command('FALSE', self.copy_command_false)
            test_results['false_result'] = false_result
            
            if false_result['success']:
                # Get row count after load
                false_final_count = self._get_table_row_count()
                false_result['rows_loaded'] = false_final_count - initial_count
                logger.info(f"Rows loaded with FALSE: {false_result['rows_loaded']:,}")
            
            # Step 4: Test with USE_VECTORIZED_SCANNER = TRUE (optimized COPY)
            logger.info("Step 4: Testing with USE_VECTORIZED_SCANNER = TRUE (optimized COPY)...")
            
            # Truncate table for clean test
            if not self._truncate_table():
                logger.error("Failed to truncate table for second test")
                return test_results
            
            # Get initial row count
            initial_count = self._get_table_row_count()
            
            # Execute COPY with vectorized scanner TRUE
            true_result = self._execute_copy_command('TRUE', self.copy_command_true)
            test_results['true_result'] = true_result
            
            if true_result['success']:
                # Get row count after load
                true_final_count = self._get_table_row_count()
                true_result['rows_loaded'] = true_final_count - initial_count
                logger.info(f"Rows loaded with TRUE: {true_result['rows_loaded']:,}")
            
            # Step 5: Compare results
            logger.info("Step 5: Comparing results...")
            if false_result['success'] and true_result['success']:
                comparison = self._compare_results(false_result, true_result)
                test_results['comparison'] = comparison
            
            test_results['test_end_time'] = datetime.now()
            
            # Step 6: Generate summary report
            self._generate_summary_report(test_results)
            
            return test_results
            
        except Exception as e:
            logger.error(f"Performance test failed: {e}")
            test_results['error'] = str(e)
            return test_results
        
        finally:
            self._disconnect_from_snowflake()
    
    def _compare_results(self, false_result: Dict[str, Any], true_result: Dict[str, Any]) -> Dict[str, Any]:
        """Compare the performance results."""
        false_time = false_result['execution_time_seconds']
        true_time = true_result['execution_time_seconds']
        
        if false_time > 0 and true_time > 0:
            time_difference = false_time - true_time
            percent_improvement = ((false_time - true_time) / false_time) * 100
            
            comparison = {
                'false_time': false_time,
                'true_time': true_time,
                'time_difference_seconds': time_difference,
                'percent_improvement': percent_improvement,
                'faster_setting': 'TRUE' if true_time < false_time else 'FALSE'
            }
            
            logger.info(f"Performance comparison:")
            logger.info(f"  FALSE (standard): {false_time:.2f} seconds")
            logger.info(f"  TRUE (optimized): {true_time:.2f} seconds")
            logger.info(f"  Difference: {time_difference:.2f} seconds")
            logger.info(f"  Improvement: {percent_improvement:.1f}%")
            logger.info(f"  Faster setting: USE_VECTORIZED_SCANNER = {comparison['faster_setting']}")
            
            return comparison
        else:
            logger.error("Cannot compare results due to execution failures")
            return {'error': 'Cannot compare due to execution failures'}
    
    def _generate_summary_report(self, test_results: Dict[str, Any]):
        """Generate a comprehensive summary report."""
        logger.info("=" * 80)
        logger.info("MODIFIED VECTORIZED SCANNER PERFORMANCE TEST SUMMARY")
        logger.info("=" * 80)
        
        total_time = test_results['test_end_time'] - test_results['test_start_time']
        logger.info(f"Total test duration: {total_time}")
        logger.info(f"Test completed at: {test_results['test_end_time']}")
        
        # Results summary
        false_result = test_results.get('false_result')
        true_result = test_results.get('true_result')
        comparison = test_results.get('comparison')
        
        if false_result and true_result:
            logger.info("")
            logger.info("PERFORMANCE RESULTS:")
            logger.info(f"  USE_VECTORIZED_SCANNER = FALSE (standard): {false_result.get('execution_time_formatted', 'FAILED')}")
            logger.info(f"  USE_VECTORIZED_SCANNER = TRUE (optimized):  {true_result.get('execution_time_formatted', 'FAILED')}")
            
            if comparison and 'percent_improvement' in comparison:
                improvement = comparison['percent_improvement']
                faster = comparison['faster_setting']
                
                if improvement > 0:
                    logger.info(f"  🚀 VECTORIZED_SCANNER = TRUE is {improvement:.1f}% faster!")
                elif improvement < 0:
                    logger.info(f"  📊 VECTORIZED_SCANNER = FALSE is {abs(improvement):.1f}% faster!")
                else:
                    logger.info(f"  ⚖️  Both settings performed equally")
                
                logger.info(f"  🏆 Winner: USE_VECTORIZED_SCANNER = {faster}")
        
        logger.info("")
        logger.info("KEY FINDINGS:")
        logger.info("  • LOAD_MODE = ADD_FILES_COPY requires USE_VECTORIZED_SCANNER = TRUE")
        logger.info("  • This is a Snowflake constraint, not a performance choice")
        logger.info("  • The TRUE setting enables additional optimizations beyond just vectorization")
        
        logger.info("")
        logger.info("RECOMMENDATION:")
        if comparison and 'faster_setting' in comparison:
            winner = comparison['faster_setting']
            logger.info(f"  Use USE_VECTORIZED_SCANNER = {winner} for optimal performance")
            if winner == 'TRUE':
                logger.info("  This also enables advanced features like ADD_FILES_COPY")
        else:
            logger.info("  Use USE_VECTORIZED_SCANNER = TRUE (required for ADD_FILES_COPY)")
        
        logger.info("")
        logger.info("Test completed successfully! 🎉")


def main():
    """Main entry point."""
    try:
        test = ModifiedVectorizedScannerTest()
        results = test.run_performance_test()
        
        if results.get('comparison') or (results.get('true_result') and results['true_result']['success']):
            logger.info("Performance test completed successfully")
            sys.exit(0)
        else:
            logger.error("Performance test failed or incomplete")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
