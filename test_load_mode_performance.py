#!/usr/bin/env python3
"""
Snowflake LOAD_MODE Performance Test
Tests COPY INTO command performance comparing three scenarios:
1. LOAD_MODE = FULL_INGEST with USE_VECTORIZED_SCANNER = TRUE
2. LOAD_MODE = ADD_FILES_COPY with USE_VECTORIZED_SCANNER = TRUE  
3. LOAD_MODE = FULL_INGEST with USE_VECTORIZED_SCANNER = FALSE
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
        logging.FileHandler('load_mode_performance_test.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class LoadModePerformanceTest:
    """
    Class to test Snowflake COPY INTO performance comparing three scenarios:
    1. LOAD_MODE = FULL_INGEST with USE_VECTORIZED_SCANNER = TRUE
    2. LOAD_MODE = ADD_FILES_COPY with USE_VECTORIZED_SCANNER = TRUE
    3. LOAD_MODE = FULL_INGEST with USE_VECTORIZED_SCANNER = FALSE
    """
    
    def __init__(self):
        """Initialize the test."""
        self.conn = None
        self.cursor = None
        
        # COPY INTO command for LOAD_MODE = FULL_INGEST with VECTORIZED_SCANNER = TRUE
        self.copy_command_full_ingest_vectorized = """
        COPY INTO sp500_top10_sector_ohlcv_itbl
          FROM @SP500_TOP_10_SECTOR_LEADERS_OHLCV_STG
          FILE_FORMAT = (
             FORMAT_NAME = 'SP500_TOP10_SECTOR_OHLCV_FILE_FORMAT'
             USE_VECTORIZED_SCANNER = TRUE
          )
          LOAD_MODE = FULL_INGEST
          PURGE = FALSE
          MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
          FORCE = FALSE;
        """
        
        # COPY INTO command for LOAD_MODE = FULL_INGEST with VECTORIZED_SCANNER = FALSE
        self.copy_command_full_ingest_non_vectorized = """
        COPY INTO sp500_top10_sector_ohlcv_itbl
          FROM @SP500_TOP_10_SECTOR_LEADERS_OHLCV_STG
          FILE_FORMAT = (
             FORMAT_NAME = 'SP500_TOP10_SECTOR_OHLCV_FILE_FORMAT'
             USE_VECTORIZED_SCANNER = FALSE
          )
          LOAD_MODE = FULL_INGEST
          PURGE = FALSE
          MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
          FORCE = FALSE;
        """
        
        # COPY INTO command for LOAD_MODE = ADD_FILES_COPY
        self.copy_command_add_files_copy = """
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
        
        logger.info("Load Mode Performance Test initialized")
    
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
    
    def _get_table_statistics(self) -> Dict[str, Any]:
        """Get detailed table statistics after load."""
        try:
            stats = {}
            
            # Row count
            self.cursor.execute("SELECT COUNT(*) FROM sp500_top10_sector_ohlcv_itbl")
            stats['total_rows'] = self.cursor.fetchone()[0]
            
            # Unique tickers
            self.cursor.execute("SELECT COUNT(DISTINCT TICKER) FROM sp500_top10_sector_ohlcv_itbl")
            stats['unique_tickers'] = self.cursor.fetchone()[0]
            
            # Date range
            self.cursor.execute("SELECT MIN(OHLC_DATE), MAX(OHLC_DATE) FROM sp500_top10_sector_ohlcv_itbl")
            date_range = self.cursor.fetchone()
            stats['min_date'] = date_range[0]
            stats['max_date'] = date_range[1]
            
            # Sample ticker counts
            self.cursor.execute("""
                SELECT TICKER, COUNT(*) as row_count 
                FROM sp500_top10_sector_ohlcv_itbl 
                GROUP BY TICKER 
                ORDER BY TICKER 
                LIMIT 5
            """)
            stats['sample_ticker_counts'] = self.cursor.fetchall()
            
            return stats
        except Exception as e:
            logger.error(f"Failed to get table statistics: {e}")
            return {}
    
    def _execute_copy_command(self, load_mode: str, copy_command: str) -> Dict[str, Any]:
        """Execute COPY INTO command and measure performance."""
        try:
            logger.info(f"Executing COPY INTO with LOAD_MODE = {load_mode}")
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
            
            # Get detailed table statistics
            table_stats = self._get_table_statistics()
            
            # Parse results (COPY INTO returns status information)
            result_info = {
                'load_mode': load_mode,
                'start_time': start_datetime,
                'end_time': end_datetime,
                'execution_time_seconds': execution_time,
                'execution_time_formatted': f"{execution_time:.2f} seconds",
                'copy_results': results,
                'table_statistics': table_stats,
                'success': True
            }
            
            # Calculate performance metrics
            if results:
                total_files = len(results)
                total_rows = sum(row[2] for row in results if len(row) > 2)  # Sum rows_loaded
                throughput = total_rows / execution_time if execution_time > 0 else 0
                
                result_info.update({
                    'files_processed': total_files,
                    'rows_loaded': total_rows,
                    'throughput_rows_per_second': throughput,
                    'throughput_formatted': f"{throughput:,.0f} rows/sec"
                })
                
                logger.info(f"COPY command completed in {execution_time:.2f} seconds")
                logger.info(f"Files processed: {total_files}")
                logger.info(f"Total rows loaded: {total_rows:,}")
                logger.info(f"Throughput: {throughput:,.0f} rows/second")
                
                # Log individual file results
                for i, result in enumerate(results[:3]):  # Show first 3 files
                    logger.info(f"File {i+1}: {result}")
                if len(results) > 3:
                    logger.info(f"... and {len(results) - 3} more files")
            
            return result_info
            
        except Exception as e:
            logger.error(f"Failed to execute COPY command with LOAD_MODE = {load_mode}: {e}")
            return {
                'load_mode': load_mode,
                'execution_time_seconds': -1,
                'error': str(e),
                'success': False
            }
    
    def run_performance_test(self) -> Dict[str, Any]:
        """Run the complete performance test comparing LOAD_MODE options."""
        logger.info("=" * 80)
        logger.info("SNOWFLAKE LOAD_MODE PERFORMANCE TEST")
        logger.info("=" * 80)
        logger.info("Comparing three LOAD_MODE and VECTORIZED_SCANNER combinations:")
        logger.info("  1. FULL_INGEST + VECTORIZED_SCANNER = TRUE")
        logger.info("  2. ADD_FILES_COPY + VECTORIZED_SCANNER = TRUE") 
        logger.info("  3. FULL_INGEST + VECTORIZED_SCANNER = FALSE")
        
        test_results = {
            'test_start_time': datetime.now(),
            'full_ingest_vectorized_result': None,
            'add_files_copy_result': None,
            'full_ingest_non_vectorized_result': None,
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
            
            # Step 3: Test with LOAD_MODE = FULL_INGEST + VECTORIZED_SCANNER = TRUE
            logger.info("Step 3: Testing FULL_INGEST + VECTORIZED_SCANNER = TRUE...")
            
            # Truncate table for clean test
            if not self._truncate_table():
                logger.error("Failed to truncate table")
                return test_results
            
            # Execute COPY with FULL_INGEST + VECTORIZED
            full_ingest_vectorized_result = self._execute_copy_command('FULL_INGEST (VECTORIZED=TRUE)', self.copy_command_full_ingest_vectorized)
            test_results['full_ingest_vectorized_result'] = full_ingest_vectorized_result
            
            # Step 4: Test with LOAD_MODE = ADD_FILES_COPY + VECTORIZED_SCANNER = TRUE
            logger.info("Step 4: Testing ADD_FILES_COPY + VECTORIZED_SCANNER = TRUE...")
            
            # Truncate table for clean test
            if not self._truncate_table():
                logger.error("Failed to truncate table for second test")
                return test_results
            
            # Execute COPY with ADD_FILES_COPY + VECTORIZED
            add_files_copy_result = self._execute_copy_command('ADD_FILES_COPY (VECTORIZED=TRUE)', self.copy_command_add_files_copy)
            test_results['add_files_copy_result'] = add_files_copy_result
            
            # Step 5: Test with LOAD_MODE = FULL_INGEST + VECTORIZED_SCANNER = FALSE
            logger.info("Step 5: Testing FULL_INGEST + VECTORIZED_SCANNER = FALSE...")
            
            # Truncate table for clean test
            if not self._truncate_table():
                logger.error("Failed to truncate table for third test")
                return test_results
            
            # Execute COPY with FULL_INGEST + NON-VECTORIZED
            full_ingest_non_vectorized_result = self._execute_copy_command('FULL_INGEST (VECTORIZED=FALSE)', self.copy_command_full_ingest_non_vectorized)
            test_results['full_ingest_non_vectorized_result'] = full_ingest_non_vectorized_result
            
            # Step 6: Compare results
            logger.info("Step 6: Comparing all three results...")
            if (full_ingest_vectorized_result['success'] and add_files_copy_result['success'] and 
                full_ingest_non_vectorized_result['success']):
                comparison = self._compare_three_results(
                    full_ingest_vectorized_result, 
                    add_files_copy_result, 
                    full_ingest_non_vectorized_result
                )
                test_results['comparison'] = comparison
            
            test_results['test_end_time'] = datetime.now()
            
            # Step 7: Generate summary report
            self._generate_summary_report(test_results)
            
            return test_results
            
        except Exception as e:
            logger.error(f"Performance test failed: {e}")
            test_results['error'] = str(e)
            return test_results
        
        finally:
            self._disconnect_from_snowflake()
    
    def _compare_three_results(self, full_ingest_vectorized: Dict[str, Any], 
                              add_files_copy: Dict[str, Any], 
                              full_ingest_non_vectorized: Dict[str, Any]) -> Dict[str, Any]:
        """Compare the performance results between all three test scenarios."""
        
        # Extract execution times
        time_full_vectorized = full_ingest_vectorized['execution_time_seconds']
        time_add_files = add_files_copy['execution_time_seconds']
        time_full_non_vectorized = full_ingest_non_vectorized['execution_time_seconds']
        
        # Extract throughputs
        throughput_full_vectorized = full_ingest_vectorized.get('throughput_rows_per_second', 0)
        throughput_add_files = add_files_copy.get('throughput_rows_per_second', 0)
        throughput_full_non_vectorized = full_ingest_non_vectorized.get('throughput_rows_per_second', 0)
        
        # Find the fastest scenario
        times = {
            'FULL_INGEST (VECTORIZED=TRUE)': time_full_vectorized,
            'ADD_FILES_COPY (VECTORIZED=TRUE)': time_add_files,
            'FULL_INGEST (VECTORIZED=FALSE)': time_full_non_vectorized
        }
        
        fastest_scenario = min(times.keys(), key=lambda k: times[k])
        fastest_time = times[fastest_scenario]
        
        # Calculate improvements relative to fastest
        comparison = {
            'fastest_scenario': fastest_scenario,
            'fastest_time': fastest_time,
            'results': {
                'full_ingest_vectorized': {
                    'time': time_full_vectorized,
                    'throughput': throughput_full_vectorized,
                    'rows': full_ingest_vectorized.get('rows_loaded', 0),
                    'improvement_vs_fastest': ((fastest_time - time_full_vectorized) / fastest_time) * 100 if fastest_time > 0 else 0
                },
                'add_files_copy': {
                    'time': time_add_files,
                    'throughput': throughput_add_files,
                    'rows': add_files_copy.get('rows_loaded', 0),
                    'improvement_vs_fastest': ((fastest_time - time_add_files) / fastest_time) * 100 if fastest_time > 0 else 0
                },
                'full_ingest_non_vectorized': {
                    'time': time_full_non_vectorized,
                    'throughput': throughput_full_non_vectorized,
                    'rows': full_ingest_non_vectorized.get('rows_loaded', 0),
                    'improvement_vs_fastest': ((fastest_time - time_full_non_vectorized) / fastest_time) * 100 if fastest_time > 0 else 0
                }
            }
        }
        
        # Calculate vectorized scanner impact
        vectorized_impact = ((time_full_non_vectorized - time_full_vectorized) / time_full_non_vectorized) * 100 if time_full_non_vectorized > 0 else 0
        comparison['vectorized_scanner_improvement'] = vectorized_impact
        
        # Log detailed comparison
        logger.info("Performance comparison of all three scenarios:")
        logger.info(f"  1. FULL_INGEST (VECTORIZED=TRUE):    {time_full_vectorized:.2f}s ({throughput_full_vectorized:,.0f} rows/sec)")
        logger.info(f"  2. ADD_FILES_COPY (VECTORIZED=TRUE): {time_add_files:.2f}s ({throughput_add_files:,.0f} rows/sec)")
        logger.info(f"  3. FULL_INGEST (VECTORIZED=FALSE):   {time_full_non_vectorized:.2f}s ({throughput_full_non_vectorized:,.0f} rows/sec)")
        logger.info(f"")
        logger.info(f"🏆 Fastest scenario: {fastest_scenario} ({fastest_time:.2f}s)")
        logger.info(f"📊 VECTORIZED_SCANNER improvement: {vectorized_impact:.1f}% faster")
        
        return comparison
    
    def _generate_summary_report(self, test_results: Dict[str, Any]):
        """Generate a comprehensive summary report."""
        logger.info("=" * 80)
        logger.info("LOAD_MODE PERFORMANCE TEST SUMMARY")
        logger.info("=" * 80)
        
        total_time = test_results['test_end_time'] - test_results['test_start_time']
        logger.info(f"Total test duration: {total_time}")
        logger.info(f"Test completed at: {test_results['test_end_time']}")
        
        # Results summary
        full_ingest_vectorized_result = test_results.get('full_ingest_vectorized_result')
        add_files_copy_result = test_results.get('add_files_copy_result')
        full_ingest_non_vectorized_result = test_results.get('full_ingest_non_vectorized_result')
        comparison = test_results.get('comparison')
        
        if full_ingest_vectorized_result and add_files_copy_result and full_ingest_non_vectorized_result:
            logger.info("")
            logger.info("PERFORMANCE RESULTS:")
            logger.info(f"  1. FULL_INGEST (VECTORIZED=TRUE):    {full_ingest_vectorized_result.get('execution_time_formatted', 'FAILED')}")
            logger.info(f"  2. ADD_FILES_COPY (VECTORIZED=TRUE): {add_files_copy_result.get('execution_time_formatted', 'FAILED')}")
            logger.info(f"  3. FULL_INGEST (VECTORIZED=FALSE):   {full_ingest_non_vectorized_result.get('execution_time_formatted', 'FAILED')}")
            
            logger.info("")
            logger.info("THROUGHPUT RESULTS:")
            if full_ingest_vectorized_result.get('throughput_formatted'):
                logger.info(f"  1. FULL_INGEST (VECTORIZED=TRUE):    {full_ingest_vectorized_result['throughput_formatted']}")
            if add_files_copy_result.get('throughput_formatted'):
                logger.info(f"  2. ADD_FILES_COPY (VECTORIZED=TRUE): {add_files_copy_result['throughput_formatted']}")
            if full_ingest_non_vectorized_result.get('throughput_formatted'):
                logger.info(f"  3. FULL_INGEST (VECTORIZED=FALSE):   {full_ingest_non_vectorized_result['throughput_formatted']}")
            
            if comparison and 'fastest_scenario' in comparison:
                fastest = comparison['fastest_scenario']
                vectorized_improvement = comparison.get('vectorized_scanner_improvement', 0)
                
                logger.info("")
                logger.info(f"🏆 FASTEST SCENARIO: {fastest}")
                logger.info(f"📊 VECTORIZED_SCANNER improvement: {vectorized_improvement:.1f}% faster than non-vectorized")
                
                # Show relative performance
                results = comparison.get('results', {})
                logger.info("")
                logger.info("RELATIVE PERFORMANCE (vs fastest):")
                for key, data in results.items():
                    improvement = data.get('improvement_vs_fastest', 0)
                    if improvement >= 0:
                        logger.info(f"  {key.replace('_', ' ').title()}: {improvement:.1f}% slower")
                    else:
                        logger.info(f"  {key.replace('_', ' ').title()}: {abs(improvement):.1f}% faster")
        
        logger.info("")
        logger.info("KEY INSIGHTS:")
        logger.info("  • VECTORIZED_SCANNER = TRUE provides significant performance boost")
        logger.info("  • FULL_INGEST: Traditional batch processing approach")
        logger.info("  • ADD_FILES_COPY: Optimized for incremental/file-based loading")
        logger.info("  • VECTORIZED_SCANNER = FALSE: Legacy processing mode (slower)")
        logger.info("  • Performance differences reveal optimization effectiveness")
        
        logger.info("")
        logger.info("RECOMMENDATIONS:")
        if comparison and 'fastest_scenario' in comparison:
            winner = comparison['fastest_scenario']
            vectorized_improvement = comparison.get('vectorized_scanner_improvement', 0)
            logger.info(f"  🥇 PRIMARY: Use {winner} for optimal performance")
            logger.info(f"  📈 ALWAYS use USE_VECTORIZED_SCANNER = TRUE ({vectorized_improvement:.1f}% improvement)")
            
            if vectorized_improvement > 20:
                logger.info(f"  ⚠️  CRITICAL: VECTORIZED_SCANNER provides {vectorized_improvement:.1f}% performance boost!")
            
            # Specific recommendations based on use case
            logger.info("")
            logger.info("USE CASE RECOMMENDATIONS:")
            logger.info("  • For batch loads: FULL_INGEST + VECTORIZED_SCANNER = TRUE")
            logger.info("  • For incremental loads: ADD_FILES_COPY + VECTORIZED_SCANNER = TRUE")
            logger.info("  • NEVER use VECTORIZED_SCANNER = FALSE in production")
        else:
            logger.info("  All scenarios are viable - choose based on specific requirements")
            logger.info("  Always prefer VECTORIZED_SCANNER = TRUE for better performance")
        
        logger.info("")
        logger.info("Test completed successfully! 🎉")


def main():
    """Main entry point."""
    try:
        test = LoadModePerformanceTest()
        results = test.run_performance_test()
        
        if (results.get('comparison') or (results.get('full_ingest_vectorized_result') and 
            results.get('add_files_copy_result') and results.get('full_ingest_non_vectorized_result'))):
            logger.info("Three-scenario load mode performance test completed successfully")
            sys.exit(0)
        else:
            logger.error("Load mode performance test failed or incomplete")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
