#!/usr/bin/env python3
"""
Snowflake LOAD_MODE Performance Test
Tests COPY INTO command performance comparing FULL_INGEST vs ADD_FILES_COPY
Both tests use USE_VECTORIZED_SCANNER = TRUE for optimal performance.
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
    Class to test Snowflake COPY INTO performance comparing LOAD_MODE options.
    Tests FULL_INGEST vs ADD_FILES_COPY with USE_VECTORIZED_SCANNER = TRUE.
    """
    
    def __init__(self):
        """Initialize the test."""
        self.conn = None
        self.cursor = None
        
        # COPY INTO command for LOAD_MODE = FULL_INGEST
        self.copy_command_full_ingest = """
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
        logger.info("Comparing LOAD_MODE performance with USE_VECTORIZED_SCANNER = TRUE:")
        logger.info("  FULL_INGEST: Traditional full table processing")
        logger.info("  ADD_FILES_COPY: Optimized incremental file processing")
        
        test_results = {
            'test_start_time': datetime.now(),
            'full_ingest_result': None,
            'add_files_copy_result': None,
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
            
            # Step 3: Test with LOAD_MODE = FULL_INGEST
            logger.info("Step 3: Testing with LOAD_MODE = FULL_INGEST...")
            
            # Truncate table for clean test
            if not self._truncate_table():
                logger.error("Failed to truncate table")
                return test_results
            
            # Execute COPY with FULL_INGEST
            full_ingest_result = self._execute_copy_command('FULL_INGEST', self.copy_command_full_ingest)
            test_results['full_ingest_result'] = full_ingest_result
            
            # Step 4: Test with LOAD_MODE = ADD_FILES_COPY
            logger.info("Step 4: Testing with LOAD_MODE = ADD_FILES_COPY...")
            
            # Truncate table for clean test
            if not self._truncate_table():
                logger.error("Failed to truncate table for second test")
                return test_results
            
            # Execute COPY with ADD_FILES_COPY
            add_files_copy_result = self._execute_copy_command('ADD_FILES_COPY', self.copy_command_add_files_copy)
            test_results['add_files_copy_result'] = add_files_copy_result
            
            # Step 5: Compare results
            logger.info("Step 5: Comparing results...")
            if full_ingest_result['success'] and add_files_copy_result['success']:
                comparison = self._compare_results(full_ingest_result, add_files_copy_result)
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
    
    def _compare_results(self, full_ingest_result: Dict[str, Any], add_files_copy_result: Dict[str, Any]) -> Dict[str, Any]:
        """Compare the performance results between LOAD_MODE options."""
        full_ingest_time = full_ingest_result['execution_time_seconds']
        add_files_copy_time = add_files_copy_result['execution_time_seconds']
        
        if full_ingest_time > 0 and add_files_copy_time > 0:
            time_difference = full_ingest_time - add_files_copy_time
            percent_improvement = ((full_ingest_time - add_files_copy_time) / full_ingest_time) * 100
            
            # Throughput comparison
            full_ingest_throughput = full_ingest_result.get('throughput_rows_per_second', 0)
            add_files_copy_throughput = add_files_copy_result.get('throughput_rows_per_second', 0)
            throughput_improvement = ((add_files_copy_throughput - full_ingest_throughput) / full_ingest_throughput) * 100 if full_ingest_throughput > 0 else 0
            
            comparison = {
                'full_ingest_time': full_ingest_time,
                'add_files_copy_time': add_files_copy_time,
                'time_difference_seconds': time_difference,
                'percent_improvement': percent_improvement,
                'faster_mode': 'ADD_FILES_COPY' if add_files_copy_time < full_ingest_time else 'FULL_INGEST',
                'full_ingest_throughput': full_ingest_throughput,
                'add_files_copy_throughput': add_files_copy_throughput,
                'throughput_improvement_percent': throughput_improvement,
                'full_ingest_rows': full_ingest_result.get('rows_loaded', 0),
                'add_files_copy_rows': add_files_copy_result.get('rows_loaded', 0)
            }
            
            logger.info(f"Performance comparison:")
            logger.info(f"  FULL_INGEST:    {full_ingest_time:.2f} seconds ({full_ingest_throughput:,.0f} rows/sec)")
            logger.info(f"  ADD_FILES_COPY: {add_files_copy_time:.2f} seconds ({add_files_copy_throughput:,.0f} rows/sec)")
            logger.info(f"  Time difference: {time_difference:.2f} seconds")
            logger.info(f"  Performance improvement: {percent_improvement:.1f}%")
            logger.info(f"  Throughput improvement: {throughput_improvement:.1f}%")
            logger.info(f"  Faster mode: LOAD_MODE = {comparison['faster_mode']}")
            
            return comparison
        else:
            logger.error("Cannot compare results due to execution failures")
            return {'error': 'Cannot compare due to execution failures'}
    
    def _generate_summary_report(self, test_results: Dict[str, Any]):
        """Generate a comprehensive summary report."""
        logger.info("=" * 80)
        logger.info("LOAD_MODE PERFORMANCE TEST SUMMARY")
        logger.info("=" * 80)
        
        total_time = test_results['test_end_time'] - test_results['test_start_time']
        logger.info(f"Total test duration: {total_time}")
        logger.info(f"Test completed at: {test_results['test_end_time']}")
        
        # Results summary
        full_ingest_result = test_results.get('full_ingest_result')
        add_files_copy_result = test_results.get('add_files_copy_result')
        comparison = test_results.get('comparison')
        
        if full_ingest_result and add_files_copy_result:
            logger.info("")
            logger.info("PERFORMANCE RESULTS:")
            logger.info(f"  LOAD_MODE = FULL_INGEST:    {full_ingest_result.get('execution_time_formatted', 'FAILED')}")
            logger.info(f"  LOAD_MODE = ADD_FILES_COPY: {add_files_copy_result.get('execution_time_formatted', 'FAILED')}")
            
            if full_ingest_result.get('throughput_formatted'):
                logger.info(f"  FULL_INGEST throughput:     {full_ingest_result['throughput_formatted']}")
            if add_files_copy_result.get('throughput_formatted'):
                logger.info(f"  ADD_FILES_COPY throughput:  {add_files_copy_result['throughput_formatted']}")
            
            if comparison and 'percent_improvement' in comparison:
                improvement = comparison['percent_improvement']
                throughput_improvement = comparison.get('throughput_improvement_percent', 0)
                faster = comparison['faster_mode']
                
                logger.info("")
                if improvement > 0:
                    logger.info(f"  🚀 ADD_FILES_COPY is {improvement:.1f}% faster!")
                    logger.info(f"  📈 Throughput improvement: {throughput_improvement:.1f}%")
                elif improvement < 0:
                    logger.info(f"  📊 FULL_INGEST is {abs(improvement):.1f}% faster!")
                    logger.info(f"  📈 Throughput difference: {abs(throughput_improvement):.1f}%")
                else:
                    logger.info(f"  ⚖️  Both modes performed equally")
                
                logger.info(f"  🏆 Winner: LOAD_MODE = {faster}")
        
        logger.info("")
        logger.info("KEY INSIGHTS:")
        logger.info("  • Both modes use USE_VECTORIZED_SCANNER = TRUE")
        logger.info("  • FULL_INGEST: Traditional batch processing approach")
        logger.info("  • ADD_FILES_COPY: Optimized for incremental/file-based loading")
        logger.info("  • Performance difference indicates optimization effectiveness")
        
        logger.info("")
        logger.info("RECOMMENDATION:")
        if comparison and 'faster_mode' in comparison:
            winner = comparison['faster_mode']
            improvement = comparison.get('percent_improvement', 0)
            logger.info(f"  Use LOAD_MODE = {winner} for optimal performance")
            if abs(improvement) > 5:  # Significant difference
                logger.info(f"  Provides {abs(improvement):.1f}% performance improvement")
            else:
                logger.info("  Performance difference is minimal - choose based on use case")
        else:
            logger.info("  Both modes are viable - choose based on specific requirements")
        
        logger.info("")
        logger.info("Test completed successfully! 🎉")


def main():
    """Main entry point."""
    try:
        test = LoadModePerformanceTest()
        results = test.run_performance_test()
        
        if results.get('comparison') or (results.get('full_ingest_result') and results.get('add_files_copy_result')):
            logger.info("Load mode performance test completed successfully")
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
