#!/usr/bin/env python3
"""
Explore the Snowflake table structure to find the correct column names.
"""

import snowflake.connector
import sys

def explore_table_structure():
    """Explore the table structure and find ticker column."""
    print("=" * 60)
    print("EXPLORING SNOWFLAKE TABLE STRUCTURE")
    print("=" * 60)
    
    try:
        # Connect to Snowflake
        print("1. Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            connection_name='DEMO_PRAJAGOPAL'
        )
        cursor = conn.cursor()
        print("   ✓ Connection successful")
        
        # Check if table exists and get its structure
        print("\n2. Checking table existence and structure...")
        
        # First, let's see what tables exist in the schema
        print("   Available tables in DEMODB.EQUITY_RESEARCH schema:")
        cursor.execute("SHOW TABLES IN SCHEMA DEMODB.EQUITY_RESEARCH")
        tables = cursor.fetchall()
        for table in tables:
            print(f"      - {table[1]}")  # table name is usually in the second column
        
        # Now let's describe the specific table
        print(f"\n3. Describing SP_SECTOR_COMPANIES table structure...")
        cursor.execute("DESCRIBE TABLE DEMODB.EQUITY_RESEARCH.SP_SECTOR_COMPANIES")
        columns = cursor.fetchall()
        
        print("   Column structure:")
        for col in columns:
            print(f"      - {col[0]} ({col[1]})")  # column name and type
        
        # Let's try to get a sample of data to see what's available
        print(f"\n4. Sample data from the table (first 5 rows)...")
        cursor.execute("SELECT * FROM DEMODB.EQUITY_RESEARCH.SP_SECTOR_COMPANIES LIMIT 5")
        sample_data = cursor.fetchall()
        
        if sample_data:
            print("   Sample rows:")
            for i, row in enumerate(sample_data, 1):
                print(f"      Row {i}: {row}")
        else:
            print("   No data found in table")
        
        # Get total row count
        print(f"\n5. Getting total row count...")
        cursor.execute("SELECT COUNT(*) FROM DEMODB.EQUITY_RESEARCH.SP_SECTOR_COMPANIES")
        total_count = cursor.fetchone()[0]
        print(f"   ✓ Total rows: {total_count}")
        
        cursor.close()
        conn.close()
        
        print(f"\n{'='*60}")
        print("✅ TABLE EXPLORATION COMPLETE")
        print(f"{'='*60}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ TABLE EXPLORATION FAILED")
        print(f"Error: {e}")
        return False

def main():
    """Main exploration function."""
    success = explore_table_structure()
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())

