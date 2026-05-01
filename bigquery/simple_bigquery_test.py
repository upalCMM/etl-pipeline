#!/usr/bin/env python3
"""
Simple BigQuery test to verify everything works
"""
import os
import sys
sys.path.insert(0, '..')

from google.cloud import bigquery
import config

print("🧪 Simple BigQuery Test")
print("=" * 60)

# Set credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.BIGQUERY_CONFIG['credentials_path']

try:
    # Initialize client
    client = bigquery.Client()
    print(f"✅ Connected to BigQuery project: {client.project}")
    
    # Check dataset
    dataset_id = config.BIGQUERY_CONFIG['dataset_id']
    print(f"📁 Target dataset: {dataset_id}")
    
    # List tables
    print("\n📋 Available tables (first 10):")
    tables = list(client.list_tables(dataset_id, max_results=10))
    
    for i, table in enumerate(tables, 1):
        print(f"  {i:2}. {table.table_id}")
    
    # If we have tables, try a simple query
    if tables:
        print("\n🧪 Testing query on first table...")
        first_table = tables[0].table_id
        
        # Build query
        query = f"""
        SELECT 
            COUNT(*) as row_count,
            MIN(_PARTITIONTIME) as earliest_partition,
            MAX(_PARTITIONTIME) as latest_partition
        FROM `{client.project}.{dataset_id}.{first_table}`
        LIMIT 1
        """
        
        try:
            result = client.query(query).result()
            for row in result:
                print(f"✅ {first_table}: {row.row_count:,} rows")
                if row.earliest_partition:
                    print(f"   Partitions from: {row.earliest_partition} to {row.latest_partition}")
        except Exception as query_error:
            print(f"⚠️  Couldn't query {first_table}: {query_error}")
            
            # Try simpler query
            simple_query = f"SELECT * FROM `{client.project}.{dataset_id}.{first_table}` LIMIT 1"
            try:
                simple_result = client.query(simple_query).result()
                print(f"✅ Can query {first_table} (simple query works)")
            except:
                print(f"❌ Cannot query {first_table}")
    
    print("\n" + "=" * 60)
    print("💡 Next steps:")
    print("1. Update scripts with correct table names")
    print("2. Test each script with LIMIT 10 first")
    print("3. Schedule working scripts")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
