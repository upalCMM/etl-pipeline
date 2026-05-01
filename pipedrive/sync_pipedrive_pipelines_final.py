#!/usr/bin/env python3
"""
ETL Script - Auto-fixed version
"""
import os
import sys

# AUTO-FIX: Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# ===== UNICODE FIX FOR WINDOWS =====
import sys
import io
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
# ==================================================

import requests
from sqlalchemy import create_engine, text
import logging
import urllib3
import sys
from datetime import datetime, timedelta

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from config import PIPEDRIVE_API_TOKEN, PIPEDRIVE_BASE_URL, DB_CONFIG

def fetch_all_pipelines():
    """Fetch ALL pipelines from Pipedrive using the exact endpoint from your M query"""
    print("🔄 FETCHING ALL PIPELINES")
    print("=" * 50)
    
    all_pipelines = []
    
    # Using the exact same endpoint structure as your M query
    url = f"https://comparemymove.pipedrive.com/api/v1/pipelines?api_token={PIPEDRIVE_API_TOKEN}"
    
    try:
        response = requests.get(url, verify=False, timeout=30)
        data = response.json()
        
        if not data.get('success'):
            error_msg = data.get('error', 'Unknown error')
            print(f"❌ API error: {error_msg}")
            return []
        
        pipelines = data.get('data', [])
        all_pipelines.extend(pipelines)
        
        print(f"📥 Pipelines Fetched: {len(pipelines)}")
        
        # Show sample of what we got
        if pipelines:
            print("Sample pipeline data:")
            for i, pipeline in enumerate(pipelines[:3]):
                print(f"  {i+1}. ID: {pipeline.get('id')}, Name: {pipeline.get('name')}, Active: {pipeline.get('active')}")
        
        return all_pipelines
        
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return []

def extract_pipeline_data(pipeline):
    """Extract pipeline data matching ALL fields from your M query"""
    return {
        'src_pipeline_id': pipeline.get('id'),
        'pipeline_name': pipeline.get('name', '').strip() or 'Unknown Pipeline',
        'pipeline_active': pipeline.get('active', False),
        'pipeline_url': pipeline.get('url_title', '')
    }

def check_dim_table_schema(engine, table_name):
    """Check the schema of dimension tables"""
    print(f"🔍 Checking {table_name} schema...")
    with engine.connect() as conn:
        try:
            result = conn.execute(text(f"""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}' 
                AND table_schema = 'dw'
                ORDER BY ordinal_position
            """))
            
            columns = result.fetchall()
            print(f"📋 {table_name} columns:")
            for col in columns:
                print(f"   - {col[0]} ({col[1]}, nullable: {col[2]})")
            
            return [col[0] for col in columns]
            
        except Exception as e:
            print(f"❌ Error checking {table_name} schema: {e}")
            return []

def create_or_alter_pipelines_table(engine):
    """Create or alter the dim_pipelines table to match the required structure"""
    print("🔄 ENSURING TABLE STRUCTURE MATCHES PIPEDRIVE DATA...")
    
    with engine.connect() as conn:
        try:
            # Check if table exists
            table_exists = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'dw' 
                    AND table_name = 'dim_pipelines'
                )
            """)).scalar()
            
            if not table_exists:
                print("📦 Creating new dim_pipelines table...")
                conn.execute(text("""
                    CREATE TABLE dw.dim_pipelines (
                        pipeline_sk SERIAL PRIMARY KEY,
                        src_pipeline_id INTEGER UNIQUE NOT NULL,
                        pipeline_name VARCHAR(255) NOT NULL,
                        pipeline_active BOOLEAN DEFAULT FALSE,
                        pipeline_url VARCHAR(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                print("✅ Created new dim_pipelines table")
            else:
                print("📦 Table exists, checking columns...")
                
            # Ensure all required columns exist (excluding pipeline_sk since it's identity)
            required_columns = {
                'src_pipeline_id': 'ADD COLUMN src_pipeline_id INTEGER UNIQUE NOT NULL',
                'pipeline_name': 'ADD COLUMN pipeline_name VARCHAR(255) NOT NULL',
                'pipeline_active': 'ADD COLUMN pipeline_active BOOLEAN DEFAULT FALSE',
                'pipeline_url': 'ADD COLUMN pipeline_url VARCHAR(255)',
                'created_at': 'ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                'updated_at': 'ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
            }
            
            existing_columns = check_dim_table_schema(engine, 'dim_pipelines')
            
            for column, alter_stmt in required_columns.items():
                if column not in existing_columns:
                    print(f"   ➕ Adding missing column: {column}")
                    try:
                        conn.execute(text(f"ALTER TABLE dw.dim_pipelines {alter_stmt}"))
                        print(f"   ✅ Added column: {column}")
                    except Exception as e:
                        print(f"   ⚠️ Could not add {column}: {e}")
            
            conn.commit()
            return True
            
        except Exception as e:
            print(f"❌ Error ensuring table structure: {e}")
            conn.rollback()
            return False

def sync_pipelines_upsert():
    """Sync pipelines using UPSERT - let PostgreSQL handle the pipeline_sk generation"""
    print("\n🔄 SYNCING PIPELINES (UPSERT MODE)")
    print("=" * 55)
    
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(connection_string)
    
    # Ensure table has correct structure
    if not create_or_alter_pipelines_table(engine):
        return False
    
    # Fetch ALL pipelines from Pipedrive
    pipelines = fetch_all_pipelines()
    
    if not pipelines:
        print("❌ No pipelines found in Pipedrive")
        return False
    
    with engine.connect() as conn:
        try:
            # Get current count before sync
            count_before = conn.execute(text("SELECT COUNT(*) FROM dw.dim_pipelines")).scalar()
            print(f"📊 Pipelines in database before sync: {count_before:,}")
            print(f"📊 Pipelines to process from Pipedrive: {len(pipelines):,}")
            
            print("🔄 Starting PIPELINES UPSERT sync...")
            print("   - NEW pipelines will be inserted (pipeline_sk auto-generated)")
            print("   - EXISTING pipelines will be updated with current data")
            
            inserted_count = 0
            updated_count = 0
            unchanged_count = 0
            error_count = 0
            error_ids = []
            
            # Process each pipeline
            for i, pipeline in enumerate(pipelines, 1):
                try:
                    pipeline_data = extract_pipeline_data(pipeline)
                    src_pipeline_id = pipeline_data['src_pipeline_id']
                    
                    # Check if pipeline already exists
                    existing_record = conn.execute(
                        text("""
                            SELECT pipeline_sk, pipeline_name, pipeline_active, pipeline_url 
                            FROM dw.dim_pipelines 
                            WHERE src_pipeline_id = :src_id
                        """),
                        {'src_id': src_pipeline_id}
                    ).fetchone()
                    
                    if existing_record:
                        # UPDATE existing record if any data changed
                        existing_sk, existing_name, existing_active, existing_url = existing_record
                        current_data = (pipeline_data['pipeline_name'], pipeline_data['pipeline_active'], pipeline_data['pipeline_url'])
                        existing_data = (existing_name, existing_active, existing_url)
                        
                        if current_data != existing_data:
                            # Update the pipeline data
                            conn.execute(text("""
                                UPDATE dw.dim_pipelines 
                                SET pipeline_name = :pipeline_name,
                                    pipeline_active = :pipeline_active,
                                    pipeline_url = :pipeline_url,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE src_pipeline_id = :src_pipeline_id
                            """), pipeline_data)
                            updated_count += 1
                            print(f"     🔄 Updated: {pipeline_data['pipeline_name']} (ID: {src_pipeline_id})")
                        else:
                            unchanged_count += 1
                    else:
                        # INSERT new pipeline (let PostgreSQL auto-generate pipeline_sk)
                        conn.execute(text("""
                            INSERT INTO dw.dim_pipelines 
                            (src_pipeline_id, pipeline_name, pipeline_active, pipeline_url)
                            VALUES (:src_pipeline_id, :pipeline_name, :pipeline_active, :pipeline_url)
                        """), pipeline_data)
                        inserted_count += 1
                        
                        # Get the auto-generated pipeline_sk
                        result = conn.execute(
                            text("SELECT pipeline_sk FROM dw.dim_pipelines WHERE src_pipeline_id = :src_id"),
                            {'src_id': src_pipeline_id}
                        )
                        new_sk = result.scalar()
                        print(f"     ✅ Inserted: {pipeline_data['pipeline_name']} (SK: {new_sk}, ID: {src_pipeline_id})")
                        
                except Exception as e:
                    print(f"❌ Error processing pipeline {pipeline.get('id')}: {e}")
                    error_count += 1
                    error_ids.append(pipeline.get('id'))
                    # Continue with next pipeline instead of stopping
                    continue
            
            # Commit all changes
            conn.commit()
            
            # Get final count
            count_after = conn.execute(text("SELECT COUNT(*) FROM dw.dim_pipelines")).scalar()
            
            print(f"\n✅ PIPELINES SYNC COMPLETED:")
            print(f"   ✅ New pipelines inserted: {inserted_count:,}")
            print(f"   🔄 Existing pipelines updated: {updated_count:,}")
            print(f"   ⏸️  Unchanged pipelines: {unchanged_count:,}")
            print(f"   ❌ Errors: {error_count:,}")
            print(f"   📊 Total in database: {count_after:,} (was {count_before:,})")
            
            if error_ids:
                print(f"Failed pipeline IDs (first 10): {error_ids[:10]}")
            
            # Show final table state
            print(f"\n📊 FINAL PIPELINES TABLE:")
            result = conn.execute(text("""
                SELECT pipeline_sk, src_pipeline_id, pipeline_name, pipeline_active, pipeline_url
                FROM dw.dim_pipelines 
                ORDER BY pipeline_sk
            """))
            
            for row in result:
                status = "ACTIVE" if row[3] else "INACTIVE"
                print(f"   SK {row[0]}: ID {row[1]} - {row[2]} [{status}]")
            
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Pipelines sync failed: {e}")
            import traceback
            print(traceback.format_exc())
            return False

def main():
    """Main function to sync pipelines"""
    print("STARTING PIPELINES SYNC TO dw.dim_pipelines")
    print("=" * 50)
    print("Table Structure: pipeline_sk | src_pipeline_id | pipeline_name | pipeline_active | pipeline_url")
    print("=" * 50)
    
    # Sync pipelines
    pipeline_success = sync_pipelines_upsert()
    
    # Final summary
    print("\n" + "=" * 60)
    print("🎯 PIPELINE SYNC SUMMARY")
    print("=" * 60)
    
    if pipeline_success:
        print("✅ PIPELINES SYNC SUCCESSFUL!")
        print("\n💡 Your dw.dim_pipelines table is now updated with:")
        print("   - All fields from Pipedrive API (id, name, active, url_title)")
        print("   - New pipelines inserted with auto-generated pipeline_sk")
        print("   - Existing pipelines updated with current data")
    else:
        print("💥 PIPELINES SYNC FAILED!")
    
    return pipeline_success

if __name__ == "__main__":
    if main():
        print("\n🎉 PIPELINES TABLE SYNC COMPLETED!")
    else:
        print("💥 PIPELINE SYNC FAILED!")
        sys.exit(1)
