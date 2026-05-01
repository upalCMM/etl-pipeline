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

def fetch_all_organisations():
    """Fetch ALL organisations from Pipedrive"""
    print("🏢 FETCHING ALL ORGANISATIONS")
    print("=" * 50)
    
    all_organisations = []
    start = 0
    limit = 500
    has_more = True
    batch_count = 0
    
    while has_more:
        batch_count += 1
        
        url = f"{PIPEDRIVE_BASE_URL}/organizations?api_token={PIPEDRIVE_API_TOKEN}&start={start}&limit={limit}"
        
        try:
            response = requests.get(url, verify=False, timeout=30)
            data = response.json()
            
            if not data.get('success'):
                error_msg = data.get('error', 'Unknown error')
                print(f"API error in batch {batch_count}: {error_msg}")
                break
            
            organisations = data.get('data', [])
            all_organisations.extend(organisations)
            
            print(f"📥 Organisations Batch {batch_count}: Fetched {len(organisations)} (Total: {len(all_organisations)})")
            
            # Check pagination
            pagination = data.get('additional_data', {}).get('pagination', {})
            has_more = pagination.get('more_items_in_collection', False)
            next_start = pagination.get('next_start', 0)
            
            # Safety check to prevent infinite loops
            if next_start == start or batch_count > 100:
                print(f" Breaking pagination loop at batch {batch_count}")
                break
                
            start = next_start
            
        except Exception as e:
            print(f"Request failed in batch {batch_count}: {e}")
            break
    
    print(f"Total organisations fetched: {len(all_organisations):,}")
    return all_organisations

def fetch_all_deals():
    """Fetch ALL deals from Pipedrive"""
    print("🤝 FETCHING ALL DEALS")
    print("=" * 40)
    
    all_deals = []
    start = 0
    limit = 500
    has_more = True
    batch_count = 0
    
    while has_more:
        batch_count += 1
        
        url = f"{PIPEDRIVE_BASE_URL}/deals?api_token={PIPEDRIVE_API_TOKEN}&start={start}&limit={limit}"
        
        try:
            response = requests.get(url, verify=False, timeout=30)
            data = response.json()
            
            if not data.get('success'):
                error_msg = data.get('error', 'Unknown error')
                print(f"API error in batch {batch_count}: {error_msg}")
                break
            
            deals = data.get('data', [])
            all_deals.extend(deals)
            
            print(f"📥 Deals Batch {batch_count}: Fetched {len(deals)} (Total: {len(all_deals)})")
            
            # Check pagination
            pagination = data.get('additional_data', {}).get('pagination', {})
            has_more = pagination.get('more_items_in_collection', False)
            next_start = pagination.get('next_start', 0)
            
            # Safety check to prevent infinite loops
            if next_start == start or batch_count > 100:
                print(f" Breaking pagination loop at batch {batch_count}")
                break
                
            start = next_start
            
        except Exception as e:
            print(f"Request failed in batch {batch_count}: {e}")
            break
    
    print(f"Total deals fetched: {len(all_deals):,}")
    return all_deals

def extract_organisation_data(organisation):
    """Extract organisation data for dim_organisation table"""
    return {
        'org_id': organisation.get('id'),
        'org_name': organisation.get('name', '').strip() or 'Unknown Organisation'
    }

def extract_deal_data(deal):
    """Extract deal data for dim_deal table"""
    return {
        'deal_id': deal.get('id'),
        'deal_name': deal.get('title', '').strip() or 'Unknown Deal'
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
            print(f"Error checking {table_name} schema: {e}")
            return []

def sync_organisations_insert_only():
    """Sync organisations - ONLY insert new records, NEVER update existing ones"""
    print("\n🏢 SYNCING ORGANISATIONS (INSERT-ONLY MODE)")
    print("=" * 55)
    
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(connection_string)
    
    # Check table exists
    existing_columns = check_dim_table_schema(engine, 'dim_organisation')
    
    if not existing_columns or 'org_id' not in existing_columns:
        print("dim_organisation table not found or missing 'org_id' column.")
        return False
    
    # Fetch ALL organisations from Pipedrive
    organisations = fetch_all_organisations()
    
    if not organisations:
        print("No organisations found in Pipedrive")
        return False
    
    with engine.connect() as conn:
        try:
            # Get current count before sync
            count_before = conn.execute(text("SELECT COUNT(*) FROM dw.dim_organisation")).scalar()
            print(f"Organisations in database before sync: {count_before:,}")
            print(f"Organisations to process from Pipedrive: {len(organisations):,}")
            
            print("🔄 Starting ORGANISATIONS INSERT-ONLY sync...")
            print("   - Only NEW organisations will be added")
            print("   - Existing organisations will remain UNCHANGED")
            
            inserted_count = 0
            skipped_count = 0
            error_count = 0
            error_ids = []
            
            # Process in batches
            batch_size = 1000
            total_batches = (len(organisations) + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(organisations))
                batch_organisations = organisations[start_idx:end_idx]
                
                print(f"   Processing batch {batch_num + 1}/{total_batches} ({len(batch_organisations)} organisations)...")
                
                batch_inserted = 0
                batch_skipped = 0
                batch_errors = 0
                
                for org in batch_organisations:
                    try:
                        org_data = extract_organisation_data(org)
                        
                        # Check if organisation already exists
                        existing_record = conn.execute(
                            text("SELECT org_id FROM dw.dim_organisation WHERE org_id = :org_id"),
                            {'org_id': org_data['org_id']}
                        ).fetchone()
                        
                        if existing_record:
                            batch_skipped += 1
                        else:
                            # Insert new organisation
                            conn.execute(text("""
                                INSERT INTO dw.dim_organisation (org_id, org_name, sync_timestamp)
                                VALUES (:org_id, :org_name, NOW())
                                ON CONFLICT (org_id) DO NOTHING
                            """), org_data)
                            batch_inserted += 1
                            
                    except Exception as e:
                        print(f" Error processing organisation {org.get('id')}: {e}")
                        batch_errors += 1
                        error_ids.append(org.get('id'))
                        continue
                
                # Commit after each batch
                conn.commit()
                
                inserted_count += batch_inserted
                skipped_count += batch_skipped
                error_count += batch_errors
                
                print(f"     Batch {batch_num + 1}: +{batch_inserted} new, {batch_skipped} existing, {batch_errors} errors")
                
                # Stop if too many errors
                if error_count > 20:
                    print("Too many errors, stopping sync")
                    break
            
            # Get final count
            count_after = conn.execute(text("SELECT COUNT(*) FROM dw.dim_organisation")).scalar()
            
            print(f"\n✅ ORGANISATIONS SYNC COMPLETED:")
            print(f"   - New organisations inserted: {inserted_count:,}")
            print(f"   - Existing organisations skipped: {skipped_count:,}")
            print(f"   - Errors: {error_count:,}")
            print(f"   - Total in database: {count_after:,} (was {count_before:,})")
            
            if error_ids:
                print(f"Failed organisation IDs (first 10): {error_ids[:10]}")
            
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"Organisations sync failed: {e}")
            import traceback
            print(traceback.format_exc())
            return False

def sync_deals_insert_only():
    """Sync deals - ONLY insert new records, NEVER update existing ones"""
    print("\n🤝 SYNCING DEALS (INSERT-ONLY MODE)")
    print("=" * 50)
    
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(connection_string)
    
    # Check table exists
    existing_columns = check_dim_table_schema(engine, 'dim_deal')
    
    if not existing_columns or 'deal_id' not in existing_columns:
        print("dim_deal table not found or missing 'deal_id' column.")
        return False
    
    # Fetch ALL deals from Pipedrive
    deals = fetch_all_deals()
    
    if not deals:
        print("No deals found in Pipedrive")
        return False
    
    with engine.connect() as conn:
        try:
            # Get current count before sync
            count_before = conn.execute(text("SELECT COUNT(*) FROM dw.dim_deal")).scalar()
            print(f"Deals in database before sync: {count_before:,}")
            print(f"Deals to process from Pipedrive: {len(deals):,}")
            
            print("🔄 Starting DEALS INSERT-ONLY sync...")
            print("   - Only NEW deals will be added")
            print("   - Existing deals will remain UNCHANGED")
            
            inserted_count = 0
            skipped_count = 0
            error_count = 0
            error_ids = []
            
            # Process in batches
            batch_size = 1000
            total_batches = (len(deals) + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(deals))
                batch_deals = deals[start_idx:end_idx]
                
                print(f"   Processing batch {batch_num + 1}/{total_batches} ({len(batch_deals)} deals)...")
                
                batch_inserted = 0
                batch_skipped = 0
                batch_errors = 0
                
                for deal in batch_deals:
                    try:
                        deal_data = extract_deal_data(deal)
                        
                        # Check if deal already exists
                        existing_record = conn.execute(
                            text("SELECT deal_id FROM dw.dim_deal WHERE deal_id = :deal_id"),
                            {'deal_id': deal_data['deal_id']}
                        ).fetchone()
                        
                        if existing_record:
                            batch_skipped += 1
                        else:
                            # Insert new deal
                            conn.execute(text("""
                                INSERT INTO dw.dim_deal (deal_id, deal_name, sync_timestamp)
                                VALUES (:deal_id, :deal_name, NOW())
                                ON CONFLICT (deal_id) DO NOTHING
                            """), deal_data)
                            batch_inserted += 1
                            
                    except Exception as e:
                        print(f" Error processing deal {deal.get('id')}: {e}")
                        batch_errors += 1
                        error_ids.append(deal.get('id'))
                        continue
                
                # Commit after each batch
                conn.commit()
                
                inserted_count += batch_inserted
                skipped_count += batch_skipped
                error_count += batch_errors
                
                print(f"     Batch {batch_num + 1}: +{batch_inserted} new, {batch_skipped} existing, {batch_errors} errors")
                
                # Stop if too many errors
                if error_count > 20:
                    print("Too many errors, stopping sync")
                    break
            
            # Get final count
            count_after = conn.execute(text("SELECT COUNT(*) FROM dw.dim_deal")).scalar()
            
            print(f"\n✅ DEALS SYNC COMPLETED:")
            print(f"   - New deals inserted: {inserted_count:,}")
            print(f"   - Existing deals skipped: {skipped_count:,}")
            print(f"   - Errors: {error_count:,}")
            print(f"   - Total in database: {count_after:,} (was {count_before:,})")
            
            if error_ids:
                print(f"Failed deal IDs (first 10): {error_ids[:10]}")
            
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"Deals sync failed: {e}")
            import traceback
            print(traceback.format_exc())
            return False

def main():
    """Main function to sync both organisations and deals"""
    print("STARTING DIMENSION TABLES SYNC")
    print("=" * 50)
    
    # Sync organisations first
    org_success = sync_organisations_insert_only()
    
    # Then sync deals
    deal_success = sync_deals_insert_only()
    
    # Final summary
    print("\n" + "=" * 60)
    print("🎯 SYNC SUMMARY")
    print("=" * 60)
    
    if org_success:
        print("Organisations: SYNC SUCCESSFUL")
    else:
        print("Organisations: SYNC FAILED")
    
    if deal_success:
        print("Deals: SYNC SUCCESSFUL")
    else:
        print("Deals: SYNC FAILED")
    
    if org_success and deal_success:
        print("\n💡 ALL DIMENSION TABLES SYNCED SUCCESSFULLY!")
        return True
    else:
        print("\n💥 SOME SYNC OPERATIONS FAILED!")
        return False

if __name__ == "__main__":
    if main():
        print("\n🎉 DIMENSION TABLES READY FOR USE!")
    else:
        print("💥 SYNC FAILED!")
        sys.exit(1)
