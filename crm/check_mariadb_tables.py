#!/usr/bin/env python3
"""
Check MariaDB tables have data
"""
import os
import sys
sys.path.insert(0, '..')
from dotenv import load_dotenv
load_dotenv('../.env')

import mysql.connector

mariadb_config = {
    'host': os.getenv('MARIADB_HOST'),
    'port': int(os.getenv('MARIADB_PORT')),
    'database': os.getenv('MARIADB_DATABASE'),
    'user': os.getenv('MARIADB_USER'),
    'password': os.getenv('MARIADB_PASSWORD')
}

try:
    conn = mysql.connector.connect(**mariadb_config)
    cursor = conn.cursor()
    
    print("✅ Connected to DDEV MariaDB")
    
    # Check removal_companies
    cursor.execute("SELECT COUNT(*) FROM removal_companies")
    companies_count = cursor.fetchone()[0]
    print(f"📊 removal_companies: {companies_count} rows")
    
    if companies_count > 0:
        cursor.execute("SELECT id, company_name, status FROM removal_companies LIMIT 5")
        print("Sample companies:")
        for row in cursor.fetchall():
            print(f"  ID: {row[0]}, Name: {row[1]}, Status: {row[2]}")
    
    # Check removal_company_logs
    cursor.execute("SELECT COUNT(*) FROM removal_company_logs")
    logs_count = cursor.fetchone()[0]
    print(f"\n📊 removal_company_logs: {logs_count} rows")
    
    if logs_count > 0:
        cursor.execute("SELECT id, company_id, action, created_at FROM removal_company_logs LIMIT 5")
        print("Sample logs:")
        for row in cursor.fetchall():
            print(f"  ID: {row[0]}, Company ID: {row[1]}, Action: {row[2]}, Date: {row[3]}")
    
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {e}")
