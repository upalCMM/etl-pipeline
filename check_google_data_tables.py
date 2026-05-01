#!/usr/bin/env python3
import psycopg2
from config import DB_CONFIG

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# List all tables in google_data schema
cursor.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'google_data'
    ORDER BY table_name
""")

tables = cursor.fetchall()
print("Tables in google_data schema:")
for table in tables:
    print(f"  - {table[0]}")

cursor.close()
conn.close()
