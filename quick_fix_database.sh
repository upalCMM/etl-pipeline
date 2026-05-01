#!/bin/bash
# Quick fix for database connection

echo "Quick Database Fix"
echo "=================="

# Check current config
echo "Current database in config.py:"
python3 -c "import config; print(f\"Database: {config.DB_CONFIG.get('database')}\")"

echo ""
echo "Updating config.py to use 'cmm_pipedrive' database..."

# Read current config.py
CONFIG_CONTENT=$(cat config.py)

# Check if database is already cmm_pipedrive
if echo "$CONFIG_CONTENT" | grep -q "'database': 'cmm_pipedrive'"; then
    echo "✅ Database already set to 'cmm_pipedrive'"
else
    # Update database name
    sed -i "s/'database': 'postgres'/'database': 'cmm_pipedrive'/g" config.py
    sed -i "s/'database': '.*'/'database': 'cmm_pipedrive'/g" config.py
    echo "✅ Updated database to 'cmm_pipedrive'"
fi

echo ""
echo "Testing fixed connection..."
python3 -c "
import psycopg2
import config

try:
    conn = psycopg2.connect(**config.DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute('SELECT current_database();')
    db = cursor.fetchone()[0]
    print(f'✅ Now connected to: {db}')
    
    cursor.execute(\"\"\"
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT LIKE 'pg_%' 
        ORDER BY schema_name;
    \"\"\")
    schemas = cursor.fetchall()
    print(f'Schemas in {db}:')
    for schema in schemas:
        print(f'  - {schema[0]}')
    
except Exception as e:
    print(f'❌ Still failing: {e}')
"
