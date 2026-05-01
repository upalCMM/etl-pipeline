#!/usr/bin/env python3
"""
Update config.py with BIGQUERY_CONFIG
"""
import os

config_path = 'config.py'

# Read current config
with open(config_path, 'r') as f:
    content = f.read()

# Check if BIGQUERY_CONFIG already exists
if 'BIGQUERY_CONFIG' in content:
    print("✅ BIGQUERY_CONFIG already exists in config.py")
else:
    print("Adding BIGQUERY_CONFIG to config.py...")
    
    # Find where to insert (after DB_CONFIG)
    lines = content.split('\n')
    new_lines = []
    
    for line in lines:
        new_lines.append(line)
        
        # Insert after DB_CONFIG
        if 'DB_CONFIG = {' in line:
            new_lines.append('')
            new_lines.append('# BigQuery Configuration')
            new_lines.append('BIGQUERY_CONFIG = {')
            new_lines.append('    \"project_id\": os.getenv(\"BIGQUERY_PROJECT\", \"iron-circuit-345510\"),')
            new_lines.append('    \"dataset_id\": os.getenv(\"BIGQUERY_DATASET\", \"google_ads_mcc\"),')
            new_lines.append('    \"credentials_path\": os.getenv(\"BIGQUERY_CREDENTIALS_PATH\", \"service-account-key.json\")')
            new_lines.append('}')
    
    # Write updated config
    with open(config_path, 'w') as f:
        f.write('\n'.join(new_lines))
    
    print("✅ Added BIGQUERY_CONFIG to config.py")

# Verify the update
print("\nUpdated config.py (relevant section):")
with open(config_path, 'r') as f:
    for line in f:
        if 'BIGQUERY_CONFIG' in line or 'DB_CONFIG' in line:
            print(line.rstrip())
