#!/bin/bash
# Fix a single script properly

SCRIPT="pipedrive/sync_ringcentral_final.py"
echo "Fixing $SCRIPT..."

# Create backup
cp "$SCRIPT" "$SCRIPT.backup"

# Create the fixed version
cat > "$SCRIPT.fixed" << 'SCRIPTFIXED'
#!/home/upalcmm/scripts/venv/bin/python
# sync_ringcentral_calls.py - Fixed version

import os
import sys

# FIX: Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Original imports
import requests
import base64
import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import logging

# FIX: Now import config from project root
try:
    import config
    from config import DB_CONFIG
except ImportError as e:
    print(f"Error importing config: {e}")
    print(f"Python path: {sys.path}")
    print(f"Current dir: {current_dir}")
    print(f"Project root: {project_root}")
    sys.exit(1)
SCRIPTFIXED

# Append the rest of the script (skip the first line which is the shebang and the config import)
tail -n +2 "$SCRIPT" | sed '/^from config import DB_CONFIG/d' | sed '/^import config/d' >> "$SCRIPT.fixed"

# Replace the original
mv "$SCRIPT.fixed" "$SCRIPT"

echo "✅ Fixed $SCRIPT"
echo ""
echo "Now testing the fixed script..."
cd pipedrive
python3 sync_ringcentral_final.py 2>&1 | head -30
cd ..
