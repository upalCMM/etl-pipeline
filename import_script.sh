#!/bin/bash
# Simple script importer

if [ $# -ne 2 ]; then
    echo "Usage: ./import_script.sh <group> <script.py>"
    echo "Groups: pipedrive, bigquery, github, crm"
    exit 1
fi

GROUP="$1"
SCRIPT="$2"
SOURCE="/mnt/c/pipedrive_postgres/$SCRIPT"
DEST="./$GROUP/$SCRIPT"

echo "Importing $SCRIPT to $GROUP group..."
cp "$SOURCE" "$DEST"

# Add shebang
sed -i '1s/^/#!\/usr\/bin\/env python3\n/' "$DEST"

# Fix paths
sed -i 's|C:\\\\pipedrive_postgres\\\\|/mnt/c/pipedrive_postgres/|g' "$DEST"
sed -i "s/'localhost'/'172.24.160.1'/g" "$DEST"

chmod +x "$DEST"
echo "✅ Done! Test with: cd $GROUP && python $SCRIPT"
