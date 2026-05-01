#!/usr/bin/env python3
from config import BIGQUERY_CONFIG
print("BIGQUERY_CONFIG contents:")
for key, value in BIGQUERY_CONFIG.items():
    print(f"  {key}: {value}")
