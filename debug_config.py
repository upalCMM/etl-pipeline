#!/usr/bin/env python3
from config import BIGQUERY_CONFIG
import json
print("Current BIGQUERY_CONFIG:")
print(json.dumps(BIGQUERY_CONFIG, indent=2))
