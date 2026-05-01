#!/usr/bin/env python3
"""
Microsoft Ads ETL – Production version
- Fetches last 30 days of campaign performance data
- Upserts into microsoft_data.microsoft_ads_daily
- Deletes downloaded ZIP after successful load
- Designed to run daily via cron
"""

import os
import sys
import json
import time
import zipfile
import csv
import re
import logging
from pathlib import Path
from datetime import datetime, timedelta

import requests
import certifi
import psycopg2
from psycopg2 import sql, extras
from dotenv import load_dotenv

# -------------------------------------------------------------------
# Load environment variables
# -------------------------------------------------------------------
load_dotenv()

# Microsoft Ads credentials
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
TENANT_ID = os.getenv("AZURE_TENANT_ID")
DEVELOPER_TOKEN = os.getenv("MSADS_DEVELOPER_TOKEN")
CUSTOMER_ID = os.getenv("MSADS_CUSTOMER_ID")
ACCOUNT_ID = os.getenv("MSADS_ACCOUNT_ID")

# PostgreSQL credentials (must match your .env variable names)
DB_HOST = os.getenv("PG_HOST")
DB_PORT = os.getenv("PG_PORT")
DB_NAME = os.getenv("PG_DB")
DB_USER = os.getenv("PG_USER")
DB_PASSWORD = os.getenv("PG_PASS")

# WSL storage path for downloaded ZIP reports – adjust if needed
REPORT_STORAGE = os.getenv("REPORT_STORAGE", "/home/upalcmm/scripts/etl_pipeline/crm")
Path(REPORT_STORAGE).mkdir(parents=True, exist_ok=True)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# SSL certificate bundle
SSL_VERIFY = certifi.where()

# -------------------------------------------------------------------
# Helper functions
# -------------------------------------------------------------------
def refresh_access_token():
    """Refresh the Microsoft Ads access token using refresh token from file."""
    token_file = Path("msads_tokens.json")
    if not token_file.exists():
        raise FileNotFoundError("msads_tokens.json not found. Run auth flow first.")
    tokens = json.loads(token_file.read_text())
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        raise ValueError("No refresh token in msads_tokens.json")

    data = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "refresh_token": refresh_token,
        "scope": "https://ads.microsoft.com/msads.manage offline_access"
    }
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    resp = requests.post(token_url, data=data, verify=SSL_VERIFY)
    resp.raise_for_status()
    new_tokens = resp.json()
    tokens.update(new_tokens)
    token_file.write_text(json.dumps(tokens, indent=2))
    logger.info("Access token refreshed.")
    return new_tokens["access_token"]

def get_date_range():
    """Return (start_date, end_date) for the last 30 full days (end = yesterday)."""
    end = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=29)  # 30 days inclusive
    return start, end

def submit_report(access_token, start_date, end_date):
    """Submit a report request for the given date range."""
    soap_request = f'''<s:Envelope xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
      <s:Header xmlns="https://bingads.microsoft.com/Reporting/v13">
        <AuthenticationToken i:nil="false">{access_token}</AuthenticationToken>
        <CustomerAccountId i:nil="false">{CUSTOMER_ID}</CustomerAccountId>
        <CustomerId i:nil="false">{CUSTOMER_ID}</CustomerId>
        <DeveloperToken i:nil="false">{DEVELOPER_TOKEN}</DeveloperToken>
      </s:Header>
      <s:Body>
        <SubmitGenerateReportRequest xmlns="https://bingads.microsoft.com/Reporting/v13">
          <ReportRequest i:nil="false" i:type="CampaignPerformanceReportRequest">
            <Format i:nil="false">Csv</Format>
            <ReportName i:nil="false">Campaign Performance Last30Days</ReportName>
            <Aggregation>Daily</Aggregation>
            <Columns i:nil="false">
              <CampaignPerformanceReportColumn>CampaignName</CampaignPerformanceReportColumn>
              <CampaignPerformanceReportColumn>CampaignType</CampaignPerformanceReportColumn>
              <CampaignPerformanceReportColumn>TimePeriod</CampaignPerformanceReportColumn>
              <CampaignPerformanceReportColumn>Impressions</CampaignPerformanceReportColumn>
              <CampaignPerformanceReportColumn>Clicks</CampaignPerformanceReportColumn>
              <CampaignPerformanceReportColumn>Conversions</CampaignPerformanceReportColumn>
              <CampaignPerformanceReportColumn>Revenue</CampaignPerformanceReportColumn>
              <CampaignPerformanceReportColumn>Spend</CampaignPerformanceReportColumn>
              <CampaignPerformanceReportColumn>Assists</CampaignPerformanceReportColumn>
            </Columns>
            <Scope i:nil="false">
              <AccountIds xmlns:a="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
                <a:long>{ACCOUNT_ID}</a:long>
              </AccountIds>
            </Scope>
            <Time i:nil="false">
              <CustomDateRangeEnd i:nil="false">
                <Day>{end_date.day}</Day><Month>{end_date.month}</Month><Year>{end_date.year}</Year>
              </CustomDateRangeEnd>
              <CustomDateRangeStart i:nil="false">
                <Day>{start_date.day}</Day><Month>{start_date.month}</Month><Year>{start_date.year}</Year>
              </CustomDateRangeStart>
            </Time>
          </ReportRequest>
        </SubmitGenerateReportRequest>
      </s:Body>
    </s:Envelope>'''

    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': 'SubmitGenerateReport'
    }
    url = "https://reporting.api.bingads.microsoft.com/Api/Advertiser/Reporting/v13/ReportingService.svc"
    resp = requests.post(url, data=soap_request.encode('utf-8'), headers=headers, verify=SSL_VERIFY)
    if resp.status_code != 200:
        logger.error(f"Submit report failed: {resp.text}")
        return None

    import xml.etree.ElementTree as ET
    root = ET.fromstring(resp.text)
    for elem in root.iter():
        if 'ReportRequestId' in elem.tag and elem.text:
            report_id = elem.text.strip()
            logger.info(f"Report submitted. ID: {report_id}")
            return report_id
    logger.error("No Report ID in response.")
    return None

def poll_for_report(access_token, report_id, max_attempts=20):
    """Poll until the report is ready and return the download URL."""
    logger.info("Polling for report completion...")
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': 'PollGenerateReport'
    }
    url = "https://reporting.api.bingads.microsoft.com/Api/Advertiser/Reporting/v13/ReportingService.svc"

    for attempt in range(max_attempts):
        time.sleep(5)
        poll_request = f'''<s:Envelope xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
          <s:Header xmlns="https://bingads.microsoft.com/Reporting/v13">
            <AuthenticationToken i:nil="false">{access_token}</AuthenticationToken>
            <CustomerAccountId i:nil="false">{CUSTOMER_ID}</CustomerAccountId>
            <CustomerId i:nil="false">{CUSTOMER_ID}</CustomerId>
            <DeveloperToken i:nil="false">{DEVELOPER_TOKEN}</DeveloperToken>
          </s:Header>
          <s:Body>
            <PollGenerateReportRequest xmlns="https://bingads.microsoft.com/Reporting/v13">
              <ReportRequestId>{report_id}</ReportRequestId>
            </PollGenerateReportRequest>
          </s:Body>
        </s:Envelope>'''
        resp = requests.post(url, data=poll_request.encode('utf-8'), headers=headers, verify=SSL_VERIFY)
        logger.debug(f"Poll attempt {attempt+1} – status {resp.status_code}")

        if resp.status_code != 200:
            continue

        if "ReportDownloadUrl" in resp.text:
            url_match = re.search('<ReportDownloadUrl>(.*?)</ReportDownloadUrl>', resp.text)
            if url_match:
                raw_url = url_match.group(1)
                clean_url = raw_url.replace('&amp;', '&')
                logger.info("Report ready.")
                return clean_url
        elif "Status" in resp.text:
            status_match = re.search('<Status>(.*?)</Status>', resp.text)
            if status_match:
                status = status_match.group(1)
                logger.info(f"Report status: {status}")
                if status in ["Error", "Failed"]:
                    return None

    logger.error("Polling timed out.")
    return None

def download_report(url, report_id):
    """Download the ZIP file to REPORT_STORAGE."""
    logger.info("Downloading report...")
    resp = requests.get(url, verify=SSL_VERIFY)
    if resp.status_code != 200:
        logger.error(f"Download failed: {resp.status_code}")
        return None
    zip_filename = f"msads_report_{report_id}.zip"
    zip_path = Path(REPORT_STORAGE) / zip_filename
    with open(zip_path, 'wb') as f:
        f.write(resp.content)
    logger.info(f"Saved ZIP to {zip_path}")
    return zip_path

def transform_and_upsert(zip_path, conn):
    """
    Extract CSV from ZIP, transform rows, upsert into table.
    Returns number of rows processed.
    """
    with zipfile.ZipFile(zip_path, 'r') as zf:
        csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
        if not csv_files:
            raise Exception("No CSV found in ZIP")
        csv_file = csv_files[0]
        with zf.open(csv_file) as f:
            lines = f.read().decode('utf-8-sig').splitlines()

            # Find header line (contains "CampaignName")
            header_index = next(i for i, line in enumerate(lines) if 'CampaignName' in line)
            reader = csv.DictReader(lines[header_index:], delimiter=',', quotechar='"')

            rows = []
            for row in reader:
                # Skip copyright footer
                campaign_raw = (row.get('CampaignName') or '').strip()
                if any(x in campaign_raw for x in ['Microsoft Corporation', 'All rights reserved', '©2026']):
                    continue

                # Split campaign name
                parts = campaign_raw.split(' - ')
                if len(parts) >= 3:
                    service = parts[0].strip()
                    location = parts[1].strip()
                    ad_type = ' - '.join(parts[2:]).strip()
                elif len(parts) == 2:
                    service = parts[0].strip()
                    location = parts[1].strip()
                    ad_type = 'Unknown'
                else:
                    service = campaign_raw
                    location = 'Unknown'
                    ad_type = 'Unknown'

                # Clean numeric fields
                def clean_num(val):
                    if not val:
                        return 0.0
                    cleaned = val.replace(',', '').replace('£', '').replace('$', '').strip()
                    try:
                        return float(cleaned)
                    except ValueError:
                        return 0.0

                # Parse date (TimePeriod format: YYYY-MM-DD)
                time_period_str = row.get('TimePeriod', '').strip()
                try:
                    time_period = datetime.strptime(time_period_str, '%Y-%m-%d').date()
                except:
                    logger.warning(f"Invalid date format: {time_period_str}, skipping row.")
                    continue

                rows.append({
                    'campaign_name': campaign_raw,
                    'service': service,
                    'location': location,
                    'ad_type': ad_type,
                    'campaign_type': (row.get('CampaignType') or '').strip(),
                    'time_period': time_period,
                    'impressions': int(clean_num(row.get('Impressions', '0'))),
                    'clicks': int(clean_num(row.get('Clicks', '0'))),
                    'conversions': clean_num(row.get('Conversions', '0')),
                    'revenue': clean_num(row.get('Revenue', '0')),
                    'spend': clean_num(row.get('Spend', '0')),
                    'assists': clean_num(row.get('Assists', '0'))
                })

            logger.info(f"Transformed {len(rows)} rows from CSV.")

            # Upsert into database
            with conn.cursor() as cur:
                upsert_sql = sql.SQL("""
                    INSERT INTO microsoft_data.microsoft_ads_daily (
                        campaign_name, service, location, ad_type, campaign_type,
                        time_period, impressions, clicks, conversions, revenue, spend, assists
                    ) VALUES %s
                    ON CONFLICT (campaign_name, time_period) DO UPDATE SET
                        service = EXCLUDED.service,
                        location = EXCLUDED.location,
                        ad_type = EXCLUDED.ad_type,
                        campaign_type = EXCLUDED.campaign_type,
                        impressions = EXCLUDED.impressions,
                        clicks = EXCLUDED.clicks,
                        conversions = EXCLUDED.conversions,
                        revenue = EXCLUDED.revenue,
                        spend = EXCLUDED.spend,
                        assists = EXCLUDED.assists
                """)
                # Convert rows to list of tuples
                values = [(
                    r['campaign_name'], r['service'], r['location'], r['ad_type'], r['campaign_type'],
                    r['time_period'], r['impressions'], r['clicks'], r['conversions'],
                    r['revenue'], r['spend'], r['assists']
                ) for r in rows]
                extras.execute_values(cur, upsert_sql, values, page_size=100)
                conn.commit()
                logger.info(f"Upserted {len(rows)} rows into database.")
                return len(rows)

def main():
    try:
        # 1. Refresh token
        access_token = refresh_access_token()

        # 2. Get date range
        start_date, end_date = get_date_range()
        logger.info(f"Requesting data from {start_date.date()} to {end_date.date()}")

        # 3. Submit report
        report_id = submit_report(access_token, start_date, end_date)
        if not report_id:
            return

        # 4. Poll for download URL
        download_url = poll_for_report(access_token, report_id)
        if not download_url:
            return

        # 5. Download ZIP
        zip_path = download_report(download_url, report_id)
        if not zip_path:
            return

        # 6. Connect to database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        try:
            # 7. Transform and upsert
            row_count = transform_and_upsert(zip_path, conn)
            logger.info(f"Successfully processed {row_count} rows.")

        finally:
            conn.close()

        # 8. Clean up ZIP
        zip_path.unlink()
        logger.info(f"Deleted {zip_path}")

    except Exception as e:
        logger.exception("ETL failed")
        sys.exit(1)

if __name__ == "__main__":
    main()