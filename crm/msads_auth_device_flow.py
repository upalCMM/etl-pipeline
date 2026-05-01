# msads_etl_final_working.py
import os
import json
import csv
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List
import requests
from dotenv import load_dotenv

load_dotenv()

class MicrosoftAdsETL:
    def __init__(self):
        """Initialize the ETL process with credentials from .env"""
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.tenant_id = os.getenv("AZURE_TENANT_ID")
        self.developer_token = os.getenv("MSADS_DEVELOPER_TOKEN")
        self.customer_id = os.getenv("MSADS_CUSTOMER_ID")
        self.account_id = os.getenv("MSADS_ACCOUNT_ID")
        
        # Validate required credentials
        if not all([self.client_id, self.tenant_id, self.developer_token, 
                   self.customer_id, self.account_id]):
            missing = []
            if not self.client_id: missing.append("AZURE_CLIENT_ID")
            if not self.tenant_id: missing.append("AZURE_TENANT_ID")
            if not self.developer_token: missing.append("MSADS_DEVELOPER_TOKEN")
            if not self.customer_id: missing.append("MSADS_CUSTOMER_ID")
            if not self.account_id: missing.append("MSADS_ACCOUNT_ID")
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        # Load tokens from file
        token_file = Path("msads_tokens.json")
        if not token_file.exists():
            raise FileNotFoundError(
                "msads_tokens.json not found. Please run msads_auth_device_flow.py first to authenticate."
            )
        
        self.tokens = json.loads(token_file.read_text())
        self.access_token = self.tokens.get("access_token")
        self.refresh_token = self.tokens.get("refresh_token")
        
        if not self.access_token:
            raise ValueError("No access token found in msads_tokens.json")
        
        # API endpoints
        self.campaign_url = "https://campaign.api.bingads.microsoft.com/Api/Advertiser/CampaignManagement/v13/CampaignManagementService.svc"
        self.reporting_url = "https://reporting.api.bingads.microsoft.com/Api/Advertiser/Reporting/v13/ReportingService.svc"
        self.customer_url = "https://clientcenter.api.bingads.microsoft.com/Api/CustomerManagement/v13/CustomerManagementService.svc"
    
    def _make_request(self, url: str, action: str, body: str) -> requests.Response:
        """Make SOAP request with authentication"""
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": action
        }
        
        # Replace placeholders
        body = body.replace("{DeveloperToken}", self.developer_token)
        body = body.replace("{AuthenticationToken}", self.access_token)
        body = body.replace("{CustomerId}", self.customer_id)
        body = body.replace("{AccountId}", self.account_id)
        
        return requests.post(url, data=body.encode('utf-8'), headers=headers)
    
    def get_user(self) -> bool:
        """Test authentication by getting user info"""
        print("Testing API connection - GetUser...")
        
        body = """<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Header>
                <DeveloperToken xmlns="https://bingads.microsoft.com/Customer/v13">{DeveloperToken}</DeveloperToken>
                <AuthenticationToken xmlns="https://bingads.microsoft.com/Customer/v13">{AuthenticationToken}</AuthenticationToken>
            </soap:Header>
            <soap:Body>
                <GetUserRequest xmlns="https://bingads.microsoft.com/Customer/v13">
                    <UserId xsi:nil="true" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"/>
                </GetUserRequest>
            </soap:Body>
        </soap:Envelope>"""
        
        response = self._make_request(self.customer_url, "GetUser", body)
        
        if response.status_code == 200:
            print("✅ Authentication successful")
            return True
        else:
            print(f"❌ Authentication failed: {response.text[:200]}")
            return False
    
    def get_campaigns(self) -> List[Dict]:
        """Get all campaigns for the account"""
        print("Fetching campaigns...")
        
        body = """<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Header>
                <DeveloperToken xmlns="https://bingads.microsoft.com/CampaignManagement/v13">{DeveloperToken}</DeveloperToken>
                <AuthenticationToken xmlns="https://bingads.microsoft.com/CampaignManagement/v13">{AuthenticationToken}</AuthenticationToken>
                <CustomerAccountId xmlns="https://bingads.microsoft.com/CampaignManagement/v13">{AccountId}</CustomerAccountId>
                <CustomerId xmlns="https://bingads.microsoft.com/CampaignManagement/v13">{CustomerId}</CustomerId>
            </soap:Header>
            <soap:Body>
                <GetCampaignsByAccountIdRequest xmlns="https://bingads.microsoft.com/CampaignManagement/v13">
                    <AccountId>{AccountId}</AccountId>
                </GetCampaignsByAccountIdRequest>
            </soap:Body>
        </soap:Envelope>"""
        
        response = self._make_request(self.campaign_url, "GetCampaignsByAccountId", body)
        
        if response.status_code != 200:
            print(f"Error fetching campaigns: {response.text[:200]}")
            return []
        
        # Parse XML response
        try:
            root = ET.fromstring(response.text)
            campaigns = []
            
            # Define namespace
            ns = {'v13': 'https://bingads.microsoft.com/CampaignManagement/v13'}
            
            for campaign in root.findall('.//v13:Campaign', ns):
                name = campaign.find('v13:Name', ns)
                cid = campaign.find('v13:Id', ns)
                ctype = campaign.find('v13:CampaignType', ns)
                
                if name is not None and name.text:
                    campaigns.append({
                        'Name': name.text,
                        'Id': cid.text if cid is not None else '',
                        'Type': ctype.text if ctype is not None else ''
                    })
            
            print(f"Found {len(campaigns)} campaigns")
            return campaigns
            
        except Exception as e:
            print(f"Error parsing campaign response: {e}")
            return []
    
    def get_report(self, start_date: str, end_date: str) -> List[Dict]:
        """
        Get campaign performance report with CORRECT element order per Microsoft's schema
        Element order must be: Format, ReportName, ReturnOnlyCompleteData, Aggregation, 
        Columns, Scope, Time
        """
        print(f"Requesting report for {start_date} to {end_date}...")
        
        body = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
                      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <soap:Header>
                <DeveloperToken xmlns="https://bingads.microsoft.com/Reporting/v13">{{DeveloperToken}}</DeveloperToken>
                <AuthenticationToken xmlns="https://bingads.microsoft.com/Reporting/v13">{{AuthenticationToken}}</AuthenticationToken>
                <CustomerAccountId xmlns="https://bingads.microsoft.com/Reporting/v13">{{AccountId}}</CustomerAccountId>
                <CustomerId xmlns="https://bingads.microsoft.com/Reporting/v13">{{CustomerId}}</CustomerId>
            </soap:Header>
            <soap:Body>
                <SubmitGenerateReportRequest xmlns="https://bingads.microsoft.com/Reporting/v13">
                    <ReportRequest xsi:type="CampaignPerformanceReportRequest">
                        <!-- 1. Format (required) -->
                        <Format>Csv</Format>
                        
                        <!-- 2. ReportName (optional) -->
                        <ReportName>Campaign Performance Report</ReportName>
                        
                        <!-- 3. ReturnOnlyCompleteData (optional) -->
                        <ReturnOnlyCompleteData>false</ReturnOnlyCompleteData>
                        
                        <!-- 4. Aggregation (required) -->
                        <Aggregation>Monthly</Aggregation>
                        
                        <!-- 5. Columns (required) - MUST come before Scope and Time -->
                        <Columns>
                            <CampaignName />
                            <CampaignType />
                            <TimePeriod />
                            <Impressions />
                            <Clicks />
                            <Conversions />
                            <Revenue />
                            <Spend />
                            <Assists />
                        </Columns>
                        
                        <!-- 6. Scope (required) - AccountIds must be wrapped in long element per schema [citation:1] -->
                        <Scope i:nil="false">
                           <AccountIds xmlns:a="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
                             <a:long>7525337</a:long>
                           </AccountIds>
                       </Scope>
                        
                                               
                        <!-- 7. Time (required) - MUST come last -->
                        <Time>
                            <CustomDateRangeStart>
                                <Year>{start_date[:4]}</Year>
                                <Month>{start_date[5:7]}</Month>
                                <Day>{start_date[8:10]}</Day>
                            </CustomDateRangeStart>
                            <CustomDateRangeEnd>
                                <Year>{end_date[:4]}</Year>
                                <Month>{end_date[5:7]}</Month>
                                <Day>{end_date[8:10]}</Day>
                            </CustomDateRangeEnd>
                        </Time>
                    </ReportRequest>
                </SubmitGenerateReportRequest>
            </soap:Body>
        </soap:Envelope>"""
        
        response = self._make_request(self.reporting_url, "SubmitGenerateReport", body)
        
        if response.status_code != 200:
            print(f"Error submitting report: {response.text[:500]}")
            return []
        
        # Parse response to get Report ID
        try:
            root = ET.fromstring(response.text)
            
            # Check for fault first
            fault = root.find('.//faultstring')
            if fault is not None:
                print(f"SOAP Fault: {fault.text}")
                return []
            
            # Look for ReportRequestId
            report_id = None
            for elem in root.iter():
                if 'ReportRequestId' in elem.tag and elem.text:
                    report_id = elem.text.strip()
                    break
            
            if report_id:
                print(f"✅ Report ID: {report_id}")
                return self._poll_report(report_id)
            else:
                print("No Report ID found in response")
                print(f"Response preview: {response.text[:300]}")
                return []
                
        except Exception as e:
            print(f"Error parsing report response: {e}")
            return []
    
    def _poll_report(self, report_id: str, max_attempts: int = 30) -> List[Dict]:
        """Poll for report completion"""
        print("Polling for report completion...")
        
        for attempt in range(max_attempts):
            time.sleep(5)
            
            body = f"""<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                <soap:Header>
                    <DeveloperToken xmlns="https://bingads.microsoft.com/Reporting/v13">{{DeveloperToken}}</DeveloperToken>
                    <AuthenticationToken xmlns="https://bingads.microsoft.com/Reporting/v13">{{AuthenticationToken}}</AuthenticationToken>
                    <CustomerAccountId xmlns="https://bingads.microsoft.com/Reporting/v13">{{AccountId}}</CustomerAccountId>
                    <CustomerId xmlns="https://bingads.microsoft.com/Reporting/v13">{{CustomerId}}</CustomerId>
                </soap:Header>
                <soap:Body>
                    <PollGenerateReportRequest xmlns="https://bingads.microsoft.com/Reporting/v13">
                        <ReportRequestId>{report_id}</ReportRequestId>
                    </PollGenerateReportRequest>
                </soap:Body>
            </soap:Envelope>"""
            
            response = self._make_request(self.reporting_url, "PollGenerateReport", body)
            
            if response.status_code == 200:
                try:
                    root = ET.fromstring(response.text)
                    
                    # Check status
                    status = None
                    for elem in root.iter():
                        if 'Status' in elem.tag and elem.text:
                            status = elem.text
                            break
                    
                    print(f"Status: {status} (attempt {attempt+1}/{max_attempts})")
                    
                    if status == "Success":
                        # Get download URL
                        for elem in root.iter():
                            if 'ReportDownloadUrl' in elem.tag and elem.text:
                                url = elem.text
                                print("Downloading report...")
                                report_response = requests.get(url)
                                return self._parse_csv(report_response.text)
                        
                        print("No download URL found in success response")
                        return []
                        
                    elif status in ["Error", "Failed"]:
                        print("Report generation failed")
                        return []
                        
                except Exception as e:
                    print(f"Error polling report: {e}")
                    continue
            else:
                print(f"Polling request failed: {response.status_code}")
        
        print("Report polling timed out")
        return []
    
    def _parse_csv(self, data: str) -> List[Dict]:
        """Parse CSV report data"""
        results = []
        lines = data.strip().split('\n')
        
        # Find header row
        header_idx = -1
        for i, line in enumerate(lines):
            if 'CampaignName' in line and 'Impressions' in line:
                header_idx = i
                break
        
        if header_idx == -1:
            print("Could not find header row in CSV")
            return results
        
        # Parse headers
        headers = [h.strip().strip('"') for h in lines[header_idx].split(',')]
        
        # Parse data rows
        for line in lines[header_idx + 1:]:
            if not line.strip() or line.strip().startswith('-'):
                continue
            
            # Simple CSV parsing (handles quoted fields)
            values = []
            current = ""
            in_quotes = False
            
            for char in line:
                if char == '"':
                    in_quotes = not in_quotes
                elif char == ',' and not in_quotes:
                    values.append(current.strip())
                    current = ""
                else:
                    current += char
            values.append(current.strip())
            
            # Create row dictionary
            row = {}
            for i, header in enumerate(headers):
                if i < len(values):
                    row[header] = values[i].strip('"')
            
            try:
                # Extract YearMonth from TimePeriod (format: YYYY-MM-DD)
                time_period = row.get('TimePeriod', '')
                year_month = time_period[:7] if len(time_period) >= 7 else ''
                
                # Convert and validate numeric values
                impressions = int(float(row.get('Impressions', 0) or 0))
                clicks = int(float(row.get('Clicks', 0) or 0))
                conversions = float(row.get('Conversions', 0) or 0)
                revenue = float(row.get('Revenue', 0) or 0)
                spend = float(row.get('Spend', 0) or 0)
                assists = float(row.get('Assists', 0) or 0)
                
                # Only add if we have meaningful data
                if impressions > 0 or clicks > 0 or spend > 0:
                    results.append({
                        'Campaign': row.get('CampaignName', ''),
                        'CampaignType': row.get('CampaignType', ''),
                        'YearMonth': year_month,
                        'Impressions': impressions,
                        'Clicks': clicks,
                        'Conversions': conversions,
                        'Revenue': revenue,
                        'Spend': spend,
                        'Assists': assists
                    })
            except (ValueError, TypeError) as e:
                # Skip rows with conversion errors
                continue
        
        print(f"Parsed {len(results)} rows of performance data")
        return results
    
    def save_to_csv(self, data: List[Dict], filename: str = None) -> str:
        """Save data to CSV file"""
        if not data:
            print("No data to save")
            return ""
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"msads_campaign_performance_{timestamp}.csv"
        
        fieldnames = ['Campaign', 'CampaignType', 'YearMonth', 'Impressions', 
                     'Clicks', 'Conversions', 'Revenue', 'Spend', 'Assists']
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        print(f"✅ Saved {len(data)} rows to {filename}")
        return filename
    
    def print_summary(self, data: List[Dict]):
        """Print summary statistics"""
        if not data:
            return
        
        total_impressions = sum(d['Impressions'] for d in data)
        total_clicks = sum(d['Clicks'] for d in data)
        total_spend = sum(d['Spend'] for d in data)
        total_conversions = sum(d['Conversions'] for d in data)
        total_revenue = sum(d['Revenue'] for d in data)
        
        print("\n" + "=" * 40)
        print("ETL Summary")
        print("=" * 40)
        print(f"Total Rows: {len(data)}")
        print(f"Total Impressions: {total_impressions:,}")
        print(f"Total Clicks: {total_clicks:,}")
        if total_impressions > 0:
            print(f"Average CTR: {(total_clicks/total_impressions*100):.2f}%")
        print(f"Total Spend: ${total_spend:,.2f}")
        print(f"Total Conversions: {total_conversions:.0f}")
        print(f"Total Revenue: ${total_revenue:,.2f}")
        if total_spend > 0:
            print(f"ROAS: {(total_revenue/total_spend):.2f}x")
    
    def run(self, start_date: str = None, end_date: str = None):
        """Run the complete ETL process"""
        print("=" * 60)
        print("Microsoft Ads ETL Pipeline")
        print("=" * 60)
        
        # Set default dates (last 30 days)
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        print(f"Period: {start_date} to {end_date}")
        print(f"Account ID: {self.account_id}")
        print(f"Customer ID: {self.customer_id}")
        print("-" * 60)
        
        # Test authentication
        if not self.get_user():
            print("❌ Authentication failed. Exiting.")
            return
        
        print("-" * 60)
        
        # Get campaigns (optional)
        campaigns = self.get_campaigns()
        
        print("-" * 60)
        
        # Get performance report
        report_data = self.get_report(start_date, end_date)
        
        # Save report if we have data
        if report_data:
            filename = self.save_to_csv(report_data)
            self.print_summary(report_data)
            print(f"\n✅ ETL Complete! Data saved to: {filename}")
        else:
            print("\n⚠️ No report data retrieved")
            print("\nTroubleshooting steps:")
            print("1. Verify in Microsoft Ads web interface that there's data for this period")
            print("2. Try a more recent date range (last 7 days)")
            print("3. Check that your developer token has reporting permissions")
            print("4. Ensure the account ID is correct for the customer ID")


def main():
    """Main entry point"""
    print("Microsoft Ads ETL Pipeline")
    print("=" * 60)
    
    # Check for .env file
    if not Path(".env").exists():
        print("❌ .env file not found.")
        print("\nPlease create a .env file with your credentials:")
        print("""
AZURE_CLIENT_ID="8a98ad8e-6d44-4316-84f9-c05b0f451a57"
AZURE_TENANT_ID="a88897ca-0e5a-4cb0-8f4e-04225f944da6"
MSADS_DEVELOPER_TOKEN="1232E5360F575602"
MSADS_CUSTOMER_ID="17026163"
MSADS_ACCOUNT_ID="2061481"
        """.strip())
        return
    
    # Check for tokens file
    if not Path("msads_tokens.json").exists():
        print("❌ msads_tokens.json not found.")
        print("\nPlease run authentication first:")
        print("python msads_auth_device_flow.py")
        return
    
    # Parse command line arguments
    import sys
    start_date = None
    end_date = None
    
    if len(sys.argv) >= 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        print(f"Using custom date range: {start_date} to {end_date}")
    
    # Run ETL
    try:
        etl = MicrosoftAdsETL()
        etl.run(start_date, end_date)
    except Exception as e:
        print(f"\n❌ ETL Failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()