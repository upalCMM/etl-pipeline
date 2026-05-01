"""
Facebook Ads Daily ETL - Supports backfill from specific start date
"""
import os
import logging
import json
import warnings
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Suppress SSL warnings
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# Load environment variables
load_dotenv()

# Configure logging
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'facebook_ads_etl.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FacebookAdsBackfillETL:
    """Facebook Ads ETL with backfill support for historical data"""
    
    def __init__(self):
        """Initialize with credentials"""
        self.access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
        self.ad_account_id = os.getenv('FACEBOOK_AD_ACCOUNT_ID')
        
        if not self.access_token or not self.ad_account_id:
            raise ValueError("FACEBOOK_ACCESS_TOKEN and FACEBOOK_AD_ACCOUNT_ID must be set")
        
        # Database settings
        self.schema_name = os.getenv('FACEBOOK_SCHEMA', 'facebook_data')
        self.table_name = os.getenv('FACEBOOK_TABLE', 'facebook_daily')
        
        # PostgreSQL connection parameters
        self.db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'cmm_pipedrive'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        }
        
        # Base URL for Facebook Graph API
        self.base_url = "https://graph.facebook.com/v19.0"
        
        # Initialize database connection
        self.engine = self._create_database_engine()
        
        logger.info(f"ETL initialized for account: {self.ad_account_id}")
    
    def _create_database_engine(self):
        """Create SQLAlchemy engine for PostgreSQL"""
        connection_string = (
            f"postgresql://{self.db_params['user']}:"
            f"{self.db_params['password']}@"
            f"{self.db_params['host']}:{self.db_params['port']}/"
            f"{self.db_params['database']}"
        )
        
        try:
            engine = create_engine(connection_string, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            return engine
        except SQLAlchemyError as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise
    
    def parse_date(self, date_str: str) -> Optional[date]:
        """Parse date string"""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str[:10], '%Y-%m-%d').date()
        except:
            return None
    
    def make_api_request(self, endpoint: str, params: Dict) -> Optional[Dict]:
        """Make a request to Facebook Graph API with retry logic"""
        url = f"{self.base_url}/{endpoint}"
        params['access_token'] = self.access_token
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=60, verify=False)
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Rate limited
                    wait_time = 2 ** attempt
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds...")
                    import time
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"HTTP Error: {str(e)}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {str(e)}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)
                    continue
                return None
        
        return None
    
    def extract_data_for_date_range(self, start_date: date, end_date: date) -> List[Dict]:
        """Extract data for a specific date range"""
        fields = [
            'ad_id',
            'ad_name',
            'adset_id',
            'adset_name',
            'campaign_id',
            'campaign_name',
            'impressions',
            'clicks',
            'spend',
            'conversions',
            'date_start'
        ]
        
        params = {
            'fields': ','.join(fields),
            'time_range': json.dumps({
                'since': start_date.strftime('%Y-%m-%d'),
                'until': end_date.strftime('%Y-%m-%d')
            }),
            'level': 'ad',
            'time_increment': 1,
            'limit': 1000
        }
        
        all_data = []
        
        try:
            endpoint = f"{self.ad_account_id}/insights"
            logger.info(f"Extracting data from {start_date} to {end_date}")
            
            while True:
                data = self.make_api_request(endpoint, params)
                if not data or 'data' not in data:
                    break
                
                all_data.extend(data['data'])
                logger.info(f"  Retrieved {len(data['data'])} records, total: {len(all_data)}")
                
                # Check for next page
                if 'paging' in data and 'next' in data['paging']:
                    next_url = data['paging']['next']
                    if 'after=' in next_url:
                        after_cursor = next_url.split('after=')[1].split('&')[0]
                        params['after'] = after_cursor
                    else:
                        break
                else:
                    break
            
            logger.info(f"Completed: {len(all_data)} total records for date range")
            return all_data
            
        except Exception as e:
            logger.error(f"Failed to extract data: {str(e)}")
            return []
    
    def extract_data_for_single_day(self, target_date: date) -> List[Dict]:
        """Extract data for a single day"""
        return self.extract_data_for_date_range(target_date, target_date)
    
    def upsert_daily_data(self, insights: List[Dict]) -> Tuple[int, int]:
        """UPSERT daily data - returns (inserted_count, updated_count)"""
        if not insights:
            return 0, 0
        
        inserted_count = 0
        updated_count = 0
        
        try:
            with self.engine.begin() as conn:
                # Check existing columns
                existing_columns = conn.execute(text(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{self.schema_name}' 
                    AND table_name = '{self.table_name}'
                """)).fetchall()
                existing_columns = [col[0] for col in existing_columns]
                
                for insight in insights:
                    try:
                        ad_id = insight.get('ad_id', '')
                        campaign_id = insight.get('campaign_id', '')
                        date_str = insight.get('date_start', '')
                        
                        if not ad_id or not campaign_id or not date_str:
                            continue
                        
                        data_date = self.parse_date(date_str)
                        if not data_date:
                            continue
                        
                        # Prepare data
                        row_data = {
                            'ad_id': ad_id,
                            'ad_name': insight.get('ad_name', ''),
                            'adset_id': insight.get('adset_id', ''),
                            'adset_name': insight.get('adset_name', ''),
                            'campaign_id': campaign_id,
                            'campaign_name': insight.get('campaign_name', ''),
                            'date': data_date,
                            'impressions': int(insight.get('impressions', 0)),
                            'clicks': int(insight.get('clicks', 0)),
                            'spend': float(insight.get('spend', 0)),
                            'conversions': int(insight.get('conversions', 0))
                        }
                        
                        # Filter to existing columns
                        filtered_row_data = {k: v for k, v in row_data.items() if k in existing_columns}
                        
                        if not filtered_row_data:
                            continue
                        
                        # Check if record exists
                        check_sql = text(f"""
                            SELECT 1 FROM {self.schema_name}.{self.table_name}
                            WHERE campaign_id = :campaign_id 
                            AND ad_id = :ad_id 
                            AND date = :date
                        """)
                        
                        exists = conn.execute(check_sql, {
                            'campaign_id': campaign_id,
                            'ad_id': ad_id,
                            'date': data_date
                        }).fetchone()
                        
                        if exists:
                            # Update
                            update_cols = []
                            update_params = {'campaign_id': campaign_id, 'ad_id': ad_id, 'date': data_date}
                            for key, value in filtered_row_data.items():
                                if key not in ['campaign_id', 'ad_id', 'date']:
                                    update_cols.append(f"{key} = :{key}")
                                    update_params[key] = value
                            
                            if update_cols:
                                update_sql = text(f"""
                                    UPDATE {self.schema_name}.{self.table_name}
                                    SET {', '.join(update_cols)}
                                    WHERE campaign_id = :campaign_id 
                                    AND ad_id = :ad_id 
                                    AND date = :date
                                """)
                                result = conn.execute(update_sql, update_params)
                                if result.rowcount > 0:
                                    updated_count += 1
                        else:
                            # Insert
                            columns = list(filtered_row_data.keys())
                            placeholders = [f":{col}" for col in columns]
                            insert_sql = text(f"""
                                INSERT INTO {self.schema_name}.{self.table_name} 
                                ({', '.join(columns)})
                                VALUES ({', '.join(placeholders)})
                            """)
                            result = conn.execute(insert_sql, filtered_row_data)
                            if result.rowcount > 0:
                                inserted_count += 1
                            
                    except Exception as e:
                        logger.warning(f"Failed to process record: {e}")
                        continue
            
            return inserted_count, updated_count
            
        except Exception as e:
            logger.error(f"Failed to UPSERT data: {str(e)}")
            return 0, 0
    
    def backfill_from_date(self, start_date: date, end_date: Optional[date] = None) -> Dict:
        """Backfill data from start_date to end_date (or yesterday)"""
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        
        logger.info(f"Starting backfill from {start_date} to {end_date}")
        
        total_inserted = 0
        total_updated = 0
        current_date = start_date
        
        while current_date <= end_date:
            logger.info(f"Processing: {current_date}")
            
            # Extract data for this day
            insights = self.extract_data_for_single_day(current_date)
            
            if insights:
                inserted, updated = self.upsert_daily_data(insights)
                total_inserted += inserted
                total_updated += updated
                logger.info(f"  Inserted: {inserted}, Updated: {updated}")
            else:
                logger.info(f"  No data for this date")
            
            # Move to next day
            current_date += timedelta(days=1)
        
        return {
            'success': True,
            'start_date': start_date,
            'end_date': end_date,
            'total_inserted': total_inserted,
            'total_updated': total_updated
        }
    
    def update_last_90_days(self) -> Dict:
        """Update/upsert data for the last 90 days"""
        end_date = date.today() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=119)  # 120 days including yesterday
        
        logger.info(f"Updating last 90 days from {start_date} to {end_date}")
        return self.backfill_from_date(start_date, end_date)
    
    def export_to_csv(self, start_date: date, end_date: date, output_file: str) -> Dict:
        """Export data for analysis to CSV"""
        logger.info(f"Exporting data from {start_date} to {end_date} to {output_file}")
        
        try:
            # Extract data from Facebook API
            insights = self.extract_data_for_date_range(start_date, end_date)
            
            if not insights:
                logger.warning("No data retrieved")
                return {'success': False, 'message': 'No data retrieved', 'records': 0}
            
            # Export to CSV
            import csv
            
            # Get all unique field names
            all_fields = set()
            for insight in insights:
                all_fields.update(insight.keys())
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=sorted(all_fields))
                writer.writeheader()
                writer.writerows(insights)
            
            logger.info(f"Exported {len(insights)} records to {output_file}")
            
            return {
                'success': True,
                'file': output_file,
                'records': len(insights),
                'start_date': start_date,
                'end_date': end_date
            }
            
        except Exception as e:
            logger.error(f"Export failed: {str(e)}")
            return {'success': False, 'error': str(e), 'records': 0}


def main():
    """Main function - can run daily ETL or backfill"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Facebook Ads ETL')
    parser.add_argument('--backfill', action='store_true', help='Run backfill')
    parser.add_argument('--start-date', type=str, help='Start date for backfill (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date for backfill (YYYY-MM-DD)')
    parser.add_argument('--export-csv', type=str, help='Export to CSV file instead of database')
    parser.add_argument('--daily', action='store_true', help='Run daily ETL (yesterday only)')
    parser.add_argument('--last-90-days', action='store_true', help='Update last 90 days of data (default)')
    
    args = parser.parse_args()
    
    try:
        etl = FacebookAdsBackfillETL()
        
        # Export to CSV mode
        if args.export_csv:
            if not args.start_date:
                logger.error("--start-date required for CSV export")
                return 1
            
            start = datetime.strptime(args.start_date, '%Y-%m-%d').date()
            end = datetime.strptime(args.end_date, '%Y-%m-%d').date() if args.end_date else date.today()
            
            result = etl.export_to_csv(start, end, args.export_csv)
            if result['success']:
                logger.info(f"Exported {result['records']} records to {result['file']}")
                return 0
            else:
                logger.error(f"Export failed: {result.get('error', 'Unknown error')}")
                return 1
        
        # Backfill mode
        elif args.backfill:
            if not args.start_date:
                logger.error("--start-date required for backfill")
                return 1
            
            start = datetime.strptime(args.start_date, '%Y-%m-%d').date()
            end = datetime.strptime(args.end_date, '%Y-%m-%d').date() if args.end_date else date.today() - timedelta(days=1)
            
            result = etl.backfill_from_date(start, end)
            logger.info(f"Backfill complete: {result['total_inserted']} inserted, {result['total_updated']} updated")
            return 0
        
        # Daily mode (yesterday only)
        elif args.daily:
            logger.info("Running daily ETL (yesterday only)")
            yesterday = date.today() - timedelta(days=1)
            result = etl.backfill_from_date(yesterday, yesterday)
            logger.info(f"Daily update complete: {result['total_inserted']} inserted, {result['total_updated']} updated")
            return 0
        
        # Default: Update last 90 days
        else:
            logger.info("Running default ETL: Updating last 90 days")
            result = etl.update_last_90_days()
            logger.info(f"90-day update complete: {result['total_inserted']} inserted, {result['total_updated']} updated")
            return 0
            
    except Exception as e:
        logger.error(f"ETL failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)