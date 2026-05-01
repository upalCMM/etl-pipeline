# CRM ETL Scripts Analysis
Generated: $(date)

## Script Dependencies:

### 1. etl_crm_partner_credit_status_final_schd.py
- Databases: PostgreSQL only
- Tables: crm_data.partner_credit_status
- Status: ✅ Confirmed working

### 2. etl_crm_partner_logs_master_final_schd.py
- Databases: MariaDB + PostgreSQL
- MariaDB Tables: removal_company_logs (compmove_core)
- PostgreSQL Tables: crm_data.partner_company_logs
- Status: ✅ Confirmed working

### 3. etl_crm_partner_master_final_schd.py
- Databases: MariaDB + PostgreSQL
- MariaDB Tables: removal_companies (compmove_core)
- PostgreSQL Tables: crm_data.partner_master_companies
- Status: ✅ Confirmed working

### 4. etl_crm_leads_final_schd.py
- Databases: Likely PostgreSQL only
- Issue: .env file path check
- Status: 🔧 Needs path fix

### 5. etl_crm_leads_company_final_schd.py
- Databases: Likely PostgreSQL only
- Issue: .env file path check
- Status: 🔧 Needs path fix

## Database Connections:
- PostgreSQL: 192.168.1.250:5432 (cmm_pipedrive)
- MariaDB: 127.0.0.1:53080 (compmove_core via DDEV)

## Required Environment Variables:
- DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
- MARIADB_HOST, MARIADB_PORT, MARIADB_DATABASE, MARIADB_USER, MARIADB_PASSWORD

## Recommendations:
1. Schedule working scripts (1-3) for daily 6 AM
2. Fix leads scripts (4-5) path issues
3. Monitor row counts for data quality
