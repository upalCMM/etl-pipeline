#!/bin/bash
# Setup 6 AM cron job for CRM ETLs

echo "Setting up 6 AM daily cron job for CRM ETLs..."

# Backup current crontab
crontab -l > crontab_backup_$(date +%Y%m%d_%H%M%S).txt 2>/dev/null

# Create new crontab
crontab -l 2>/dev/null > mycron || touch mycron

# Add CRM ETL job
echo "# CRM ETL Pipeline - Daily 6 AM" >> mycron
echo "0 6 * * * cd /home/upalcmm/scripts/etl_pipeline && ./schedule_crm_etls.sh >> /home/upalcmm/scripts/etl_pipeline/logs/crm/cron.log 2>&1" >> mycron
echo "" >> mycron

# Install new crontab
crontab mycron
rm mycron

echo "✅ Cron job added successfully!"
echo ""
echo "📋 Current crontab:"
crontab -l
echo ""
echo "⏰ Will run daily at 6 AM"
echo "📁 Logs: ~/scripts/etl_pipeline/logs/crm/"
