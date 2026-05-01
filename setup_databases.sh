#!/bin/bash
# Database setup helper

echo "DATABASE SETUP HELPER"
echo "===================="
echo ""

echo "1. PostgreSQL Setup:"
echo "   Current connection failing for user 'postgres'"
echo ""
echo "   To fix PostgreSQL password:"
echo "   sudo -u postgres psql"
echo "   ALTER USER postgres PASSWORD 'your_new_password';"
echo "   \\q"
echo ""
echo "   Then update config.py DB_CONFIG password"
echo ""

echo "2. MySQL Setup:"
echo "   Check if MySQL is installed:"
sudo systemctl status mysql 2>/dev/null | head -5 || echo "MySQL not installed"
echo ""
echo "   If MySQL is not installed:"
echo "   sudo apt install mysql-server -y"
echo "   sudo mysql_secure_installation"
echo ""
echo "3. Test connections after fixing passwords:"
echo "   Run: python test_db_connection.py"
echo ""

echo "4. Quick test (skip database operations):"
echo "   Run ETL in test mode: ./run_all_etl_final.sh --test"
