#!/bin/bash

# Drop existing database and user
sudo -u postgres psql -c "DROP DATABASE IF EXISTS betsson_dw;"
sudo -u postgres psql -c "DROP USER IF EXISTS betsson_user;"

# Find and update pg_hba.conf for password authentication
PG_HBA=$(sudo -u postgres psql -t -P format=unaligned -c 'SHOW hba_file;')
sudo sed -i '/^local.*all.*all.*peer$/s/peer/md5/' "$PG_HBA"
sudo systemctl restart postgresql

# Create new database and user
sudo -u postgres psql -c "CREATE USER betsson_user WITH PASSWORD 'secure_password';"
sudo -u postgres psql -c "CREATE DATABASE betsson_dw OWNER betsson_user;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE betsson_dw TO betsson_user;"

# Create schema
psql -U betsson_user -d betsson_dw -f src/sql/create_schema.sql
