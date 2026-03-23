#!/bin/bash
# Runs as /docker-entrypoint-initdb.d/00-init-databases.sh
# Creates additional databases and imports seed data before archival.sql runs.

set -e

echo "Creating additional databases..."
mysql -u root -p"${MARIADB_ROOT_PASSWORD}" <<-EOSQL
    CREATE DATABASE IF NOT EXISTS datacentre;
    CREATE DATABASE IF NOT EXISTS userconfig;
    CREATE DATABASE IF NOT EXISTS lapsed_users;
EOSQL

if [ -f /seed/userconfig.sql ]; then
    echo "Importing userconfig seed data..."
    mysql -u root -p"${MARIADB_ROOT_PASSWORD}" userconfig < /seed/userconfig.sql
    echo "userconfig import complete."
else
    echo "No userconfig.sql seed file found, skipping import."
fi
