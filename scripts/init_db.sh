#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE olist_raw;
    CREATE DATABASE olist_warehouse;
    GRANT ALL PRIVILEGES ON DATABASE olist_raw TO airflow;
    GRANT ALL PRIVILEGES ON DATABASE olist_warehouse TO airflow;
EOSQL
