#!/usr/bin/env bash
source .env
source hosts

set -euo pipefail

POSTGRESQL_TABLES=(movieid movieinfo moviereview reviewstorage plot castinfo)

POSTGRES_USER=admin
POSTGRES_PASSWORD=admin
POSTGRES_DATABASE=mediamicroservices
HOST=localhost
PORT=5432

echo "[INFO] creating movieid table"
PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -h "$HOST" -p "$PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DATABASE" \
  -c "CREATE TABLE IF NOT EXISTS movieid (
        title VARCHAR(125) PRIMARY KEY,
        movie_id VARCHAR(125) UNIQUE
      );"

echo "[INFO] creating movieinfo table"
PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -h "$HOST" -p "$PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DATABASE" \
  -c "CREATE TABLE IF NOT EXISTS movieinfo (
        movie_id VARCHAR(125) PRIMARY KEY,
        title VARCHAR(125),
        plot_id INT
      );"

echo "[INFO] creating moviereview table"
PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -h "$HOST" -p "$PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DATABASE" \
  -c "CREATE TABLE IF NOT EXISTS moviereview (
        movie_id VARCHAR(125) PRIMARY KEY
      );"

echo "[INFO] creating reviewstorage table"
PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -h "$HOST" -p "$PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DATABASE" \
  -c "CREATE TABLE IF NOT EXISTS reviewstorage (
        movie_id VARCHAR(125) PRIMARY KEY
      );"

echo "[INFO] creating plot table"
PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -h "$HOST" -p "$PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DATABASE" \
  -c "CREATE TABLE IF NOT EXISTS plot (
        plot_id VARCHAR(125) PRIMARY KEY
      );"

echo "[INFO] creating castinfo table"
PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -h "$HOST" -p "$PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DATABASE" \
  -c "CREATE TABLE IF NOT EXISTS castinfo (
        cast_info_id VARCHAR(125) PRIMARY KEY
      );"

echo "[INFO] checking tables for node at $HOST:$PORT"
PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -h "$HOST" -p "$PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DATABASE" \
  -c "\dt public.*; SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('${POSTGRESQL_TABLES[*]}');"


echo "done!"
