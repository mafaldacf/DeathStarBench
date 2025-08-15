#!/usr/bin/env bash
source .env
source hosts

set -euo pipefail

SCYLLA_KEYSPACE=mediamicroservices
HOST=localhost
PORT=9042

echo "[info] creating keyspace and tables on $MASTER_HOST:$MASTER_PORT"

cql=$(cat <<'CQL'
CREATE KEYSPACE IF NOT EXISTS mediamicroservices
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};`

USE mediamicroservices;

CREATE TABLE IF NOT EXISTS movieid_by_title (
  title text PRIMARY KEY,
  movie_id text
);

-- application-enforced uniqueness for movie_id:
CREATE TABLE IF NOT EXISTS movieid_by_movie_id (
  movie_id text PRIMARY KEY,
  title text
);

CREATE TABLE IF NOT EXISTS movieinfo (
  movie_id text PRIMARY KEY,
  title text,
  plot_id int
);

CREATE TABLE IF NOT EXISTS moviereview (
  movie_id text PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS reviewstorage (
  movie_id text PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS plot (
  plot_id text PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS castinfo (
  cast_info_id text PRIMARY KEY
);
CQL
)

echo "$cql" | cqlsh_cmd "$HOST" "$PORT"


echo "[info] verifying tables in keyspace $SCYLLA_KEYSPACE on $HOST:$PORT"
cqlsh_cmd "$HOST" "$PORT" -e "DESCRIBE TABLES IN $SCYLLA_KEYSPACE;"

echo "done!"
