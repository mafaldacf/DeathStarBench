#!/bin/bash
set -e

for db in movieid movieinfo moviereview reviewstorage plot castinfo; do
  echo "Creating database: $db"
  curl -s -X PUT "http://admin:admin@localhost:5984/$db"
done
