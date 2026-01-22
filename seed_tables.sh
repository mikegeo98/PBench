#!/bin/sh
# Create placeholder tables and seed a few rows for all benchmark databases.

DATABEND_HOST=${DATABEND_HOST:-localhost}
DATABEND_PORT=${DATABEND_PORT:-8000}
DATABEND_URL="http://${DATABEND_HOST}:${DATABEND_PORT}/v1/query/"

execute_sql() {
  sql="$1"
  curl -s -u "root:" --request POST "$DATABEND_URL" \
    --header 'Content-Type: application/json' \
    --data-raw "{\"sql\": \"$sql\"}" >/dev/null
}

create_and_seed_db() {
  db="$1"
  execute_sql "CREATE DATABASE IF NOT EXISTS ${db}"
  for tbl in table_0 table_1 table_2 table_3 table_4; do
    execute_sql "CREATE TABLE IF NOT EXISTS ${db}.${tbl} (id INT, col1 INT, col2 INT, col3 INT)"
    execute_sql "INSERT INTO ${db}.${tbl} VALUES (1, 10, 1, 1), (2, 20, 2, 2), (3, 30, 3, 3)"
  done
}

for db in tpch500m tpch1g tpch5g tpch9g tpcds1g tpcds2g imdb llm; do
  echo "Creating tables in database $db"
  create_and_seed_db "$db"
done

echo "Seed tables created."
