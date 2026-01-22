#!/bin/sh
# Initialize Databend databases for PBench

DATABEND_HOST=${DATABEND_HOST:-localhost}
DATABEND_PORT=${DATABEND_PORT:-8000}
DATABEND_URL="http://${DATABEND_HOST}:${DATABEND_PORT}/v1/query/"

echo "Initializing Databend databases..."

# Function to execute SQL
execute_sql() {
    local sql="$1"
    curl -s -u "root:" --request POST "$DATABEND_URL" \
        --header 'Content-Type: application/json' \
        --data-raw "{\"sql\": \"$sql\"}" | grep -q '"state":"Succeeded"'

    if [ $? -eq 0 ]; then
        echo "✓ $sql"
    else
        echo "✗ Failed: $sql"
    fi
}

# Create TPCH databases
execute_sql "CREATE DATABASE IF NOT EXISTS tpch500m"
execute_sql "CREATE DATABASE IF NOT EXISTS tpch1g"
execute_sql "CREATE DATABASE IF NOT EXISTS tpch5g"
execute_sql "CREATE DATABASE IF NOT EXISTS tpch9g"

# Create TPCDS databases
execute_sql "CREATE DATABASE IF NOT EXISTS tpcds1g"
execute_sql "CREATE DATABASE IF NOT EXISTS tpcds2g"

# Create other databases
execute_sql "CREATE DATABASE IF NOT EXISTS imdb"
execute_sql "CREATE DATABASE IF NOT EXISTS llm"

# Create test tables in each database
for db in tpch500m tpch1g tpch5g tpch9g tpcds1g tpcds2g imdb llm; do
    execute_sql "CREATE TABLE IF NOT EXISTS $db.test_table (id INT, value VARCHAR)"
    execute_sql "INSERT INTO $db.test_table VALUES (1, 'test')"
done

echo ""
echo "Database initialization complete!"
echo ""
echo "Verify with:"
echo "curl -u 'root:' --request POST 'http://localhost:8000/v1/query/' \\"
echo "  --header 'Content-Type: application/json' \\"
echo "  --data-raw '{\"sql\": \"SHOW DATABASES\"}'"
