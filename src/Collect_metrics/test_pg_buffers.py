"""Quick test to check if PG root node buffers are cumulative or not."""
import json
import psycopg2

conn = psycopg2.connect(dbname="tpch20g", port=5432)
conn.autocommit = True

# Simple query with a known plan shape (join = parent with children)
sql = """EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT COUNT(*) FROM lineitem l JOIN orders o ON l.l_orderkey = o.o_orderkey
WHERE l.l_shipdate < '1994-01-01' AND o.o_orderdate < '1994-01-01'"""

with conn.cursor() as cur:
    cur.execute(sql)
    plan = cur.fetchone()[0][0]

root = plan["Plan"]

def print_node(node, depth=0):
    indent = "  " * depth
    hit = node.get("Shared Hit Blocks", 0)
    read = node.get("Shared Read Blocks", 0)
    print(f"{indent}{node['Node Type']}: hit={hit}, read={read}, total={hit+read}")
    for child in node.get("Plans", []):
        print_node(child, depth + 1)

print("=== Buffer stats per node ===")
print_node(root)

# Now sum recursively
def sum_all(node):
    h = node.get("Shared Hit Blocks", 0)
    r = node.get("Shared Read Blocks", 0)
    for child in node.get("Plans", []):
        ch, cr = sum_all(child)
        h += ch
        r += cr
    return h, r

root_hit = root.get("Shared Hit Blocks", 0)
root_read = root.get("Shared Read Blocks", 0)
sum_hit, sum_read = sum_all(root)

print(f"\n=== Comparison ===")
print(f"Root only:       hit={root_hit}, read={root_read}, total={root_hit+root_read}")
print(f"Recursive sum:   hit={sum_hit}, read={sum_read}, total={sum_hit+sum_read}")
print(f"Ratio (sum/root): {(sum_hit+sum_read)/(root_hit+root_read):.2f}x")

conn.close()
