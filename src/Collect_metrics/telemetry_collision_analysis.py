#!/usr/bin/env python3
"""
Telemetry Collision Analysis
=============================
Find queries with nearly identical telemetry (N-M matching problem)
that are structurally fundamentally different.

This demonstrates the flaw in telemetry-based synthesis:
two queries can look the same in metrics but be completely different SQL.
"""

import json
import re
import os
import sys
from itertools import combinations
from dataclasses import dataclass, field
from typing import List, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "metrics_witho", "output")


# ─── Query structural analysis ──────────────────────────────────────────────

@dataclass
class QueryStructure:
    """Structural fingerprint of a SQL query."""
    query_text: str
    source: str  # e.g., "TPCH-Q1", "TPCDS-Q14", "IMDB-1a"
    # Telemetry
    avg_cpu_time: float
    avg_scan_bytes: float
    avg_duration: float
    has_filter: bool
    has_join: bool
    has_agg: bool
    has_sort: bool
    # Structural features (extracted from SQL text)
    num_tables: int = 0
    num_joins: int = 0
    num_subqueries: int = 0
    has_cte: bool = False
    has_exists: bool = False
    has_in_subquery: bool = False
    has_union: bool = False
    has_case_when: bool = False
    has_having: bool = False
    has_limit: bool = False
    has_window_func: bool = False
    has_correlated_subquery: bool = False
    has_self_join: bool = False
    has_left_join: bool = False
    has_group_by: bool = False
    has_distinct: bool = False
    has_like: bool = False
    has_between: bool = False
    has_or_predicate: bool = False
    num_predicates: int = 0
    num_aggregations: int = 0
    num_sorts: int = 0
    tables: list = field(default_factory=list)
    query_type: str = ""  # "scan-heavy", "join-heavy", "subquery-nested", etc.

    @property
    def telemetry_vector(self):
        return (self.avg_cpu_time, self.avg_scan_bytes, self.avg_duration,
                int(self.has_filter), int(self.has_join),
                int(self.has_agg), int(self.has_sort))

    @property
    def operator_signature(self):
        return (int(self.has_filter), int(self.has_join),
                int(self.has_agg), int(self.has_sort))

    @property
    def structural_signature(self):
        """A rich structural fingerprint."""
        return (self.num_tables, self.num_joins, self.num_subqueries,
                self.has_cte, self.has_exists, self.has_in_subquery,
                self.has_union, self.has_case_when, self.has_having,
                self.has_window_func, self.has_correlated_subquery,
                self.has_self_join, self.has_left_join, self.has_or_predicate,
                self.num_predicates, self.num_aggregations, self.num_sorts)


def extract_structure(sql: str) -> dict:
    """Parse SQL text to extract structural features."""
    sql_upper = sql.upper()
    sql_clean = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)

    # Count FROM-clause tables (rough heuristic)
    # Match table references after FROM and JOIN
    from_matches = re.findall(r'\bFROM\b\s+(\w+)', sql_upper)
    join_matches = re.findall(r'\bJOIN\b\s+(\w+)', sql_upper)
    comma_tables = []
    # Count comma-separated tables in FROM clauses
    from_blocks = re.findall(r'\bFROM\b\s+((?:\w+(?:\s+(?:AS\s+)?\w+)?\s*,\s*)*\w+)', sql_upper)
    for block in from_blocks:
        parts = [p.strip().split()[0] for p in block.split(',') if p.strip()]
        comma_tables.extend(parts)

    all_tables = set(from_matches + join_matches + comma_tables)
    # Remove SQL keywords that might be false positives
    keywords = {'SELECT', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'UNION',
                'INSERT', 'UPDATE', 'DELETE', 'SET', 'VALUES', 'INTO', 'AS',
                'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR', 'NOT',
                'NULL', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS', 'ON',
                'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS', 'FULL',
                'ASC', 'DESC', 'BY', 'ALL', 'ANY', 'SOME', 'TRUE', 'FALSE',
                'WITH', 'RECURSIVE', 'LATERAL', 'ROLLUP', 'CUBE',
                'SUM', 'AVG', 'COUNT', 'MIN', 'MAX', 'STDDEV_SAMP'}
    all_tables = [t for t in all_tables if t not in keywords and len(t) > 1]

    # Count subqueries (nested SELECT)
    selects = re.findall(r'\bSELECT\b', sql_upper)
    num_subqueries = max(0, len(selects) - 1)

    # Count predicates (AND/OR in WHERE clauses, rough)
    where_matches = re.findall(r'\bWHERE\b', sql_upper)
    and_matches = re.findall(r'\bAND\b', sql_upper)
    or_matches = re.findall(r'\bOR\b', sql_upper)
    num_predicates = len(and_matches) + len(or_matches) + len(where_matches)

    # Count aggregation functions
    agg_funcs = re.findall(r'\b(SUM|AVG|COUNT|MIN|MAX|STDDEV_SAMP)\s*\(', sql_upper)

    # Self-join detection
    table_aliases = re.findall(r'\b(\w+)\s+(?:AS\s+)?(\w+)\b', sql_upper)
    table_names_list = [t[0] for t in table_aliases if t[0] not in keywords]
    has_self_join = len(table_names_list) != len(set(table_names_list))

    # Correlated subquery detection (subquery references outer table)
    has_correlated = bool(re.search(
        r'\(\s*SELECT.*?WHERE.*?\w+\.\w+\s*=\s*\w+\.\w+.*?\)',
        sql_upper, re.DOTALL
    )) and num_subqueries > 0

    return {
        'num_tables': len(all_tables),
        'tables': sorted(set(t.lower() for t in all_tables)),
        'num_joins': len(join_matches) + max(0, len(all_tables) - 1),
        'num_subqueries': num_subqueries,
        'has_cte': bool(re.search(r'\bWITH\b\s+\w+\s+AS\s*\(', sql_upper)),
        'has_exists': 'EXISTS' in sql_upper,
        'has_in_subquery': bool(re.search(r'\bIN\s*\(\s*SELECT\b', sql_upper)),
        'has_union': 'UNION' in sql_upper,
        'has_case_when': 'CASE' in sql_upper and 'WHEN' in sql_upper,
        'has_having': bool(re.search(r'\bHAVING\b', sql_upper)),
        'has_limit': 'LIMIT' in sql_upper,
        'has_window_func': bool(re.search(r'\bOVER\s*\(', sql_upper)),
        'has_correlated_subquery': has_correlated,
        'has_self_join': has_self_join,
        'has_left_join': 'LEFT' in sql_upper and 'JOIN' in sql_upper,
        'has_group_by': 'GROUP BY' in sql_upper,
        'has_distinct': 'DISTINCT' in sql_upper,
        'has_like': 'LIKE' in sql_upper,
        'has_between': 'BETWEEN' in sql_upper,
        'has_or_predicate': bool(re.search(r'\bWHERE\b.*\bOR\b', sql_upper, re.DOTALL)),
        'num_predicates': num_predicates,
        'num_aggregations': len(agg_funcs),
        'num_sorts': len(re.findall(r'\bORDER\s+BY\b', sql_upper)),
    }


def classify_query_type(qs: QueryStructure) -> str:
    """Classify query into a structural category."""
    types = []
    if qs.num_tables <= 1 and qs.num_subqueries == 0:
        types.append("single-table-scan")
    if qs.num_tables >= 5:
        types.append("multi-way-join")
    elif qs.num_tables >= 2 and qs.num_joins > 0:
        types.append("join")
    if qs.has_cte:
        types.append("CTE")
    if qs.has_exists or qs.has_in_subquery:
        types.append("semi-join")
    if qs.has_correlated_subquery:
        types.append("correlated-subquery")
    if qs.num_subqueries >= 2:
        types.append("deeply-nested")
    if qs.has_union:
        types.append("UNION")
    if qs.has_window_func:
        types.append("window-func")
    if qs.has_self_join:
        types.append("self-join")
    if qs.has_case_when and qs.num_aggregations >= 2:
        types.append("pivot-like")
    if qs.has_having:
        types.append("HAVING")
    if qs.has_or_predicate and qs.num_predicates >= 8:
        types.append("complex-predicate")
    if not types:
        types.append("simple")
    return " + ".join(types)


# ─── Telemetry distance metrics ─────────────────────────────────────────────

def telemetry_distance(a: QueryStructure, b: QueryStructure) -> float:
    """Normalized distance between two queries in telemetry space.
    Returns 0.0 for identical telemetry, 1.0+ for very different."""
    # Normalize each dimension
    cpu_diff = abs(a.avg_cpu_time - b.avg_cpu_time) / max(a.avg_cpu_time, b.avg_cpu_time, 0.001)
    scan_diff = abs(a.avg_scan_bytes - b.avg_scan_bytes) / max(a.avg_scan_bytes, b.avg_scan_bytes, 1.0)
    dur_diff = abs(a.avg_duration - b.avg_duration) / max(a.avg_duration, b.avg_duration, 0.001)
    # Operator signature match (0 or 1 per dimension)
    op_diff = sum(x != y for x, y in zip(a.operator_signature, b.operator_signature)) / 4.0
    # Weighted combination
    return 0.30 * cpu_diff + 0.30 * scan_diff + 0.25 * dur_diff + 0.15 * op_diff


def structural_distance(a: QueryStructure, b: QueryStructure) -> float:
    """Measure how structurally different two queries are.
    Higher = more different."""
    score = 0.0
    # Table count difference
    score += abs(a.num_tables - b.num_tables) * 1.5
    # Subquery depth difference
    score += abs(a.num_subqueries - b.num_subqueries) * 3.0
    # Join count difference
    score += abs(a.num_joins - b.num_joins) * 1.0
    # Predicate count difference
    score += abs(a.num_predicates - b.num_predicates) * 0.5
    # Aggregation count difference
    score += abs(a.num_aggregations - b.num_aggregations) * 1.0
    # Sort count difference
    score += abs(a.num_sorts - b.num_sorts) * 1.0
    # Boolean feature differences (each mismatch = 2 points)
    bool_features = [
        'has_cte', 'has_exists', 'has_in_subquery', 'has_union',
        'has_case_when', 'has_having', 'has_window_func',
        'has_correlated_subquery', 'has_self_join', 'has_left_join',
        'has_or_predicate', 'has_distinct', 'has_like', 'has_between'
    ]
    for feat in bool_features:
        if getattr(a, feat) != getattr(b, feat):
            score += 2.0
    # Table overlap (Jaccard distance)
    if a.tables and b.tables:
        set_a, set_b = set(a.tables), set(b.tables)
        jaccard = 1.0 - len(set_a & set_b) / max(len(set_a | set_b), 1)
        score += jaccard * 5.0
    # Query type mismatch
    if a.query_type != b.query_type:
        score += 5.0
    return score


# ─── Load and process ───────────────────────────────────────────────────────

def load_metrics(filepath: str, workload_name: str) -> List[QueryStructure]:
    """Load a metrics JSON file and extract structures."""
    with open(filepath) as f:
        data = json.load(f)

    queries = []
    for i, entry in enumerate(data):
        sql = entry['query'].split('@')[0].strip()

        # Extract structural features
        struct = extract_structure(sql)

        qs = QueryStructure(
            query_text=sql,
            source=f"{workload_name}-Q{i+1}",
            avg_cpu_time=entry.get('avg_cpu_time', 0),
            avg_scan_bytes=entry.get('avg_scan_bytes', 0),
            avg_duration=entry.get('avg_duration', 0),
            has_filter=bool(entry.get('filter', 0)),
            has_join=bool(entry.get('join', 0)),
            has_agg=bool(entry.get('agg', 0)),
            has_sort=bool(entry.get('sort', 0)),
            **struct
        )
        qs.query_type = classify_query_type(qs)
        queries.append(qs)

    return queries


def find_collisions(queries: List[QueryStructure],
                    telemetry_threshold: float = 0.25,
                    structural_threshold: float = 10.0,
                    top_n: int = 20) -> List[Tuple]:
    """Find pairs with low telemetry distance but high structural distance."""
    collisions = []

    for a, b in combinations(queries, 2):
        t_dist = telemetry_distance(a, b)
        if t_dist > telemetry_threshold:
            continue
        s_dist = structural_distance(a, b)
        if s_dist < structural_threshold:
            continue
        # Collision ratio: how much more structurally different vs telemetry similar
        ratio = s_dist / max(t_dist, 0.001)
        collisions.append((a, b, t_dist, s_dist, ratio))

    collisions.sort(key=lambda x: x[4], reverse=True)
    return collisions[:top_n]


# ─── Display ─────────────────────────────────────────────────────────────────

def truncate_sql(sql: str, max_len: int = 120) -> str:
    """Truncate SQL for display."""
    sql = ' '.join(sql.split())
    if len(sql) > max_len:
        return sql[:max_len] + "..."
    return sql


def print_collision(idx: int, a: QueryStructure, b: QueryStructure,
                    t_dist: float, s_dist: float, ratio: float):
    """Pretty-print a collision pair."""
    print(f"\n{'='*80}")
    print(f"  COLLISION #{idx}: Telemetry distance = {t_dist:.3f} | "
          f"Structural distance = {s_dist:.1f} | Ratio = {ratio:.1f}x")
    print(f"{'='*80}")

    for label, q in [("Query A", a), ("Query B", b)]:
        print(f"\n  [{label}] {q.source}")
        print(f"    Type:       {q.query_type}")
        print(f"    SQL:        {truncate_sql(q.query_text)}")
        print(f"    Tables:     {q.num_tables} ({', '.join(q.tables[:6])}{'...' if len(q.tables) > 6 else ''})")
        print(f"    Joins:      {q.num_joins}    Subqueries: {q.num_subqueries}    Predicates: {q.num_predicates}    Sorts: {q.num_sorts}")
        print(f"    Aggregates: {q.num_aggregations}")
        features = []
        if q.has_cte: features.append("CTE")
        if q.has_exists: features.append("EXISTS")
        if q.has_in_subquery: features.append("IN(subq)")
        if q.has_union: features.append("UNION")
        if q.has_case_when: features.append("CASE/WHEN")
        if q.has_having: features.append("HAVING")
        if q.has_window_func: features.append("WINDOW")
        if q.has_correlated_subquery: features.append("CORRELATED")
        if q.has_self_join: features.append("SELF-JOIN")
        if q.has_left_join: features.append("LEFT JOIN")
        if q.has_or_predicate: features.append("OR-predicate")
        if q.has_distinct: features.append("DISTINCT")
        print(f"    Features:   {', '.join(features) if features else '(none)'}")

    print(f"\n  Telemetry comparison:")
    print(f"    {'Metric':<18} {'Query A':>14} {'Query B':>14} {'Diff%':>10}")
    print(f"    {'─'*18} {'─'*14} {'─'*14} {'─'*10}")

    for name, va, vb in [
        ("CPU time (ms)", a.avg_cpu_time, b.avg_cpu_time),
        ("Scan bytes", a.avg_scan_bytes, b.avg_scan_bytes),
        ("Duration (s)", a.avg_duration, b.avg_duration),
    ]:
        denom = max(va, vb, 0.001)
        diff_pct = abs(va - vb) / denom * 100
        if name == "Scan bytes":
            print(f"    {name:<18} {va:>14,.0f} {vb:>14,.0f} {diff_pct:>9.1f}%")
        else:
            print(f"    {name:<18} {va:>14.3f} {vb:>14.3f} {diff_pct:>9.1f}%")

    print(f"    Operator sig:   A={a.operator_signature}  B={b.operator_signature}  "
          f"{'SAME' if a.operator_signature == b.operator_signature else 'DIFFER'}")

    # Structural differences
    diffs = []
    if a.num_tables != b.num_tables:
        diffs.append(f"tables: {a.num_tables} vs {b.num_tables}")
    if a.num_subqueries != b.num_subqueries:
        diffs.append(f"subqueries: {a.num_subqueries} vs {b.num_subqueries}")
    if a.has_cte != b.has_cte:
        diffs.append(f"CTE: {a.has_cte} vs {b.has_cte}")
    if a.has_exists != b.has_exists:
        diffs.append(f"EXISTS: {a.has_exists} vs {b.has_exists}")
    if a.has_union != b.has_union:
        diffs.append(f"UNION: {a.has_union} vs {b.has_union}")
    if a.has_window_func != b.has_window_func:
        diffs.append(f"WINDOW: {a.has_window_func} vs {b.has_window_func}")
    if a.has_correlated_subquery != b.has_correlated_subquery:
        diffs.append(f"CORRELATED: {a.has_correlated_subquery} vs {b.has_correlated_subquery}")
    if a.query_type != b.query_type:
        diffs.append(f"type: [{a.query_type}] vs [{b.query_type}]")

    table_overlap = set(a.tables) & set(b.tables)
    table_only_a = set(a.tables) - set(b.tables)
    table_only_b = set(b.tables) - set(a.tables)
    if table_only_a or table_only_b:
        diffs.append(f"tables only in A: {table_only_a or '∅'}")
        diffs.append(f"tables only in B: {table_only_b or '∅'}")

    print(f"\n  Key structural differences:")
    for d in diffs:
        print(f"    → {d}")


def print_summary_table(collisions: List[Tuple]):
    """Print a compact summary table."""
    print(f"\n{'='*100}")
    print(f"  SUMMARY: Top {len(collisions)} Telemetry Collisions (similar metrics, different structure)")
    print(f"{'='*100}")
    print(f"  {'#':<3} {'Query A':<16} {'Query B':<16} {'Tel.Dist':>9} {'Str.Dist':>9} "
          f"{'Ratio':>7} {'Type A':<25} {'Type B':<25}")
    print(f"  {'─'*3} {'─'*16} {'─'*16} {'─'*9} {'─'*9} {'─'*7} {'─'*25} {'─'*25}")

    for i, (a, b, t_dist, s_dist, ratio) in enumerate(collisions, 1):
        type_a = a.query_type[:24]
        type_b = b.query_type[:24]
        print(f"  {i:<3} {a.source:<16} {b.source:<16} {t_dist:>9.3f} {s_dist:>9.1f} "
              f"{ratio:>6.0f}x {type_a:<25} {type_b:<25}")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print("Telemetry Collision Analysis")
    print("=" * 60)
    print("Finding queries with similar telemetry but different structure")
    print()

    # Load all workloads
    all_queries = []
    files = [
        ("TPCH-tpch1g-sql-metrics.json", "TPCH"),
        ("tpcds_all-tpcds1g-sql-metrics.json", "TPCDS"),
        ("imdb-imdb-sql-metrics.json", "IMDB"),
    ]

    for filename, workload in files:
        filepath = os.path.join(OUTPUT_DIR, filename)
        if os.path.exists(filepath):
            queries = load_metrics(filepath, workload)
            print(f"  Loaded {len(queries):>4} queries from {workload}")
            all_queries.extend(queries)
        else:
            print(f"  SKIP: {filename} not found")

    print(f"\n  Total: {len(all_queries)} queries")
    total_pairs = len(all_queries) * (len(all_queries) - 1) // 2
    print(f"  Analyzing {total_pairs:,} pairs...\n")

    # Find collisions - within same workload and across workloads
    print("\n" + "=" * 60)
    print("  CROSS-WORKLOAD COLLISIONS")
    print("  (Queries from DIFFERENT benchmarks with similar telemetry)")
    print("=" * 60)

    cross_collisions = find_collisions(
        all_queries,
        telemetry_threshold=0.30,
        structural_threshold=8.0,
        top_n=15
    )

    if cross_collisions:
        print_summary_table(cross_collisions)
        # Show top 5 in detail
        for i, (a, b, t_dist, s_dist, ratio) in enumerate(cross_collisions[:5], 1):
            print_collision(i, a, b, t_dist, s_dist, ratio)
    else:
        print("  No cross-workload collisions found. Relaxing thresholds...")
        cross_collisions = find_collisions(
            all_queries,
            telemetry_threshold=0.40,
            structural_threshold=5.0,
            top_n=15
        )
        if cross_collisions:
            print_summary_table(cross_collisions)
            for i, (a, b, t_dist, s_dist, ratio) in enumerate(cross_collisions[:5], 1):
                print_collision(i, a, b, t_dist, s_dist, ratio)

    # Within-workload analysis
    for workload_name in ["TPCH", "TPCDS", "IMDB"]:
        wl_queries = [q for q in all_queries if q.source.startswith(workload_name)]
        if len(wl_queries) < 2:
            continue

        print(f"\n\n{'='*60}")
        print(f"  WITHIN {workload_name}: Telemetry Collisions")
        print(f"{'='*60}")

        wl_collisions = find_collisions(
            wl_queries,
            telemetry_threshold=0.35,
            structural_threshold=5.0,
            top_n=10
        )

        if not wl_collisions:
            wl_collisions = find_collisions(
                wl_queries,
                telemetry_threshold=0.45,
                structural_threshold=3.0,
                top_n=10
            )

        if wl_collisions:
            print_summary_table(wl_collisions)
            for i, (a, b, t_dist, s_dist, ratio) in enumerate(wl_collisions[:3], 1):
                print_collision(i, a, b, t_dist, s_dist, ratio)
        else:
            print("  No significant collisions found in this workload.")

    # Final analysis: operator signature collision stats
    print(f"\n\n{'='*60}")
    print("  OPERATOR SIGNATURE ANALYSIS")
    print(f"{'='*60}")
    print("  How many structurally different queries share the same")
    print("  operator signature (filter/join/agg/sort)?")
    print()

    from collections import defaultdict
    sig_groups = defaultdict(list)
    for q in all_queries:
        sig_groups[q.operator_signature].append(q)

    for sig, group in sorted(sig_groups.items(), key=lambda x: -len(x[1])):
        sig_str = f"filter={sig[0]} join={sig[1]} agg={sig[2]} sort={sig[3]}"
        types = set(q.query_type for q in group)
        sources = set(q.source.split('-')[0] for q in group)
        print(f"  Signature ({sig_str}): {len(group)} queries, "
              f"{len(types)} structural types, workloads: {sources}")
        if len(types) > 1:
            for t in sorted(types):
                count = sum(1 for q in group if q.query_type == t)
                print(f"    - {t}: {count} queries")

    # Print the key insight
    print(f"\n\n{'='*60}")
    print("  KEY INSIGHT: The N-M Matching Problem")
    print(f"{'='*60}")
    most_common_sig = max(sig_groups.items(), key=lambda x: len(x[1]))
    sig, group = most_common_sig
    types = set(q.query_type for q in group)
    print(f"\n  The most common operator signature ({sig}) matches "
          f"{len(group)} queries")
    print(f"  across {len(types)} fundamentally different structural types.")
    print(f"\n  This means telemetry-based synthesis would treat ALL of these")
    print(f"  as interchangeable — but they stress completely different")
    print(f"  engine components (hash joins, sort-merge, nested loops,")
    print(f"  subquery evaluation, window functions, CTE materialization, etc.)")

    # Scan bytes collision: queries with very different scan volumes
    # but same CPU and duration
    print(f"\n\n{'='*60}")
    print("  SCAN VOLUME VS DURATION DECOUPLING")
    print(f"{'='*60}")
    print("  Queries where scan bytes differ 10x+ but duration is similar:")
    print()

    scan_decoupled = []
    for a, b in combinations(all_queries, 2):
        dur_diff = abs(a.avg_duration - b.avg_duration) / max(a.avg_duration, b.avg_duration, 0.001)
        scan_ratio = max(a.avg_scan_bytes, b.avg_scan_bytes) / max(min(a.avg_scan_bytes, b.avg_scan_bytes), 1)
        if dur_diff < 0.20 and scan_ratio > 10:
            scan_decoupled.append((a, b, dur_diff, scan_ratio))

    scan_decoupled.sort(key=lambda x: x[3], reverse=True)
    if scan_decoupled:
        print(f"  Found {len(scan_decoupled)} pairs. Top examples:")
        print(f"  {'Query A':<16} {'Query B':<16} {'Dur A':>8} {'Dur B':>8} {'Scan A':>14} {'Scan B':>14} {'Scan Ratio':>11}")
        print(f"  {'─'*16} {'─'*16} {'─'*8} {'─'*8} {'─'*14} {'─'*14} {'─'*11}")
        for a, b, dur_diff, scan_ratio in scan_decoupled[:10]:
            print(f"  {a.source:<16} {b.source:<16} "
                  f"{a.avg_duration:>8.3f} {b.avg_duration:>8.3f} "
                  f"{a.avg_scan_bytes:>14,.0f} {b.avg_scan_bytes:>14,.0f} "
                  f"{scan_ratio:>10.1f}x")
        print(f"\n  → Scan bytes alone cannot predict query duration!")
        print(f"    A query scanning 100x more data can finish in the same time")
        print(f"    as one scanning very little (due to different join strategies,")
        print(f"    predicate selectivity, caching effects, etc.)")


if __name__ == '__main__':
    main()
