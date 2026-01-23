#!/usr/bin/env python3
"""Execute DDL SQL files against Databend."""
import argparse
import json
import os
import re
import sys
import requests
from pathlib import Path


def execute_sql(sql: str, host: str, port: int) -> dict:
    """Execute a single SQL statement."""
    url = f"http://{host}:{port}/v1/query/"
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        url,
        headers=headers,
        auth=("root", ""),
        json={"sql": sql}
    )
    return response.json()


def run_ddl_file(filepath: Path, host: str, port: int, verbose: bool = True):
    """Run all SQL statements from a DDL file."""
    content = filepath.read_text()

    # Remove comments and split by semicolons
    # Remove single-line comments
    content = re.sub(r'--.*$', '', content, flags=re.MULTILINE)
    # Remove multi-line comments
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)

    # Split by semicolons, keeping track of statements
    statements = [s.strip() for s in content.split(';') if s.strip()]

    print(f"\n{'='*60}")
    print(f"Running: {filepath.name}")
    print(f"{'='*60}")

    success_count = 0
    error_count = 0

    for stmt in statements:
        if not stmt:
            continue

        # Get a short description of the statement
        first_line = stmt.split('\n')[0][:60]

        result = execute_sql(stmt, host=host, port=port)

        if result.get("error"):
            error_msg = result["error"].get("message", "Unknown error")
            # Truncate long error messages
            if len(error_msg) > 100:
                error_msg = error_msg[:100] + "..."
            print(f"✗ {first_line}...")
            print(f"  Error: {error_msg}")
            error_count += 1
        else:
            if verbose:
                print(f"✓ {first_line}...")
            success_count += 1

    print(f"\nSummary: {success_count} succeeded, {error_count} failed")
    return success_count, error_count


def parse_args():
    parser = argparse.ArgumentParser(description="Run DDL files against Databend.")
    parser.add_argument(
        "--host",
        default=os.getenv("DATABEND_HOST", "localhost"),
        help="Databend host (default: env DATABEND_HOST or localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("DATABEND_PORT", 8000)),
        help="Databend HTTP port (default: env DATABEND_PORT or 8000)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    ddl_dir = Path(__file__).parent

    # Run DDL files in order
    ddl_files = sorted(ddl_dir.glob("*.sql"))

    total_success = 0
    total_errors = 0

    for ddl_file in ddl_files:
        success, errors = run_ddl_file(ddl_file, host=args.host, port=args.port)
        total_success += success
        total_errors += errors

    print(f"\n{'='*60}")
    print(f"Total: {total_success} succeeded, {total_errors} failed")
    print(f"{'='*60}")

    return 0 if total_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
