from __future__ import annotations

import ast
import hashlib
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from src.Workloads.create_redset_aggregates import (
    OUTPUT_COLUMNS,
    WindowSpec,
    build_rows,
    resolve_start,
    write_rows,
)


ROOT = Path(__file__).resolve().parents[2]
REDSET_PARQUET = ROOT / "original-workload-files" / "Redset" / "full.parquet"


class RedsetAggregateTests(unittest.TestCase):
    def _build_small_window(self):
        spec = WindowSpec(
            instance_id="0",
            database_id="0",
            start=datetime.fromisoformat("2024-03-01 01:00:00"),
            duration_seconds=600,
            slot_seconds=300,
            subslot_seconds=30,
        )
        rows = build_rows(
            str(REDSET_PARQUET),
            spec,
            seed=42,
            filter_mode="proxy",
            sort_mode="deterministic",
        )
        return spec, rows

    def test_schema_and_required_columns(self):
        spec, rows = self._build_small_window()
        self.assertEqual(len(rows), spec.slot_count)

        for row in rows:
            self.assertEqual(set(row.keys()), set(OUTPUT_COLUMNS))
            for arr_col in [
                "cputime_interval",
                "scanbytes_interval",
                "duration_interval",
                "memory_interval",
                "filter_interval",
                "sort_interval",
                "agg_interval",
                "join_interval",
            ]:
                arr = ast.literal_eval(row[arr_col])
                self.assertEqual(len(arr), spec.subslot_count)

    def test_invariants(self):
        _, rows = self._build_small_window()

        for row in rows:
            cpu_sum = float(row["cputime_sum"])
            scan_sum = float(row["scanbytes_sum"])
            cpu_interval = ast.literal_eval(row["cputime_interval"])
            scan_interval = ast.literal_eval(row["scanbytes_interval"])

            self.assertGreaterEqual(cpu_sum, 0.0)
            self.assertGreaterEqual(scan_sum, 0.0)
            self.assertGreaterEqual(sum(cpu_interval), 0.0)
            self.assertGreaterEqual(sum(scan_interval), 0.0)
            self.assertLessEqual(abs(sum(cpu_interval) - cpu_sum), 1e-5)
            self.assertLessEqual(abs(sum(scan_interval) - scan_sum), 1e-5)

            for k in ["join", "agg", "sort", "filter", "proj"]:
                v = row[k]
                if v is None:
                    continue
                val = float(v)
                self.assertGreaterEqual(val, 0.0)
                self.assertLessEqual(val, 1.0)

            for arr_col in ["filter_interval", "sort_interval", "agg_interval", "join_interval"]:
                arr = ast.literal_eval(row[arr_col])
                for x in arr:
                    self.assertGreaterEqual(float(x), 0.0)
                    self.assertLessEqual(float(x), 1.0)

    def test_deterministic_output(self):
        spec = WindowSpec(
            instance_id="0",
            database_id="0",
            start=datetime.fromisoformat("2024-03-01 01:00:00"),
            duration_seconds=600,
            slot_seconds=300,
            subslot_seconds=30,
        )
        rows_a = build_rows(
            str(REDSET_PARQUET),
            spec,
            seed=123,
            filter_mode="deterministic",
            sort_mode="deterministic",
        )
        rows_b = build_rows(
            str(REDSET_PARQUET),
            spec,
            seed=123,
            filter_mode="deterministic",
            sort_mode="deterministic",
        )
        self.assertEqual(rows_a, rows_b)

        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = Path(tmpdir) / "a.csv"
            path_b = Path(tmpdir) / "b.csv"
            write_rows(rows_a, str(path_a))
            write_rows(rows_b, str(path_b))
            h1 = hashlib.sha256(path_a.read_bytes()).hexdigest()
            h2 = hashlib.sha256(path_b.read_bytes()).hexdigest()
            self.assertEqual(h1, h2)

    def test_pipeline_columns_present(self):
        _, rows = self._build_small_window()
        self.assertTrue(rows)
        self.assertTrue(all(row["databaseid"] == "0:0" for row in rows))
        # ILP-required columns
        ilp_cols = {
            "cputime_sum",
            "scanbytes_sum",
            "avg_durationtime",
            "filter",
            "join",
            "agg",
            "sort",
        }
        # SA-required columns
        sa_cols = {
            "cputime_interval",
            "scanbytes_interval",
            "filter_interval",
            "sort_interval",
            "join_interval",
            "agg_interval",
        }
        cols = set(rows[0].keys())
        self.assertTrue(ilp_cols.issubset(cols))
        self.assertTrue(sa_cols.issubset(cols))

    def test_resolve_start_from_data_when_not_provided(self):
        dt = resolve_start(str(REDSET_PARQUET), "0", "0", None)
        # Redset timestamps are in 2024 for this dataset.
        self.assertEqual(dt.year, 2024)
        self.assertIsNotNone(dt.tzinfo)


if __name__ == "__main__":
    unittest.main()
