from __future__ import annotations

import ast
import csv
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from src.Workloads.create_snowset_aggregates import WindowSpec, build_rows


ROOT = Path(__file__).resolve().parents[2]
GOLDEN_DIR = ROOT / "src" / "Workloads" / "Snowset"
SNOWSET_MAIN_PARQUET = ROOT / "original-workload-files" / "Snowset" / "snowset-main.parquet"
TS_EXPLOSION_PARQUET = ROOT / "original-workload-files" / "Snowset" / "ts-explosion.parquet"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _to_float_or_none(v: str) -> float | None:
    s = str(v).strip()
    if s == "":
        return None
    return float(s)


class SnowsetAggregateTests(unittest.TestCase):
    maxDiff = None

    def assert_close(self, a: float | None, b: float | None, tol: float = 3e-6) -> None:
        if a is None or b is None:
            self.assertIsNone(a)
            self.assertIsNone(b)
            return
        self.assertLessEqual(abs(a - b), tol)

    def assert_array_close(self, a: str, b: str, tol: float = 3e-6) -> None:
        arr_a = ast.literal_eval(a)
        arr_b = ast.literal_eval(b)
        self.assertEqual(len(arr_a), len(arr_b))
        for x, y in zip(arr_a, arr_b):
            self.assertLessEqual(abs(float(x) - float(y)), tol)

    def test_window_spec_validation(self):
        spec = WindowSpec(
            database_id="1",
            start=datetime(2018, 2, 22, 8, 35),
            duration_seconds=3600,
            slot_seconds=300,
            subslot_seconds=30,
        )
        self.assertEqual(spec.slot_count, 12)
        self.assertEqual(spec.subslot_count, 10)
        self.assertEqual(spec.end, spec.start + timedelta(seconds=3600))

    def test_rebuild_existing_snowset_goldens_from_parquet(self):
        golden_files = sorted(GOLDEN_DIR.glob("workload1h-5m-30s_*.csv"))
        self.assertTrue(golden_files, "No Snowset golden CSV files found.")

        for golden_path in golden_files:
            with self.subTest(golden=golden_path.name):
                golden_rows = _read_csv(golden_path)
                self.assertTrue(golden_rows, f"{golden_path} is empty")

                first = golden_rows[0]
                dbid = first["databaseid"]
                start = datetime.fromisoformat(first["qminute"])
                slot_seconds = 300
                if len(golden_rows) > 1:
                    t1 = datetime.fromisoformat(golden_rows[1]["qminute"])
                    slot_seconds = int((t1 - start).total_seconds())
                duration_seconds = slot_seconds * len(golden_rows)
                spec = WindowSpec(
                    database_id=dbid,
                    start=start,
                    duration_seconds=duration_seconds,
                    slot_seconds=slot_seconds,
                    subslot_seconds=30,
                )

                rebuilt_rows = build_rows(
                    str(SNOWSET_MAIN_PARQUET),
                    str(TS_EXPLOSION_PARQUET),
                    spec,
                )
                self.assertEqual(len(rebuilt_rows), len(golden_rows))

                for rebuilt, golden in zip(rebuilt_rows, golden_rows):
                    self.assertEqual(str(rebuilt["databaseid"]), str(golden["databaseid"]))
                    self.assertEqual(rebuilt["qminute"], golden["qminute"])

                    for key in [
                        "cputime_sum",
                        "scanbytes_sum",
                        "avg_durationtime",
                        "avg_memoryused",
                        "join",
                        "agg",
                        "sort",
                        "filter",
                        "proj",
                    ]:
                        self.assert_close(
                            _to_float_or_none(
                                str(rebuilt[key]) if rebuilt[key] is not None else ""
                            ),
                            _to_float_or_none(golden[key]),
                            tol=3e-6,
                        )

                    for key in [
                        "cputime_interval",
                        "scanbytes_interval",
                        "duration_interval",
                        "memory_interval",
                        "filter_interval",
                        "sort_interval",
                        "agg_interval",
                        "join_interval",
                    ]:
                        self.assert_array_close(str(rebuilt[key]), golden[key], tol=3e-6)


if __name__ == "__main__":
    unittest.main()
