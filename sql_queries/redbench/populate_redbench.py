#!/usr/bin/env python3
"""Populate redbench with all JOB queries and sampled CEB queries.

Default behavior:
- Copies all files from ../job into ./job/.
- Samples X SQL files per ../ceb/<template>/ into ./ceb/ (flat), prefixing
  filenames with the template name.

Sampling is deterministic with a base seed.
"""

from __future__ import annotations

import argparse
import hashlib
import random
import shutil
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate sql_queries/redbench with JOB and sampled CEB queries."
    )
    parser.add_argument(
        "-n",
        "--per-template",
        type=int,
        required=True,
        help="Number of random CEB queries to sample per template folder.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Base random seed (default: 42).",
    )
    parser.add_argument(
        "--clean-ceb",
        action="store_true",
        help="Delete redbench/ceb before writing sampled CEB queries.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete redbench/job and redbench/ceb before repopulating (keeps this script).",
    )
    parser.add_argument(
        "--skip-job-copy",
        action="store_true",
        help="Do not recopy JOB files into redbench/job.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned actions without copying/deleting files.",
    )
    return parser.parse_args()


def stable_template_rng(base_seed: int, template_name: str) -> random.Random:
    # Derive a stable per-template seed so adding/removing templates does not
    # change samples for other templates.
    seed_material = f"{base_seed}:{template_name}".encode("utf-8")
    digest = hashlib.sha256(seed_material).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def copy_job_files(job_dir: Path, redbench_job_dir: Path, dry_run: bool) -> int:
    if not job_dir.is_dir():
        raise FileNotFoundError(f"JOB directory not found: {job_dir}")

    if dry_run:
        print(f"[dry-run] ensure dir {redbench_job_dir}")
    else:
        redbench_job_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    for src in sorted(job_dir.iterdir(), key=lambda p: p.name):
        if not src.is_file():
            continue
        dst = redbench_job_dir / src.name
        if dry_run:
            print(f"[dry-run] copy JOB {src} -> {dst}")
        else:
            shutil.copy2(src, dst)
        copied += 1
    return copied


def remove_dir(path: Path, dry_run: bool) -> None:
    if not path.exists():
        return
    if dry_run:
        print(f"[dry-run] remove directory {path}")
    else:
        shutil.rmtree(path)


def sample_ceb_templates(
    ceb_dir: Path,
    redbench_ceb_dir: Path,
    per_template: int,
    seed: int,
    dry_run: bool,
) -> tuple[int, int]:
    if not ceb_dir.is_dir():
        raise FileNotFoundError(f"CEB directory not found: {ceb_dir}")

    template_count = 0
    total_sampled = 0

    if dry_run:
        print(f"[dry-run] ensure dir {redbench_ceb_dir}")
    else:
        redbench_ceb_dir.mkdir(parents=True, exist_ok=True)

    for template_dir in sorted(
        (p for p in ceb_dir.iterdir() if p.is_dir()), key=lambda p: p.name
    ):
        sql_files = sorted(
            [p for p in template_dir.iterdir() if p.is_file() and p.suffix == ".sql"],
            key=lambda p: p.name,
        )
        if not sql_files:
            print(f"warning: no .sql files in template {template_dir.name}", file=sys.stderr)
            continue

        template_count += 1
        n = min(per_template, len(sql_files))
        if n < per_template:
            print(
                f"warning: template {template_dir.name} has only {len(sql_files)} files; "
                f"sampling {n}",
                file=sys.stderr,
            )

        rng = stable_template_rng(seed, template_dir.name)
        chosen = sorted(rng.sample(sql_files, n), key=lambda p: p.name)

        for src in chosen:
            dst = redbench_ceb_dir / f"{template_dir.name}__{src.name}"
            if dry_run:
                print(f"[dry-run] copy CEB {src} -> {dst}")
            else:
                if dst.exists():
                    raise FileExistsError(f"Destination already exists: {dst}")
                shutil.copy2(src, dst)

        total_sampled += len(chosen)

    return template_count, total_sampled


def main() -> int:
    args = parse_args()
    if args.per_template < 0:
        print("--per-template must be >= 0", file=sys.stderr)
        return 2

    redbench_dir = Path(__file__).resolve().parent
    sql_queries_dir = redbench_dir.parent
    job_dir = sql_queries_dir / "job"
    ceb_dir = sql_queries_dir / "ceb"
    redbench_job_dir = redbench_dir / "job"
    redbench_ceb_dir = redbench_dir / "ceb"

    print(f"redbench_dir: {redbench_dir}")
    print(f"job_dir:     {job_dir}")
    print(f"ceb_dir:     {ceb_dir}")
    print(f"job_out_dir: {redbench_job_dir}")
    print(f"ceb_out_dir: {redbench_ceb_dir}")
    print(f"per_template: {args.per_template}")
    print(f"seed:         {args.seed}")

    if args.clean:
        remove_dir(redbench_job_dir, dry_run=args.dry_run)
        remove_dir(redbench_ceb_dir, dry_run=args.dry_run)
    elif args.clean_ceb:
        remove_dir(redbench_ceb_dir, dry_run=args.dry_run)

    if not args.skip_job_copy:
        copied_job = copy_job_files(job_dir, redbench_job_dir, dry_run=args.dry_run)
    else:
        copied_job = 0

    template_count, total_sampled = sample_ceb_templates(
        ceb_dir=ceb_dir,
        redbench_ceb_dir=redbench_ceb_dir,
        per_template=args.per_template,
        seed=args.seed,
        dry_run=args.dry_run,
    )

    print(
        "done: "
        f"job_files_copied={copied_job} "
        f"ceb_templates={template_count} "
        f"ceb_files_sampled={total_sampled}"
    )
    if args.dry_run:
        print("dry-run mode: no files were modified")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
