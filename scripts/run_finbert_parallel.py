#!/usr/bin/env python3
"""
Orchestrate CPU-parallel FinBERT scoring by ticker shards.

Spawns N worker processes, each calling finbert_score_tickers.py on a subset of tickers.
Safe to re-run: workers skip tickers already cached.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def available_tickers_from_parquet(root: Path) -> list[str]:
    out: list[str] = []
    for p in root.glob("ticker=*"):
        if p.is_dir():
            out.append(p.name.split("=", 1)[1])
    return sorted(out)


def chunk_list(xs: list[str], n: int) -> list[list[str]]:
    n = max(1, int(n))
    buckets = [[] for _ in range(n)]
    for i, x in enumerate(xs):
        buckets[i % n].append(x)
    return [b for b in buckets if b]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet-dir", required=True)
    ap.add_argument("--processed-dir", required=True)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--tickers", default="")  # optional comma list
    ap.add_argument("--years", default="")    # optional comma list
    ap.add_argument("--batch-size", type=int, default=64)
    ap.add_argument("--max-tokens", type=int, default=450)
    ap.add_argument("--model", default="ProsusAI/finbert")
    args = ap.parse_args()

    parquet_dir = Path(args.parquet_dir)
    processed_dir = Path(args.processed_dir)

    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    if not tickers:
        tickers = available_tickers_from_parquet(parquet_dir)

    shards = chunk_list(tickers, args.workers)
    worker_script = Path(__file__).with_name("finbert_score_tickers.py")

    procs: list[subprocess.Popen] = []
    for k, shard in enumerate(shards, 1):
        cmd = [
            sys.executable,
            str(worker_script),
            "--parquet-dir",
            str(parquet_dir),
            "--processed-dir",
            str(processed_dir),
            "--tickers",
            ",".join(shard),
            "--years",
            args.years,
            "--batch-size",
            str(args.batch_size),
            "--max-tokens",
            str(args.max_tokens),
            "--model",
            args.model,
        ]
        print(f"Starting worker {k}/{len(shards)}: {len(shard)} tickers")
        procs.append(subprocess.Popen(cmd))

    exit_codes = [p.wait() for p in procs]
    bad = [c for c in exit_codes if c != 0]
    if bad:
        print(f"Some workers failed: {exit_codes}")
        return 1

    print("All workers finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

