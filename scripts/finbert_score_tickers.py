#!/usr/bin/env python3
"""
Score FinBERT per ticker (CPU-safe parallel worker).

Designed to be launched as multiple independent processes from a notebook:
each process receives a shard of tickers and writes per-ticker parquet files.

Outputs:
  <processed_dir>/finbert_by_ticker/ticker=<TICKER>.parquet
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd


def available_tickers_from_parquet(root: Path) -> list[str]:
    out: list[str] = []
    for p in root.glob("ticker=*"):
        if p.is_dir():
            out.append(p.name.split("=", 1)[1])
    return sorted(out)


def chunk_text(text: str, max_tokens: int) -> list[str]:
    if not isinstance(text, str) or not text.strip():
        return []
    words = text.split()
    # heuristic chunking; we still enforce truncation at inference
    step = max(50, int(max_tokens * 0.75))
    return [" ".join(words[i : i + step]) for i in range(0, len(words), step)]


def aggregate_chunk_scores(chunk_res_list: list[dict]) -> dict:
    vals = {"fb_positive": [], "fb_negative": [], "fb_neutral": []}
    for d in chunk_res_list:
        for k in vals:
            v = d.get(k)
            if v is not None:
                vals[k].append(v)
    if all(len(vals[k]) == 0 for k in vals):
        return {"fb_positive": None, "fb_negative": None, "fb_neutral": None}
    return {k: float(np.mean(vals[k])) if len(vals[k]) else None for k in vals}


def score_one_ticker(
    *,
    finbert,
    parquet_dir: Path,
    processed_dir: Path,
    ticker: str,
    max_tokens: int,
    batch_size: int,
    years: list[int] | None,
) -> None:
    out_dir = processed_dir / "finbert_by_ticker"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"ticker={ticker}.parquet"

    if out_path.exists():
        print(f"[{ticker}] cached — skipping.")
        return

    id_cols = ["transcriptid", "transcriptcomponentid", "ticker", "call_year"]
    filters = [("ticker", "in", [ticker])]
    if years:
        filters.append(("call_year", "in", years))

    to_score = pd.read_parquet(
        parquet_dir,
        engine="pyarrow",
        filters=filters or None,
        columns=id_cols + ["componenttext"],
    ).reset_index(drop=True)

    print(f"[{ticker}] turns: {len(to_score):,}")
    t0 = time.time()

    chunk_texts: list[tuple[int, int, str]] = []
    row_chunk_results: dict[int, list[dict]] = {}
    scored_dict: dict[int, dict] = {}

    for row_idx, text in enumerate(to_score["componenttext"].fillna("")):
        chunks = chunk_text(text, max_tokens=max_tokens)
        if not chunks:
            scored_dict[row_idx] = {"fb_positive": None, "fb_negative": None, "fb_neutral": None}
            continue
        row_chunk_results[row_idx] = []
        for j, ch in enumerate(chunks):
            chunk_texts.append((row_idx, j, ch))

    for i in range(0, len(chunk_texts), batch_size):
        batch = chunk_texts[i : i + batch_size]
        texts = [t for (_, _, t) in batch]
        outputs = finbert(texts, truncation=True, max_length=512)

        for (row_idx, _, _), out in zip(batch, outputs):
            label_map = {d["label"].lower(): float(d["score"]) for d in out}
            row_chunk_results[row_idx].append(
                {
                    "fb_positive": label_map.get("positive"),
                    "fb_negative": label_map.get("negative"),
                    "fb_neutral": label_map.get("neutral"),
                }
            )

    for row_idx, chunk_res in row_chunk_results.items():
        scored_dict[row_idx] = aggregate_chunk_scores(chunk_res)

    scores_df = pd.DataFrame([{"row_idx": k, **v} for k, v in scored_dict.items()]).set_index("row_idx")

    fb_df = to_score[id_cols].copy().join(scores_df)
    fb_df["fb_net"] = fb_df["fb_positive"] - fb_df["fb_negative"]

    probs = fb_df[["fb_positive", "fb_negative", "fb_neutral"]]
    all_na = probs.isna().all(axis=1)
    fb_df["fb_label"] = "na"
    fb_df.loc[~all_na, "fb_label"] = (
        probs.loc[~all_na].fillna(-1.0).idxmax(axis=1).str.replace("fb_", "", regex=False)
    )

    fb_df.to_parquet(out_path, index=False, engine="pyarrow")
    print(f"[{ticker}] wrote {len(fb_df):,} → {out_path} ({(time.time()-t0)/60:.1f} min)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet-dir", required=True)
    ap.add_argument("--processed-dir", required=True)
    ap.add_argument("--tickers", default="")
    ap.add_argument("--workers", type=int, default=1)  # unused here; orchestrator handles workers
    ap.add_argument("--batch-size", type=int, default=64)
    ap.add_argument("--max-tokens", type=int, default=450)
    ap.add_argument("--years", default="")
    ap.add_argument("--model", default="ProsusAI/finbert")
    args = ap.parse_args()

    parquet_dir = Path(args.parquet_dir)
    processed_dir = Path(args.processed_dir)

    years = [int(x) for x in args.years.split(",") if x.strip()] if args.years else None

    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    if not tickers:
        tickers = available_tickers_from_parquet(parquet_dir)

    # Limit threads per process so 4 workers don't oversubscribe.
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    os.environ.setdefault("VECLIB_MAXIMUM_THREADS", "1")
    os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")

    from transformers import AutoTokenizer, pipeline
    import torch

    # Also cap PyTorch threads explicitly (important on CPU when multi-processing).
    try:
        torch.set_num_threads(1)
        torch.set_num_interop_threads(1)
    except Exception:
        pass

    print(f"Loading {args.model} (CPU) ...")
    tok = AutoTokenizer.from_pretrained(args.model)
    finbert = pipeline(
        "text-classification",
        model=args.model,
        tokenizer=tok,
        top_k=None,
        device=-1,
        batch_size=args.batch_size,
    )

    for t in tickers:
        score_one_ticker(
            finbert=finbert,
            parquet_dir=parquet_dir,
            processed_dir=processed_dir,
            ticker=t,
            max_tokens=args.max_tokens,
            batch_size=args.batch_size,
            years=years,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

