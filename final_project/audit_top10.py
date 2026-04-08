"""
Script-first, cache-friendly Top-10 audit runner for the novelty pipeline.

Goal: make iteration testing reproducible WITHOUT executing the full notebook.

Design:
- Load dev-only subset of FINAL.csv (either user-provided or auto-generated).
- Build raw turns from transcript_speaker_indices.csv via novelty_helpers.build_raw_turn_table.
- Build exec chunks via novelty_helpers.build_exec_chunks.
- Compute novelty embeddings with SentenceTransformer and a per-ticker historical centroid.
- Compute salience using salience_dictionary.score_sentence on the *display text* (chunk_text).
- Print Top-10 segments for dev tickers to stdout.

This is intentionally narrow: it supports the dev tickers audit use-case.
"""

from __future__ import annotations

import argparse
import math
import pickle
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd

import novelty_helpers as nh
from salience_dictionary import score_sentence


DEFAULT_DEV_TICKERS = ("AAPL", "MSFT", "NVDA")
# NOTE: SentenceTransformer/Torch segfaults on some Python 3.13 builds.
# For audit iteration/testing we default to a deterministic hashing TF-IDF space
# (no fitting, no torch dependency). The CLI keeps --embedding-model only for
# interface compatibility; it is ignored by the hashing backend.
DEFAULT_EMBEDDING_MODEL_NAME = "hashing-tfidf"


def _print_header(title: str) -> None:
    print(f"\n=== {title} ===\n")


def _validate_step12_turn_boundaries(df_calls: pd.DataFrame, speaker_indices_csv: Path, tid: int) -> None:
    """
    CHANGE_AUDIT.md Step 12: validate transcript_to_turn boundaries on RAW text.
    Prints the first few turns to stdout for manual comparison to R validation.
    """
    idx_df = nh.load_transcript_speaker_indices(speaker_indices_csv)
    wc = nh.word_counts_for_transcript(idx_df, tid)
    raw_txt = str(
        df_calls.loc[df_calls["transcriptid"].astype(int) == int(tid), "full_transcript_text"].iloc[0]
    )
    turns = nh.transcript_to_turn(raw_txt, wc)
    _print_header(f"Step 12 boundary check (transcriptid={tid})")
    for i, t in enumerate(turns[:6]):
        t0 = re.sub(r"\s+", " ", str(t)).strip()
        print(f"{i:02d} | {t0[:120]}")
    print(f"\n(turn count) {len(turns)}")


def _validate_step11_13_15_tables(
    df_calls: pd.DataFrame,
    speaker_indices_csv: Path,
    *,
    tid: int,
) -> None:
    """
    CHANGE_AUDIT.md Steps 11/13/15: raw turn table + exec chunks + alignment sanity.
    """
    raw_turn_df = nh.build_raw_turn_table(df_calls, speaker_indices_csv)
    _print_header("Step 11 raw_turn_df sanity")
    sub = raw_turn_df[raw_turn_df["transcriptid"].astype(int) == int(tid)]
    print(f"raw_turn_df rows: {len(raw_turn_df):,}")
    if not sub.empty:
        print(f"raw_turn_df[{tid}] rows: {len(sub)}")
        print("speaker types:", dict(sub["speakertypename"].value_counts().head(6)))
        print("first turn:", str(sub.iloc[0]["turn_text"])[:120])
    else:
        print(f"raw_turn_df[{tid}] not found")

    exec_chunks = nh.build_exec_chunks(raw_turn_df)
    _print_header("Step 13 exec_chunks sanity")
    ecs = exec_chunks[exec_chunks["transcriptid"].astype(int) == int(tid)]
    print(f"exec_chunks rows: {len(exec_chunks):,}")
    if not ecs.empty:
        print(f"exec_chunks[{tid}] rows: {len(ecs)}")
        print("first exec_text:", str(ecs.iloc[0]["exec_text"])[:120])
        print("first context_text:", str(ecs.iloc[0]["context_text"])[:120])
        # No row should have empty exec_text
        empty_exec = int((ecs["exec_text"].astype(str).str.strip().str.len() == 0).sum())
        print("empty exec_text rows:", empty_exec)
    else:
        print(f"exec_chunks[{tid}] not found")

    _print_header("Step 15 alignment (exec_text vs chunk_text vs chunk_index)")
    if ecs.empty:
        return
    n_exec = len(ecs["exec_text"].tolist())
    n_chunk = len(ecs["chunk_text"].tolist())
    n_idx = len(ecs["chunk_index"].tolist())
    print(f"lengths: exec_text={n_exec}, chunk_text={n_chunk}, chunk_index={n_idx}")
    assert n_exec == n_chunk == n_idx, "exec/chunk/index lists are not aligned"


def _slug(items: Iterable[str]) -> str:
    return "_".join(re.sub(r"[^A-Za-z0-9]+", "_", x.strip().upper()).strip("_") for x in items)


def _cache_dir() -> Path:
    d = Path(__file__).resolve().parent / "pkl_cache"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _pkl(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        pickle.dump(obj, f)


def _unpkl(path: Path):
    with path.open("rb") as f:
        return pickle.load(f)


def _ensure_dev_final_subset(src_final_csv: Path, dev_tickers: tuple[str, ...]) -> Path:
    out = _cache_dir() / f"FINAL__dev_{_slug(dev_tickers)}.csv"
    if out.exists() and out.stat().st_size > 0:
        return out

    tickers = {t.upper() for t in dev_tickers}
    wrote_header = False
    for chunk in pd.read_csv(src_final_csv, low_memory=False, chunksize=5000):
        keep = chunk["ticker"].astype(str).str.upper().isin(tickers)
        sub = chunk.loc[keep]
        if sub.empty:
            continue
        sub.to_csv(out, mode="a", header=not wrote_header, index=False)
        wrote_header = True
    if not wrote_header:
        raise RuntimeError(f"No rows found for dev tickers {sorted(tickers)} in {src_final_csv}")
    return out


def _cosine_distance_sparse_row(x_row, c_row) -> float:
    """
    x_row: 1×D sparse row, assumed L2-normalized (||x||=1).
    c_row: 1×D sparse row (centroid sum/mean), not necessarily normalized.
    """
    dot = float(x_row.multiply(c_row).sum())
    c_norm = float(np.sqrt(c_row.multiply(c_row).sum()))
    if c_norm <= 0:
        return float("nan")
    cos = dot / c_norm
    return float(1.0 - cos)


@dataclass(frozen=True)
class SegmentRow:
    ticker: str
    quarter_str: str
    transcriptid: int
    chunk_index: int
    novelty_score: float
    salience_score: float
    top_cluster: str
    text: str


def _top_cluster_from_breakdown(breakdown: dict[str, float]) -> str:
    if not breakdown:
        return ""
    return max(breakdown, key=breakdown.get)


def _hashing_matrix(texts: list[str]):
    """
    Vectorize texts into a deterministic L2-normalized hashing TF-IDF-like space.

    - No fitting step (stable across runs)
    - No torch dependency
    """
    from sklearn.feature_extraction.text import HashingVectorizer

    vec = HashingVectorizer(
        n_features=2**18,
        alternate_sign=False,
        norm="l2",
        lowercase=True,
        token_pattern=r"(?u)\b[\w']+\b",
        ngram_range=(1, 2),
    )
    return vec.transform(texts)


def _build_segments_for_call(
    exec_chunks_for_tid: pd.DataFrame,
    *,
    mode: str,
) -> tuple[list[str], list[str], list[int]]:
    """
    Returns (segs_score, segs_display, seg_idx).

    - mode="chunk": score+display are chunk_text
    - mode="turns": score is exec_text; display is chunk_text (context+exec)
    """
    if mode not in ("chunk", "turns"):
        raise ValueError(f"unknown mode={mode!r}")

    seg_idx = exec_chunks_for_tid["chunk_index"].astype(int).tolist()
    chunk_texts = exec_chunks_for_tid["chunk_text"].astype(str).tolist()
    exec_texts = exec_chunks_for_tid["exec_text"].astype(str).tolist()

    if mode == "chunk":
        segs_score = chunk_texts
        segs_display = chunk_texts
    else:
        segs_score = exec_texts
        segs_display = chunk_texts

    # Gate on exec_text for turns mode (avoids misalignment and junk)
    triples = []
    for ex, sc, disp, ci in zip(exec_texts, segs_score, segs_display, seg_idx):
        if len(str(ex).strip()) <= 20:
            continue
        triples.append((sc, disp, int(ci)))
    if not triples:
        return [], [], []
    segs_score2, segs_display2, seg_idx2 = map(list, zip(*triples))
    return list(segs_score2), list(segs_display2), [int(x) for x in seg_idx2]


def run_top10(
    *,
    final_csv: Path,
    speaker_indices_csv: Path,
    dev_tickers: tuple[str, ...],
    mode: str,
    embedding_model_name: str,
    cache_tag: str,
    max_hist_segs_per_call: int,
    max_new_segs_per_call: int,
    min_text_len: int,
    min_words: int,
    min_salience: float,
) -> list[SegmentRow]:
    # Load + basic filters (mirror notebook defaults loosely)
    df = nh.load_and_clean_final_csv(final_csv, min_text_len=min_text_len)
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df = df[df["ticker"].isin({t.upper() for t in dev_tickers})].copy()
    df = df.sort_values(["ticker", "quarter"]).reset_index(drop=True)

    # Apply project sample constraints (≥2 prior, etc.)
    df, flow = nh.apply_project_spec_sample_pipeline(df)
    if df.empty:
        raise RuntimeError("No rows left after sample pipeline for the selected dev tickers.")

    # Build turns + exec chunks
    raw_turn_df = nh.build_raw_turn_table(df, speaker_indices_csv)
    exec_chunks = nh.build_exec_chunks(raw_turn_df)

    # Index exec chunks by transcriptid
    exec_chunks = exec_chunks.sort_values(["transcriptid", "chunk_index"]).reset_index(drop=True)

    out_rows: list[SegmentRow] = []
    for ticker, g in df.groupby("ticker", sort=False):
        g = g.sort_values("quarter").reset_index(drop=True)
        hist_sum = None
        hist_n = 0

        for i, row in g.iterrows():
            tid = int(row["transcriptid"])
            qtr = str(row["quarter_str"])
            call_chunks = exec_chunks[exec_chunks["transcriptid"].astype(int) == tid]
            if call_chunks.empty:
                continue

            segs_score, segs_disp, seg_idx = _build_segments_for_call(call_chunks, mode=mode)
            if not segs_score:
                continue

            # historical accumulation (first 2 calls only)
            if i < 2:
                segs_score = segs_score[:max_hist_segs_per_call]
                X = _hashing_matrix(segs_score)
                from scipy import sparse  # type: ignore

                s = sparse.csr_matrix(X.sum(axis=0))
                hist_sum = s if hist_sum is None else (hist_sum + s)
                hist_n += int(X.shape[0])
                continue

            if hist_sum is None or hist_n <= 0:
                continue
            centroid = hist_sum

            # score new call segments
            segs_score = segs_score[:max_new_segs_per_call]
            segs_disp = segs_disp[: len(segs_score)]
            seg_idx = seg_idx[: len(segs_score)]
            X = _hashing_matrix(segs_score)

            year = int(pd.Period(qtr).start_time.year) if qtr else None
            for j in range(X.shape[0]):
                novelty = _cosine_distance_sparse_row(X[j], centroid)
                disp = str(segs_disp[j])
                if len(disp.split()) < int(min_words):
                    continue
                sal, breakdown = score_sentence(disp, year=year)
                if float(sal) < float(min_salience):
                    continue
                top_cluster = _top_cluster_from_breakdown(breakdown)
                out_rows.append(
                    SegmentRow(
                        ticker=str(ticker),
                        quarter_str=qtr,
                        transcriptid=tid,
                        chunk_index=int(seg_idx[j]),
                        novelty_score=float(novelty) if novelty == novelty else float("nan"),
                        salience_score=float(sal),
                        top_cluster=str(top_cluster),
                        text=disp,
                    )
                )

            # add this call to history AFTER scoring (no lookahead)
            Xh = _hashing_matrix(segs_score[:max_hist_segs_per_call])
            from scipy import sparse  # type: ignore

            s = sparse.csr_matrix(Xh.sum(axis=0))
            hist_sum = s if hist_sum is None else (hist_sum + s)
            hist_n += int(Xh.shape[0])

    # Rank like the notebook copy: novelty then salience
    out_rows = [r for r in out_rows if not (math.isnan(r.novelty_score) or math.isinf(r.novelty_score))]
    out_rows.sort(key=lambda r: (r.novelty_score, r.salience_score), reverse=True)
    return out_rows[:10]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--final-csv", default="FINAL.csv", help="Path to FINAL.csv (or dev subset).")
    ap.add_argument(
        "--speaker-indices-csv",
        default="transcript_speaker_indices.csv",
        help="Path to transcript_speaker_indices.csv.",
    )
    ap.add_argument("--mode", choices=["chunk", "turns"], default="chunk")
    ap.add_argument("--dev-tickers", default=",".join(DEFAULT_DEV_TICKERS))
    ap.add_argument("--embedding-model", default=DEFAULT_EMBEDDING_MODEL_NAME)
    ap.add_argument("--cache-tag", default="audit_v1")
    ap.add_argument("--max-hist-segs", type=int, default=60)
    ap.add_argument("--max-new-segs", type=int, default=80)
    ap.add_argument("--min-text-len", type=int, default=200)
    ap.add_argument("--min-words", type=int, default=8)
    ap.add_argument("--min-salience", type=float, default=0.30)
    ap.add_argument(
        "--checkpoint",
        choices=["none", "step12", "step11_13_15"],
        default="none",
        help="Print CHANGE_AUDIT.md checkpoint validations before Top-10.",
    )
    ap.add_argument(
        "--check-transcriptid",
        type=int,
        default=1128909,
        help="transcriptid used for checkpoint validation prints.",
    )
    ap.add_argument(
        "--use-dev-final-subset",
        action="store_true",
        help="If set, create/use a dev-only FINAL subset in pkl_cache/ and use that for the run.",
    )
    args = ap.parse_args()

    here = Path(__file__).resolve().parent
    dev_tickers = tuple(t.strip().upper() for t in args.dev_tickers.split(",") if t.strip())
    if not dev_tickers:
        dev_tickers = DEFAULT_DEV_TICKERS

    final_csv = (here / args.final_csv).resolve() if not Path(args.final_csv).is_absolute() else Path(args.final_csv)
    speaker_csv = (here / args.speaker_indices_csv).resolve() if not Path(args.speaker_indices_csv).is_absolute() else Path(args.speaker_indices_csv)

    if args.use_dev_final_subset:
        # Always derive from the full FINAL.csv in final_project/ unless an absolute path was given.
        src = (here / "FINAL.csv") if final_csv.name != "FINAL.csv" else final_csv
        final_csv = _ensure_dev_final_subset(src, dev_tickers)

    # Load calls once so checkpoint validators can inspect raw transcript text.
    df_calls = nh.load_and_clean_final_csv(final_csv, min_text_len=int(args.min_text_len))
    df_calls["ticker"] = df_calls["ticker"].astype(str).str.upper()
    df_calls = df_calls[df_calls["ticker"].isin(set(dev_tickers))].copy()
    df_calls = df_calls.sort_values(["ticker", "quarter"]).reset_index(drop=True)
    df_calls, _flow = nh.apply_project_spec_sample_pipeline(df_calls)

    if args.checkpoint == "step12":
        _validate_step12_turn_boundaries(df_calls, speaker_csv, int(args.check_transcriptid))
    elif args.checkpoint == "step11_13_15":
        _validate_step11_13_15_tables(df_calls, speaker_csv, tid=int(args.check_transcriptid))

    top10 = run_top10(
        final_csv=final_csv,
        speaker_indices_csv=speaker_csv,
        dev_tickers=dev_tickers,
        mode=args.mode,
        embedding_model_name=args.embedding_model,
        cache_tag=args.cache_tag,
        max_hist_segs_per_call=int(args.max_hist_segs),
        max_new_segs_per_call=int(args.max_new_segs),
        min_text_len=int(args.min_text_len),
        min_words=int(args.min_words),
        min_salience=float(args.min_salience),
    )

    print(f"=== Top 10 Most Novel + Salient Segments (mode={args.mode}) ===\n")
    for r in top10:
        print(f"Ticker : {r.ticker}  |  Quarter: {r.quarter_str} | Transcript: {r.transcriptid} | Chunk: {r.chunk_index}")
        print(f"Novelty: {r.novelty_score:.3f}  |  Salience: {r.salience_score:.3f}  |  Cluster: {r.top_cluster}")
        txt = re.sub(r"\s+", " ", str(r.text)).strip()
        print(f"Text   : {txt[:800]}{'...' if len(txt) > 800 else ''}")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

