#!/usr/bin/env python3
"""
Compute DATA BLOCK values for build_presentation.js (run from repo root or final_project).
Writes / overwrites the JS constants by printing a patch or full block.
"""
from __future__ import annotations

import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PKL = ROOT / "pkl_cache"
sys.path.insert(0, str(ROOT))

import novelty_helpers as nh
from salience_dictionary import score_sentence

NOVELTY_FUNNEL = 0.60
SALIENCE_T = 0.30
MACRO_F0 = 0.40
MACRO_K = 15.0
LLM_PICKLE = PKL / "llm_intent_cache_project_spec_turns.pkl"

NOVELTY_BY_MODE = {
    "chunk": PKL / "novelty_scores_chunk_novelty_all_mini.pkl",
    "turns": PKL / "novelty_scores_turns_novelty_all_mini.pkl",
    "full_turns": PKL / "novelty_scores_full_turns_novelty_all_mini.pkl",
}


def js_escape(s: str) -> str:
    return json.dumps(s)[1:-1]


def main() -> None:
    print("Loading FINAL.csv (clean)...", flush=True)
    final_df = nh.load_and_clean_final_csv(ROOT / "FINAL.csv")
    funnel_total_events = len(final_df)

    # --- Novelty histograms + medians (all modes) ---
    novelty_hist = {"labels": ["0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9"]}
    novelty_medians: dict[str, float] = {}
    for mode, path in NOVELTY_BY_MODE.items():
        with open(path, "rb") as f:
            recs = pickle.load(f)
        dfm = pd.DataFrame(recs)
        scores = dfm["novelty_score"].astype(float).clip(0, 1)
        counts, _ = np.histogram(scores, bins=10, range=(0, 1))
        novelty_hist[mode] = [int(x) for x in counts]
        novelty_medians[mode] = round(float(scores.median()), 3)

    # --- Full_turns funnel + signal_df + penalty ---
    with open(NOVELTY_BY_MODE["full_turns"], "rb") as f:
        ft = pd.DataFrame(pickle.load(f))
    n_total_turns = len(ft)

    high = ft[ft["novelty_score"] >= NOVELTY_FUNNEL].copy()
    n_novel = len(high)

    print(f"Scoring salience for {n_novel:,} segments (novelty>={NOVELTY_FUNNEL})...", flush=True)
    sal_scores = []
    top_clusters = []
    for i, (_, row) in enumerate(high.iterrows(), start=1):
        yr = int(row["fiscal_year"]) if pd.notna(row.get("fiscal_year")) else 2020
        score, breakdown = score_sentence(str(row["sentence"]), year=yr)
        top_clusters.append(max(breakdown, key=breakdown.get) if breakdown else None)
        sal_scores.append(float(score))
        if i % 5000 == 0 or i == n_novel:
            print(f"  {i:,} / {n_novel:,}", flush=True)

    high = high.reset_index(drop=True)
    high["salience_score"] = sal_scores
    high["top_cluster"] = top_clusters
    cand = high[high["salience_score"] >= SALIENCE_T].copy()
    n_sal = len(cand)

    with open(LLM_PICKLE, "rb") as f:
        intent = pickle.load(f)
    cand["llm_yes"] = cand["sentence"].map(
        lambda s: bool(intent[s]) if s in intent else False
    )
    flagged = cand[cand["llm_yes"]].copy()
    n_llm = len(flagged)

    signal_df, penalty_df = nh.macro_adjust_signal_pipeline(
        flagged,
        penalty_f0=MACRO_F0,
        penalty_k=MACRO_K,
    )
    n_adj = len(signal_df)

    funnel = [
        ("Total Turns (full_turns)", n_total_turns),
        ("Novelty ≥ 0.60", n_novel),
        ("Salience ≥ 0.30", n_sal),
        ("LLM Gate = YES", n_llm),
        ("Adjusted Novelty Score Computed", n_adj),
    ]
    funnel_survival_pct = round(100.0 * n_llm / n_total_turns, 1)

    # Clusters after LLM gate (use signal_df post-macro; same top_cluster as pre-merge)
    vc = signal_df["top_cluster"].value_counts().head(8)
    cluster_counts = [{"cluster": str(k), "count": int(v)} for k, v in vc.items()]
    cluster_total = int(len(signal_df))

    # Penalty histogram on cross-section cells
    pm = penalty_df["penalty_mult"].astype(float)
    p_counts, _ = np.histogram(pm, bins=11, range=(0.0, 1.0))
    penalty_hist_counts = [int(x) for x in p_counts]
    penalty_pct_penalized = round(float((pm < 0.5).mean() * 100), 1)
    penalty_median = round(float(pm.median()), 3)
    penalty_total_cells = int(len(penalty_df))

    # Scatter: dev tickers, firm-quarter means
    dev = signal_df[signal_df["ticker"].isin(["AAPL", "MSFT", "NVDA"])].copy()
    agg = (
        dev.groupby(["ticker", "quarter_str"], as_index=False)[
            ["adjusted_novelty", "close_to_open_return"]
        ]
        .mean()
        .dropna(subset=["adjusted_novelty", "close_to_open_return"])
    )
    if len(agg) >= 2:
        scatter_corr = round(
            float(agg["adjusted_novelty"].corr(agg["close_to_open_return"])), 3
        )
    else:
        scatter_corr = 0.0
    scatter_n = int(len(agg))
    scatter_points = []
    for _, r in agg.sort_values(["ticker", "quarter_str"]).iterrows():
        scatter_points.append(
            {
                "ticker": str(r["ticker"]),
                "quarter": str(r["quarter_str"]),
                "x": round(float(r["adjusted_novelty"]), 6),
                "y": round(float(r["close_to_open_return"]), 6),
            }
        )

    # Top 10 examples: post-gate segments by adjusted_novelty (substantive vs raw novelty ties)
    ex = signal_df.nlargest(10, "adjusted_novelty")
    examples = []
    for _, r in ex.iterrows():
        sent = str(r["sentence"]).replace("\n", " ").strip()
        insight = (sent[:220] + "…") if len(sent) > 220 else sent
        examples.append(
            {
                "ticker": str(r["ticker"]),
                "quarter": str(r["quarter_str"]),
                "novelty": f"{float(r['novelty_score']):.3f}",
                "salience": f"{float(r['salience_score']):.1f}",
                "cluster": str(r["top_cluster"])[:16],
                "insight": insight,
            }
        )

    # Emit JS fragment
    lines = []
    lines.append("const FUNNEL = [")
    for label, count in funnel:
        lines.append(f'  {{ label: "{label}", count: {count} }},')
    lines.append("];")
    lines.append(f"const FUNNEL_TOTAL_EVENTS = {funnel_total_events};")
    lines.append(f"const FUNNEL_SURVIVAL_PCT = {funnel_survival_pct};")
    lines.append("")
    lines.append("const NOVELTY_HIST = {")
    lines.append(f'  labels: {json.dumps(novelty_hist["labels"])},')
    lines.append(f'  chunk:      {json.dumps(novelty_hist["chunk"])},')
    lines.append(f'  turns:      {json.dumps(novelty_hist["turns"])},')
    lines.append(f'  full_turns: {json.dumps(novelty_hist["full_turns"])},')
    lines.append("};")
    # Plain object literal so `NOVELTY_MEDIANS[m.label]` works in deck template
    nm = novelty_medians
    lines.append(
        "const NOVELTY_MEDIANS = { "
        f"chunk: {nm['chunk']}, turns: {nm['turns']}, full_turns: {nm['full_turns']} "
        "};"
    )
    lines.append("")
    lines.append("const CLUSTER_COUNTS = [")
    for c in cluster_counts:
        lines.append(f'  {{ cluster: {json.dumps(c["cluster"])}, count: {c["count"]} }},')
    lines.append("];")
    lines.append(f"const CLUSTER_TOTAL = {cluster_total};")
    lines.append("")
    lines.append('const PENALTY_HIST = {')
    lines.append('  labels: ["0.0","0.1","0.2","0.3","0.4","0.5","0.6","0.7","0.8","0.9","1.0"],')
    lines.append(f"  counts: {json.dumps(penalty_hist_counts)},")
    lines.append("};")
    lines.append(f"const PENALTY_PCT_PENALIZED = {penalty_pct_penalized};")
    lines.append(f"const PENALTY_MEDIAN = {penalty_median};")
    lines.append(f"const PENALTY_TOTAL_CELLS = {penalty_total_cells};")
    lines.append("")
    lines.append("const SCATTER_POINTS = [")
    for p in scatter_points:
        lines.append(
            f'  {{ ticker: {json.dumps(p["ticker"])}, quarter: {json.dumps(p["quarter"])}, x: {p["x"]}, y: {p["y"]} }},'
        )
    lines.append("];")
    lines.append(f"const SCATTER_CORR = {scatter_corr};")
    lines.append(f"const SCATTER_N = {scatter_n};")
    lines.append("")
    lines.append("const EXAMPLES = [")
    for e in examples:
        lines.append(
            "  { "
            f'ticker:{json.dumps(e["ticker"])}, quarter:{json.dumps(e["quarter"])}, '
            f'novelty:{json.dumps(e["novelty"])}, salience:{json.dumps(e["salience"])}, '
            f'cluster:{json.dumps(e["cluster"])}, insight:{json.dumps(e["insight"])} '
            "},"
        )
    lines.append("];")

    out_path = ROOT / "scripts" / "presentation_data_block.js"
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {out_path}", flush=True)


if __name__ == "__main__":
    main()
