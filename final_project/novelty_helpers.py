"""
Helpers for novelty_driven_mean_reversion.ipynb — sample/CAR, macro adjustment, regressions/FF5.

Imported by the notebook; keep orchestration, parameters, and outputs in the notebook.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Dev: small ticker sample + scoped pickle paths
# ---------------------------------------------------------------------------

DevTickers = Optional[Tuple[str, ...]]  # sorted unique uppercase, e.g. ("AAPL", "MSFT", "NVDA")


def dev_tickers_normalized(
    raw: Union[None, str, Iterable[str]],
) -> DevTickers:
    """
    None / empty → None.
    One string or iterable of strings → sorted unique uppercase tuple (stable cache filenames).
    """
    if raw is None:
        return None
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        return (s.upper(),)
    items: list[str] = []
    for x in raw:
        s = str(x).strip()
        if s:
            items.append(s.upper())
    if not items:
        return None
    return tuple(sorted(set(items)))


def dev_ticker_normalized(dev_ticker: Optional[str]) -> Optional[str]:
    """Backward compat: first ticker only, or None."""
    t = dev_tickers_normalized(dev_ticker)
    if not t:
        return None
    return t[0]


def dev_cache_slug_from_tickers(dev_tickers: DevTickers) -> Optional[str]:
    if not dev_tickers:
        return None
    parts = ["".join(c if c.isalnum() else "_" for c in tick) for tick in dev_tickers]
    return "_".join(parts)


def dev_scoped_cache_path(
    base_path: Union[str, Path],
    dev_tickers: DevTickers,
) -> Path:
    """e.g. novelty_scores.pkl → novelty_scores__dev_AAPL_MSFT_NVDA.pkl"""
    base = Path(base_path)
    slug = dev_cache_slug_from_tickers(dev_tickers)
    if not slug:
        return base
    return base.with_name(f"{base.stem}__dev_{slug}{base.suffix}")


def filter_baseline_dict(firm_baselines: dict, dev_tickers: DevTickers) -> dict:
    if not dev_tickers:
        return firm_baselines
    allow = set(dev_tickers)
    return {k: v for k, v in firm_baselines.items() if str(k[0]).upper() in allow}


def filter_novelty_records(records: list, dev_tickers: DevTickers) -> list:
    if not dev_tickers:
        return list(records)
    allow = set(dev_tickers)
    return [r for r in records if str(r.get("ticker", "")).upper() in allow]


def dev_mode_active(dev_tickers: DevTickers) -> bool:
    return bool(dev_tickers)


# ---------------------------------------------------------------------------
# Transcript → turns (speaker / component word counts)
# ---------------------------------------------------------------------------

_SPEAKER_INDEX_USECOLS = [
    "transcriptid",
    "transcriptcomponentid",
    "componentorder",
    "word_count",
]


def transcript_to_turn(transcript: str, word_counts: Sequence[int]) -> List[str]:
    """
    Port of R `transcript_to_turn`: trim/squish text, replace literal ``...`` with
    space, tokenize on whitespace, assign the first ``sum(word_counts)`` tokens to
    turns in order, paste each turn.

    ``word_counts`` must match the vendor table row order for that transcript
    (sorted by ``componentorder``, tie-break ``transcriptcomponentid``).
    """
    t = transcript.strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\.{3}", " ", t)
    all_words = t.split()
    counts = [int(c) for c in word_counts]
    n = sum(counts)
    if n == 0:
        return []
    if len(all_words) < n:
        all_words = all_words + [""] * (n - len(all_words))
    else:
        all_words = all_words[:n]
    out: List[str] = []
    idx = 0
    for c in counts:
        chunk = all_words[idx : idx + c]
        idx += c
        out.append(" ".join(chunk).strip())
    return out


def load_transcript_speaker_indices(
    path: Union[str, Path],
    *,
    usecols: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """Read speaker/component index file; coerce numeric fields and drop bad rows."""
    path = Path(path)
    cols = list(usecols) if usecols is not None else _SPEAKER_INDEX_USECOLS
    df = pd.read_csv(path, usecols=cols, low_memory=False)
    need = {"transcriptid", "componentorder", "word_count"}
    if not need <= set(df.columns):
        raise ValueError(f"{path} missing columns {need - set(df.columns)}")
    df = df.copy()
    df["word_count"] = pd.to_numeric(df["word_count"], errors="coerce")
    df["componentorder"] = pd.to_numeric(df["componentorder"], errors="coerce")
    drop_subset = ["transcriptid", "componentorder", "word_count"]
    df = df.dropna(subset=drop_subset)
    return df


def word_counts_for_transcript(
    indices_df: pd.DataFrame,
    transcriptid: Union[int, float],
) -> Optional[List[int]]:
    """Return per-turn word counts in ``componentorder`` (then component id) order."""
    tid = int(transcriptid)
    sub = indices_df.loc[indices_df["transcriptid"] == tid]
    if sub.empty:
        return None
    sort_cols: List[str] = ["componentorder"]
    if "transcriptcomponentid" in sub.columns:
        sort_cols.append("transcriptcomponentid")
    sub = sub.sort_values(sort_cols, kind="mergesort")
    return sub["word_count"].astype(int).tolist()


def build_transcript_word_counts_index(
    path: Union[str, Path],
    *,
    transcript_ids: Optional[Iterable[Union[int, float]]] = None,
) -> Dict[int, List[int]]:
    """
    ``transcriptid`` → list of word counts in presentation order.

    Rows are ordered by ``componentorder`` then ``transcriptcomponentid``. For some
    calls the vendor file **aggregates** multiple fine-grained turns into one row, so
    ``len(counts)`` can be much smaller than analyst/Q&A-level splits from another
    pipeline; align gold-standard tests with that pipeline’s word-count vector.

    If ``transcript_ids`` is set, only those IDs are kept (chunked read; lower RAM).
    """
    path = Path(path)
    want: Optional[set[int]] = None
    if transcript_ids is not None:
        want = {int(x) for x in transcript_ids}

    acc: Dict[int, List[Tuple[int, int, int]]] = defaultdict(list)
    chunksize = 400_000
    for chunk in pd.read_csv(
        path,
        usecols=_SPEAKER_INDEX_USECOLS,
        chunksize=chunksize,
        low_memory=False,
    ):
        chunk["word_count"] = pd.to_numeric(chunk["word_count"], errors="coerce")
        chunk["componentorder"] = pd.to_numeric(chunk["componentorder"], errors="coerce")
        chunk = chunk.dropna(subset=["transcriptid", "componentorder", "word_count"])
        if want is not None:
            chunk = chunk[chunk["transcriptid"].isin(want)]
        if chunk.empty:
            continue
        tci = pd.to_numeric(chunk["transcriptcomponentid"], errors="coerce").fillna(-1).astype(int)
        for tid, co, wc, tc in zip(
            chunk["transcriptid"].astype(int),
            chunk["componentorder"].astype(int),
            chunk["word_count"].astype(int),
            tci,
        ):
            acc[int(tid)].append((co, tc, wc))

    out: Dict[int, List[int]] = {}
    for tid, triples in acc.items():
        triples.sort(key=lambda x: (x[0], x[1]))
        out[tid] = [w for _, _, w in triples]
    return out


# ---------------------------------------------------------------------------
# Raw turn table + derived executive chunks + analyst attention
# ---------------------------------------------------------------------------

RAW_TURN_COLS_DEFAULT: Tuple[str, ...] = (
    "transcriptid",
    "transcriptcomponentid",
    "componentorder",
    "transcriptcomponenttypename",
    "transcriptpersonname",
    "speakertypename",
    "prorank",
    "word_count",
)


def build_raw_turn_table(
    final_df: pd.DataFrame,
    speaker_indices_path: Union[str, Path],
    *,
    text_col: str = "full_transcript_text",
    transcriptid_col: str = "transcriptid",
    keep_index_cols: Sequence[str] = RAW_TURN_COLS_DEFAULT,
) -> pd.DataFrame:
    """
    Source-of-truth per-turn table.

    - Orders turns by (componentorder, transcriptcomponentid)
    - Splits the *raw* transcript text using `transcript_to_turn` and `word_count`
    - Attaches speaker/component metadata from `transcript_speaker_indices.csv`

    Returns one row per turn with `turn_index` (0-based) and `turn_text`.
    """
    speaker_indices_path = Path(speaker_indices_path)
    if transcriptid_col not in final_df.columns or text_col not in final_df.columns:
        raise KeyError(f"final_df must contain {transcriptid_col!r} and {text_col!r}")

    # Read only needed rows from the index file (chunked by transcriptid set)
    tids = {int(x) for x in final_df[transcriptid_col].dropna().astype(int).unique()}
    idx_map = build_transcript_word_counts_index(speaker_indices_path, transcript_ids=tids)

    # Load metadata for those transcript IDs (chunked to avoid full file in memory)
    cols = list(keep_index_cols)
    meta_rows: list[pd.DataFrame] = []
    for chunk in pd.read_csv(speaker_indices_path, usecols=cols, chunksize=400_000, low_memory=False):
        chunk = chunk[chunk["transcriptid"].astype(int).isin(tids)]
        if not chunk.empty:
            meta_rows.append(chunk)
    if not meta_rows:
        return pd.DataFrame(
            columns=[
                transcriptid_col,
                "turn_index",
                "turn_text",
                *[c for c in cols if c != transcriptid_col],
            ]
        )
    meta = pd.concat(meta_rows, ignore_index=True)
    meta["transcriptid"] = meta["transcriptid"].astype(int)
    meta["componentorder"] = pd.to_numeric(meta["componentorder"], errors="coerce")
    meta["word_count"] = pd.to_numeric(meta["word_count"], errors="coerce")
    meta = meta.dropna(subset=["componentorder", "word_count"])

    sort_cols = ["componentorder"]
    if "transcriptcomponentid" in meta.columns:
        sort_cols.append("transcriptcomponentid")
    meta = meta.sort_values(["transcriptid", *sort_cols], kind="mergesort")

    # Build per-transcript turns, aligned to metadata row order
    text_by_tid = (
        final_df[[transcriptid_col, text_col]]
        .dropna(subset=[transcriptid_col, text_col])
        .assign(**{transcriptid_col: lambda d: d[transcriptid_col].astype(int)})
        .drop_duplicates(subset=[transcriptid_col], keep="last")
        .set_index(transcriptid_col)[text_col]
        .to_dict()
    )

    out_parts: list[pd.DataFrame] = []
    for tid, grp in meta.groupby("transcriptid", sort=False):
        wc = idx_map.get(int(tid))
        raw_txt = text_by_tid.get(int(tid))
        if not wc or raw_txt is None:
            continue
        turns = transcript_to_turn(str(raw_txt), wc)
        if len(turns) != len(grp):
            # If something is off, skip rather than silently misalign metadata.
            continue
        g = grp.copy().reset_index(drop=True)
        g["turn_index"] = np.arange(len(g), dtype=int)
        g["turn_text"] = turns
        out_parts.append(g)

    if not out_parts:
        return pd.DataFrame()
    out = pd.concat(out_parts, ignore_index=True)
    return out


def build_exec_chunks(
    raw_turn_df: pd.DataFrame,
    *,
    exec_speaker_label: str = "Executives",
    operator_label: str = "Operator",
    analyst_label: str = "Analysts",
    min_chars: int = 20,
) -> pd.DataFrame:
    """
    Build executive-scored chunks by attaching *preceding* context to each executive turn.

    Rules (per transcript, in turn order):
    - Buffer all non-executive turns since last executive chunk.
    - When an executive turn arrives, create a chunk = buffer + that exec turn.
      (This merges: operator open → exec; operator+analyst question → exec answer.)
    - Consecutive executive turns produce distinct chunks (buffer empty).
    - Remaining buffered turns at end are dropped.

    Output: one row per chunk with `chunk_index`, `chunk_text`, and summary flags.
    """
    need = {"transcriptid", "turn_index", "turn_text", "speakertypename"}
    if not need <= set(raw_turn_df.columns):
        raise KeyError(f"raw_turn_df missing columns {need - set(raw_turn_df.columns)}")

    d = raw_turn_df.copy()
    d["transcriptid"] = d["transcriptid"].astype(int)
    d["turn_index"] = d["turn_index"].astype(int)
    d["speakertypename"] = d["speakertypename"].astype(str)
    d = d.sort_values(["transcriptid", "turn_index"], kind="mergesort")

    chunks: list[dict] = []
    for tid, grp in d.groupby("transcriptid", sort=False):
        buf_turns: list[str] = []
        buf_has_operator = False
        buf_has_analyst = False
        chunk_i = 0

        for _, r in grp.iterrows():
            st = str(r["speakertypename"])
            txt = str(r["turn_text"])
            if st == exec_speaker_label:
                exec_turn_index = int(r["turn_index"])
                context_text = " ".join(p.strip() for p in buf_turns if p and str(p).strip()).strip()
                exec_text = str(txt).strip()
                chunk_text = (context_text + " " + exec_text).strip() if context_text else exec_text
                if len(chunk_text.strip()) >= min_chars:
                    chunks.append(
                        {
                            "transcriptid": int(tid),
                            "chunk_index": int(chunk_i),
                            "exec_turn_index": exec_turn_index,
                            # Full context+exec chunk (for inspection / optional features)
                            "chunk_text": chunk_text.strip(),
                            # Exec-only text (use this for novelty/salience/LLM scoring)
                            "exec_text": exec_text,
                            # Context-only text (operator + analysts) preceding the exec answer
                            "context_text": context_text,
                            "has_operator_context": bool(buf_has_operator),
                            "has_analyst_context": bool(buf_has_analyst),
                        }
                    )
                    chunk_i += 1
                buf_turns = []
                buf_has_operator = False
                buf_has_analyst = False
            else:
                buf_turns.append(txt)
                if st == operator_label:
                    buf_has_operator = True
                if st == analyst_label:
                    buf_has_analyst = True

    return pd.DataFrame(chunks)


def analyst_attention_features_per_call(
    raw_turn_df: pd.DataFrame,
    *,
    analyst_label: str = "Analysts",
) -> pd.DataFrame:
    """
    Per-transcript analyst attention features (no lookahead by construction).
    Merge onto your event panel by transcriptid / (ticker, quarter_str) as needed.
    """
    need = {"transcriptid", "speakertypename"}
    if not need <= set(raw_turn_df.columns):
        raise KeyError(f"raw_turn_df missing columns {need - set(raw_turn_df.columns)}")
    d = raw_turn_df.copy()
    d["is_analyst"] = d["speakertypename"].astype(str) == analyst_label
    if "word_count" in d.columns:
        d["word_count"] = pd.to_numeric(d["word_count"], errors="coerce").fillna(0).astype(int)
    else:
        d["word_count"] = 0
    out = (
        d.groupby("transcriptid", sort=False)
        .agg(
            n_turns=("turn_index", "count"),
            n_analyst_turns=("is_analyst", "sum"),
            analyst_words=("word_count", lambda x: int(x[d.loc[x.index, "is_analyst"]].sum())),
            total_words=("word_count", "sum"),
        )
        .reset_index()
    )
    out["analyst_turn_share"] = out["n_analyst_turns"] / out["n_turns"].replace(0, np.nan)
    out["analyst_word_share"] = out["analyst_words"] / out["total_words"].replace(0, np.nan)
    return out


def rolling_unique_analysts_by_cluster(
    analyst_cluster_df: pd.DataFrame,
    *,
    date_col: str,
    cluster_col: str,
    analyst_col: str,
    window_days: int = 90,
    out_col: str = "trailing_unique_analysts",
) -> pd.DataFrame:
    """
    Rolling analyst-attention by topic/cluster with **no lookahead**.

    Input must contain one row per analyst utterance (or analyst chunk), with:
    - ``date_col``: call date / timestamp (datetime-like)
    - ``cluster_col``: dominant topic label
    - ``analyst_col``: analyst identifier (name or ID)

    For each row, computes the number of **unique analysts** that spoke in the same
    cluster in the **preceding** ``window_days`` (default 90 ≈ 3 months), excluding
    the current row’s timestamp.

    Returns a copy with the additional ``out_col``.
    """
    need = {date_col, cluster_col, analyst_col}
    if not need <= set(analyst_cluster_df.columns):
        raise KeyError(f"analyst_cluster_df missing columns {need - set(analyst_cluster_df.columns)}")

    d = analyst_cluster_df.copy()
    d[date_col] = pd.to_datetime(d[date_col], errors="coerce")
    d = d.dropna(subset=[date_col, cluster_col, analyst_col])
    d[cluster_col] = d[cluster_col].astype(str)
    d[analyst_col] = d[analyst_col].astype(str)
    d = d.sort_values([cluster_col, date_col], kind="mergesort").reset_index(drop=True)

    out_vals = np.zeros(len(d), dtype=int)
    win = pd.Timedelta(days=int(window_days))

    # Sliding window per cluster: maintain analyst counts inside (t - win, t)
    from collections import defaultdict as _dd

    counts = _dd(int)
    cur_cluster: Optional[str] = None
    start = 0

    for i in range(len(d)):
        cl = d.at[i, cluster_col]
        t = d.at[i, date_col]

        if cur_cluster != cl:
            cur_cluster = cl
            counts = _dd(int)
            start = i

        # Drop rows older than the trailing window
        while start < i and (t - d.at[start, date_col]) > win:
            a = d.at[start, analyst_col]
            counts[a] -= 1
            if counts[a] <= 0:
                del counts[a]
            start += 1

        # Count analysts strictly before current row, then add current row
        out_vals[i] = len(counts)
        a_i = d.at[i, analyst_col]
        counts[a_i] += 1

    d[out_col] = out_vals
    return d

# ---------------------------------------------------------------------------
# (A) FINAL.csv column sets + load skeleton
# ---------------------------------------------------------------------------

RET_COLS: list[str] = [f"ret_t{j}" for j in list(range(-15, 0)) + list(range(0, 16))]

USE_COLS_FINAL: list[str] = [
    "transcriptid",
    "ticker",
    "companyname",
    "mostimportantdateutc",
    "full_transcript_text",
    "close_to_open_return",
    "close_price_call_day",
    "open_price_next_day",
    "permno",
    "word_count",
    "transcript_length",
    "gvkey",
    "fiscal_period_end",
    "report_date",
    "fiscal_year",
    "fiscal_quarter",
    "compustat_actual_revenue",
    "ibes_ticker",
    "ibes_anndats",
    "ibes_mean_est_eps",
    "ibes_actual_eps",
    "ibes_raw_surp_eps",
    "ibes_sue_eps",
] + RET_COLS

RET_EVENT_WINDOW: list[str] = [f"ret_t{j}" for j in (-1, 0, 1, 2, 3, 4, 5)]
PRE_DRIFT_COLS: list[str] = [f"ret_t{j}" for j in range(-15, 0)]
RET_CAR_COMPONENTS: list[str] = [f"ret_t{j}" for j in (-1, 0, 1, 2, 3, 4, 5)]


def load_and_clean_final_csv(
    path: Union[str, Path],
    *,
    min_text_len: int = 200,
) -> pd.DataFrame:
    """Read FINAL.csv with project usecols, rename Compustat fiscal fields, add quarter columns, basic text filter."""
    path = Path(path)
    raw = pd.read_csv(path, usecols=USE_COLS_FINAL, low_memory=False)
    raw = raw.rename(
        columns={
            "fiscal_year": "compustat_fiscal_year",
            "fiscal_quarter": "compustat_fiscal_quarter",
        }
    )
    raw["quarter"] = pd.to_datetime(raw["mostimportantdateutc"], errors="coerce").dt.to_period("Q")
    raw["fiscal_year"] = raw["quarter"].dt.year
    raw["quarter_str"] = raw["quarter"].astype(str)
    raw = raw.dropna(subset=["full_transcript_text", "quarter"])
    raw = raw[raw["full_transcript_text"].str.len() > min_text_len]
    return raw


def project_spec_sample_table(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Apply spec filters; return (filtered_df, flow table)."""
    rows: list[tuple[str, int]] = []
    d = df.copy()
    rows.append(("Start (after §1 text clean)", len(d)))

    d = d[d["permno"].notna()]
    rows.append(("Drop missing permno", len(d)))

    d = d[d["word_count"] >= 100]
    rows.append(("Drop word_count < 100", len(d)))

    d = d.sort_values(["ticker", "quarter"])
    d["_prior_n"] = d.groupby("ticker", sort=False).cumcount()
    d = d[d["_prior_n"] >= 2]
    d = d.drop(columns=["_prior_n"])
    rows.append(("Keep ≥2 prior transcripts (same ticker)", len(d)))

    for c in RET_EVENT_WINDOW:
        d = d[d[c].notna()]
    rows.append(("Drop if any ret in [t-1, t+5] missing", len(d)))

    tbl = pd.DataFrame(rows, columns=["Step", "N"])
    tbl["Lost_vs_prev_step"] = tbl["N"].shift(1) - tbl["N"]
    return d, tbl


def winsorize_columns(
    df: pd.DataFrame,
    cols: Iterable[str],
    *,
    lo: float = 0.01,
    hi: float = 0.99,
) -> pd.DataFrame:
    """Clip selected columns to [quantile(lo), quantile(hi)] in-place on a copy."""
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            continue
        lo_v, hi_v = out[c].quantile(lo), out[c].quantile(hi)
        out[c] = out[c].clip(lo_v, hi_v)
    return out


def add_pre_drift_and_sue(df: pd.DataFrame) -> pd.DataFrame:
    """Add pre_drift (sum of PRE_DRIFT_COLS if all present) and sue alias."""
    out = df.copy()
    pre_mat = out[PRE_DRIFT_COLS]
    out["pre_drift"] = np.where(pre_mat.notna().all(axis=1), pre_mat.sum(axis=1), np.nan)
    if "ibes_sue_eps" in out.columns:
        out["sue"] = out["ibes_sue_eps"]
    return out


def apply_project_spec_sample_pipeline(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Filter + winsorize event-window returns + close_to_open + pre_drift + sue."""
    filtered, flow = project_spec_sample_table(df)
    win_cols = [c for c in RET_EVENT_WINDOW + ["close_to_open_return"] if c in filtered.columns]
    filtered = winsorize_columns(filtered, win_cols)
    filtered = add_pre_drift_and_sue(filtered)
    return filtered, flow


def build_event_panel_extras(
    df: pd.DataFrame,
    *,
    merge_keys: Tuple[str, str] = ("ticker", "quarter_str"),
) -> pd.DataFrame:
    """
    One row per (ticker, quarter_str): IDs, SUE, pre-drift, I/B/E/S extras, CAR sums.
    Expects winsorized dailies on df. Drops ingredient ret_t* after summing.
    """
    keys = list(merge_keys)
    extra = (
        list(keys)
        + [
            "permno",
            "gvkey",
            "word_count",
            "transcriptid",
            "sue",
            "pre_drift",
            "ibes_raw_surp_eps",
            "ibes_mean_est_eps",
        ]
        + RET_CAR_COMPONENTS
    )
    extra = [c for c in extra if c in df.columns]
    panel = df[extra].drop_duplicates(subset=keys, keep="last")

    for c in RET_CAR_COMPONENTS:
        if c not in panel.columns:
            raise KeyError(f"{c} missing on df — run load + sample pipeline first.")

    panel = panel.copy()
    panel["CAR_m1_p1"] = panel["ret_t-1"] + panel["ret_t0"] + panel["ret_t1"]
    panel["CAR_p2_p5"] = (
        panel["ret_t2"] + panel["ret_t3"] + panel["ret_t4"] + panel["ret_t5"]
    )
    panel = panel.drop(columns=list(RET_CAR_COMPONENTS), errors="ignore")
    return panel


def merge_event_panel_into_firm_quarter(
    firm_quarter_signal: pd.DataFrame,
    df_events: pd.DataFrame,
    *,
    merge_keys: Tuple[str, str] = ("ticker", "quarter_str"),
) -> pd.DataFrame:
    """Left-merge CARs and fundamentals from event-level df into firm-quarter signal table."""
    panel = build_event_panel_extras(df_events, merge_keys=merge_keys)
    return firm_quarter_signal.merge(panel, on=list(merge_keys), how="left")


# ---------------------------------------------------------------------------
# (B) Macro cross-market penalty + adjusted novelty
# ---------------------------------------------------------------------------

def compute_cross_market_penalty_table(
    signal_df: pd.DataFrame,
    *,
    penalty_f0: float = 0.40,
    penalty_k: float = 15.0,
    macro_threshold: Optional[float] = None,
) -> pd.DataFrame:
    """
    Cluster × quarter cross-market frequency f and smooth penalty multiplier.

    `project_specification_rio_zac.pdf` §3.3: penalty_mult(f) = 1 / (1 + exp(k * (f - f0))),
    with f0 = 0.40 and k = 15 (inflection / steepness; grid search noted in memo).

    Frequency f is still computed on the current notebook's quarter × cluster slice (proxy until
    a trailing three-month prior window is wired through the panel).

    Deprecated: pass ``macro_threshold=`` to set ``penalty_f0`` only (backward compat).
    """
    if macro_threshold is not None:
        penalty_f0 = float(macro_threshold)
    sdf = signal_df.copy()
    sdf["quarter_str"] = sdf["quarter_str"].astype(str)
    sdf["top_cluster"] = sdf["top_cluster"].astype(str)

    cross_section = (
        sdf.groupby(["quarter_str", "top_cluster"])["ticker"]
        .nunique()
        .reset_index(name="n_firms")
    )
    firms_per_quarter = (
        sdf.groupby("quarter_str")["ticker"].nunique().reset_index(name="total_firms")
    )
    cross_section = cross_section.merge(firms_per_quarter, on="quarter_str")
    cross_section["cross_market_freq"] = (
        cross_section["n_firms"] / cross_section["total_firms"]
    )
    f = cross_section["cross_market_freq"].astype(float)
    cross_section["penalty_mult"] = 1.0 / (1.0 + np.exp(penalty_k * (f - penalty_f0)))
    cross_section["is_systemic"] = f >= penalty_f0
    return cross_section


def merge_penalty_into_signal_df(
    signal_df: pd.DataFrame,
    cross_section: pd.DataFrame,
) -> pd.DataFrame:
    """Idempotent merge of cross_market_freq + penalty_mult onto segment-level signal_df."""
    merge_keys = ["cross_market_freq", "penalty_mult"]
    drop_candidates = [
        c
        for base in merge_keys
        for c in (base, f"{base}_x", f"{base}_y")
        if c in signal_df.columns
    ]
    out = signal_df.drop(columns=drop_candidates, errors="ignore")
    _cs = cross_section[
        ["quarter_str", "top_cluster", "cross_market_freq", "penalty_mult"]
    ].drop_duplicates(subset=["quarter_str", "top_cluster"], keep="last")
    out = out.merge(
        _cs,
        on=["quarter_str", "top_cluster"],
        how="left",
        validate="many_to_one",
    )
    out["penalty_mult"] = out["penalty_mult"].fillna(1.0)
    return out


def apply_adjusted_novelty_product(signal_df: pd.DataFrame) -> pd.DataFrame:
    """novelty × sqrt(salience) × penalty_mult (current notebook specification)."""
    out = signal_df.copy()
    sal = pd.to_numeric(out["salience_score"], errors="coerce").fillna(0.0).clip(lower=0.0)
    out["adjusted_novelty"] = (
        out["novelty_score"] * np.sqrt(sal) * out["penalty_mult"]
    )
    return out


def macro_adjust_signal_pipeline(
    signal_df: pd.DataFrame,
    *,
    penalty_f0: float = 0.40,
    penalty_k: float = 15.0,
    macro_threshold: Optional[float] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Drop rows missing top_cluster (same as original notebook), merge penalty, set adjusted_novelty.
    Returns (updated_signal_df, cross_section_table).

    Cross-market penalty follows `project_specification_rio_zac.pdf` §3.3 (sigmoid in f).
    ``macro_threshold`` is deprecated alias for ``penalty_f0``.
    """
    sdf = signal_df.dropna(subset=["top_cluster"]).copy()
    sdf["quarter_str"] = sdf["quarter_str"].astype(str)
    sdf["top_cluster"] = sdf["top_cluster"].astype(str)
    cross_section = compute_cross_market_penalty_table(
        sdf,
        penalty_f0=penalty_f0,
        penalty_k=penalty_k,
        macro_threshold=macro_threshold,
    )
    sdf = merge_penalty_into_signal_df(sdf, cross_section)
    sdf = apply_adjusted_novelty_product(sdf)
    return sdf, cross_section


# ---------------------------------------------------------------------------
# Aggregation helper
# ---------------------------------------------------------------------------


def first_mode(s: pd.Series):
    """Mode for firm-quarter aggregation; NaN if empty."""
    s = s.dropna()
    if s.empty:
        return np.nan
    m = s.mode()
    return m.iloc[0] if len(m) else np.nan


def quintile_within_quarter(x: pd.Series) -> pd.Series:
    """pd.qcut into 5 bins per group; NaN if <5 distinct values."""
    if x.nunique() < 5:
        return pd.Series(np.nan, index=x.index)
    out = pd.qcut(x, q=5, labels=[1, 2, 3, 4, 5], duplicates="drop")
    return pd.to_numeric(out, errors="coerce")


# ---------------------------------------------------------------------------
# (C) Regressions + Fama–French 5
# ---------------------------------------------------------------------------


def fit_ols_cluster(
    formula: str,
    data: pd.DataFrame,
    *,
    cluster_col: str,
):
    """
    OLS with clustered covariance (e.g. cluster_col='quarter_str' as coarse event-time proxy).
    Requires statsmodels.
    """
    import statsmodels.formula.api as smf

    d = data.dropna(subset=[cluster_col]).copy()
    mod = smf.ols(formula, data=d)
    return mod.fit(cov_type="cluster", cov_kwds={"groups": d[cluster_col]})


def _fetch_ff5_monthly_from_french_zip(
    start: Optional[pd.Timestamp] = None,
    end: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """Download Ken French monthly 5-factor CSV (zip) — no pandas_datareader required."""
    import io
    import urllib.request
    import zipfile

    url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip"
    with urllib.request.urlopen(url, timeout=120) as resp:
        raw_zip = resp.read()
    zf = zipfile.ZipFile(io.BytesIO(raw_zip))
    inner = [n for n in zf.namelist() if n.upper().endswith(".CSV")][0]
    with zf.open(inner) as fh:
        raw_csv = fh.read()
    # Line 4 (0-based) is the header: ,Mkt-RF,SMB,...
    df = pd.read_csv(io.BytesIO(raw_csv), skiprows=4, header=0)
    df = df.rename(columns={df.columns[0]: "YYYYMM"})
    s = df["YYYYMM"].astype(str).str.strip()
    ok = s.str.fullmatch(r"\d{6}")
    df = df.loc[ok].copy()
    df["YYYYMM"] = df["YYYYMM"].astype(int)
    idx = pd.to_datetime(df["YYYYMM"].astype(str), format="%Y%m") + pd.offsets.MonthEnd(0)
    df = df.drop(columns=["YYYYMM"])
    df.index = idx
    df.columns = [str(c).replace("-", "_").replace(" ", "_") for c in df.columns]
    df = df.astype(float) / 100.0
    if start is not None:
        df = df[df.index >= pd.Timestamp(start)]
    if end is not None:
        df = df[df.index <= pd.Timestamp(end)]
    return df


def fetch_ff5_monthly(
    start: Optional[pd.Timestamp] = None,
    end: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """
    Monthly Fama–French 5 factors (+ RF). Returns decimals (e.g. 0.012 = 1.2%).
    Index: month-end timestamps. Tries pandas_datareader, then direct French zip.
    """
    try:
        from pandas_datareader import data as web

        ff = web.DataReader(
            "F-F_Research_Data_5_Factors_2x3",
            "famafrench",
            start=start,
            end=end,
        )[0]
        ff = ff.copy()
        ff.index = pd.to_datetime(ff.index.to_timestamp())
        ff.columns = [str(c).replace("-", "_").replace(" ", "_") for c in ff.columns]
        return ff.astype(float) / 100.0
    except Exception:
        return _fetch_ff5_monthly_from_french_zip(start=start, end=end)


def align_ls_with_ff5(
    ls_monthly: pd.Series,
    factors: pd.DataFrame,
) -> pd.DataFrame:
    """Inner-join dependent variable (LS return) with factor columns."""
    y = ls_monthly.copy()
    y.name = "LS"
    y.index = pd.to_datetime(y.index)
    fac = factors.copy()
    fac.index = pd.to_datetime(fac.index)
    df = pd.DataFrame(y).join(fac, how="inner")
    return df


def ff5_alpha_regression(
    ls_monthly: pd.Series,
    *,
    factors: Optional[pd.DataFrame] = None,
    excess_returns: bool = True,
):
    """
    Regress LS portfolio return on FF5. ls_monthly should be in decimals (0.01 = 1%).
    If excess_returns and 'RF' in factors, subtract RF from LS and drop RF from X (use Mkt_RF only as in standard spec).
    Actually standard: y = LS_excess, X = [1, Mkt_RF, SMB, HML, RMW, CMA] all excess — French file gives Mkt-RF already excess of RF.
    So y should be LS - RF if LS is raw. We assume ls_monthly is already excess return vs RF; else set excess_returns False and include RF.

    Returns statsmodels OLS results.
    """
    import statsmodels.api as sm

    if factors is None:
        factors = fetch_ff5_monthly(
            start=ls_monthly.index.min(),
            end=ls_monthly.index.max(),
        )

    df = align_ls_with_ff5(ls_monthly, factors)
    xcols = [c for c in ["Mkt_RF", "SMB", "HML", "RMW", "CMA"] if c in df.columns]
    if len(xcols) != 5:
        raise ValueError(f"Expected 5 factor columns; got {xcols} from {list(df.columns)}")

    y = df["LS"].astype(float)
    if excess_returns and "RF" in df.columns:
        y = y - df["RF"].astype(float)

    X = sm.add_constant(df[xcols].astype(float))
    return sm.OLS(y, X).fit()
