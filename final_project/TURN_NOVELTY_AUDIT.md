# Turn-based novelty audit (full trail)

Last updated: 2026-04-07  
Workspace: `/Users/zacgarland/r_projects/wrds`  
Primary pipeline notebook: `final_project/novelty_driven_mean_reversion.ipynb`  
Helper module: `final_project/novelty_helpers.py`

## Executive summary

We migrated from **sentence-level** transcript segmentation to **turn-aligned** segmentation based on `final_project/transcript_speaker_indices.csv`, then added:

- **Raw turn table** as the source-of-truth (turn text + speaker metadata).
- **Executive-scored chunks** (buffer operator/analyst context → attach to the next executive response).
- **Analyst attention layer** (rolling unique analysts by topic, no lookahead).
- **Cache relocation** into `final_project/pkl_cache/` and dev-scoped sidecar pickles.

**Current unresolved issue:** analyst questions are still appearing as standalone “signals” in outputs, implying that (in some cases) the pipeline is not using the intended `exec_chunks` segmentation end-to-end, and/or is silently falling back to sentence splitting.

## “Last known good” reference

User-provided: **chat id `19600634-75cd-4d74-90fe-2ce8cccea5ec`**.

Audit note:
- That exact transcript file was **not present as a standalone JSONL** in the local transcript store at time of writing.
- The only occurrence of that UUID found was in the *current* conversation log (i.e., not the “last known good” run).

Impact:
- We cannot mechanically diff the exact prior working state from that chat.
- This document therefore records the code + notebook state as observed in the repo **now**, and reconstructs the trail from the existing implementation and artifacts.

## Symptom definition (what “still being picked up” means)

Observed behavior: In “Top 10 Most Novel + Salient …” outputs, some rows show text that appears to be **analyst/operator question text** without the subsequent executive response included, suggesting one of:

1. We are scoring **sentence-level** segments (fallback path), not `exec_chunks`, or
2. `exec_chunks` are being built, but the “scored text” for novelty/salience/LLM is not the intended one, or
3. Cached novelty/salience/LLM decisions are being reused from a previous segmentation geometry.

## Files touched (relevant)

### `final_project/novelty_helpers.py`

#### Turn splitting: R → Python port
- **Function:** `transcript_to_turn(transcript, word_counts)`  
  - Trims, squishes whitespace, replaces literal `...` with a space, tokenizes on whitespace.
  - Groups tokens into turns by `word_counts` order.
  - Pads/truncates to `sum(word_counts)` to match R behavior.

Key lines:
- `t = transcript.strip()`
- `t = re.sub(r"\s+", " ", t)`
- `t = re.sub(r"\.{3}", " ", t)`
- `all_words = t.split()`

#### Speaker indices ingestion
- **Function:** `build_transcript_word_counts_index(path, transcript_ids=None)`  
  - Reads `transcript_speaker_indices.csv` in chunks.
  - Sorts by `(componentorder, transcriptcomponentid)` and stores `word_count` vectors by `transcriptid`.

#### Raw turn table (source of truth)
- **Function:** `build_raw_turn_table(final_df, speaker_indices_path, ...)`
  - Joins `final_df` transcript text to speaker index rows.
  - Splits **raw transcript text** using `transcript_to_turn`.
  - Produces one row per turn with `turn_index` and `turn_text`.

**Important behavior (risk of silent drop):**
- If `len(turns) != len(grp)` for a transcript, the transcript is **skipped**:
  - `if len(turns) != len(grp): continue`
  - This can cause `raw_turn_df` to be missing transcripts → `exec_chunks` missing → downstream fallback to sentence split in the notebook.

#### Executive chunking (operator/analyst context + exec response)
- **Function:** `build_exec_chunks(raw_turn_df, exec_speaker_label="Executives", operator_label="Operator", analyst_label="Analysts", ...)`
  - Buffers non-executive turns.
  - When an “exec” turn arrives, emits one chunk:
    - `context_text`: joined buffer
    - `exec_text`: the exec turn text
    - `chunk_text`: `context_text + exec_text` (if context exists)
  - Drops any trailing buffer at end.

**Critical assumption / likely root cause:**
- The default `exec_speaker_label` is `"Executives"`.
- If `raw_turn_df["speakertypename"]` uses a different label (e.g. `"Executive"`, `"Management"`, `"Company"`, etc.), then **no exec turns match**, `chunks` becomes empty, and the notebook will fall back to sentence splitting.

#### Analyst attention
- **Function:** `analyst_attention_features_per_call(raw_turn_df, analyst_label="Analysts")`
- **Function:** `rolling_unique_analysts_by_cluster(..., window_days=90, ...)`
  - Computes trailing unique analysts per cluster, excluding current timestamp (no lookahead).

---

### `final_project/test_transcript_turns.py`

Purpose:
- Validates that for transcript `3294948`, the `word_count` vector from `transcript_speaker_indices.csv` matches:
  - total token count after `transcript_to_turn` preprocessing
  - first-20-character prefixes per turn (R oracle).

Key audit value:
- Confirms the **tokenization/squish/ellipsis** behavior matches R for at least one known transcript.
- Does **not** validate chunking logic, speaker labels, or fallback behavior.

---

### `final_project/novelty_driven_mean_reversion.ipynb`

#### Turn/chunk wiring (segment source)
- Builds `raw_turn_df = nh.build_raw_turn_table(df, SPEAKER_INDICES_CSV)`
- Builds `exec_chunks = nh.build_exec_chunks(raw_turn_df)`
- Creates lookup dicts:
  - `CHUNKS_BY_TID`: transcriptid → list of `chunk_text`
  - `EXEC_TEXT_BY_TID`: transcriptid → list of `exec_text`
  - `CONTEXT_TEXT_BY_TID`: transcriptid → list of `context_text`

Segment function:
- `transcript_segments(txt, transcriptid, min_chars=20)`:
  - returns `CHUNKS_BY_TID[transcriptid]` if present
  - else falls back to `split_sentences(clean_text(txt))`

**Audit implication:** if chunk build fails for a transcript (missing from dict), you silently revert to **sentence mode**.

#### Novelty scoring geometry (most recently edited)

The novelty loop was changed to compute novelty on **exec-only text**, while keeping the stored/display text as full chunk text:

- **Centroid/history built from:** `EXEC_TEXT_BY_TID[tid]`
- **Novelty embedding/scoring uses:** `segs_score = EXEC_TEXT_BY_TID[tid]`
- **Recorded `sentence` field uses:** `segs_full = CHUNKS_BY_TID[tid]`

Safety check added:
- if `len(segs_score) != len(segs_full)`: skip row

#### Salience + LLM filters

Salience and LLM intent gates operate on:
- `row["sentence"]`

Given the above novelty edits, this means:
- novelty ranking is driven by exec response (if chunks exist)
- salience + LLM see full chunk context (operator/analyst context + exec)

**Important mismatch with helper docstring:** `build_exec_chunks` currently comments “use exec_text for novelty/salience/LLM scoring”, but notebook currently uses:
- novelty: exec-only
- salience/LLM: full chunk

---

### Cache directory: `final_project/pkl_cache/`

Observed files (dev-scoped):
- `novelty_baselines_turns__dev_AAPL_MSFT_NVDA.pkl`
- `novelty_scores_turns__dev_AAPL_MSFT_NVDA.pkl`
- `novelty_baselines_exec_novelty_chunk_text_v1__dev_AAPL_MSFT_NVDA.pkl`
- `novelty_scores_exec_novelty_chunk_text_v1__dev_AAPL_MSFT_NVDA.pkl`
- `llm_intent_cache_project_spec_turns__dev_AAPL_MSFT_NVDA.pkl`

Notebook cache key:
- `EMBEDDING_CACHE_TAG` was changed to: `exec_novelty_chunk_text_v1`

**Audit implication:** “still not resolved” is unlikely to be a *simple* stale novelty pickle, but stale **LLM intent cache** can still create confusing continuity if the same `sentence` keys repeat across runs.

## High-probability root causes to audit next (ordered)

### 1) Chunking not applied because speaker labels don’t match

If `raw_turn_df["speakertypename"]` does not equal `"Executives"` exactly, then:
- `build_exec_chunks` produces **zero chunks** for that transcript
- `CHUNKS_BY_TID` is missing that `transcriptid`
- `transcript_segments()` falls back to **sentence splitting**
- analyst questions can appear as standalone segments/signals

**Concrete check to run in notebook:**
- Print unique speaker labels:
  - `raw_turn_df["speakertypename"].value_counts().head(20)`
- Validate `exec_chunks` coverage:
  - `exec_chunks.groupby("transcriptid").size().describe()`
  - For a failing `transcriptid`, check:
    - `raw_turn_df[raw_turn_df.transcriptid==TID]["speakertypename"].value_counts()`

### 2) Raw turn table drops transcripts due to turn-count mismatch

If for some transcript:
- `sum(word_count)` doesn’t match tokenization, or
- speaker index rows mismatch split results

Then `build_raw_turn_table` currently **skips** that transcript entirely.

Downstream effect:
- No raw turns → no chunks → fallback → sentence scoring.

**Concrete check:**
- Measure how many transcripts in `df` are missing in `raw_turn_df`:
  - `set(df.transcriptid) - set(raw_turn_df.transcriptid)`
- For a failing transcript, log:
  - word count vector length
  - token count after preprocessing

### 3) `CHUNKS_BY_TID` exists but segment indices drift vs exec lists

The novelty loop now skips rows when:
- `len(EXEC_TEXT_BY_TID[tid]) != len(CHUNKS_BY_TID[tid])`

If this is happening frequently, novelty records may be missing or partially missing, and downstream merges/prints may show unexpected items (or reusing old cached rows).

**Concrete check:**
- Count mismatches:
  - `sum(len(EXEC_TEXT_BY_TID[t]) != len(CHUNKS_BY_TID[t]) for t in EXEC_TEXT_BY_TID)`

### 4) Cached “intent” decisions key off full text

The LLM intent cache key is the `sentence` string. If:
- `sentence` text repeats across many segments
- or `sentence` text comes from fallback sentence splitting for some transcripts

Then LLM results can look “sticky” across runs.

## Recommended instrumentation (to make failures obvious)

Add a “segmentation provenance” flag per novelty record:

- `segment_source`: `"exec_chunks"` or `"sentence_fallback"`
- `segment_speaker_mode`: `"exec_only_novelty"` etc.

Then in the Top 10 printout, include:
- transcriptid
- chunk_index
- `segment_source`
- first N chars of context + exec separately (already supported via dicts)

## Action plan to resolve (minimum set)

1. **Verify `speakertypename` labels** in `raw_turn_df` match `build_exec_chunks` defaults.  
   - If not, either:
     - change defaults to match the file, or
     - pass the correct labels from the notebook.

2. **Quantify missing transcripts** between `df` and `raw_turn_df`.  
   - If large, the turn split alignment is failing often → fix that first.

3. **For a known failing transcript (e.g., `1128909`)**:
   - confirm `CHUNKS_BY_TID[tid]` exists and length > 0
   - print `CONTEXT_TEXT_BY_TID[tid][i]` + `EXEC_TEXT_BY_TID[tid][i]` for the suspect chunk index.

4. After making any segmentation fix, **bump `EMBEDDING_CACHE_TAG`** again and optionally clear:
   - `pkl_cache/novelty_scores_*__dev_*.pkl`
   - `pkl_cache/llm_intent_cache_*.pkl` (only if you want a clean LLM gate rerun)

## Notes on the current “exec-only novelty” change

This change was introduced to prevent novelty from being driven by analyst questions, but it does **not** explain cases where the displayed text contains *only* analyst text. That symptom most strongly points to:

- fallback sentence splitting being used, or
- chunk construction not actually including the exec response for that segment,

both of which are addressed by the root-cause checks above.

## Note: `min_segment_words` filter

As of 2026-04-07, there is **no** `min_segment_words` (or similar) filter in the notebook.  
The only length filter in the segmentation path is `min_chars` (default 20) inside `transcript_segments()`.

If a prior “working” run filtered out short segments by word-count (e.g. to prevent tiny conversational chunks like “No, that’s perfect.” from dominating novelty), that filter is **not present** in the current codebase and is a plausible contributor to “Top 10” drift.

