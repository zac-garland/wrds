"""
Tests for turn-aligned transcript splitting (`novelty_helpers.transcript_to_turn`).

Integration test for transcript **3294948**: word counts come from
**`transcript_speaker_indices.csv`** (via `build_transcript_word_counts_index`); first-20
character prefixes are checked against the R reference table.

Run from `final_project`:

    python test_transcript_turns.py
    SKIP_TRANSCRIPT_INTEGRATION=1 python test_transcript_turns.py   # synthetic tests only
    python test_transcript_turns.py --review               # rebuild test_output_turn_3294948.txt
"""

from __future__ import annotations

import os
import re
import unittest
from pathlib import Path

import pandas as pd

import novelty_helpers as nh

FINAL_PATH = Path(__file__).resolve().parent / "FINAL.csv"
INDICES_PATH = Path(__file__).resolve().parent / "transcript_speaker_indices.csv"
REVIEW_TRANSCRIPT_ID = 3294948
OUTPUT_PATH = Path(__file__).resolve().parent / "test_output_turn_3294948.txt"

# First 20 characters per turn (R `substr(turn, 1, 20)`); oracle for correct CSV + tokenization.
TRANSCRIPT_3294948_PREFIX_20: list[str] = [
    "Good morning. My nam",
    "Thank you, Julianne.",
    "Thank you, Devin. Go",
    "Thanks, Michael. Tur",
    "Thank you, Sachin. J",
    "[Operator Instructio",
    "I'm glad you're doin",
    "Sanjay, thanks for t",
    "Our next question co",
    "Okay. Great. Sachin ",
    "Right. So tokenizati",
    "Our next question co",
    "Michael, I want to a",
    "Right. So when I loo",
    "Our next question co",
    "We saw some volume a",
    "Thanks for your ques",
    "I noted Sachin, that",
    "I wouldn't miss my [",
    "Our next question co",
    "Just want to ask abo",
    "Look, I mean, the re",
    "And you see us winni",
    "Our next question co",
    "I want to dig in a l",
    "Right. So maybe I st",
    "And then on your que",
    "Our next question co",
    "Sachin. First of all",
    "So sure, Darrin. Tha",
    "Our next question co",
    "I wanted to dig in a",
    "Thank you. Great que",
    "I'll just quickly ju",
    "Sachin, thank you fo",
    "Our next question co",
    "And good to hear you",
    "Right. So value-adde",
    "Our next question co",
    "Great. With the Q4 o",
    "Sure. So first of al",
    "Our next question co",
    "Let me echo my warm ",
    "Thank you. So -- we ",
    "Our next question co",
    "Pass my well wishes ",
    "Sorry, I just want t",
    "Yes.",
    "Yes Okay. Looking to",
    "Our next question co",
    "Michael, I was wonde",
    "Right. Ramzi, thanks",
    "Our next question co",
    "I appreciate you squ",
    "Right. It's interest",
    "I think we're out of",
    "Well, I just want to",
    "This concludes today",
]


def _preprocess_word_counting_text(full_transcript: str) -> str:
    """Same tokenization prep as `novelty_helpers.transcript_to_turn` (before split)."""
    t = full_transcript.strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\.{3}", " ", t)
    return t


def _token_count(full_transcript: str) -> int:
    return len(_preprocess_word_counting_text(full_transcript).split())


def _word_counts_from_indices_csv(transcriptid: int) -> list[int] | None:
    if not INDICES_PATH.is_file():
        return None
    d = nh.build_transcript_word_counts_index(
        INDICES_PATH, transcript_ids=[transcriptid]
    )
    wc = d.get(int(transcriptid))
    return wc if wc else None


def _load_full_transcript_text(transcriptid: int) -> str | None:
    if not FINAL_PATH.is_file():
        return None
    reader = pd.read_csv(
        FINAL_PATH,
        usecols=["transcriptid", "full_transcript_text"],
        chunksize=200_000,
        low_memory=False,
        iterator=True,
    )
    try:
        for chunk in reader:
            hit = chunk.loc[chunk["transcriptid"] == transcriptid]
            if not hit.empty:
                return str(hit.iloc[0]["full_transcript_text"])
    finally:
        reader.close()
    return None


class TestTranscriptToTurn(unittest.TestCase):
    def test_matches_r_style_grouping(self) -> None:
        text = "  a  b  ...  c   d e  "
        self.assertEqual(nh.transcript_to_turn(text, [2, 2]), ["a b", "c d"])

    def test_padding_when_fewer_words_than_index(self) -> None:
        out = nh.transcript_to_turn("only", [2, 1])
        self.assertEqual(out, ["only", ""])

    @unittest.skipIf(
        os.environ.get("SKIP_TRANSCRIPT_INTEGRATION", "").lower() in ("1", "true", "yes"),
        "SKIP_TRANSCRIPT_INTEGRATION set",
    )
    @unittest.skipUnless(
        FINAL_PATH.is_file() and INDICES_PATH.is_file(),
        "need FINAL.csv and transcript_speaker_indices.csv in final_project/",
    )
    def test_3294948_csv_word_counts_match_r_prefixes(self) -> None:
        txt = _load_full_transcript_text(REVIEW_TRANSCRIPT_ID)
        wc = _word_counts_from_indices_csv(REVIEW_TRANSCRIPT_ID)
        self.assertIsNotNone(txt, msg="transcriptid not found in FINAL.csv")
        self.assertIsNotNone(wc, msg="transcriptid not found in transcript_speaker_indices.csv")
        assert wc is not None and txt is not None

        n_words = _token_count(txt)
        self.assertEqual(
            sum(wc),
            n_words,
            msg=(
                "Sum of CSV word_count must equal token count after transcript_to_turn "
                f"preprocessing (got sum={sum(wc)} vs {n_words} tokens)"
            ),
        )
        turns = nh.transcript_to_turn(txt, wc)
        self.assertEqual(
            len(turns),
            len(wc),
            msg=f"Expected one string per CSV row ({len(wc)}), got {len(turns)}",
        )
        self.assertEqual(
            len(turns),
            len(TRANSCRIPT_3294948_PREFIX_20),
            msg=(
                f"CSV has {len(turns)} turns; update TRANSCRIPT_3294948_PREFIX_20 "
                f"({len(TRANSCRIPT_3294948_PREFIX_20)} refs) if the call layout changed"
            ),
        )
        for i, (t, exp) in enumerate(zip(turns, TRANSCRIPT_3294948_PREFIX_20)):
            got = t[:20] if len(t) >= 20 else t
            self.assertEqual(
                got,
                exp,
                msg=f"turn {i}: got {got!r} expected {exp!r}",
            )


def write_review_artifact() -> None:
    lines: list[str] = []
    lines.append(f"transcriptid={REVIEW_TRANSCRIPT_ID}")
    lines.append(f"FINAL.csv exists: {FINAL_PATH.is_file()}")
    lines.append(f"transcript_speaker_indices.csv exists: {INDICES_PATH.is_file()}")
    lines.append("Word counts: loaded from CSV (componentorder / transcriptcomponentid sort).")
    lines.append("")

    if not FINAL_PATH.is_file() or not INDICES_PATH.is_file():
        lines.append("SKIP: need FINAL.csv and transcript_speaker_indices.csv")
        OUTPUT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    txt = _load_full_transcript_text(REVIEW_TRANSCRIPT_ID)
    if txt is None:
        lines.append("SKIP: transcriptid not in FINAL.csv")
        OUTPUT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    wc = _word_counts_from_indices_csv(REVIEW_TRANSCRIPT_ID)
    if wc is None:
        lines.append("SKIP: transcriptid not in transcript_speaker_indices.csv")
        OUTPUT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    turns = nh.transcript_to_turn(txt, wc)
    lines.append(
        f"n_turns={len(turns)}  sum(word_counts)={sum(wc)}  tokens={_token_count(txt)}"
    )
    lines.append("")
    for i, t in enumerate(turns):
        preview = t if len(t) <= 500 else t[:500] + "…"
        lines.append(f"--- turn {i} ({len(t)} chars) ---")
        lines.append(preview)
        lines.append("")

    OUTPUT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    import sys

    if "--review" in sys.argv:
        write_review_artifact()
        print(f"Wrote {OUTPUT_PATH}")
        raise SystemExit(0)
    unittest.main()
